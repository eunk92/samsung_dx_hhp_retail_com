"""
Walmart Detail 페이지 크롤러 (Playwright 기반)
- 개별 실행: test_mode=True (기본값), batch_id 입력
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR에서 수집한 모든 제품 처리
"""

import sys
import os
import time
import random
import traceback
import psycopg2
from datetime import datetime
from lxml import html
from playwright.sync_api import sync_playwright

# 공통 환경 설정
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from config import DB_CONFIG
from common.base_crawler import BaseCrawler
from common import data_extractor


class WalmartDetailCrawler(BaseCrawler):
    """
    Walmart Detail 페이지 크롤러 (Playwright 기반)
    BaseCrawler 상속으로 공통 메서드 사용
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
                             - True: 1개 제품만 크롤링
                             - False: 전체 제품 크롤링
            batch_id (str): 배치 ID (기본값: None)
                           - None: 기본값 사용 (w_20251125_190111)
                           - 문자열: 통합 크롤러에서 전달된 배치 ID 사용
        """
        super().__init__()  # BaseCrawler 초기화
        self.test_mode = test_mode
        self.account_name = 'Walmart'
        self.page_type = 'detail'
        self.batch_id = batch_id

        # Playwright 객체
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 테스트 설정
        self.test_count = 1  # 테스트 모드에서 처리할 제품 수

    def connect_db(self):
        """DB 연결"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_xpaths(self):
        """XPath 셀렉터 로드"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM hhp_xpath_selectors
                WHERE account_name = %s AND page_type = %s AND is_active = TRUE
            """, (self.account_name, self.page_type))

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def setup_playwright(self):
        """Playwright 브라우저 설정"""
        try:
            self.playwright = sync_playwright().start()

            # Chromium 브라우저 사용
            self.browser = self.playwright.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--start-maximized',
                    '--lang=en-US'
                ]
            )

            # 컨텍스트 생성
            self.context = self.browser.new_context(
                viewport=None,  # None으로 설정하여 --start-maximized 옵션 활성화
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US'
            )

            # 스텔스 스크립트 주입
            self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)

            # 페이지 생성
            self.page = self.context.new_page()

            print("[OK] Playwright browser initialized")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup Playwright: {e}")
            return False

    def handle_captcha(self):
        """CAPTCHA 자동 해결"""
        try:
            print("[INFO] Checking for CAPTCHA...")

            # 페이지 로딩 대기
            time.sleep(3)

            # CAPTCHA 버튼 찾기 (더 엄격한 셀렉터만 사용)
            captcha_selectors = [
                'button:has-text("PRESS & HOLD")',
                'div:has-text("PRESS & HOLD")',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                '[aria-label*="press"]',
                '[class*="PressHold"]',
                '[class*="presshold"]',
                '[class*="captcha"]',
                '[id*="captcha"]'
            ]

            button = None
            for selector in captcha_selectors:
                try:
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=5000):
                        # 텍스트 확인 - PRESS와 HOLD가 모두 포함되어야 함
                        text = temp_button.inner_text(timeout=2000).upper()
                        if ('PRESS' in text and 'HOLD' in text) or 'CAPTCHA' in text:
                            button = temp_button
                            print(f"[OK] CAPTCHA detected with selector: {selector}")
                            print(f"[DEBUG] Button text: {text}")
                            break
                except:
                    continue

            if not button:
                print("[INFO] No CAPTCHA detected")
                return True

            # 자동 CAPTCHA 해결 시도
            print("[OK] Attempting to solve CAPTCHA automatically...")

            box = button.bounding_box()
            if box:
                # 버튼 중앙 좌표
                center_x = box['x'] + box['width'] / 2
                center_y = box['y'] + box['height'] / 2

                # 마우스 이동
                self.page.mouse.move(center_x, center_y)
                time.sleep(random.uniform(0.3, 0.6))

                # Press & Hold
                self.page.mouse.down()
                print("[INFO] Holding button...")
                hold_time = random.uniform(7, 9)
                print(f"[INFO] Holding for {hold_time:.1f} seconds...")
                time.sleep(hold_time)
                self.page.mouse.up()

                print("[OK] CAPTCHA button released")
                time.sleep(random.uniform(3, 5))

                # 성공 확인
                try:
                    if not button.is_visible(timeout=3000):
                        print("[OK] CAPTCHA solved successfully")
                        return True
                    else:
                        print("[WARNING] CAPTCHA still visible after automatic attempt")
                        print("[INFO] Waiting 60 seconds for manual intervention...")
                        time.sleep(60)

                        if not button.is_visible(timeout=2000):
                            print("[OK] CAPTCHA solved (likely manually)")
                            return True
                        else:
                            print("[WARNING] CAPTCHA still present")
                            return False
                except:
                    print("[OK] CAPTCHA appears to be solved")
                    return True
            else:
                print("[WARNING] Could not get button position")
                return False

        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("="*60)
        print(f"[INFO] Walmart Detail Crawler Initialization (Playwright)")
        print(f"[INFO] Test Mode: {'ON (1 product)' if self.test_mode else 'OFF (all products)'}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. XPath 셀렉터 로드
        if not self.load_xpaths():
            return False

        # 3. Playwright 설정
        if not self.setup_playwright():
            return False

        # 4. 배치 ID 설정 (없으면 기본값 사용)
        if not self.batch_id:
            self.batch_id = 'w_20251125_190111'
            print(f"[INFO] Using default Batch ID: {self.batch_id}")
        else:
            print(f"[INFO] Batch ID received: {self.batch_id}\n")

        return True

    def load_product_list(self):
        """
        wmart_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회

        Returns: list: 제품 정보 리스트
        """
        try:
            cursor = self.db_conn.cursor()

            # product_list에서 hhp_retail_com으로 매핑할 필드 조회
            query = """
                SELECT DISTINCT ON (product_url)
                    retailer_sku_name,
                    final_sku_price,
                    original_sku_price,
                    offer,
                    pick_up_availability,
                    shipping_availability,
                    delivery_availability,
                    sku_status,
                    retailer_membership_discounts,
                    available_quantity_for_purchase,
                    inventory_status,
                    main_rank,
                    bsr_rank,
                    product_url,
                    calendar_week,
                    crawl_strdatetime,
                    page_type
                FROM wmart_hhp_product_list
                WHERE account_name = %s
                  AND batch_id = %s
                  AND product_url IS NOT NULL
                ORDER BY product_url, id
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()

            cursor.close()

            # 딕셔너리 변환
            product_list = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'page_type': None,  # Detail에서 결정
                    'retailer_sku_name': row[0],
                    'final_sku_price': row[1],
                    'original_sku_price': row[2],
                    'offer': row[3],
                    'pick_up_availability': row[4],
                    'shipping_availability': row[5],
                    'delivery_availability': row[6],
                    'sku_status': row[7],
                    'retailer_membership_discounts': row[8],
                    'available_quantity_for_purchase': row[9],
                    'inventory_status': row[10],
                    'main_rank': row[11],
                    'bsr_rank': row[12],
                    'product_url': row[13],
                    'calendar_week': row[14],
                    'crawl_strdatetime': row[15],
                    'page_type':row[16]
                }
                product_list.append(product)

            # 테스트 모드일 경우 1개만 반환
            if self.test_mode:
                product_list = product_list[:self.test_count]

            print(f"[INFO] Loaded {len(product_list)} products from wmart_hhp_product_list")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

    def crawl_detail(self, product, first_product=False):
        """
        제품 상세 페이지 크롤링

        Args:
            product (dict): product_list에서 조회한 제품 정보
            first_product (bool): 첫 번째 제품 여부

        Returns: dict: 결합된 제품 데이터 (product_list + Detail 필드)
        """
        try:
            product_url = product.get('product_url')
            if not product_url:
                print("[WARNING] Product URL is missing, skipping")
                return product

            print(f"[INFO] Crawling detail page: {product_url}")

            # 상세 페이지 로드 (동적 로드 감지)
            print(f"[INFO] Loading product page...")
            self.page.goto(product_url, wait_until="domcontentloaded", timeout=90000)

            # 페이지 로딩 완료 대기
            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
                print(f"[INFO] Page loaded completely")
            except Exception:
                print(f"[INFO] Network idle timeout, proceeding...")

            time.sleep(random.uniform(2, 3))

            # 첫 번째 제품일 경우 CAPTCHA 처리
            if first_product:
                if not self.handle_captcha():
                    print("[WARNING] CAPTCHA handling failed")

                # CAPTCHA 해결 후 페이지 재로드 대기
                print("[INFO] Waiting for page to reload after CAPTCHA...")
                time.sleep(random.uniform(3, 5))

            # HTML 파싱
            page_html = self.page.content()
            tree = html.fromstring(page_html)

            # Detail 필드 추출 (각 필드별로 개별 예외 처리)
            # item은 product_url에서 추출 (Walmart item ID)
            item = None
            try:
                if product_url:
                    import re

                    # 방법 1: /ip/제품명/숫자 패턴에서 숫자 추출
                    ip_match = re.search(r'/ip/[^/]+/(\d+)', product_url)
                    if ip_match:
                        item = ip_match.group(1)
                        print(f"[DEBUG] Extracted item ID from /ip/ pattern: {item}")
                    else:
                        # 방법 2: URL 인코딩된 패턴에서 추출 (%2F숫자%3F 형식)
                        encoded_match = re.search(r'%2F(\d+)%3F', product_url)
                        if encoded_match:
                            item = encoded_match.group(1)
                            print(f"[DEBUG] Extracted item ID from URL encoded pattern: {item}")
                        else:
                            # 방법 3: 마지막 / 다음의 숫자만 추출
                            last_segment = product_url.rstrip('/').split('/')[-1]
                            item_with_params = last_segment.split('?')[0]
                            number_match = re.search(r'(\d+)$', item_with_params)
                            if number_match:
                                item = number_match.group(1)
                                print(f"[DEBUG] Extracted item ID from last segment: {item}")
                            else:
                                print(f"[WARNING] Could not extract numeric item ID from URL: {product_url[:100]}...")
                                item = None
            except Exception as e:
                print(f"[WARNING] Failed to extract item from URL: {e}")
                item = None

            # 추가 필드 추출 (각 필드별 예외 처리)
            try:
                number_of_ppl_purchased_yesterday = self.extract_with_fallback(tree, self.xpaths.get('number_of_ppl_purchased_yesterday', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract number_of_ppl_purchased_yesterday: {e}")
                number_of_ppl_purchased_yesterday = None

            try:
                number_of_ppl_added_to_carts = self.extract_with_fallback(tree, self.xpaths.get('number_of_ppl_added_to_carts', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract number_of_ppl_added_to_carts: {e}")
                number_of_ppl_added_to_carts = None

            try:
                sku_popularity = self.extract_with_fallback(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract sku_popularity: {e}")
                sku_popularity = None

            try:
                savings = self.extract_with_fallback(tree, self.xpaths.get('savings', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract savings: {e}")
                savings = None

            try:
                discount_type = self.extract_with_fallback(tree, self.xpaths.get('discount_type', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract discount_type: {e}")
                discount_type = None

            try:
                shipping_info_xpath = self.xpaths.get('shipping_info', {}).get('xpath')
                if shipping_info_xpath:
                    # XPath 결과가 리스트인 경우 (//text() 사용 시)
                    shipping_info_raw = tree.xpath(shipping_info_xpath)
                    if isinstance(shipping_info_raw, list):
                        # 모든 텍스트를 합치고 normalize
                        shipping_info = ' '.join([text.strip() for text in shipping_info_raw if text.strip()])
                    else:
                        shipping_info = shipping_info_raw
                else:
                    shipping_info = None
            except Exception as e:
                print(f"[WARNING] Failed to extract shipping_info: {e}")
                shipping_info = None

            # 스펙 정보 추출 (View full specifications 버튼 클릭)
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None

            try:
                print("[INFO] Scrolling to and clicking 'View full specifications' button...")

                # 버튼이 있는 위치로 스크롤
                spec_button = self.page.locator("//button[@aria-label='View full specifications']").first
                if spec_button.is_visible(timeout=5000):
                    spec_button.scroll_into_view_if_needed()
                    time.sleep(random.uniform(1, 2))
                    spec_button.click()

                    # 동적 로드 감지: 모달이 완전히 열릴 때까지 대기
                    try:
                        print("[INFO] Waiting for specifications modal to load...")
                        self.page.wait_for_selector("//button[@aria-label='Close']", timeout=5000, state='visible')
                        time.sleep(1)
                    except Exception:
                        print("[WARNING] Modal load timeout, proceeding...")
                        time.sleep(2)

                    # 모달이 열린 후 HTML 다시 파싱
                    modal_html = self.page.content()
                    modal_tree = html.fromstring(modal_html)

                    # Service Provider (hhp_carrier) 추출 - 개별 예외 처리
                    try:
                        hhp_carrier = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract hhp_carrier from modal: {e}")
                        hhp_carrier = None

                    # HD Capacity (hhp_storage) 추출 - 개별 예외 처리
                    try:
                        hhp_storage = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract hhp_storage from modal: {e}")
                        hhp_storage = None

                    # Color (hhp_color) 추출 - 개별 예외 처리
                    try:
                        hhp_color = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_color', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract hhp_color from modal: {e}")
                        hhp_color = None

                    print(f"[INFO] Extracted specs - Carrier: {hhp_carrier}, Storage: {hhp_storage}, Color: {hhp_color}")

                    # 닫기 버튼 클릭
                    close_button = self.page.locator("//button[@aria-label='Close']").first
                    if close_button.is_visible(timeout=3000):
                        close_button.click()
                        time.sleep(random.uniform(1, 2))
                        print("[INFO] Modal closed successfully")
                else:
                    print("[WARNING] 'View full specifications' button not found, using fallback XPath")

            except Exception as e:
                print(f"[WARNING] Failed to extract specs from modal: {e}")
                # 에러 발생 시 기존 방식으로 fallback (각 필드별 예외 처리)
                try:
                    hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
                except Exception as e2:
                    print(f"[WARNING] Failed to extract hhp_carrier (fallback): {e2}")
                    hhp_carrier = None

                try:
                    hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                except Exception as e2:
                    print(f"[WARNING] Failed to extract hhp_storage (fallback): {e2}")
                    hhp_storage = None

                try:
                    hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
                except Exception as e2:
                    print(f"[WARNING] Failed to extract hhp_color (fallback): {e2}")
                    hhp_color = None

            # ========== 2단계: 유사 제품 추출 ==========
            print("[INFO] Starting similar products extraction...")
            retailer_sku_name_similar = None

            # similar_products_container XPath 가져오기
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')

            if similar_products_container_xpath:
                print(f"[INFO] Searching for similar products section...")

                # 페이지 최하단으로 스크롤 (유사 제품은 보통 아래쪽에 위치)
                try:
                    # 점진적으로 스크롤하면서 유사 제품 섹션 찾기
                    scroll_height = self.page.evaluate("document.body.scrollHeight")
                    current_position = 0
                    scroll_step = 500
                    found_similar_section = False

                    print(f"[INFO] Page height: {scroll_height}px, starting scroll search for similar products...")

                    while current_position < scroll_height:
                        self.page.evaluate(f"window.scrollTo(0, {current_position});")
                        time.sleep(0.5)

                        # 유사 제품 섹션이 보이는지 확인
                        try:
                            similar_section = self.page.locator(similar_products_container_xpath).first
                            if similar_section.is_visible(timeout=1000):
                                print(f"[INFO] Similar products section found at {current_position}px")
                                similar_section.scroll_into_view_if_needed()
                                time.sleep(1)
                                found_similar_section = True
                                break
                        except:
                            pass

                        current_position += scroll_step

                    if not found_similar_section:
                        print("[WARNING] Could not find similar products section after scrolling entire page")

                    # HTML 다시 파싱
                    page_html = self.page.content()
                    tree = html.fromstring(page_html)
                    print(f"[INFO] Re-parsed HTML after scrolling to similar products")

                    # 유사 제품 카드들을 찾기
                    try:
                        product_cards = tree.xpath(similar_products_container_xpath)
                        print(f"[DEBUG] similar_products_container xpath: {similar_products_container_xpath}")
                        print(f"[DEBUG] Found {len(product_cards)} product cards")

                        if product_cards:
                            similar_product_names = []

                            # 제품명 XPath 가져오기
                            name_xpath = self.xpaths.get('similar_product_name', {}).get('xpath')
                            print(f"[DEBUG] similar_product_name xpath: {name_xpath}")

                            # 각 제품 카드에서 제품명만 추출
                            for idx, card in enumerate(product_cards, 1):
                                try:
                                    if name_xpath:
                                        name_results = card.xpath(name_xpath)
                                        if name_results:
                                            name = name_results[0]
                                            similar_product_names.append(name)
                                            print(f"[DEBUG] Product {idx} name: {name}")
                                        else:
                                            print(f"[WARNING] Product {idx}: name xpath returned empty")
                                except Exception as name_error:
                                    print(f"[WARNING] Failed to extract product {idx} name: {name_error}")
                                    continue

                            print(f"[INFO] Extracted {len(similar_product_names)} similar product names")

                            # 모든 유사 제품명을 ||| 구분자로 연결
                            retailer_sku_name_similar = ' ||| '.join(similar_product_names) if similar_product_names else None
                        else:
                            print(f"[INFO] No similar product cards found")
                            retailer_sku_name_similar = None

                    except Exception as similar_error:
                        print(f"[WARNING] Failed to extract similar products: {similar_error}")
                        print(f"[INFO] Continuing with other data extraction...")
                        retailer_sku_name_similar = None

                except Exception as scroll_error:
                    print(f"[WARNING] Error during scrolling for similar products: {scroll_error}")
                    retailer_sku_name_similar = None
            else:
                print("[WARNING] similar_products_container xpath not found in config")

            # ========== 3단계: 리뷰 섹션 데이터 추출 (HTML에서) ==========
            # HTML 다시 파싱 (스크롤 후 업데이트된 DOM)
            page_html = self.page.content()
            tree = html.fromstring(page_html)

            # 리뷰 관련 필드 (data_extractor 후처리) - 각 필드별 예외 처리
            count_of_reviews = None
            try:
                count_of_reviews_raw = self.extract_with_fallback(tree, self.xpaths.get('count_of_reviews', {}).get('xpath'))
                count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_reviews: {e}")
                count_of_reviews = None

            star_rating = None
            try:
                star_rating_raw = self.extract_with_fallback(tree, self.xpaths.get('star_rating', {}).get('xpath'))
                star_rating = data_extractor.extract_rating(star_rating_raw, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract star_rating: {e}")
                star_rating = None

            count_of_star_ratings = None
            try:
                count_of_star_ratings = data_extractor.extract_star_ratings_count(
                    tree,
                    count_of_reviews,
                    self.xpaths.get('count_of_star_ratings', {}).get('xpath'),
                    self.account_name
                )
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_star_ratings: {e}")
                count_of_star_ratings = None

            # ========== 4단계: 리뷰 더보기 버튼 클릭 및 상세 리뷰 추출 ==========
            # 리뷰 데이터 추출: "See All Reviews" 또는 유사한 버튼 클릭 후 추출
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            print(f"[DEBUG] reviews_button_xpath: {reviews_button_xpath}")

            if reviews_button_xpath:
                review_button_found = False

                # 페이지 상단으로 이동
                print(f"[INFO] Scrolling from top to find review button")
                self.page.evaluate("window.scrollTo(0, 0);")
                time.sleep(1)

                # 페이지 전체 높이 계산
                scroll_height = self.page.evaluate("document.body.scrollHeight")
                current_position = 0
                scroll_step = 400  # 400px씩 스크롤

                print(f"[INFO] Page height: {scroll_height}px, starting scroll search for review button...")

                # 여러 XPath 시도 (Walmart 리뷰 버튼 패턴)
                reviews_button_xpaths = [
                    reviews_button_xpath,
                    '//button[contains(., "See all reviews")]',
                    '//a[contains(., "See all reviews")]',
                    '//button[contains(text(), "reviews")]',
                    '//a[contains(text(), "reviews")]',
                    '//button[contains(@aria-label, "reviews")]'
                ]

                # 페이지 끝까지 스크롤하면서 리뷰 버튼 찾기
                while current_position < scroll_height:
                    # 각 스크롤 위치에서 여러 XPath 시도
                    for xpath in reviews_button_xpaths:
                        try:
                            # Playwright locator로 버튼 찾기
                            review_button = self.page.locator(xpath).first

                            if review_button.is_visible(timeout=1000):
                                # 버튼을 찾았으면 화면 중앙으로 스크롤
                                print(f"[INFO] Review button found at {current_position}px with xpath: {xpath[:50]}...")
                                review_button.scroll_into_view_if_needed()
                                time.sleep(2)

                                # 클릭 시도
                                try:
                                    review_button.click()
                                    print(f"[INFO] Review button clicked successfully")
                                    review_button_found = True
                                    time.sleep(5)  # 리뷰 페이지 로딩 대기
                                    break
                                except Exception as click_err:
                                    print(f"[WARNING] Click failed: {click_err}")
                                    continue

                        except Exception as e:
                            # 이 xpath로 못 찾으면 다음 xpath 시도
                            if "timeout" not in str(e).lower():
                                print(f"[DEBUG] XPath {xpath[:30]}... failed: {e}")
                            continue

                    # 버튼을 찾았으면 전체 루프 종료
                    if review_button_found:
                        break

                    # 못 찾았으면 계속 스크롤
                    current_position += scroll_step
                    self.page.evaluate(f"window.scrollTo(0, {current_position});")
                    time.sleep(0.5)

                if not review_button_found:
                    print(f"[WARNING] Could not find review button after scrolling entire page, skipping review extraction")

                # 버튼을 찾았을 때만 리뷰 추출
                if review_button_found:
                    try:
                        # 리뷰 페이지가 완전히 로드될 때까지 대기
                        detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                        if detailed_review_xpath:
                            try:
                                # 동적 로드 감지: 리뷰가 로드될 때까지 대기
                                print(f"[INFO] Waiting for reviews to load dynamically...")
                                self.page.wait_for_selector(
                                    detailed_review_xpath,
                                    timeout=30000,
                                    state='visible'
                                )
                                # 추가 대기: JavaScript 렌더링 완료 확인
                                time.sleep(2)
                                print(f"[INFO] Review page fully loaded")
                            except Exception as wait_error:
                                print(f"[WARNING] Timeout waiting for reviews page: {wait_error}")
                                print(f"[INFO] Attempting to proceed anyway...")
                                time.sleep(5)

                            # 여러 페이지에서 리뷰 수집 (최대 20개)
                            all_reviews = []
                            current_page = 1
                            max_reviews = 20

                            while len(all_reviews) < max_reviews:
                                print(f"[INFO] Extracting reviews from page {current_page}...")

                                # 동적 로드 감지: 페이지 이동 후 리뷰가 다시 로드될 때까지 대기
                                if current_page > 1:
                                    try:
                                        print(f"[INFO] Waiting for page {current_page} reviews to load...")
                                        self.page.wait_for_load_state('networkidle', timeout=10000)
                                        time.sleep(2)
                                    except Exception:
                                        print(f"[INFO] Network idle timeout, proceeding...")
                                        time.sleep(1)

                                # 현재 페이지의 HTML 파싱
                                page_html = self.page.content()
                                tree = html.fromstring(page_html)

                                # 리뷰 본문 추출
                                reviews_list = tree.xpath(detailed_review_xpath)

                                if reviews_list:
                                    print(f"[INFO] Found {len(reviews_list)} reviews on page {current_page}")

                                    # 리뷰 추가 (최대 20개까지만)
                                    for review in reviews_list:
                                        if len(all_reviews) >= max_reviews:
                                            break

                                        # lxml Element인 경우 text_content() 사용
                                        if hasattr(review, 'text_content'):
                                            review_text = review.text_content()
                                        else:
                                            review_text = review

                                        # 줄바꿈 제거 및 포맷팅
                                        cleaned_review = review_text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                        cleaned_review = ' '.join(cleaned_review.split())
                                        all_reviews.append(cleaned_review)

                                    print(f"[INFO] Total reviews collected so far: {len(all_reviews)}")
                                else:
                                    print(f"[WARNING] No reviews found on page {current_page}")

                                # 20개를 수집했거나 더 이상 페이지가 없으면 종료
                                if len(all_reviews) >= max_reviews:
                                    print(f"[INFO] Collected maximum {max_reviews} reviews, stopping pagination")
                                    break

                                # 다음 페이지 버튼 찾기
                                try:
                                    next_page_num = current_page + 1
                                    next_page_xpath = f'//a[@data-automation-id="page-number" and text()="{next_page_num}"]'
                                    next_page_button = self.page.locator(next_page_xpath).first

                                    if next_page_button.is_visible(timeout=3000):
                                        print(f"[INFO] Clicking page {next_page_num} button...")
                                        next_page_button.scroll_into_view_if_needed()
                                        time.sleep(1)
                                        next_page_button.click()
                                        time.sleep(3)  # 페이지 로딩 대기
                                        current_page = next_page_num
                                    else:
                                        print(f"[INFO] No more pages found after page {current_page}")
                                        break
                                except Exception as page_error:
                                    print(f"[WARNING] Failed to navigate to next page: {page_error}")
                                    break

                            # 수집된 리뷰 포맷팅
                            if all_reviews:
                                formatted_reviews = []
                                for idx, review in enumerate(all_reviews, 1):
                                    formatted_reviews.append(f"review{idx} - {review}")

                                detailed_review_content = ' ||| '.join(formatted_reviews)
                                print(f"[SUCCESS] Total extracted {len(all_reviews)} reviews from {current_page} page(s)")
                            else:
                                print(f"[WARNING] No reviews found on review page")
                        else:
                            print(f"[WARNING] detailed_review_content xpath not found")
                    except Exception as e:
                        print(f"[WARNING] Failed to extract reviews from page: {e}")
                        traceback.print_exc()
            else:
                print(f"[WARNING] reviews_button xpath not found in config")

            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'number_of_ppl_purchased_yesterday': number_of_ppl_purchased_yesterday,
                'number_of_ppl_added_to_carts': number_of_ppl_added_to_carts,
                'sku_popularity': sku_popularity,
                'savings': savings,
                'discount_type': discount_type,
                'shipping_info': shipping_info,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'hhp_carrier': hhp_carrier,
                'retailer_sku_name_similar': retailer_sku_name_similar,
                'detailed_review_content': detailed_review_content
            })

            return combined_data

        except Exception as e:
            print(f"[ERROR] Failed to crawl detail page {product.get('product_url')}: {e}")
            traceback.print_exc()
            # 에러 발생 시 product_list 데이터만 반환
            return product

    def save_to_retail_com(self, products):
        """
        hhp_retail_com 테이블에 저장

        Args: products (list): 결합된 제품 데이터 리스트

        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            insert_query = """
                INSERT INTO hhp_retail_com (
                    country, product, item, account_name, page_type,
                    count_of_reviews, retailer_sku_name, product_url,
                    star_rating, count_of_star_ratings,
                    number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts,
                    sku_popularity, savings, discount_type,
                    final_sku_price, original_sku_price,
                    offer,
                    pick_up_availability, shipping_availability, delivery_availability,
                    shipping_info,
                    available_quantity_for_purchase, inventory_status, sku_status,
                    retailer_membership_discounts,
                    hhp_storage, hhp_color, hhp_carrier,
                    retailer_sku_name_similar,
                    detailed_review_content,
                    main_rank, bsr_rank,
                    calendar_week, crawl_strdatetime, batch_id, 
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """

            saved_count = 0

            for product in products:
                cursor.execute(insert_query, (
                    'US',
                    'HHP',
                    product.get('item'),
                    self.account_name,
                    product.get('page_type'),
                    product.get('count_of_reviews'),
                    product.get('retailer_sku_name'),
                    product.get('product_url'),
                    product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('number_of_ppl_purchased_yesterday'),
                    product.get('number_of_ppl_added_to_carts'),
                    product.get('sku_popularity'),
                    product.get('savings'),
                    product.get('discount_type'),
                    product.get('final_sku_price'),
                    product.get('original_sku_price'),
                    product.get('offer'),
                    product.get('pick_up_availability'),
                    product.get('shipping_availability'),
                    product.get('delivery_availability'),
                    product.get('shipping_info'),
                    product.get('available_quantity_for_purchase'),
                    product.get('inventory_status'),
                    product.get('sku_status'),
                    product.get('retailer_membership_discounts'),
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('hhp_carrier'),
                    product.get('retailer_sku_name_similar'),
                    product.get('detailed_review_content'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('calendar_week'),
                    current_time,
                    self.batch_id
                ))
                saved_count += 1

            self.db_conn.commit()
            cursor.close()

            print(f"[SUCCESS] Saved {saved_count} products to hhp_retail_com")
            for i, product in enumerate(products[:3], 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                item = product.get('item', 'N/A')
                print(f"[{i}] {sku_name[:40]}... (Item: {item})")
            if len(products) > 3:
                print(f"... and {len(products) - 3} more products")

            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return 0

    def run(self):
        """
        크롤러 실행
        Returns: bool: 성공 시 True, 실패 시 False
        """
        try:
            # 초기화
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            # product_list 조회
            product_list = self.load_product_list()
            if not product_list:
                print("[WARNING] No products found in wmart_hhp_product_list")
                return False

            print("\n" + "="*60)
            print(f"[INFO] Starting Walmart Detail page crawling...")
            print(f"[INFO] Total products to crawl: {len(product_list)}")
            print("="*60 + "\n")

            # 모든 제품 상세 페이지 크롤링
            total_saved = 0

            for i, product in enumerate(product_list, 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] Processing: {sku_name[:50]}...")

                # 첫 번째 제품은 CAPTCHA 처리
                first_product = (i == 1)
                combined_data = self.crawl_detail(product, first_product=first_product)

                # 1개 제품마다 즉시 DB에 저장
                saved_count = self.save_to_retail_com([combined_data])
                total_saved += saved_count

                # 페이지 간 대기
                time.sleep(random.uniform(3, 5))

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Walmart Detail Crawler Finished")
            print(f"[RESULT] Total products processed: {len(product_list)}")
            print(f"[RESULT] Total products saved: {total_saved}")
            print(f"[RESULT] Batch ID: {self.batch_id}")
            print("="*60 + "\n")

            return True

        except Exception as e:
            print(f"[ERROR] Crawler execution failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 리소스 정리
            if self.page:
                self.page.close()
                print("[INFO] Page closed")
            if self.context:
                self.context.close()
                print("[INFO] Context closed")
            if self.browser:
                self.browser.close()
                print("[INFO] Browser closed")
            if self.playwright:
                self.playwright.stop()
                print("[INFO] Playwright stopped")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] Database connection closed")


def main():
    """
    개별 실행 시 진입점 (테스트 모드 ON)
    """
    crawler = WalmartDetailCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Walmart Detail Crawler completed successfully")
    else:
        print("\n[FAILED] Walmart Detail Crawler failed")


if __name__ == '__main__':
    main()