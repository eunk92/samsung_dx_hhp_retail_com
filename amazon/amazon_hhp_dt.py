"""
Amazon Detail 페이지 크롤러
- 개별 실행: test_mode=True (기본값), batch_id=None (최신 batch_id 자동 조회)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- 테스트 모드: 1개 제품만 크롤링
- 운영 모드: 전체 제품 크롤링
"""

import sys
import os
import time
import traceback
import random
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common import data_extractor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class AmazonDetailCrawler(BaseCrawler):
    """
    Amazon Detail 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
                             - True: 1개 제품만 크롤링
                             - False: 전체 제품 크롤링
            batch_id (str): 배치 ID (기본값: None)
                           - None: DB에서 최신 batch_id 자동 조회
                           - 문자열: 통합 크롤러에서 전달된 batch_id 사용
        """
        super().__init__()

        self.test_mode = test_mode
        self.batch_id = batch_id
        self.account_name = 'Amazon'
        self.page_type = 'detail'
        self.cookies_loaded = False  # 쿠키 로드 여부 플래그

        # 테스트 설정
        self.test_count = 1  # 테스트 모드에서 크롤링할 제품 수

    def initialize(self):
        """
        크롤러 초기화 작업
        - DB 연결
        - batch_id 자동 조회 (None인 경우)
        - XPath 셀렉터 로드
        - WebDriver 설정
        - 로그 정리

        Returns:
            bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] Amazon Detail Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (1 product only)' if self.test_mode else 'OFF (all products)'}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. 배치 ID 설정 (없으면 기본값 사용)
        if not self.batch_id:
            self.batch_id = 'a_20251125_212207'
           
        # 3. XPath 셀렉터 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        # 4. WebDriver 설정
        self.setup_driver()

        # 5. 쿠키 로드 (일관된 세션 유지)
        self.cookies_loaded = self.load_cookies(self.account_name)

        # 6. 오래된 로그 정리
        self.cleanup_old_logs()

        return True

    def load_product_list(self):
        """
        amazon_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회
        - hhp_retail_com 테이블에 들어갈 필드 중 product_list에서 가져올 수 있는 것들만 조회
        - 나머지 필드(리뷰, 별점, 스펙 등)는 상세 페이지에서 추출
        - 테스트 모드: 1개만 반환

        Returns:
            list: 제품 정보 리스트 (hhp_retail_com 테이블 기준 필드 매핑)
        """
        try:
            cursor = self.db_conn.cursor()

            # PRD 기준: hhp_retail_com 테이블에 필요한 필드 중 product_list에서 가져올 것들만 SELECT
            query = """
                SELECT
                    account_name,
                    page_type,
                    retailer_sku_name,
                    number_of_units_purchased_past_month,
                    final_sku_price,
                    original_sku_price,
                    shipping_info,
                    available_quantity_for_purchase,
                    discount_type,
                    main_rank,
                    bsr_rank,
                    product_url,
                    calendar_week,
                    batch_id
                FROM amazon_hhp_product_list
                WHERE account_name = %s
                  AND batch_id = %s
                  AND product_url IS NOT NULL
                ORDER BY main_rank ASC NULLS LAST, bsr_rank ASC NULLS LAST
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            # hhp_retail_com 테이블 필드명과 일치하도록 매핑
            products = []
            for row in rows:
                product = {
                    # === product_list에서 가져오는 필드 (14개) ===
                    'account_name': self.account_name,
                    'page_type': row[1],
                    'retailer_sku_name': row[2],
                    'number_of_units_purchased_past_month': row[3],
                    'final_sku_price': row[4],
                    'original_sku_price': row[5],
                    'shipping_info': row[6],
                    'available_quantity_for_purchase': row[7],
                    'discount_type': row[8],
                    'main_rank': row[9],
                    'bsr_rank': row[10],
                    'product_url': row[11],
                    'calendar_week': row[12],
                    'batch_id': row[13]
                }
                products.append(product)

            # 테스트 모드일 경우 1개만 반환
            if self.test_mode:
                products = products[:self.test_count]

            print(f"[INFO] Loaded {len(products)} products from amazon_hhp_product_list")
            return products

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            return []

    def extract_asin_from_url(self, product_url):
        """
        product_url에서 Amazon ASIN 코드 추출

        Args: product_url (str): 제품 URL

        Returns: str: ASIN 코드 또는 None
        """
        import re

        if not product_url:
            return None

        # 패턴 1: /dp/{ASIN}/ 형식
        match = re.search(r'/dp/([A-Z0-9]{10})/', product_url)
        if match:
            return match.group(1)

        # 패턴 2: %2fdp%2F{ASIN}% 형식 (URL 인코딩)
        match = re.search(r'%2[fF]dp%2[fF]([A-Z0-9]{10})%', product_url)
        if match:
            return match.group(1)

        return None

    def handle_captcha(self):
        """CAPTCHA 자동 해결 (Selenium 기반)"""
        try:
            print("[INFO] Checking for CAPTCHA...")
            time.sleep(2)

            page_html = self.driver.page_source.lower()

            # CAPTCHA 키워드 확인
            captcha_keywords = ['captcha', 'robot', 'human verification', 'press & hold', 'press and hold']
            if not any(keyword in page_html for keyword in captcha_keywords):
                print("[INFO] No CAPTCHA detected")
                return True

            print("[WARNING] CAPTCHA detected!")

            # Amazon CAPTCHA 버튼 셀렉터
            captcha_selectors = [
                (By.XPATH, "//button[contains(text(), 'Continue shopping')]"),
                (By.XPATH, "//button[contains(@aria-label, 'CAPTCHA')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.ID, "captchacharacters"),  # 텍스트 입력형 CAPTCHA
                (By.XPATH, "//form[@action='/errors/validateCaptcha']"),
            ]

            captcha_button = None
            captcha_type = None

            # CAPTCHA 버튼 찾기
            for by, selector in captcha_selectors:
                try:
                    element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    if element.is_displayed():
                        captcha_button = element
                        captcha_type = "button" if by != By.ID else "input"
                        print(f"[OK] CAPTCHA element found: {selector}")
                        break
                except:
                    continue

            if not captcha_button:
                print("[WARNING] CAPTCHA keywords found but element not located")
                print("[INFO] Waiting 45 seconds for manual intervention...")
                time.sleep(45)
                return True

            # 텍스트 입력형 CAPTCHA (수동 입력 필요)
            if captcha_type == "input":
                print("[INFO] Text CAPTCHA detected - requires manual input")
                print("[INFO] Waiting 60 seconds for you to solve it...")
                time.sleep(60)
                return True

            # 버튼 클릭형 CAPTCHA 자동 해결 시도
            print("[INFO] Attempting to solve button CAPTCHA automatically...")

            try:
                # ActionChains로 자연스러운 클릭
                actions = ActionChains(self.driver)
                actions.move_to_element(captcha_button)
                actions.pause(random.uniform(0.5, 1.0))
                actions.click()
                actions.perform()

                print("[INFO] CAPTCHA button clicked")
                time.sleep(random.uniform(3, 5))

                # 성공 확인
                new_page_html = self.driver.page_source.lower()
                if not any(keyword in new_page_html for keyword in captcha_keywords):
                    print("[OK] CAPTCHA solved successfully")
                    return True
                else:
                    print("[WARNING] CAPTCHA still present after automatic attempt")
                    print("[INFO] Waiting 60 seconds for manual intervention...")
                    time.sleep(60)
                    return True

            except Exception as e:
                print(f"[WARNING] Could not click CAPTCHA button: {e}")
                print("[INFO] Waiting 60 seconds for manual intervention...")
                time.sleep(60)
                return True

        except Exception as e:
            print(f"[ERROR] CAPTCHA handling failed: {e}")
            traceback.print_exc()
            return False

    def extract_reviews(self, item, max_reviews=20):
        """
        제품 리뷰 페이지에서 리뷰 내용 추출

        Args:
            item (str): Amazon ASIN 코드 (예: B0ABCD1234)
            max_reviews (int): 추출할 최대 리뷰 개수 (기본값: 20)

        Returns:
            str: 구분자(|||)로 연결된 리뷰 내용 문자열 또는 None

        Examples:
            - "Review 1 content ||| Review 2 content ||| Review 3 content"
        """
        try:
            if not item:
                print(f"[WARNING] ASIN is required for review extraction")
                return None

            # 리뷰 페이지 URL 생성
            review_url = f"https://www.amazon.com/product-reviews/{item}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"

            print(f"[INFO] Navigating to review page for ASIN: {item}")
            self.driver.get(review_url)
            time.sleep(10)  # 페이지 로딩 대기

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # ===== 로그인 요구 페이지 감지 =====
            # 로그인 페이지로 리다이렉트되었는지 확인
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                print(f"[ERROR] Login required for review page. Current URL: {current_url}")
                print(f"[INFO] Please run: python amazon_login.py to refresh cookies")
                return data_extractor.get_no_reviews_text(self.account_name)

            # Bot 감지 페이지 확인 (CAPTCHA 또는 "Sorry, we just need to make sure you're not a robot")
            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                print(f"[ERROR] Bot detection triggered on review page")
                print(f"[INFO] Amazon detected automated access. Please:")
                print(f"       1. Run: python amazon_login.py")
                print(f"       2. Manually solve CAPTCHA if prompted")
                print(f"       3. Wait before retrying")
                return data_extractor.get_no_reviews_text(self.account_name)
            # ===== 로그인 감지 끝 =====

            # 리뷰 내용 추출 (XPath - DB에서 로드)
            review_texts = tree.xpath(self.xpaths.get('detailed_review_content', {}).get('xpath') or '')

            if not review_texts:
                print(f"[WARNING] No reviews found for ASIN: {item}")
                return data_extractor.get_no_reviews_text(self.account_name)

            # 최대 개수만큼만 선택
            review_texts = review_texts[:max_reviews]

            # 각 리뷰 처리: 줄바꿈(\n, \r)을 공백 하나로 치환, 앞뒤 공백 제거
            cleaned_reviews = []
            for review in review_texts:
                if review.strip():
                    cleaned = ' '.join(review.split())
                    cleaned_reviews.append(cleaned)

            # 구분자로 연결
            result = ' ||| '.join(cleaned_reviews)

            print(f"[INFO] Extracted {len(cleaned_reviews)} reviews (Total length: {len(result)} chars)")
            return result

        except Exception as e:
            print(f"[ERROR] Failed to extract reviews: {e}")
            traceback.print_exc()
            return None

    def crawl_detail(self, product):
        """
        특정 제품의 상세 페이지 크롤링

        흐름:
        1. Detail 페이지 로드 및 모든 데이터 추출
        2. 리뷰 페이지로 이동하여 detailed_review_content 추출

        Args: product (dict): product_list 테이블에서 조회한 제품 정보

        Returns: dict: product_list 데이터 + Detail 데이터 결합
        """
        try:
            product_url = product['product_url']
            print(f"\n[INFO] Crawling detail page: {product_url}")

            # === STEP 1: Detail 페이지 로드 및 데이터 추출 ===
            self.driver.get(product_url)
            time.sleep(10)  # 페이지 로딩 대기

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # ===== Detail 페이지 로그인 감지 =====
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                print(f"[ERROR] Login required for detail page. Current URL: {current_url}")
                print(f"[INFO] Please run: python amazon_login.py to refresh cookies")
                return product  # 기본 product_list 데이터만 반환

            # Bot 감지 확인 및 CAPTCHA 자동 해결
            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                print(f"[WARNING] Bot detection triggered on detail page")
                print(f"[INFO] Attempting to solve CAPTCHA automatically...")

                if self.handle_captcha():
                    print(f"[OK] CAPTCHA handled, retrying page load...")
                    self.driver.get(product_url)
                    time.sleep(5)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                else:
                    print(f"[ERROR] CAPTCHA handling failed")
                    return product  # 기본 product_list 데이터만 반환
            # ===== 로그인 감지 끝 =====

            # === 고정값 및 URL 파싱 ===
            country = 'SEA'
            product_type = 'HHP'

            # item: product_url에서 ASIN 추출
            item = self.extract_asin_from_url(product_url)

            # === 모든 필드를 None으로 초기화 ===
            trade_in = None
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None
            count_of_reviews = None
            star_rating = None
            count_of_star_ratings = None
            summarized_review_content = None
            sku_popularity = None
            bundle = None
            retailer_membership_discounts = None
            rank_1 = None
            rank_2 = None

            # === 개별 필드 추출 (각각 try-except 처리) ===
            try:
                trade_in = self.extract_with_fallback(tree, self.xpaths.get('trade_in', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract trade_in: {e}")

            try:
                hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract hhp_carrier: {e}")

            try:
                # 기타 상세 정보
                sku_popularity = self.extract_with_fallback(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract sku_popularity: {e}")

            try:
                bundle = self.extract_with_fallback(tree, self.xpaths.get('bundle', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract bundle: {e}")

            try:
                retailer_membership_discounts = self.extract_with_fallback(tree, self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract retailer_membership_discounts: {e}")

            # === "Additional details" + "Item details" 버튼 클릭하여 정보 확장 ===
            additional_details_found = False
            try:
                # 페이지 아래로 스크롤 (Additional details 섹션까지)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(2)

                # "Additional details" 버튼 찾기
                expand_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Additional details')]/ancestor::a"))
                )

                # 버튼까지 스크롤
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", expand_button)
                time.sleep(1)

                # 클릭
                expand_button.click()
                time.sleep(1)  # 확장 애니메이션 대기
                print(f"[INFO] 'Additional details' section expanded")
                additional_details_found = True

                # "Item details" 버튼도 클릭
                try:
                    item_details_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Item details')]/ancestor::a"))
                    )
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item_details_button)
                    time.sleep(1)
                    item_details_button.click()
                    time.sleep(1)
                    print(f"[INFO] 'Item details' section expanded")
                except Exception as e:
                    print(f"[WARNING] Could not expand 'Item details': {e}")

                # 확장 후 HTML 다시 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

            except Exception as e:
                print(f"[WARNING] Could not expand 'Additional details': {e}")
                print(f"[INFO] Using fallback: static Product Information table...")

            # === hhp_storage, hhp_color, rank_1, rank_2 추출 ===
            if additional_details_found:
                # Additional details 확장된 경우: 기본 XPath로 추출
                try:
                    hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_storage: {e}")

                try:
                    hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_color: {e}")

                try:
                    rank_1 = self.extract_with_fallback(tree, self.xpaths.get('rank_1', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_1: {e}")

                try:
                    rank_2 = self.extract_with_fallback(tree, self.xpaths.get('rank_2', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_2: {e}")
            else:
                # Fallback: 정적 Product Information 테이블에서 추출
                try:
                    fallback_storage_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Memory Storage Capacity')]/following-sibling::td/text()"
                    hhp_storage = self.extract_with_fallback(tree, fallback_storage_xpath)
                    if hhp_storage:
                        print(f"[INFO] hhp_storage extracted from fallback table: {hhp_storage}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_storage (fallback): {e}")

                try:
                    fallback_color_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Color')]/following-sibling::td/text()"
                    hhp_color = self.extract_with_fallback(tree, fallback_color_xpath)
                    if hhp_color:
                        print(f"[INFO] hhp_color extracted from fallback table: {hhp_color}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_color (fallback): {e}")

                try:
                    fallback_rank1_xpath = "//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[1]//span/text()"
                    rank_1 = self.extract_with_fallback(tree, fallback_rank1_xpath)
                    if rank_1:
                        print(f"[INFO] rank_1 extracted from fallback table: {rank_1}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_1 (fallback): {e}")

                try:
                    fallback_rank2_xpath = "//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[2]//span/text()"
                    rank_2 = self.extract_with_fallback(tree, fallback_rank2_xpath)
                    if rank_2:
                        print(f"[INFO] rank_2 extracted from fallback table: {rank_2}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_2 (fallback): {e}")


            # === 리뷰 섹션으로 이동 (리뷰 링크 클릭) ===
            try:
                # 페이지 맨 위로 스크롤
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                # HTML 다시 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

                # 리뷰 링크 클릭
                review_link = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "acrCustomerReviewLink"))
                )
                review_link.click()
                time.sleep(2)  # 리뷰 섹션 로딩 대기

                # 리뷰 섹션 이동 후 HTML 다시 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                print(f"[INFO] Navigated to review section")
            except Exception as e:
                print(f"[WARNING] Could not navigate to review section: {e}")

            # === 리뷰수/별점/별점별리뷰수 데이터 추출 및 후처리 ===
            try:
                # count_of_reviews: 원본 추출 후 리뷰 개수 추출
                count_of_reviews_raw = self.extract_with_fallback(tree, self.xpaths.get('count_of_reviews', {}).get('xpath'))
                count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw)
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_reviews: {e}")

            try:
                # star_rating: 원본 추출 후 별점 추출
                star_rating_raw = self.extract_with_fallback(tree, self.xpaths.get('star_rating', {}).get('xpath'))
                star_rating = data_extractor.extract_rating(star_rating_raw)
            except Exception as e:
                print(f"[WARNING] Failed to extract star_rating: {e}")

            try:
                # count_of_star_ratings: 별점 분포 계산
                count_of_star_ratings_xpath = self.xpaths.get('count_of_star_ratings', {}).get('xpath')
                count_of_star_ratings = data_extractor.extract_star_ratings_count(tree, count_of_reviews, count_of_star_ratings_xpath, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_star_ratings: {e}")

            # === Detail 페이지에서 추출 가능한 모든 필드 ===
            try:
                # summarized_review_content: AI 요약 리뷰 (Detail 페이지)
                summarized_review_content = self.extract_with_fallback(tree, self.xpaths.get('summarized_review_content', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract summarized_review_content: {e}")

            print(f"[INFO] Detail page extraction completed")

            # === STEP 2: 리뷰 페이지로 이동하여 detailed_review_content 추출 ===
            #detailed_review_content = self.extract_reviews(item, max_reviews=20) if item else None
            detailed_review_content = None

            # Detail 페이지에서 추출할 필드 (hhp_retail_com 테이블 구조 기준)
            detail_data = {
                'country': country,
                'product': product_type,
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'sku_popularity': sku_popularity,
                'bundle': bundle,
                'trade_in': trade_in,
                'retailer_membership_discounts': retailer_membership_discounts,
                'rank_1': rank_1,
                'rank_2': rank_2,
                'hhp_carrier': hhp_carrier,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'summarized_review_content': summarized_review_content,
                'detailed_review_content': detailed_review_content,
            }

            # product_list 데이터 + Detail 데이터 결합
            combined_data = {**product, **detail_data}

            return combined_data

        except Exception as e:
            print(f"[ERROR] Failed to crawl detail page {product.get('product_url')}: {e}")
            traceback.print_exc()
            # 에러 발생 시 product_list 데이터만 반환 (Detail 필드는 NULL)
            return product

    def save_to_retail_com(self, products):
        """
        수집된 데이터를 hhp_retail_com 테이블에 저장
        - product_list에서 가져온 필드 + Detail 페이지에서 추출한 필드 결합
        - 1개 제품마다 즉시 저장 (리뷰 데이터 메모리 효율)

        Args: products (list): 결합된 제품 데이터 리스트

        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        print(f"[INFO] Saving {len(products)} products to hhp_retail_com...\n")

        try:
            cursor = self.db_conn.cursor()

            # hhp_retail_com 테이블 구조에 맞춤
            # product_list에서 가져온 필드 + detail 페이지에서 추출한 필드
            insert_query = """
                INSERT INTO hhp_retail_com (
                    country, product, item, account_name, page_type,
                    retailer_sku_name, product_url,
                    count_of_reviews, star_rating, count_of_star_ratings,
                    sku_popularity, bundle, trade_in,
                    retailer_membership_discounts,
                    rank_1, rank_2,
                    hhp_carrier, hhp_storage, hhp_color,
                    detailed_review_content, summarized_review_content,
                    final_sku_price, original_sku_price,
                    shipping_info, available_quantity_for_purchase,
                    discount_type, main_rank, bsr_rank,
                    number_of_units_purchased_past_month,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
            """

            saved_count = 0
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Detail 크롤러 실행 시점

            for product in products:
                # 디버그: 실제 저장되는 값 출력
                values = (
                    # Detail 페이지에서 추출한 필드
                    product.get('country'),
                    product.get('product'),
                    product.get('item'),
                    product.get('account_name'),
                    product.get('page_type'),
                    product.get('retailer_sku_name'),
                    product.get('product_url'),
                    product.get('count_of_reviews'),
                    product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('sku_popularity'),
                    product.get('bundle'),
                    product.get('trade_in'),
                    product.get('retailer_membership_discounts'),
                    product.get('rank_1'),
                    product.get('rank_2'),
                    product.get('hhp_carrier'),
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('detailed_review_content'),
                    product.get('summarized_review_content'),
                    # product_list에서 가져온 필드
                    product.get('final_sku_price'),
                    product.get('original_sku_price'),
                    product.get('shipping_info'),
                    product.get('available_quantity_for_purchase'),
                    product.get('discount_type'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('number_of_units_purchased_past_month'),
                    product.get('calendar_week'),
                    current_time,  # Detail 크롤러 실행 시점의 현재 시간 사용
                    product.get('batch_id')  # 배치 ID
                )

                print(f"\n[DEBUG] VALUES being inserted:")
                for i, val in enumerate(values, 1):
                    print(f"  [{i}] {val}")

                cursor.execute(insert_query, values)
                saved_count += 1

            self.db_conn.commit()
            cursor.close()

            # 저장된 제품 샘플 출력 (처음 3개)
            print(f"[SUCCESS] Saved {saved_count} products to hhp_retail_com")
            for i, product in enumerate(products[:3], 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                item = product.get('item', 'N/A')
                print(f"[{i}] {sku_name[:40]}... (ASIN: {item})")
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

        Returns:
            bool: 성공 시 True, 실패 시 False
        """
        try:
            # 초기화
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            # product_list 테이블에서 해당 batch_id의 모든 제품 로드
            product_list = self.load_product_list()

            if not product_list:
                print(f"[WARNING] No products found for batch_id: {self.batch_id}")
                return False

            print("\n" + "="*60)
            print(f"[INFO] Starting Amazon Detail page crawling...")
            print(f"[INFO] Total products to crawl: {len(product_list)}")
            print("="*60 + "\n")

            # 모든 제품 상세 페이지 크롤링
            total_saved = 0

            for i, product in enumerate(product_list, 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] Processing: {sku_name[:50]}...")

                combined_data = self.crawl_detail(product)

                # 첫 제품 크롤링 후 쿠키 저장 (세션 고정, 이후 제품에서 재사용)
                if not self.cookies_loaded and i == 1:
                    self.save_cookies(self.account_name)
                    self.cookies_loaded = True

                # 1개 제품마다 즉시 DB에 저장 (리뷰 데이터가 클 수 있어 메모리 효율)
                saved_count = self.save_to_retail_com([combined_data])
                total_saved += saved_count

                # 페이지 간 대기
                time.sleep(5)

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Amazon Detail Crawler Finished")
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
            if self.driver:
                self.driver.quit()
                print("[INFO] WebDriver closed")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] Database connection closed")


def main():
    """
    개별 실행 시 진입점 (테스트 모드)
    - 최신 batch_id를 DB에서 자동 조회하여 1개 제품만 크롤링
    """
    crawler = AmazonDetailCrawler(test_mode=True, batch_id=None)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Amazon Detail Crawler completed successfully")
    else:
        print("\n[FAILED] Amazon Detail Crawler failed")

    return success


if __name__ == '__main__':
    success = main()
    if not success:
        exit(1)