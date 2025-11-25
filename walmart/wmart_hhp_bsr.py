"""
Walmart BSR 페이지 크롤러 (Playwright 기반)
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- BSR 페이지에서 제품 리스트 수집 (bsr_rank 포함)
- 테스트 모드: 1페이지에서 2개 제품만 수집
- 운영 모드: 2페이지 크롤링
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


class WalmartBSRCrawler(BaseCrawler):
    """
    Walmart BSR 페이지 크롤러 (Playwright 기반)
    BaseCrawler 상속으로 공통 메서드 사용
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
            batch_id (str): 배치 ID (기본값: None)
        """
        super().__init__()  # BaseCrawler 초기화
        self.test_mode = test_mode
        self.account_name = 'Walmart'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None

        # Playwright 객체
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 테스트 설정
        self.test_page = 1
        self.test_count = 1

        # BSR 순위 카운터
        self.current_bsr_rank = 0

    def connect_db(self):
        """DB 연결"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def format_walmart_price(self, price_result):
        """
        Walmart 가격 결과를 정제하여 $XX.XX 형식으로 변환

        Args:
            price_result: XPath로 추출한 가격 결과 (리스트 또는 문자열)

        Returns:
            str: 정제된 가격 문자열 (예: '$39.88') 또는 None
        """
        if not price_result:
            return None

        try:
            # 리스트인 경우 (//text() 사용 시)
            if isinstance(price_result, list):
                # 공백 제거하고 빈 문자열 제외
                parts = [p.strip() for p in price_result if p.strip()]

                if not parts:
                    return None

                # 패턴: ['$', 'XX', 'XX'] - 달러, 정수부, 소수부
                if len(parts) >= 3:
                    # $ 찾기
                    dollar_idx = None
                    for i, p in enumerate(parts):
                        if '$' in p:
                            dollar_idx = i
                            break

                    if dollar_idx is not None and dollar_idx + 2 < len(parts):
                        # 정수부와 소수부 가져오기
                        dollars = parts[dollar_idx + 1]  # 정수부
                        cents = parts[dollar_idx + 2]    # 소수부

                        # 숫자인지 확인
                        if dollars.isdigit() and cents.isdigit():
                            return f"${dollars}.{cents}"

                # 모든 텍스트를 연결해서 처리
                price_result = ''.join(parts)

            # 문자열인 경우
            if isinstance(price_result, str):
                # 이미 완전한 가격 형식인 경우
                if '$' in price_result and '.' in price_result:
                    return price_result.strip()

                # $와 숫자만 추출
                import re
                match = re.search(r'\$(\d+)\.?(\d{2})?', price_result)
                if match:
                    dollars = match.group(1)
                    cents = match.group(2) if match.group(2) else '00'
                    return f"${dollars}.{cents}"

            return None

        except Exception as e:
            print(f"[WARNING] Price formatting failed: {e}")
            return None

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

    def load_page_url(self):
        """페이지 URL 템플릿 로드"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT url_template
                FROM hhp_target_page_url
                WHERE account_name = %s AND page_type = %s
            """, (self.account_name, self.page_type))

            result = cursor.fetchone()
            cursor.close()

            if result:
                self.url_template = result[0]
                print("[OK] Loaded URL template")
                return True
            else:
                print("[ERROR] URL template not found")
                return False

        except Exception as e:
            print(f"[ERROR] Failed to load URL: {e}")
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

            # CAPTCHA 버튼 찾기 (더 많은 셀렉터 추가)
            captcha_selectors = [
                'button:has-text("PRESS & HOLD")',
                'div:has-text("PRESS & HOLD")',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                'button:has-text("PRESS")',
                'div:has-text("PRESS")',
                '[aria-label*="press"]',
                '[class*="PressHold"]',
                '[class*="presshold"]',
                '[class*="captcha"]',
                '[id*="captcha"]',
                'button',  # 모든 버튼 확인
                'div[role="button"]'
            ]

            button = None
            for selector in captcha_selectors:
                try:
                    # 더 긴 타임아웃 사용
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=5000):
                        # 텍스트 확인
                        text = temp_button.inner_text(timeout=2000).upper()
                        if 'PRESS' in text or 'HOLD' in text or 'CAPTCHA' in text:
                            button = temp_button
                            print(f"[OK] CAPTCHA detected with selector: {selector}")
                            print(f"[DEBUG] Button text: {text}")
                            break
                except:
                    continue

            if not button:
                # CAPTCHA 키워드 확인
                page_content = self.page.content().lower()
                if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha', 'human verification']):
                    print("[WARNING] CAPTCHA keywords found but button not located")
                    print("[INFO] Please solve CAPTCHA manually...")
                    print("[INFO] Waiting 45 seconds for manual intervention...")
                    time.sleep(45)
                    return True
                else:
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
        print(f"[INFO] Walmart BSR Crawler Initialization (Playwright)")
        print(f"[INFO] Test Mode: {'ON (2 products from page 1)' if self.test_mode else 'OFF (2 pages)'}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. XPath 셀렉터 로드
        if not self.load_xpaths():
            return False

        # 3. URL 템플릿 로드
        if not self.load_page_url():
            return False

        # 4. Playwright 설정
        if not self.setup_playwright():
            return False

        # 5. 배치 ID 및 캘린더 주차 생성 (BaseCrawler 상속 메서드 사용)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)
            print(f"[INFO] Batch ID generated: {self.batch_id}")
        else:
            print(f"[INFO] Batch ID received: {self.batch_id}")

        self.calendar_week = self.generate_calendar_week()
        print(f"[INFO] Calendar Week: {self.calendar_week}\n")

        return True

    def crawl_page(self, page_number):
        """
        특정 페이지 크롤링

        Args: page_number (int): 페이지 번호

        Returns: list: 수집된 제품 데이터 리스트
        """
        try:
            # URL 생성
            url = self.url_template.replace('{page}', str(page_number))
            print(f"\n[INFO] Crawling page {page_number}: {url}")

            # 페이지 로드
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # CAPTCHA 처리 (handle_captcha가 성공 indicator를 감지하고 완전히 사라질 때까지 대기)
            self.handle_captcha()

            # CAPTCHA 해결 후 제품 로딩을 위한 추가 대기
            print("[INFO] Waiting for products to load after CAPTCHA...")
            time.sleep(5)

            # HTML 파싱
            page_html = self.page.content()
            tree = html.fromstring(page_html)

            # 제품 리스트 XPath
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            # 제품 아이템 추출
            base_containers = tree.xpath(base_container_xpath)
            print(f"[INFO] Found {len(base_containers)} products on page {page_number}")

            # 테스트/운영 모드에 따라 처리
            if self.test_mode:
                containers_to_process = base_containers[:self.test_count]
            else:
                containers_to_process = base_containers

            products = []
            for item in containers_to_process:
                try:
                    # bsr_rank 순번 증가 (main_rank와 동일한 방식)
                    self.current_bsr_rank += 1

                    # product_url 추출 및 절대 경로 변환
                    product_url_raw = self.extract_with_fallback(item, self.xpaths.get('product_url', {}).get('xpath'))
                    product_url = f"https://www.walmart.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    # 가격 추출 및 정제
                    final_price_xpath = self.xpaths.get('final_sku_price', {}).get('xpath')
                    final_price_raw = item.xpath(final_price_xpath) if final_price_xpath else None
                    final_sku_price = self.format_walmart_price(final_price_raw)

                    # retailer_membership_discounts 추출 및 "W+" 결합
                    membership_discounts_raw = self.extract_with_fallback(item, self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))
                    retailer_membership_discounts = f"{membership_discounts_raw} W+" if membership_discounts_raw else None

                    # sku_status_1, sku_status_2 추출 및 결합
                    sku_status_1 = self.extract_with_fallback(item, self.xpaths.get('sku_status_1', {}).get('xpath'))
                    sku_status_2 = self.extract_with_fallback(item, self.xpaths.get('sku_status_2', {}).get('xpath'))
                    sku_status_parts = [s for s in [sku_status_1, sku_status_2] if s]
                    sku_status = ', '.join(sku_status_parts) if sku_status_parts else None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.extract_with_fallback(item, self.xpaths.get('retailer_sku_name', {}).get('xpath')),
                        'final_sku_price': final_sku_price,
                        'original_sku_price': self.extract_with_fallback(item, self.xpaths.get('original_sku_price', {}).get('xpath')),
                        'offer': self.extract_with_fallback(item, self.xpaths.get('offer', {}).get('xpath')),
                        'pick_up_availability': self.extract_with_fallback(item, self.xpaths.get('pick_up_availability', {}).get('xpath')),
                        'shipping_availability': self.extract_with_fallback(item, self.xpaths.get('shipping_availability', {}).get('xpath')),
                        'delivery_availability': self.extract_with_fallback(item, self.xpaths.get('delivery_availability', {}).get('xpath')),
                        'sku_status': sku_status,
                        'retailer_membership_discounts': retailer_membership_discounts,
                        'available_quantity_for_purchase': self.extract_with_fallback(item, self.xpaths.get('available_quantity_for_purchase', {}).get('xpath')),
                        'inventory_status': self.extract_with_fallback(item, self.xpaths.get('inventory_status', {}).get('xpath')),
                        'bsr_rank': self.current_bsr_rank,
                        'product_url': product_url,
                        'calendar_week': self.calendar_week,
                        'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    products.append(product_data)

                except Exception as e:
                    print(f"[WARNING] Failed to extract product data, skipping: {e}")
                    continue

            return products

        except Exception as e:
            print(f"[ERROR] Failed to crawl page {page_number}: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """
        수집된 제품 데이터를 wmart_hhp_product_list 테이블에 저장
        - 중복 확인: batch_id + product_url 조합으로 체크
        - 존재하면 UPDATE (bsr_rank만), 없으면 INSERT
        - INSERT는 20개씩 배치 처리 (부분 실패 방지)

        Args: products (list): 제품 데이터 리스트
        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0

            # 1단계: UPDATE와 INSERT 분리
            products_to_update = []
            products_to_insert = []

            for product in products:
                # 중복 확인 (batch_id + product_url)
                exists = self.check_product_exists(
                    self.account_name,
                    product['batch_id'],
                    product['product_url']
                )

                if exists:
                    products_to_update.append(product)
                else:
                    products_to_insert.append(product)

            # 2단계: UPDATE 처리 (개별 실행)
            update_query = """
                UPDATE wmart_hhp_product_list
                SET bsr_rank = %s
                WHERE account_name = %s
                  AND batch_id = %s
                  AND product_url = %s
            """

            for product in products_to_update:
                try:
                    cursor.execute(update_query, (
                        product['bsr_rank'],
                        self.account_name,
                        product['batch_id'],
                        product['product_url']
                    ))
                    self.db_conn.commit()
                    update_count += 1
                except Exception as update_error:
                    print(f"[WARNING] Failed to update product {product.get('product_url')}: {update_error}")
                    self.db_conn.rollback()
                    continue

            # 3단계: INSERT 배치 처리 (20개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO wmart_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, original_sku_price, offer,
                        pick_up_availability, shipping_availability, delivery_availability,
                        sku_status, retailer_membership_discounts,
                        available_quantity_for_purchase, inventory_status,
                        bsr_rank, product_url,
                        calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

                BATCH_SIZE = 20

                for batch_start in range(0, len(products_to_insert), BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, len(products_to_insert))
                    batch_products = products_to_insert[batch_start:batch_end]

                    try:
                        # 배치를 튜플 리스트로 변환
                        values_list = []
                        for product in batch_products:
                            one_product = (
                                product['account_name'],
                                product['page_type'],
                                product['retailer_sku_name'],
                                product['final_sku_price'],
                                product['original_sku_price'],
                                product['offer'],
                                product['pick_up_availability'],
                                product['shipping_availability'],
                                product['delivery_availability'],
                                product['sku_status'],
                                product['retailer_membership_discounts'],
                                product['available_quantity_for_purchase'],
                                product['inventory_status'],
                                product['bsr_rank'],
                                product['product_url'],
                                product['calendar_week'],
                                product['crawl_strdatetime'],
                                product['batch_id']
                            )
                            values_list.append(one_product)

                        # 배치 INSERT
                        cursor.executemany(insert_query, values_list)
                        self.db_conn.commit()

                        insert_count += len(batch_products)
                        print(f"[INFO] Inserted batch {batch_start+1}-{batch_end} ({len(batch_products)} products)")

                    except Exception as batch_error:
                        print(f"[ERROR] Failed to insert batch {batch_start+1}-{batch_end}: {batch_error}")
                        self.db_conn.rollback()
                        continue

            cursor.close()

            # 진행 상황 요약 출력
            print(f"[SUCCESS] Saved {insert_count + update_count} products to database (INSERT: {insert_count}, UPDATE: {update_count})")
            for i, product in enumerate(products[:3], 1):
                sku_name = product['retailer_sku_name'] or 'N/A'
                print(f"[{i}] {sku_name[:50]}... - bsr_rank: {product['bsr_rank']}")
            if len(products) > 3:
                print(f"... and {len(products) - 3} more products")

            return insert_count + update_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
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

            # 크롤링 시작
            print("\n" + "="*60)
            print("[INFO] Starting Walmart BSR page crawling...")
            print("="*60 + "\n")

            total_products = 0

            if self.test_mode:
                # 테스트 모드: 설정된 페이지만 크롤링 및 DB 저장
                products = self.crawl_page(self.test_page)
                saved_count = self.save_products(products)
                total_products += saved_count
            else:
                # 운영 모드: 100개 제품 수집될 때까지 페이지 크롤링 (DB 저장)
                max_products = 100
                page_num = 1

                while total_products < max_products:
                    print(f"[INFO] Current progress: {total_products}/{max_products} products collected")

                    products = self.crawl_page(page_num)

                    if not products:
                        print(f"[WARNING] No products found at page {page_num}, stopping crawler...")
                        break

                    saved_count = self.save_products(products)
                    total_products += saved_count

                    # 목표 달성 확인
                    if total_products >= max_products:
                        print(f"[INFO] Target reached: {total_products} products collected")
                        break

                    # 다음 페이지로 이동
                    page_num += 1

                    # 페이지 간 대기
                    time.sleep(30)

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Walmart BSR Crawler Finished")
            print(f"[RESULT] Total products collected: {total_products}")
            print(f"[RESULT] Batch ID: {self.batch_id}")
            print("="*60 + "\n")

            return True

        except Exception as e:
            print(f"[ERROR] Crawler execution failed: {e}")
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
    crawler = WalmartBSRCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Walmart BSR Crawler completed successfully")
    else:
        print("\n[FAILED] Walmart BSR Crawler failed")


if __name__ == '__main__':
    main()