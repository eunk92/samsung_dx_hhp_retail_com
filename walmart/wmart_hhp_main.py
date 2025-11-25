"""
Walmart Main 페이지 크롤러 (Playwright 기반)
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- Main 페이지에서 제품 리스트 수집 (main_rank 자동 계산)
- main_rank는 페이지 관계없이 1부터 순차 증가
- 테스트 모드: 1개 제품만 수집
- 운영 모드: 최대 300개 제품까지 수집
- CAPTCHA 자동 해결 기능 포함
"""

import sys
import os
import time
import traceback
import random
import psycopg2
from datetime import datetime
from lxml import html
from playwright.sync_api import sync_playwright

# 공통 환경 설정
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from config import DB_CONFIG


class WalmartMainCrawler:
    """
    Walmart Main 페이지 크롤러 (Playwright 기반)
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
            batch_id (str): 배치 ID (기본값: None)
        """
        self.test_mode = test_mode
        self.account_name = 'Walmart'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None

        # Playwright 객체
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # DB 연결
        self.db_conn = None
        self.xpaths = {}

        # 테스트 설정
        self.test_count = 1

        # 운영 모드 설정
        self.max_products = 300
        self.current_rank = 0

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
                SELECT field_name, xpath
                FROM hhp_xpath_selectors
                WHERE account_name = %s AND page_type = %s
            """, (self.account_name, self.page_type))

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {'xpath': row[1]}

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
                SELECT page_url
                FROM hhp_page_urls
                WHERE account_name = %s AND page_type = %s
            """, (self.account_name, self.page_type))

            result = cursor.fetchone()
            cursor.close()

            if result:
                self.url_template = result[0]
                print(f"[OK] Loaded URL template")
                return True
            else:
                print("[ERROR] URL template not found")
                return False

        except Exception as e:
            print(f"[ERROR] Failed to load URL: {e}")
            return False

    def generate_batch_id(self):
        """배치 ID 생성"""
        now = datetime.now()
        return f"w_{now.strftime('%Y%m%d_%H%M%S')}"

    def generate_calendar_week(self):
        """캘린더 주차 생성"""
        now = datetime.now()
        return now.strftime('%Y-W%U')

    def setup_playwright(self):
        """Playwright 브라우저 설정"""
        try:
            self.playwright = sync_playwright().start()

            # Chrome 브라우저 사용
            self.browser = self.playwright.chromium.launch(
                headless=False,
                channel="chrome",
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
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US'
            )

            # 스텔스 스크립트 주입
            self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
            """)

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

            # CAPTCHA 버튼 찾기
            captcha_selectors = [
                'text=PRESS & HOLD',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                'button:has-text("PRESS")',
                'button:has-text("HOLD")',
                '[class*="captcha"]',
                '[class*="PressHold"]'
            ]

            button = None
            for selector in captcha_selectors:
                try:
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=2000):
                        button = temp_button
                        print(f"[OK] CAPTCHA detected with selector: {selector}")
                        break
                except:
                    continue

            if not button:
                # CAPTCHA 키워드 확인
                page_content = self.page.content().lower()
                if any(keyword in page_content for keyword in ['press & hold', 'captcha', 'human verification']):
                    print("[WARNING] CAPTCHA keywords found but button not located")
                    print("[INFO] Waiting 30 seconds for manual intervention...")
                    time.sleep(30)
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

    def crawl_page(self, page_number):
        """특정 페이지 크롤링"""
        try:
            url = self.url_template.replace('{page}', str(page_number))
            print(f"\n[INFO] Crawling page {page_number}: {url}")

            # 페이지 로드
            self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(random.uniform(3, 5))

            # 첫 페이지일 경우 CAPTCHA 처리
            if page_number == 1:
                if not self.handle_captcha():
                    print("[WARNING] CAPTCHA handling failed")

                time.sleep(random.uniform(3, 5))

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
            for idx, item in enumerate(containers_to_process, 1):
                try:
                    self.current_rank += 1

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'main_rank': self.current_rank,
                        'calendar_week': self.calendar_week,
                        'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    # 각 필드 추출 (try-except로 감싸기)
                    try:
                        product_url_raw = item.xpath(self.xpaths.get('product_url', {}).get('xpath'))
                        if product_url_raw:
                            product_url_raw = product_url_raw[0] if isinstance(product_url_raw, list) else product_url_raw
                            product_data['product_url'] = f"https://www.walmart.com{product_url_raw}" if product_url_raw.startswith('/') else product_url_raw
                        else:
                            product_data['product_url'] = None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract product_url for product {idx}: {e}")
                        product_data['product_url'] = None

                    try:
                        retailer_sku_name_raw = item.xpath(self.xpaths.get('retailer_sku_name', {}).get('xpath'))
                        product_data['retailer_sku_name'] = retailer_sku_name_raw[0] if retailer_sku_name_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract retailer_sku_name for product {idx}: {e}")
                        product_data['retailer_sku_name'] = None

                    try:
                        final_sku_price_raw = item.xpath(self.xpaths.get('final_sku_price', {}).get('xpath'))
                        product_data['final_sku_price'] = final_sku_price_raw[0] if final_sku_price_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract final_sku_price for product {idx}: {e}")
                        product_data['final_sku_price'] = None

                    try:
                        original_sku_price_raw = item.xpath(self.xpaths.get('original_sku_price', {}).get('xpath'))
                        product_data['original_sku_price'] = original_sku_price_raw[0] if original_sku_price_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract original_sku_price for product {idx}: {e}")
                        product_data['original_sku_price'] = None

                    try:
                        offer_raw = item.xpath(self.xpaths.get('offer', {}).get('xpath'))
                        product_data['offer'] = offer_raw[0] if offer_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract offer for product {idx}: {e}")
                        product_data['offer'] = None

                    try:
                        pickup_raw = item.xpath(self.xpaths.get('pick_up_availability', {}).get('xpath'))
                        product_data['pick_up_availability'] = pickup_raw[0] if pickup_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract pick_up_availability for product {idx}: {e}")
                        product_data['pick_up_availability'] = None

                    try:
                        shipping_raw = item.xpath(self.xpaths.get('shipping_availability', {}).get('xpath'))
                        product_data['shipping_availability'] = shipping_raw[0] if shipping_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract shipping_availability for product {idx}: {e}")
                        product_data['shipping_availability'] = None

                    try:
                        delivery_raw = item.xpath(self.xpaths.get('delivery_availability', {}).get('xpath'))
                        product_data['delivery_availability'] = delivery_raw[0] if delivery_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract delivery_availability for product {idx}: {e}")
                        product_data['delivery_availability'] = None

                    try:
                        sku_status_raw = item.xpath(self.xpaths.get('sku_status', {}).get('xpath'))
                        product_data['sku_status'] = sku_status_raw[0] if sku_status_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract sku_status for product {idx}: {e}")
                        product_data['sku_status'] = None

                    try:
                        membership_raw = item.xpath(self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))
                        product_data['retailer_membership_discounts'] = membership_raw[0] if membership_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract retailer_membership_discounts for product {idx}: {e}")
                        product_data['retailer_membership_discounts'] = None

                    try:
                        quantity_raw = item.xpath(self.xpaths.get('available_quantity_for_purchase', {}).get('xpath'))
                        product_data['available_quantity_for_purchase'] = quantity_raw[0] if quantity_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract available_quantity_for_purchase for product {idx}: {e}")
                        product_data['available_quantity_for_purchase'] = None

                    try:
                        inventory_raw = item.xpath(self.xpaths.get('inventory_status', {}).get('xpath'))
                        product_data['inventory_status'] = inventory_raw[0] if inventory_raw else None
                    except Exception as e:
                        print(f"[WARNING] Failed to extract inventory_status for product {idx}: {e}")
                        product_data['inventory_status'] = None

                    products.append(product_data)

                except Exception as e:
                    print(f"[WARNING] Failed to process product {idx}/{len(containers_to_process)}, skipping: {e}")
                    continue

            return products

        except Exception as e:
            print(f"[ERROR] Failed to crawl page {page_number}: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """제품 데이터 DB 저장 (10개씩 배치)"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()

            insert_query = """
                INSERT INTO wmart_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    final_sku_price, original_sku_price, offer,
                    pick_up_availability, shipping_availability, delivery_availability,
                    sku_status, retailer_membership_discounts,
                    available_quantity_for_purchase, inventory_status,
                    main_rank, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            BATCH_SIZE = 10
            total_saved = 0

            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
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
                            product['main_rank'],
                            product['product_url'],
                            product['calendar_week'],
                            product['crawl_strdatetime'],
                            product['batch_id']
                        )
                        values_list.append(one_product)

                    cursor.executemany(insert_query, values_list)
                    self.db_conn.commit()

                    total_saved += len(batch_products)
                    print(f"[INFO] Saved batch {batch_start+1}-{batch_end} ({len(batch_products)} products)")

                except Exception as batch_error:
                    print(f"[ERROR] Failed to save batch {batch_start+1}-{batch_end}: {batch_error}")
                    self.db_conn.rollback()
                    continue

            cursor.close()

            print(f"[SUCCESS] Saved {total_saved}/{len(products)} products to database")
            for i, product in enumerate(products[:3], 1):
                sku_name = product['retailer_sku_name'] or 'N/A'
                print(f"[{i}] {sku_name[:50]}... - {product['final_sku_price']}")
            if len(products) > 3:
                print(f"... and {len(products) - 3} more products")

            return total_saved

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            return 0

    def initialize(self):
        """크롤러 초기화"""
        print("\n" + "="*60)
        print(f"[INFO] Walmart Main Crawler Initialization (Playwright)")
        print(f"[INFO] Test Mode: {'ON (1 product)' if self.test_mode else 'OFF (max 300 products)'}")
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

        # 5. 배치 ID 및 캘린더 주차 생성
        if not self.batch_id:
            self.batch_id = self.generate_batch_id()
            print(f"[INFO] Batch ID generated: {self.batch_id}")
        else:
            print(f"[INFO] Batch ID received: {self.batch_id}")

        self.calendar_week = self.generate_calendar_week()
        print(f"[INFO] Calendar Week: {self.calendar_week}")

        return True

    def run(self):
        """크롤러 실행"""
        try:
            # 초기화
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            # 크롤링 시작
            print("\n" + "="*60)
            print("[INFO] Starting Walmart Main page crawling...")
            print("="*60 + "\n")

            total_products = 0

            if self.test_mode:
                # 테스트 모드: 1개 제품만
                self.current_rank = 0
                products = self.crawl_page(1)
                saved_count = self.save_products(products)
                total_products += saved_count
            else:
                # 운영 모드: 300개까지
                self.current_rank = 0
                page_num = 1

                while total_products < self.max_products:
                    products = self.crawl_page(page_num)

                    if not products:
                        print(f"[WARNING] No products found at page {page_num}")
                        if page_num > 1:
                            print(f"[INFO] No more products available, stopping...")
                            break
                    else:
                        remaining = self.max_products - total_products
                        products_to_save = products[:remaining]

                        saved_count = self.save_products(products_to_save)
                        total_products += saved_count

                        print(f"[INFO] Progress: {total_products}/{self.max_products} products collected")

                        if total_products >= self.max_products:
                            print(f"[INFO] Reached target product count ({self.max_products}), stopping...")
                            break

                    time.sleep(random.uniform(5, 10))
                    page_num += 1

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Walmart Main Crawler Finished")
            print(f"[RESULT] Total products collected: {total_products}")
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
    """개별 실행 시 진입점 (테스트 모드 ON)"""
    crawler = WalmartMainCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Walmart Main Crawler completed successfully")
    else:
        print("\n[FAILED] Walmart Main Crawler failed")


if __name__ == '__main__':
    main()
