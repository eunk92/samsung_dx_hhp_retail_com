"""
Walmart Main 페이지 크롤러 (Playwright 기반)

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Main 페이지에서 제품 리스트 수집 (main_rank 자동 계산)
- main_rank는 페이지 관계없이 1부터 순차 증가
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집
- CAPTCHA 자동 해결 기능 포함

================================================================================
저장 테이블
================================================================================
- wmart_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import random
import re
import traceback
from datetime import datetime
from lxml import html
from playwright.sync_api import sync_playwright

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class WalmartMainCrawler(BaseCrawler):
    """
    Walmart Main 페이지 크롤러 (Playwright 기반)
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
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

        self.test_count = 3  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.current_rank = 0

    def format_walmart_price(self, price_result):
        """Walmart 가격 결과를 $XX.XX 형식으로 변환"""
        if not price_result:
            return None

        try:
            if isinstance(price_result, list):
                parts = [p.strip() for p in price_result if p.strip()]
                if not parts:
                    return None

                if len(parts) >= 3:
                    dollar_idx = None
                    for i, p in enumerate(parts):
                        if '$' in p:
                            dollar_idx = i
                            break

                    if dollar_idx is not None and dollar_idx + 2 < len(parts):
                        dollars = parts[dollar_idx + 1]
                        cents = parts[dollar_idx + 2]
                        if dollars.isdigit() and cents.isdigit():
                            return f"${dollars}.{cents}"

                price_result = ''.join(parts)

            if isinstance(price_result, str):
                if '$' in price_result and '.' in price_result:
                    return price_result.strip()

                match = re.search(r'\$(\d+)\.?(\d{2})?', price_result)
                if match:
                    dollars = match.group(1)
                    cents = match.group(2) if match.group(2) else '00'
                    return f"${dollars}.{cents}"

            return None

        except Exception as e:
            print(f"[WARNING] Price formatting failed: {e}")
            return None

    def setup_playwright(self):
        """Playwright 브라우저 설정"""
        try:
            # Windows TEMP 폴더 문제 해결
            temp_dir = 'C:\\Temp'
            os.makedirs(temp_dir, exist_ok=True)
            os.environ['TEMP'] = temp_dir
            os.environ['TMP'] = temp_dir

            self.playwright = sync_playwright().start()

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

            self.context = self.browser.new_context(
                viewport=None,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US'
            )

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
            traceback.print_exc()
            return False

    def handle_captcha(self):
        """CAPTCHA 자동 해결"""
        try:
            time.sleep(random.uniform(1, 5))

            captcha_selectors = [
                'button:has-text("PRESS & HOLD")',
                'div:has-text("PRESS & HOLD")',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                '[class*="captcha"]',
                '[id*="captcha"]'
            ]

            button = None
            for selector in captcha_selectors:
                try:
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=5000):
                        text = temp_button.inner_text(timeout=2000).upper()
                        if 'PRESS' in text or 'HOLD' in text or 'CAPTCHA' in text:
                            button = temp_button
                            print(f"[WARNING] CAPTCHA detected")
                            break
                except:
                    continue

            if not button:
                page_content = self.page.content().lower()
                if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha']):
                    time.sleep(random.uniform(43, 47))
                    return True
                return True

            box = button.bounding_box()
            if box:
                center_x = box['x'] + box['width'] / 2
                center_y = box['y'] + box['height'] / 2

                self.page.mouse.move(center_x, center_y)
                time.sleep(random.uniform(0.3, 0.6))

                self.page.mouse.down()
                hold_time = random.uniform(7, 9)
                time.sleep(hold_time)
                self.page.mouse.up()

                time.sleep(random.uniform(3, 5))

                try:
                    if not button.is_visible(timeout=3000):
                        print("[OK] CAPTCHA solved")
                        return True
                    else:
                        time.sleep(random.uniform(58, 62))
                        return True
                except:
                    return True

            return False

        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → Playwright 설정 → batch_id 생성 → 로그 정리"""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 2. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 3. URL 템플릿 로드
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            print(f"[ERROR] Initialize failed: URL template load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 4. Playwright 설정
        if not self.setup_playwright():
            print("[ERROR] Initialize failed: Playwright setup failed")
            return False

        # 5. batch_id 생성
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)

        # 6. calendar_week 생성 및 로그 정리
        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, calendar_week={self.calendar_week}")
        return True

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → CAPTCHA 처리 → HTML 파싱 → 제품 데이터 추출"""
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(random.uniform(3, 5))

            if page_number == 1:
                self.handle_captcha()
                time.sleep(random.uniform(3, 5))

            page_html = self.page.content()
            tree = html.fromstring(page_html)

            base_containers = tree.xpath(base_container_xpath)

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    self.current_rank += 1

                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.walmart.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    final_price_xpath = self.xpaths.get('final_sku_price', {}).get('xpath')
                    final_price_raw = item.xpath(final_price_xpath) if final_price_xpath else None
                    final_sku_price = self.format_walmart_price(final_price_raw)

                    membership_discounts_raw = self.safe_extract(item, 'retailer_membership_discounts')
                    retailer_membership_discounts = f"{membership_discounts_raw} W+" if membership_discounts_raw else None

                    sku_status_1 = self.safe_extract(item, 'sku_status_1')
                    sku_status_2 = self.safe_extract(item, 'sku_status_2')
                    sku_status_parts = [s for s in [sku_status_1, sku_status_2] if s]
                    sku_status = ', '.join(sku_status_parts) if sku_status_parts else None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'final_sku_price': final_sku_price,
                        'original_sku_price': self.safe_extract(item, 'original_sku_price'),
                        'offer': self.safe_extract(item, 'offer'),
                        'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                        'shipping_availability': self.safe_extract(item, 'shipping_availability'),
                        'delivery_availability': self.safe_extract(item, 'delivery_availability'),
                        'sku_status': sku_status,
                        'retailer_membership_discounts': retailer_membership_discounts,
                        'available_quantity_for_purchase': self.safe_extract(item, 'available_quantity_for_purchase'),
                        'inventory_status': self.safe_extract(item, 'inventory_status'),
                        'main_rank': self.current_rank,
                        'page_number': page_number,
                        'product_url': product_url,
                        'calendar_week': self.calendar_week,
                        'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    products.append(product_data)

                except Exception as e:
                    print(f"[ERROR] Product {idx} extract failed: {e}")
                    traceback.print_exc()
                    continue

            print(f"[INFO] Page {page_number}: {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: BATCH_SIZE 배치 → RETRY_SIZE 배치 → 1개씩 (3-tier retry)"""
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
                    main_rank, main_page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            BATCH_SIZE = 20
            RETRY_SIZE = 5
            total_saved = 0

            def product_to_tuple(product):
                return (
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
                    product['page_number'],
                    product['product_url'],
                    product['calendar_week'],
                    product['crawl_strdatetime'],
                    product['batch_id']
                )

            def save_batch(batch_products):
                values_list = [product_to_tuple(p) for p in batch_products]
                cursor.executemany(insert_query, values_list)
                self.db_conn.commit()
                return len(batch_products)

            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    total_saved += save_batch(batch_products)

                except Exception:
                    self.db_conn.rollback()

                    for sub_start in range(0, len(batch_products), RETRY_SIZE):
                        sub_end = min(sub_start + RETRY_SIZE, len(batch_products))
                        sub_batch = batch_products[sub_start:sub_end]

                        try:
                            total_saved += save_batch(sub_batch)

                        except Exception:
                            self.db_conn.rollback()

                            for single_product in sub_batch:
                                try:
                                    cursor.execute(insert_query, product_to_tuple(single_product))
                                    self.db_conn.commit()
                                    total_saved += 1
                                except Exception as single_error:
                                    print(f"[ERROR] DB save failed: {(single_product.get('retailer_sku_name') or 'N/A')[:30]}: {single_error}")
                                    query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                                    print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                                    traceback.print_exc()
                                    self.db_conn.rollback()

            cursor.close()
            return total_saved

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return 0

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_products = 0
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            page_num = 1

            while total_products < target_products:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    remaining = target_products - total_products
                    products_to_save = products[:remaining]
                    saved_count = self.save_products(products_to_save)
                    total_products += saved_count

                    if total_products >= target_products:
                        break

                time.sleep(random.uniform(5, 10))
                page_num += 1

            print(f"[DONE] Page: {page_num}, Saved: {total_products}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            if self.db_conn:
                self.db_conn.close()


def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = WalmartMainCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
