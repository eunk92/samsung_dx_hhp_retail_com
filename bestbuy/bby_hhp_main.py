"""
BestBuy Main 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Main 페이지에서 제품 리스트 수집
- main_rank는 페이지 관계없이 1부터 순차 증가
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집

================================================================================
저장 테이블
================================================================================
- bby_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import traceback
from datetime import datetime
from lxml import html
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class BestBuyMainCrawler(BaseCrawler):
    """
    BestBuy Main 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None

        self.test_count = 3  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.current_rank = 0

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 1개월 전 로그 정리"""
        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            return False
        self.setup_driver()

        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)

        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        return True

    def scroll_to_bottom(self):
        """스크롤: 300px씩 점진적 스크롤 → 페이지네이션 보이면 종료"""
        try:
            scroll_step = 300
            current_position = 0

            for _ in range(50):
                is_pagination_visible = self.driver.execute_script("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """)

                if is_pagination_visible:
                    break

                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(3)

                total_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 페이지네이션까지 스크롤 → HTML 파싱(최대 3회) → 제품 데이터 추출"""
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(10)

            self.scroll_to_bottom()
            time.sleep(30)

            base_containers = []
            expected_products = 24

            for attempt in range(1, 4):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                if len(base_containers) >= expected_products:
                    break

                if attempt < 3:
                    time.sleep(10)

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    self.current_rank += 1

                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'final_sku_price': self.safe_extract(item, 'final_sku_price'),
                        'savings': self.safe_extract(item, 'savings'),
                        'comparable_pricing': self.safe_extract(item, 'comparable_pricing'),
                        'offer': self.safe_extract(item, 'offer'),
                        'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                        'shipping_availability': self.safe_extract(item, 'shipping_availability'),
                        'delivery_availability': self.safe_extract(item, 'delivery_availability'),
                        'sku_status': self.safe_extract(item, 'sku_status'),
                        'promotion_type': self.safe_extract(item, 'promotion_type'),
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
                    continue

            print(f"[INFO] Page {page_number}: {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            return []

    def save_products(self, products):
        """DB 저장: BATCH_SIZE 배치 → RETRY_SIZE 배치 → 1개씩 (3-tier retry)"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_query = """
                INSERT INTO bby_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    final_sku_price, savings, comparable_pricing,
                    offer, pick_up_availability, shipping_availability, delivery_availability,
                    sku_status, promotion_type, main_rank, main_page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                    product['savings'],
                    product['comparable_pricing'],
                    product['offer'],
                    product['pick_up_availability'],
                    product['shipping_availability'],
                    product['delivery_availability'],
                    product['sku_status'],
                    product['promotion_type'],
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
                                    print(f"[ERROR] DB save failed: {single_product.get('retailer_sku_name', 'N/A')[:30]}: {single_error}")
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

                time.sleep(30)
                page_num += 1

            print(f"[DONE] Page: {page_num}, Saved: {total_products}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            return False

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = BestBuyMainCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()