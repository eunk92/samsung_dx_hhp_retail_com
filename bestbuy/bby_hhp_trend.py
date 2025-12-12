"""
BestBuy Trend 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Trend 페이지에서 제품 리스트 수집 (trend_rank 포함)
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: 단일 페이지 전체 크롤링

================================================================================
저장 테이블
================================================================================
- bby_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import random
import traceback
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class BestBuyTrendCrawler(BaseCrawler):
    """
    BestBuy Trend 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'trend'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0

        self.test_count = 1  # 테스트 모드
        self.excluded_keywords = [
            'Screen Magnifier', 'mount', 'holder', 'cable', 'adapter', 'stand', 'wallet'
        ]  # 제외할 키워드 리스트 (retailer_sku_name에 포함 시 수집 제외)

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 로그 정리"""
        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            return False
        self.setup_driver()

        # batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True)

        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        return True

    def crawl_page(self):
        """페이지 크롤링: 페이지 로드 → HTML 파싱(최대 3회) → 제품 데이터 추출"""
        try:
            url = self.url_template

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(random.uniform(25, 35))

            base_containers = []
            expected_products = 10

            for attempt in range(1, 4):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                if len(base_containers) >= expected_products:
                    break

                if attempt < 3:
                    time.sleep(random.uniform(8, 12))

            target_products = self.test_count if self.test_mode else len(base_containers)
            containers_to_process = base_containers[:target_products]

            products = []
            for idx, item in enumerate(containers_to_process, 1):
                try:
                    # 제외 키워드 필터링 (먼저 수행)
                    retailer_sku_name = self.safe_extract(item, 'retailer_sku_name') or ''
                    if self.excluded_keywords and any(keyword.lower() in retailer_sku_name.lower() for keyword in self.excluded_keywords):
                        print(f"[SKIP] 제외 키워드 포함: {retailer_sku_name[:40]}...")
                        continue

                    self.current_rank += 1

                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    # savings 추출 후 "Save " 제거
                    savings_raw = self.safe_extract(item, 'savings')
                    savings = savings_raw.replace('Save ', '') if savings_raw else None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': retailer_sku_name,
                        'final_sku_price': self.safe_extract(item, 'final_sku_price'),
                        'savings': savings,
                        'comparable_pricing': self.safe_extract(item, 'comparable_pricing'),
                        'trend_rank': self.current_rank,
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

            print(f"[INFO] Trend page: {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Trend page failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: 중복 확인 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
        if not products:
            return {'insert': 0, 'update': 0}

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0

            products_to_update = []
            products_to_insert = []

            for product in products:
                exists = self.check_product_exists(
                    self.account_name,
                    product['batch_id'],
                    product['product_url']
                )
                if exists:
                    products_to_update.append(product)
                else:
                    products_to_insert.append(product)

            # UPDATE 처리
            update_query = """
                UPDATE bby_hhp_product_list
                SET trend_rank = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for product in products_to_update:
                try:
                    cursor.execute(update_query, (
                        product['trend_rank'],
                        self.account_name,
                        product['batch_id'],
                        product['product_url']
                    ))
                    self.db_conn.commit()
                    update_count += 1
                except Exception:
                    self.db_conn.rollback()

            # INSERT 처리 (3-tier retry: BATCH_SIZE → RETRY_SIZE → 1개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO bby_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, savings, comparable_pricing, trend_rank,
                        product_url, calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

                BATCH_SIZE = 20
                RETRY_SIZE = 5

                def product_to_tuple(product):
                    return (
                        product['account_name'],
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['final_sku_price'],
                        product['savings'],
                        product['comparable_pricing'],
                        product['trend_rank'],
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

                for batch_start in range(0, len(products_to_insert), BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, len(products_to_insert))
                    batch_products = products_to_insert[batch_start:batch_end]

                    try:
                        insert_count += save_batch(batch_products)

                    except Exception:
                        self.db_conn.rollback()

                        for sub_start in range(0, len(batch_products), RETRY_SIZE):
                            sub_end = min(sub_start + RETRY_SIZE, len(batch_products))
                            sub_batch = batch_products[sub_start:sub_end]

                            try:
                                insert_count += save_batch(sub_batch)

                            except Exception:
                                self.db_conn.rollback()

                                for single_product in sub_batch:
                                    try:
                                        cursor.execute(insert_query, product_to_tuple(single_product))
                                        self.db_conn.commit()
                                        insert_count += 1
                                    except Exception as single_error:
                                        print(f"[ERROR] DB save failed: {(single_product.get('retailer_sku_name') or 'N/A')[:30]}: {single_error}")
                                        query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                                        print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                                        traceback.print_exc()
                                        self.db_conn.rollback()

            cursor.close()
            return {'insert': insert_count, 'update': update_count}

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return {'insert': 0, 'update': 0}

    def run(self):
        """실행: initialize() → crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            self.current_rank = 0
            products = self.crawl_page()

            if not products:
                print("[ERROR] No products found")
                return False

            result = self.save_products(products)

            print(f"[DONE] Update: {result['update']}, Insert: {result['insert']}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = BestBuyTrendCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
