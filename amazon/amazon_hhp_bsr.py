"""
Amazon BSR 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- BSR 페이지에서 제품 리스트 수집 (bsr_rank 포함)
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집

================================================================================
저장 테이블
================================================================================
- amazon_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import traceback
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class AmazonBSRCrawler(BaseCrawler):
    """
    Amazon BSR 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Amazon'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.cookies_loaded = False

        self.test_count = 5  # 테스트 모드
        self.max_products = 100  # 운영 모드

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 로그 정리"""
        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            return False
        self.setup_driver_stealth(self.account_name)  # Amazon만 강화된 봇 감지 회피 적용
        # BSR 크롤러는 쿠키 로드 안함 (순위 변동 방지)

        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)

        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        return True

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → HTML 파싱 → 제품 데이터 추출"""
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(30)

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)
            base_containers = tree.xpath(base_container_xpath)

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.amazon.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    # bsr_rank 추출 및 후처리 (# 및 쉼표 제거)
                    bsr_rank_raw = self.safe_extract(item, 'bsr_rank')
                    bsr_rank = bsr_rank_raw.replace('#', '').replace(',', '').strip() if bsr_rank_raw else None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'final_sku_price': self.safe_extract(item, 'final_sku_price'),
                        'bsr_rank': bsr_rank,
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
                UPDATE amazon_hhp_product_list
                SET bsr_rank = %s, bsr_page_number = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for product in products_to_update:
                try:
                    cursor.execute(update_query, (
                        product['bsr_rank'],
                        product['page_number'],
                        self.account_name,
                        product['batch_id'],
                        product['product_url']
                    ))
                    self.db_conn.commit()
                    update_count += 1
                except Exception:
                    self.db_conn.rollback()

            # INSERT 처리 (3-tier retry)
            if products_to_insert:
                insert_query = """
                    INSERT INTO amazon_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, bsr_rank, bsr_page_number, product_url,
                        calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                        product['bsr_rank'],
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
                                        traceback.print_exc()
                                        self.db_conn.rollback()

            cursor.close()
            return {'insert': insert_count, 'update': update_count}

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return {'insert': 0, 'update': 0}

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_insert = 0
            total_update = 0
            target_products = self.test_count if self.test_mode else self.max_products
            page_num = 1

            while (total_insert + total_update) < target_products:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    remaining = target_products - (total_insert + total_update)
                    products_to_save = products[:remaining]
                    result = self.save_products(products_to_save)
                    total_insert += result['insert']
                    total_update += result['update']

                    if (total_insert + total_update) >= target_products:
                        break

                time.sleep(30)
                page_num += 1

            print(f"[DONE] Page: {page_num}, Update: {total_update}, Insert: {total_insert}, batch_id: {self.batch_id}")
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
    crawler = AmazonBSRCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
