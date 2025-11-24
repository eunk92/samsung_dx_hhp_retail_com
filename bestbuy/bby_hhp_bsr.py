"""
BestBuy BSR 페이지 크롤러
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- BSR 페이지에서 제품 리스트 수집 (bsr_rank 포함)
- 테스트 모드: 1페이지에서 2개 제품만 수집
- 운영 모드: 2페이지 크롤링
"""

import sys
import os
import time
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class BestBuyBSRCrawler(BaseCrawler):
    """
    BestBuy BSR 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화
        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
            batch_id (str): 배치 ID (기본값: None)
        """
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0  # 추출 순서대로 rank 부여

        # 테스트 설정
        self.test_page = 1
        self.test_count = 1

        # 운영 모드 설정
        self.max_products = 100  # 운영 모드 목표: 100개 제품 수집

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] BestBuy BSR Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (1 product from page 1)' if self.test_mode else 'OFF (2 pages)'}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. XPath 셀렉터 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        # 3. URL 템플릿 로드
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            return False

        # 4. WebDriver 설정
        self.setup_driver()

        # 5. 배치 ID 및 캘린더 주차 생성
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)
            print(f"[INFO] Batch ID generated: {self.batch_id}")
        else:
            print(f"[INFO] Batch ID received: {self.batch_id}")

        self.calendar_week = self.generate_calendar_week()
        print(f"[INFO] Calendar Week: {self.calendar_week}")

        # 7. 오래된 로그 정리
        self.cleanup_old_logs()

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
            self.driver.get(url)
            time.sleep(30)

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # 제품 리스트 XPath
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            # 제품 아이템 추출
            base_containers = tree.xpath(base_container_xpath)
            print(f"[INFO] Found {len(base_containers)} products on page {page_number}")

            # 테스트 모드일 때는 설정된 범위만, 운영 모드일 때는 전체 처리
            if self.test_mode:
                containers_to_process = base_containers[:self.test_count]
            else:
                containers_to_process = base_containers

            products = []
            for item in containers_to_process:
                try:
                    # product_url 추출 및 절대 경로 변환
                    product_url_raw = self.extract_with_fallback(item, self.xpaths.get('product_url', {}).get('xpath'))
                    product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    # 추출 순서대로 rank 증가
                    self.current_rank += 1

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.extract_with_fallback(item, self.xpaths.get('retailer_sku_name', {}).get('xpath')),
                        'final_sku_price': self.extract_with_fallback(item, self.xpaths.get('final_sku_price', {}).get('xpath')),
                        'savings': self.extract_with_fallback(item, self.xpaths.get('savings', {}).get('xpath')),
                        'comparable_pricing': self.extract_with_fallback(item, self.xpaths.get('comparable_pricing', {}).get('xpath')),
                        'offer': self.extract_with_fallback(item, self.xpaths.get('offer', {}).get('xpath')),
                        'pick_up_availability': self.extract_with_fallback(item, self.xpaths.get('pick_up_availability', {}).get('xpath')),
                        'shipping_availability': self.extract_with_fallback(item, self.xpaths.get('shipping_availability', {}).get('xpath')),
                        'delivery_availability': self.extract_with_fallback(item, self.xpaths.get('delivery_availability', {}).get('xpath')),
                        'sku_status': self.extract_with_fallback(item, self.xpaths.get('sku_status', {}).get('xpath')),
                        'promotion_type': self.extract_with_fallback(item, self.xpaths.get('promotion_type', {}).get('xpath')),
                        'bsr_rank': self.current_rank,
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
            return []

    def save_products(self, products):
        """
        수집된 제품 데이터를 bby_hhp_product_list 테이블에 저장
        - 중복 확인: batch_id + product_url 조합으로 체크
        - 존재하면 UPDATE (bsr_rank만), 없으면 INSERT (BSR 수집 필드)
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
                UPDATE bby_hhp_product_list
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
                    INSERT INTO bby_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, savings, comparable_pricing,
                        offer, pick_up_availability, shipping_availability, delivery_availability,
                        sku_status, promotion_type, bsr_rank, product_url,
                        calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                                product['savings'],
                                product['comparable_pricing'],
                                product['offer'],
                                product['pick_up_availability'],
                                product['shipping_availability'],
                                product['delivery_availability'],
                                product['sku_status'],
                                product['promotion_type'],
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
            print("[INFO] Starting BestBuy BSR page crawling...")
            print("="*60 + "\n")

            total_products = 0

            if self.test_mode:
                # 테스트 모드: 설정된 페이지만 크롤링 및 DB 저장
                products = self.crawl_page(self.test_page)
                saved_count = self.save_products(products)
                total_products += saved_count
            else:
                # 운영 모드: 100개 제품이 수집될 때까지 계속 크롤링
                page_num = 1

                while total_products < self.max_products:
                    products = self.crawl_page(page_num)

                    if not products:
                        print(f"[WARNING] No products found at page {page_num}")
                        # 빈 페이지면 종료
                        if page_num > 1:
                            print(f"[INFO] No more products available, stopping...")
                            break
                    else:
                        # 100개 초과 방지: 남은 개수만큼만 저장
                        remaining = self.max_products - total_products
                        products_to_save = products[:remaining]

                        saved_count = self.save_products(products_to_save)
                        total_products += saved_count

                        print(f"[INFO] Progress: {total_products}/{self.max_products} products collected")

                        # 100개 도달 확인
                        if total_products >= self.max_products:
                            print(f"[INFO] Reached target product count ({self.max_products}), stopping...")
                            break

                    # 페이지 간 대기
                    time.sleep(30)
                    page_num += 1

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] BestBuy BSR Crawler Finished")
            print(f"[RESULT] Total products collected: {total_products}")
            print(f"[RESULT] Batch ID: {self.batch_id}")
            print("="*60 + "\n")

            return True

        except Exception as e:
            print(f"[ERROR] Crawler execution failed: {e}")
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
    개별 실행 시 진입점 (테스트 모드 ON)
    """
    crawler = BestBuyBSRCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy BSR Crawler completed successfully")
    else:
        print("\n[FAILED] BestBuy BSR Crawler failed")


if __name__ == '__main__':
    main()