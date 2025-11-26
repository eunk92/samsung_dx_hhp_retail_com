"""
Amazon Main 페이지 크롤러
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- Main 페이지에서 제품 리스트 수집 (main_rank 자동 계산)
- main_rank는 페이지 관계없이 1부터 순차 증가 (1페이지: 1~22, 2페이지: 23~44...)
- 운영 모드는 최대 400개 제품까지만 수집
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


class AmazonMainCrawler(BaseCrawler):
    """
    Amazon Main 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
                             - True: 테스트 설정에 따라 크롤링
                             - False: 전체 페이지 크롤링
            batch_id (str): 배치 ID (기본값: None)
                           - None: 자동 생성
                           - 문자열: 통합 크롤러에서 전달된 배치 ID 사용
        """
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Amazon'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.cookies_loaded = False  # 쿠키 로드 여부 플래그

        # 테스트 설정
        self.test_count = 5         # 테스트 모드에서 수집할 제품 개수

        # 운영 모드 설정
        self.max_products = 300     # 운영 모드 최대 제품 수
        self.current_rank = 0       # 현재 main_rank 추적용 (페이지 관계없이 연속 증가)

    def initialize(self):
        """
        크롤러 초기화 작업
        - DB 연결
        - XPath 셀렉터 로드
        - URL 템플릿 로드
        - WebDriver 설정
        - 배치 ID 생성
        - 로그 정리

        Returns(bool): 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] Amazon Main Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (' + str(self.test_count) + ' products)' if self.test_mode else 'OFF (max ' + str(self.max_products) + ' products)'}")
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

        # 5. 쿠키 로드 (일관된 세션 유지)
        self.cookies_loaded = self.load_cookies(self.account_name)

        # 6. 배치 ID 및 캘린더 주차 생성
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
            time.sleep(30)  # 페이지 로딩 30s 대기

            # 첫 페이지 로드 후 쿠키 저장 (세션 고정)
            if not self.cookies_loaded and page_number == 1:
                self.save_cookies(self.account_name)
                self.cookies_loaded = True

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

            # 전체 제품 처리 (개수 제한은 run()에서 관리)
            containers_to_process = base_containers

            products = []
            for idx, item in enumerate(containers_to_process, 1):
                try:
                    # main_rank 계산 (페이지별 연속 증가)
                    self.current_rank += 1

                    # 각 필드 개별 추출 (예외 처리)
                    # product_url 추출 및 절대 경로 변환
                    try:
                        product_url_raw = self.extract_with_fallback(item, self.xpaths.get('product_url', {}).get('xpath'))
                        product_url = f"https://www.amazon.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw
                    except Exception as e:
                        print(f"[WARNING] Failed to extract product_url: {e}")
                        product_url = None

                    try:
                        retailer_sku_name = self.extract_with_fallback(item, self.xpaths.get('retailer_sku_name', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract retailer_sku_name: {e}")
                        retailer_sku_name = None

                    try:
                        number_of_units_purchased_past_month = self.extract_with_fallback(item, self.xpaths.get('number_of_units_purchased_past_month', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract number_of_units_purchased_past_month: {e}")
                        number_of_units_purchased_past_month = None

                    try:
                        final_sku_price = self.extract_with_fallback(item, self.xpaths.get('final_sku_price', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract final_sku_price: {e}")
                        final_sku_price = None

                    try:
                        original_sku_price = self.extract_with_fallback(item, self.xpaths.get('original_sku_price', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract original_sku_price: {e}")
                        original_sku_price = None

                    try:
                        shipping_info = self.extract_with_fallback(item, self.xpaths.get('shipping_info', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract shipping_info: {e}")
                        shipping_info = None

                    try:
                        available_quantity_for_purchase = self.extract_with_fallback(item, self.xpaths.get('available_quantity_for_purchase', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract available_quantity_for_purchase: {e}")
                        available_quantity_for_purchase = None

                    try:
                        discount_type = self.extract_with_fallback(item, self.xpaths.get('discount_type', {}).get('xpath'))
                    except Exception as e:
                        print(f"[WARNING] Failed to extract discount_type: {e}")
                        discount_type = None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': retailer_sku_name,
                        'number_of_units_purchased_past_month': number_of_units_purchased_past_month,
                        'final_sku_price': final_sku_price,
                        'original_sku_price': original_sku_price,
                        'shipping_info': shipping_info,
                        'available_quantity_for_purchase': available_quantity_for_purchase,
                        'discount_type': discount_type,
                        'main_rank': self.current_rank,  # 1부터 시작하여 페이지 관계없이 순차 증가
                        'product_url': product_url,
                        'calendar_week': self.calendar_week,
                        'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    products.append(product_data)

                except Exception as e:
                    print(f"[WARNING] Failed to extract product {idx}/{len(containers_to_process)}, skipping: {e}")
                    continue

            return products

        except Exception as e:
            print(f"[ERROR] Failed to crawl page {page_number}: {e}")
            return []

    def save_products(self, products):
        """
        수집된 제품 데이터를 amazon_hhp_product_list 테이블에 저장
        - 50개씩 배치로 나눠서 저장 (부분 실패 방지)

        Args: products (list): 제품 데이터 리스트
        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()

            insert_query = """
                INSERT INTO amazon_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    number_of_units_purchased_past_month, final_sku_price, original_sku_price,
                    shipping_info, available_quantity_for_purchase, discount_type,
                    main_rank, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            # 배치 크기 설정 (20개씩 나눠서 저장)
            BATCH_SIZE = 20
            total_saved = 0

            # 20개씩 쪼개서 저장 (실패 시 5개 → 1개씩 재시도)
            RETRY_SIZE = 5  # 2차 재시도 배치 크기

            def product_to_tuple(product):
                """제품 데이터를 INSERT용 튜플로 변환"""
                return (
                    product['account_name'],
                    product['page_type'],
                    product['retailer_sku_name'],
                    product['number_of_units_purchased_past_month'],
                    product['final_sku_price'],
                    product['original_sku_price'],
                    product['shipping_info'],
                    product['available_quantity_for_purchase'],
                    product['discount_type'],
                    product['main_rank'],
                    product['product_url'],
                    product['calendar_week'],
                    product['crawl_strdatetime'],
                    product['batch_id']
                )

            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    # 1차: 20개 배치 INSERT 시도
                    values_list = [product_to_tuple(p) for p in batch_products]
                    cursor.executemany(insert_query, values_list)
                    self.db_conn.commit()
                    total_saved += len(batch_products)
                    print(f"[INFO] Saved batch {batch_start+1}-{batch_end} ({len(batch_products)} products)")

                except Exception as batch_error:
                    # 1차 실패 → 2차: 5개씩 재시도
                    print(f"[WARNING] Batch {batch_start+1}-{batch_end} failed, retrying with {RETRY_SIZE}...")
                    self.db_conn.rollback()

                    for retry_start in range(0, len(batch_products), RETRY_SIZE):
                        retry_end = min(retry_start + RETRY_SIZE, len(batch_products))
                        retry_products = batch_products[retry_start:retry_end]

                        try:
                            retry_values = [product_to_tuple(p) for p in retry_products]
                            cursor.executemany(insert_query, retry_values)
                            self.db_conn.commit()
                            total_saved += len(retry_products)
                            print(f"[INFO] Retry saved {retry_start+1}-{retry_end} ({len(retry_products)} products)")

                        except Exception as retry_error:
                            # 2차 실패 → 3차: 1개씩 재시도
                            print(f"[WARNING] Retry batch {retry_start+1}-{retry_end} failed, trying one by one...")
                            self.db_conn.rollback()

                            for single_product in retry_products:
                                try:
                                    cursor.execute(insert_query, product_to_tuple(single_product))
                                    self.db_conn.commit()
                                    total_saved += 1
                                except Exception as single_error:
                                    print(f"[ERROR] Failed to save product: {single_error}")
                                    self.db_conn.rollback()
                                    continue  # 실패한 1개만 스킵

            cursor.close()

            # 진행 상황 요약 출력 (테스트/운영 모드 동일)
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
            print("[INFO] Starting Amazon Main page crawling...")
            print("="*60 + "\n")

            # 목표 제품 수 설정 (테스트 모드: test_count, 운영 모드: max_products)
            target_products = self.test_count if self.test_mode else self.max_products
            total_products = 0
            self.current_rank = 0  # 순위 초기화
            page_num = 1

            while total_products < target_products:
                print(f"[INFO] Current progress: {total_products}/{target_products} products collected")

                products = self.crawl_page(page_num)

                if not products:
                    print(f"[WARNING] No products found at page {page_num}, stopping crawler...")
                    break

                # 목표 초과 방지: 남은 개수만큼만 저장
                remaining = target_products - total_products
                products_to_save = products[:remaining]

                saved_count = self.save_products(products_to_save)
                total_products += saved_count

                # 목표 달성 확인
                if total_products >= target_products:
                    print(f"[INFO] Target reached: {total_products} products collected")
                    break

                # 다음 페이지로 이동
                page_num += 1

                # 페이지 간 대기
                time.sleep(30)

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Amazon Main Crawler Finished")
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
    crawler = AmazonMainCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Amazon Main Crawler completed successfully")
    else:
        print("\n[FAILED] Amazon Main Crawler failed")


if __name__ == '__main__':
    main()