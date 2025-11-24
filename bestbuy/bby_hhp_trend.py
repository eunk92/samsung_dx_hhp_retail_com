"""
BestBuy Trending Deals 페이지 크롤러
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- Trending Deals 페이지에서 제품 리스트 수집 (trend_rank 포함)
- 테스트 모드: 2개 제품만 수집
- 운영 모드: 단일 페이지 전체 크롤링
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


class BestBuyTrendCrawler(BaseCrawler):
    """
    BestBuy Trending Deals 페이지 크롤러
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
        self.page_type = 'trend'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0  # 추출 순서대로 rank 부여

        # 테스트 설정
        self.test_page = 1
        self.test_count = 2

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] BestBuy Trending Deals Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (2 products)' if self.test_mode else 'OFF (all products)'}")
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

    def crawl_page(self):
        """
        Trending Deals 페이지 크롤링 (단일 페이지)

        Returns: list: 수집된 제품 데이터 리스트
        """
        try:
            # URL 로드 (고정 URL, 페이지 번호 없음)
            url = self.url_template
            print(f"\n[INFO] Crawling Trending Deals page: {url}")

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
            print(f"[INFO] Found {len(base_containers)} trending products")

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
                        'trend_rank': self.current_rank,
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
            print(f"[ERROR] Failed to crawl Trending Deals page: {e}")
            return []

    def save_products(self, products):
        """
        수집된 제품 데이터를 bby_hhp_product_list 테이블에 저장
        - Trending Deals는 매번 새로운 데이터로 INSERT (중복 확인 없음)
        - INSERT는 20개씩 배치 처리 (부분 실패 방지)

        Args: products (list): 제품 데이터 리스트
        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0

            # INSERT 쿼리
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

            # 20개씩 배치 처리
            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

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
                            product['trend_rank'],
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
            print(f"[SUCCESS] Saved {insert_count} products to database")
            for i, product in enumerate(products[:3], 1):
                sku_name = product['retailer_sku_name'] or 'N/A'
                print(f"[{i}] {sku_name[:50]}... - trend_rank: {product['trend_rank']}")
            if len(products) > 3:
                print(f"... and {len(products) - 3} more products")

            return insert_count

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
            print("[INFO] Starting BestBuy Trending Deals crawling...")
            print("="*60 + "\n")

            # Trending Deals 페이지 크롤링 (단일 페이지)
            products = self.crawl_page()
            saved_count = self.save_products(products)
            total_products = saved_count

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] BestBuy Trending Deals Crawler Finished")
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
    crawler = BestBuyTrendCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy Trending Deals Crawler completed successfully")
    else:
        print("\n[FAILED] BestBuy Trending Deals Crawler failed")


if __name__ == '__main__':
    main()