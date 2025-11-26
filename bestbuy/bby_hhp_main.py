"""
BestBuy Main 페이지 크롤러
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- Main 페이지에서 제품 리스트 수집 (main_rank 자동 계산)
- main_rank는 페이지 관계없이 1부터 순차 증가
- 운영 모드는 최대 400개 제품까지만 수집
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
        self.account_name = 'Bestbuy'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None

        self.test_count = 3  # 테스트 모드 목표: 3개 제품 수집
        self.max_products = 300  # 운영 모드 목표: 300개 제품 수집
        self.current_rank = 0

    def initialize(self):
        """
        크롤러 초기화 작업
        - DB 연결
        - XPath 셀렉터 로드
        - URL 템플릿 로드
        - WebDriver 설정
        - 배치 ID 생성
        - 로그 정리

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] BestBuy Main Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (1 product from page 1)' if self.test_mode else 'OFF (max 400 products, up to 19 pages)'}")
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

    def scroll_to_bottom(self):
        """
        페이지네이션 버튼이 나타날 때까지 스크롤
        - 300px씩 점진적으로 스크롤
        - 페이지네이션이 보이면 스크롤 종료
        """
        try:
            scroll_step = 300  # 300px씩 스크롤
            current_position = 0
            max_scroll_attempts = 100  # 무한 루프 방지

            # 페이지네이션 XPath (DB에서 로드)
            pagination_xpath = self.xpaths.get('pagination', {}).get('xpath')

            for _ in range(max_scroll_attempts):
                # 페이지네이션이 보이는지 확인
                pagination_elements = self.driver.find_elements(By.XPATH, pagination_xpath)
                if pagination_elements:
                    print(f"[INFO] Pagination found, stopping scroll")
                    break

                # 300px 아래로 스크롤
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")

                # 콘텐츠 로드 대기
                time.sleep(3)

                # 페이지 끝에 도달했는지 확인
                total_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_position >= total_height:
                    print(f"[INFO] Reached page bottom")
                    break

            # 최종 로드 대기
            time.sleep(2)
            print(f"[INFO] Scroll completed, page fully loaded")

        except Exception as e:
            print(f"[WARNING] Scroll failed: {e}")

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

            # 제품 리스트 XPath
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            # 1. 페이지 로드 -> 스크롤 -> 30초 대기
            self.driver.get(url)
            time.sleep(10)  # 초기 로드 대기

            print(f"[INFO] Scrolling to load all products...")
            self.scroll_to_bottom()
            time.sleep(30)  # 스크롤 후 30초 대기

            # 제품 추출 (최대 3번 시도, 리로드 없이 HTML 재파싱)
            base_containers = []
            max_retries = 3
            expected_products = 24  # 1페이지 기준 24개 제품

            for attempt in range(1, max_retries + 1):
                # HTML 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

                # 제품 아이템 추출
                base_containers = tree.xpath(base_container_xpath)
                print(f"[INFO] Found {len(base_containers)} products on page {page_number} (attempt {attempt}/{max_retries})")

                # 24개 이상 로드되었으면 성공
                if len(base_containers) >= expected_products:
                    print(f"[INFO] All products loaded successfully")
                    break

                # 24개 미만이면 대기 후 재파싱
                if attempt < max_retries:
                    print(f"[WARNING] Only {len(base_containers)} products found, waiting 10s and retrying...")
                    time.sleep(10)  # 10초 대기 후 재파싱
                else:
                    print(f"[WARNING] Could not load {expected_products} products after {max_retries} attempts, proceeding with {len(base_containers)} products")

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    # main_rank 계산 (페이지별 연속 증가)
                    self.current_rank += 1

                    # product_url 추출 및 절대 경로 변환
                    product_url_raw = self.extract_with_fallback(item, self.xpaths.get('product_url', {}).get('xpath'))
                    product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

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
                        'main_rank': self.current_rank,
                        'page_number': page_number,
                        'product_url': product_url,
                        'calendar_week': self.calendar_week,
                        'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    products.append(product_data)

                except Exception as e:
                    print(f"[WARNING] Failed to extract product {idx}/{len(base_containers)}, skipping: {e}")
                    continue

            return products

        except Exception as e:
            print(f"[ERROR] Failed to crawl page {page_number}: {e}")
            return []

    def save_products(self, products):
        """
        수집된 제품 데이터를 bby_hhp_product_list 테이블에 저장
        - 50개씩 배치로 나눠서 저장 (부분 실패 방지)

        Args: products (list): 제품 데이터 리스트
        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()

            insert_query = """
                INSERT INTO bby_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    final_sku_price, savings, comparable_pricing,
                    offer, pick_up_availability, shipping_availability, delivery_availability,
                    sku_status, promotion_type, main_rank, page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            # 배치 크기 설정 (50개씩 나눠서 저장)
            BATCH_SIZE = 10
            total_saved = 0

            # 50개씩 쪼개서 저장
            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    # 현재 배치(50개)를 튜플 리스트로 변환
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
                            product['main_rank'],
                            product['page_number'],
                            product['product_url'],
                            product['calendar_week'],
                            product['crawl_strdatetime'],
                            product['batch_id']
                        )
                        values_list.append(one_product)

                    # 50개 배치 INSERT
                    cursor.executemany(insert_query, values_list)

                    # 50개 저장할 때마다 즉시 COMMIT
                    self.db_conn.commit()

                    total_saved += len(batch_products)
                    print(f"[INFO] Saved batch {batch_start+1}-{batch_end} ({len(batch_products)} products)")

                except Exception as batch_error:
                    # 현재 배치만 실패, 다음 배치는 계속 진행
                    print(f"[ERROR] Failed to save batch {batch_start+1}-{batch_end}: {batch_error}")
                    self.db_conn.rollback()
                    continue

            cursor.close()

            # 진행 상황 요약 출력
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
            print("[INFO] Starting BestBuy Main page crawling...")
            print("="*60 + "\n")

            total_products = 0

            # 목표 제품 수 설정 (테스트/운영 모드에 따라)
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            page_num = 1

            while total_products < target_products:
                products = self.crawl_page(page_num)

                if not products:
                    print(f"[WARNING] No products found at page {page_num}")
                    # 연속 2페이지 빈 페이지면 종료
                    if page_num > 1:
                        print(f"[INFO] No more products available, stopping...")
                        break
                else:
                    # 목표 초과 방지: 남은 개수만큼만 저장
                    remaining = target_products - total_products
                    products_to_save = products[:remaining]

                    saved_count = self.save_products(products_to_save)
                    total_products += saved_count

                    print(f"[INFO] Progress: {total_products}/{target_products} products collected")

                    # 목표 도달 확인
                    if total_products >= target_products:
                        print(f"[INFO] Reached target product count ({target_products}), stopping...")
                        break

                # 페이지 간 대기
                time.sleep(30)
                page_num += 1

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] BestBuy Main Crawler Finished")
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
    crawler = BestBuyMainCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy Main Crawler completed successfully")
    else:
        print("\n[FAILED] BestBuy Main Crawler failed")


if __name__ == '__main__':
    main()