"""
Amazon Main 페이지 크롤러

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
- 쿠키로드 안함

================================================================================
저장 테이블
================================================================================
- amazon_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import random
import re
import traceback
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from common.base_crawler import BaseCrawler


class AmazonMainCrawler(BaseCrawler):
    """
    Amazon Main 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Amazon'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.cookies_loaded = False
        self.current_rank = 0
        self.standalone = batch_id is None
        self.test_count = 1  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.max_pages = 20  # 최대 페이지 수
        self.saved_urls = set()  # 중복 URL 추적용
        self.excluded_keywords = [
            'Screen Magnifier', 'mount', 'holder', 'cable', 'adapter', 'stand', 'wallet'
        ]  # 제외할 키워드 리스트 (retailer_sku_name에 포함 시 수집 제외)

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 1개월 전 로그 정리"""
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

        # 4. WebDriver 설정
        try:
            self.setup_driver_stealth(self.account_name)  # Amazon만 강화된 봇 감지 회피 적용
        except Exception as e:
            print(f"[ERROR] Initialize failed: WebDriver setup failed - {e}")
            traceback.print_exc()
            return False

        # 5. batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True)

        # 6. calendar_week 생성 및 로그 정리
        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, calendar_week={self.calendar_week}")
        return True

    def is_throttled(self):
        """현재 페이지가 쓰로틀링 상태인지 확인"""
        page_source = self.driver.page_source.lower()
        return "request was throttled" in page_source or "please wait a moment and refresh" in page_source

    def restart_browser(self, url):
        """브라우저 재시작: 드라이버 종료 → 새 드라이버 생성 → URL 접근"""
        try:
            print("[INFO] Closing browser...")
            if self.driver:
                self.driver.quit()

            print("[INFO] Waiting before restart...")
            time.sleep(random.uniform(10, 15))

            print("[INFO] Starting new browser...")
            self.setup_driver_stealth(self.account_name)

            print(f"[INFO] Accessing URL: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(8, 12))

            return True
        except Exception as e:
            print(f"[ERROR] Browser restart failed: {e}")
            return False

    def check_and_handle_throttling(self, page_number, url, max_retries=2, max_browser_restarts=3):
        """쓰로틀링 메시지 감지 및 처리"""
        # 1단계: 새로고침 재시도
        for retry in range(max_retries):
            if self.is_throttled():
                print(f"[WARNING] Throttling detected on page {page_number} (refresh attempt {retry + 1}/{max_retries})")
                print("[INFO] Waiting before refresh...")
                time.sleep(random.uniform(15, 20))

                print("[INFO] Refreshing page...")
                self.driver.refresh()
                time.sleep(random.uniform(8, 12))
            else:
                print("[OK] No throttling detected")
                return True

        # 2단계: URL 직접 접근 시도
        if self.is_throttled():
            print(f"[WARNING] Still throttled after {max_retries} refreshes. Trying direct URL access...")
            time.sleep(random.uniform(20, 25))

            print(f"[INFO] Accessing URL directly: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(10, 15))

            if not self.is_throttled():
                print("[OK] Direct URL access successful")
                return True

        # 3단계: 브라우저 재시작 시도
        for restart_attempt in range(max_browser_restarts):
            if not self.is_throttled():
                return True

            print(f"[WARNING] Still throttled. Restarting browser (attempt {restart_attempt + 1}/{max_browser_restarts})...")

            if not self.restart_browser(url):
                print(f"[ERROR] Browser restart attempt {restart_attempt + 1} failed")
                continue

            time.sleep(random.uniform(5, 8))

            if not self.is_throttled():
                print(f"[OK] Browser restart successful on attempt {restart_attempt + 1}")
                return True

        print(f"[ERROR] Still throttled after {max_browser_restarts} browser restarts")
        return False

    def check_and_handle_sorry_page(self, max_retries=3):
        """Sorry/Robot check 페이지 감지 및 처리"""
        for attempt in range(max_retries):
            page_source = self.driver.page_source.lower()
            title = self.driver.title.lower()

            # Sorry/Robot check 페이지 감지 (처음 2000자만 확인)
            is_sorry_page = (
                'sorry' in title or
                'robot check' in title or
                'sorry' in page_source[:2000] or
                'robot check' in page_source[:2000]
            )

            if is_sorry_page:
                print(f"[WARNING] Sorry/Robot check page detected (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"[INFO] Refreshing page in 3-5 seconds...")
                    time.sleep(random.uniform(3, 5))
                    self.driver.refresh()
                    print(f"[INFO] Page refreshed, waiting for load...")
                    time.sleep(random.uniform(4, 6))
                    continue
                else:
                    print(f"[ERROR] Still sorry page after {max_retries} retries")
                    return False
            else:
                if attempt > 0:
                    print(f"[OK] Page loaded successfully after {attempt} refresh(es)")
                return True

        return False

    def handle_captcha(self):
        """CAPTCHA 자동 해결"""
        try:
            time.sleep(1)
            page_html = self.driver.page_source.lower()

            captcha_keywords = ['captcha', 'robot', 'human verification', 'press & hold', 'press and hold']
            if not any(keyword in page_html for keyword in captcha_keywords):
                return True

            captcha_selectors = [
                (By.XPATH, "//button[contains(text(), 'Continue shopping')]"),
                (By.XPATH, "//button[contains(@aria-label, 'CAPTCHA')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.ID, "captchacharacters"),
                (By.XPATH, "//form[@action='/errors/validateCaptcha']"),
            ]

            captcha_button = None
            captcha_type = None

            for by, selector in captcha_selectors:
                try:
                    element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    if element.is_displayed():
                        captcha_button = element
                        captcha_type = "button" if by != By.ID else "input"
                        break
                except:
                    continue

            if not captcha_button:
                return True

            if captcha_type == "input":
                print("[WARNING] CAPTCHA 입력 필요 - 60초 대기...")
                time.sleep(60)
                return True

            try:
                actions = ActionChains(self.driver)
                actions.move_to_element(captcha_button)
                actions.pause(random.uniform(0.5, 1.0))
                actions.click()
                actions.perform()
                time.sleep(random.uniform(3, 5))

                new_page_html = self.driver.page_source.lower()
                if not any(keyword in new_page_html for keyword in captcha_keywords):
                    print("[OK] CAPTCHA 자동 해결 성공")
                    return True
                else:
                    print("[WARNING] CAPTCHA 자동 해결 실패 - 60초 대기...")
                    time.sleep(60)
                    return True

            except Exception:
                print("[WARNING] CAPTCHA 클릭 실패 - 60초 대기...")
                time.sleep(60)
                return True

        except Exception as e:
            print(f"[ERROR] CAPTCHA handling failed: {e}")
            traceback.print_exc()
            return False

    def normalize_amazon_url(self, url):
        """Amazon URL을 /dp/ASIN 기준으로 정규화 (중복 판별용)"""
        if not url:
            return None

        try:
            # /dp/XXXXXXXXXX/ 이후 잘라내기
            match = re.search(r'(https://www\.amazon\.com/[^/]+/dp/[A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return match.group(1)

            # /dp/로 바로 시작하는 경우
            match = re.search(r'(https://www\.amazon\.com/dp/[A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return match.group(1)

            # 인코딩된 URL (%2Fdp%2F) 처리
            match = re.search(r'(https://www\.amazon\.com/[^/]+%2Fdp%2F[A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return match.group(1)

            # 인코딩된 URL (%2Fdp%2F) - /dp/로 바로 시작하는 경우
            match = re.search(r'(https://www\.amazon\.com%2Fdp%2F[A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return match.group(1)

            return url
        except Exception:
            return url

    def scroll_to_bottom(self):
        """페이지 하단까지 스크롤 (전체 콘텐츠 로드용)"""
        try:
            current_position = 0
            while True:
                scroll_step = random.randint(250, 350)
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.5, 0.7))
                total_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_position >= total_height:
                    break
            time.sleep(random.uniform(1, 2))
        except Exception as e:
            print(f"[WARNING] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 파싱 → 16개 미만시 스크롤/대기 후 재시도 → 제품 데이터 추출"""
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(random.uniform(8, 12))

            # Sorry/Robot check 페이지 처리
            if not self.check_and_handle_sorry_page(max_retries=3):
                print(f"[SKIP] Skipping page {page_number} due to persistent sorry/robot check page")
                return []

            # 쓰로틀링 처리
            if not self.check_and_handle_throttling(page_number, url):
                print(f"[SKIP] Skipping page {page_number} due to throttling")
                return []

            # 추가 대기 (봇 감지 후 안정화)
            time.sleep(random.uniform(3, 5))

            # 16개 검증 (최대 3회 재시도: 파싱 → 스크롤 → 대기 후 재파싱)
            base_containers = []
            expected_products = 16

            for attempt in range(1, 4):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                if len(base_containers) >= expected_products:
                    break

                if attempt == 1:
                    # 1차 실패: 스크롤 후 재시도
                    print(f"[WARNING] Page {page_number}: {len(base_containers)}/{expected_products} products, scrolling...")
                    self.scroll_to_bottom()
                    time.sleep(random.uniform(3, 5))
                elif attempt == 2:
                    # 2차 실패: 대기 후 재시도
                    print(f"[WARNING] Page {page_number}: {len(base_containers)}/{expected_products} products, waiting...")
                    time.sleep(random.uniform(5, 8))

            print(f"[INFO] Page {page_number}: {len(base_containers)} products found")

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.amazon.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    # number_of_units_purchased_past_month 추출 및 변환 (3K+ → 3000, 3M+ → 3000000)
                    number_of_units_purchased_past_month_raw = self.safe_extract(item, 'number_of_units_purchased_past_month')
                    number_of_units_purchased_past_month = None
                    if number_of_units_purchased_past_month_raw:
                        # 숫자 바로 뒤에 K 또는 M이 있는지 확인 (예: 3K+, 100M+)
                        match = re.search(r'(\d+)\s*([KkMm])?', number_of_units_purchased_past_month_raw)
                        if match:
                            num = int(match.group(1))
                            suffix = match.group(2).upper() if match.group(2) else None
                            if suffix == 'M':
                                number_of_units_purchased_past_month = str(num * 1000000)
                            elif suffix == 'K':
                                number_of_units_purchased_past_month = str(num * 1000)
                            else:
                                number_of_units_purchased_past_month = str(num)

                    # available_quantity_for_purchase: 숫자만 추출
                    available_quantity_for_purchase = None
                    available_quantity_for_purchase_raw = self.safe_extract(item, 'available_quantity_for_purchase')
                    if available_quantity_for_purchase_raw:
                        match = re.search(r'(\d+)', available_quantity_for_purchase_raw)
                        if match:
                            available_quantity_for_purchase = match.group(1)

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'number_of_units_purchased_past_month': number_of_units_purchased_past_month,
                        'final_sku_price': self.safe_extract(item, 'final_sku_price'),
                        'original_sku_price': self.safe_extract(item, 'original_sku_price'),
                        'shipping_info': self.safe_extract_join(item, 'shipping_info', separator=", "),
                        'available_quantity_for_purchase': available_quantity_for_purchase,
                        'discount_type': self.safe_extract(item, 'discount_type'),
                        'main_rank': 0,  # save_products()에서 재할당
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
        """DB 저장: 중복 제거 → BATCH_SIZE 배치 → RETRY_SIZE 배치 → 1개씩 (3-tier retry)"""
        if not products:
            return 0

        # 키워드 필터링, 중복 제거 및 rank 재할당
        unique_products = []
        for product in products:
             
            # 제외 키워드 필터링 (먼저 수행)
            retailer_sku_name = product.get('retailer_sku_name') or ''           
            if self.excluded_keywords and any(keyword.lower() in retailer_sku_name.lower() for keyword in self.excluded_keywords):
                print(f"[SKIP] 제외 키워드 포함: {retailer_sku_name[:40]}...")
                continue

            # 중복 URL 제외
            product_url = product.get('product_url')
            normalized_url = self.normalize_amazon_url(product_url)
            if normalized_url and normalized_url in self.saved_urls:
                print(f"[SKIP] 중복 URL: {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                continue

            if normalized_url:
                self.saved_urls.add(normalized_url)
            unique_products.append(product)

        # rank 재할당 (중복 제거 후 순차적으로)
        for i, product in enumerate(unique_products):
            product['main_rank'] = self.current_rank + i + 1

        # current_rank 업데이트
        if unique_products:
            self.current_rank += len(unique_products)

        if not unique_products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_query = """
                INSERT INTO amazon_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    number_of_units_purchased_past_month, final_sku_price, original_sku_price,
                    shipping_info, available_quantity_for_purchase, discount_type,
                    main_rank, main_page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                    product['number_of_units_purchased_past_month'],
                    product['final_sku_price'],
                    product['original_sku_price'],
                    product['shipping_info'],
                    product['available_quantity_for_purchase'],
                    product['discount_type'],
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

            for batch_start in range(0, len(unique_products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(unique_products))
                batch_products = unique_products[batch_start:batch_end]

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
            last_page = 0

            while total_products < target_products and page_num <= self.max_pages:
                products = self.crawl_page(page_num)
                last_page = page_num

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

                time.sleep(random.uniform(28, 32))
                page_num += 1

            print(f"[DONE] Page: {last_page}, Saved: {total_products}, batch_id: {self.batch_id}")
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
            if self.standalone:
                input("Press Enter to exit...")



def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = AmazonMainCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
