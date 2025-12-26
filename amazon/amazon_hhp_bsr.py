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
        self.standalone = batch_id is None
        self.test_count = 1  # 테스트 모드
        self.max_products = 100  # 운영 모드
        self.max_pages = 2  # 최대 페이지 수
        self.crawled_urls = set()  # 페이지 간 중복 방지용 (정규화 URL)

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 로그 정리"""
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

    def check_and_handle_throttling(self, page_number, url, max_refresh_retries=10, max_browser_restarts=3):
        """쓰로틀링 메시지 감지 및 처리

        흐름:
        1. 쓰로틀링 감지 → 새로고침 최대 10회
        2. 10회 후에도 쓰로틀링 → URL 직접 접근
        3. 여전히 쓰로틀링 → 크롬 재시작
        4. 재시작 후 다시 새로고침 10회 → URL 직접 접근 반복
        5. 크롬 재시작 최대 3회
        """
        for restart_attempt in range(max_browser_restarts + 1):  # 0: 초기, 1~3: 재시작
            # 새로고침 10회 시도
            for retry in range(max_refresh_retries):
                if not self.is_throttled():
                    if retry > 0 or restart_attempt > 0:
                        print("[OK] No throttling detected")
                    return True

                print(f"[WARNING] Throttling detected on page {page_number} (refresh {retry + 1}/{max_refresh_retries}, browser restart {restart_attempt}/{max_browser_restarts})")
                print("[INFO] Waiting before refresh...")
                time.sleep(random.uniform(2, 3))

                print("[INFO] Refreshing page...")
                self.driver.refresh()
                time.sleep(random.uniform(3, 5))

            # 새로고침 10회 후에도 쓰로틀링 → URL 직접 접근
            if self.is_throttled():
                print(f"[WARNING] Still throttled after {max_refresh_retries} refreshes. Trying direct URL access...")
                time.sleep(random.uniform(8, 10))

                print(f"[INFO] Accessing URL directly: {url[:80]}...")
                self.driver.get(url)
                time.sleep(random.uniform(8, 10))

                if not self.is_throttled():
                    print("[OK] Direct URL access successful")
                    return True

            # URL 직접 접근 후에도 쓰로틀링 → 크롬 재시작 (마지막 시도가 아닐 때만)
            if self.is_throttled() and restart_attempt < max_browser_restarts:
                print(f"[WARNING] Still throttled after URL access. Restarting browser (attempt {restart_attempt + 1}/{max_browser_restarts})...")

                if not self.restart_browser(url):
                    print(f"[ERROR] Browser restart attempt {restart_attempt + 1} failed")
                    continue

                time.sleep(random.uniform(5, 8))

        if self.is_throttled():
            print(f"[ERROR] Still throttled after all attempts")
            return False

        return True

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
        """Amazon URL에서 ASIN 추출 후 표준 URL로 정규화 (중복 판별용, DB 저장은 원본 URL 사용)"""
        if not url:
            return None

        try:
            # 1. 일반 URL: /dp/ASIN
            match = re.search(r'/dp/([A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return f"https://www.amazon.com/dp/{match.group(1)}"

            # 2. URL 인코딩된 sspa URL: %2Fdp%2FASIN
            match = re.search(r'%2Fdp%2F([A-Z0-9]{10})', url, re.IGNORECASE)
            if match:
                return f"https://www.amazon.com/dp/{match.group(1)}"

            # ASIN 추출 실패 시 원본 URL 반환
            return url
        except Exception:
            return url

    def build_existing_urls_cache(self, account_name, batch_id):
        """DB에서 기존 URL을 조회하여 정규화 URL → 원본 URL 딕셔너리 생성 (1회 조회)"""
        try:
            cursor = self.db_conn.cursor()
            query = """
                SELECT product_url FROM amazon_hhp_product_list
                WHERE account_name = %s AND batch_id = %s
            """
            cursor.execute(query, (account_name, batch_id))
            rows = cursor.fetchall()
            cursor.close()

            existing_urls = {}
            for (db_url,) in rows:
                normalized = self.normalize_amazon_url(db_url)
                if normalized:
                    existing_urls[normalized] = db_url

            return existing_urls

        except Exception as e:
            print(f"[WARNING] build_existing_urls_cache failed: {e}")
            return {}

    def scroll_to_bottom(self, max_iterations=50):
        """페이지 하단까지 스크롤 (전체 콘텐츠 로드용, 최대 반복 횟수 제한)"""
        try:
            current_position = 0
            for _ in range(max_iterations):
                scroll_step = random.randint(205, 350)
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

    def wait_for_products(self, base_container_xpath, expected_count=50, max_retries=3):
        """제품이 expected_count개 이상 로드될 때까지 대기 (부족하면 스크롤 후 재시도)"""
        base_containers = []
        for attempt in range(max_retries):
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)
            base_containers = tree.xpath(base_container_xpath)

            if len(base_containers) >= expected_count:
                print(f"[OK] {len(base_containers)} products found")
                return base_containers

            if attempt < max_retries - 1:
                print(f"[WARNING] Only {len(base_containers)} products found (attempt {attempt + 1}/{max_retries}), scrolling...")
                self.scroll_to_bottom()
                time.sleep(random.uniform(3, 5))

        print(f"[WARNING] Only {len(base_containers)} products found after {max_retries} attempts")
        return base_containers

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 스크롤 → HTML 파싱 → 제품 데이터 추출"""
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

            # 페이지 하단까지 스크롤 (전체 콘텐츠 로드)
            self.scroll_to_bottom()

            # 제품 50개 이상 로드될 때까지 대기 (부족하면 스크롤 후 재시도)
            base_containers = self.wait_for_products(base_container_xpath, expected_count=50, max_retries=3)

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
        """DB 저장: 정규화된 URL로 중복 확인 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
        if not products:
            return {'insert': 0, 'update': 0}

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0

            products_to_update = []
            products_to_insert = []

            # 1회 DB 조회로 기존 URL 캐시 생성
            existing_urls = self.build_existing_urls_cache(self.account_name, self.batch_id)

            for product in products:
                # URL 정규화
                normalized_url = self.normalize_amazon_url(product['product_url'])

                # 1. 페이지 간 중복 체크 (이미 수집한 URL → 스킵)
                if normalized_url in self.crawled_urls:
                    continue
                self.crawled_urls.add(normalized_url)

                # 2. DB 캐시에서 기존 URL 체크 → UPDATE / INSERT 분류
                matched_url = existing_urls.get(normalized_url)
                if matched_url:
                    product['matched_url'] = matched_url  # DB에서 매칭된 원본 URL 저장
                    products_to_update.append(product)
                else:
                    products_to_insert.append(product)

            # UPDATE 처리 (정규화된 URL로 매칭된 원본 URL 사용)
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
                        product['matched_url']  # DB에 저장된 원본 URL 사용
                    ))
                    self.db_conn.commit()
                    update_count += 1
                except Exception as e:
                    print(f"[WARNING] UPDATE failed: {product.get('matched_url', 'N/A')[:50]}: {e}")
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
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_insert = 0
            total_update = 0
            target_products = self.test_count if self.test_mode else self.max_products
            page_num = 1

            while (total_insert + total_update) < target_products and page_num <= self.max_pages:
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

                time.sleep(random.uniform(10, 15))
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
