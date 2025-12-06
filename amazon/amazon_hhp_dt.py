"""
Amazon Detail 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: batch_id=None (하드코딩된 batch_id 사용)
- 통합 크롤러: batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR에서 수집한 모든 제품 처리

================================================================================
저장 테이블
================================================================================
- hhp_retail_com (상세 정보 + 리뷰)
"""

import sys
import os
import time
import traceback
import random
import re
import subprocess
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common import data_extractor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

# 재시도 설정
MAX_RETRY = 3


class AmazonDetailCrawler(BaseCrawler):
    """
    Amazon Detail 페이지 크롤러
    """

    def __init__(self, batch_id=None, login_success=None, test_mode=False):
        """초기화. batch_id: 통합 크롤러에서 전달, login_success: 로그인 성공 여부, test_mode: 테스트 모드 여부"""
        super().__init__()
        self.batch_id = batch_id
        self.account_name = 'Amazon'
        self.page_type = 'detail'
        self.cookies_loaded = False
        self.login_success = login_success
        self.test_mode = test_mode
        self.standalone = batch_id is None

    def extract_review_count(self, text):
        """리뷰 개수 텍스트에서 숫자 추출 (쉼표 유지)"""
        match = re.search(r'[\d,]+', text) if text else None
        return match.group(0) if match else None

    def extract_rating(self, text):
        """별점 텍스트에서 숫자 추출 (소수점 포함)"""
        match = re.search(r'\d+\.?\d*', text) if text else None
        return match.group(0) if match else None

    def convert_units_purchased_past(self, raw_value):
        """구매 수량 변환 (3K+ → 3000, 3M+ → 3000000)"""
        if not raw_value:
            return None
        # 숫자 바로 뒤에 K 또는 M이 있는지 확인 (예: 3K+, 100M+)
        match = re.search(r'(\d+)\s*([KkMm])?', raw_value)
        if not match:
            return None
        num = int(match.group(1))
        suffix = match.group(2).upper() if match.group(2) else None
        if suffix == 'M':
            return str(num * 1000000)
        elif suffix == 'K':
            return str(num * 1000)
        return str(num)

    def initialize(self):
        """초기화: batch_id 설정 → DB 연결 → XPath 로드 → WebDriver 설정 → 로그 정리"""
        # 1. batch_id 설정
        if not self.batch_id:
            self.batch_id = 'a_20251206_154228'

        # 2. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 3. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 4. WebDriver 설정
        try:
            self.setup_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: WebDriver setup failed - {e}")
            traceback.print_exc()
            return False

        # 5. 쿠키 로드
        if self.login_success is False:
            self.cookies_loaded = False
        else:
            self.cookies_loaded = self.load_cookies(self.account_name)

        # 6. 로그 정리
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, cookies_loaded={self.cookies_loaded}")
        return True

    def scroll_to_bottom(self):
        """페이지 하단까지 스크롤 (전체 콘텐츠 로드용) - 70% 스크롤"""
        try:
            # 전체 높이의 70%만 스크롤
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            target_height = int(total_height * 0.7)

            current_position = 0
            while current_position < target_height:
                scroll_step = random.randint(400, 600)
                current_position += scroll_step
                if current_position > target_height:
                    current_position = target_height
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.3, 0.5))

            time.sleep(random.uniform(0.5, 1))
        except Exception as e:
            print(f"[WARNING] Scroll failed: {e}")
            traceback.print_exc()

    def run_login_and_reload_cookies(self):
        """로그인 스크립트 실행 후 쿠키 갱신"""
        try:
            login_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amazon_hhp_login.py')

            if not os.path.exists(login_script):
                print(f"[ERROR] Login script not found: {login_script}")
                return False

            result = subprocess.run(
                ['python', login_script],
                capture_output=True,
                text=True,
                timeout=180
            )

            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            if result.returncode == 0 or 'LOGIN SUCCESSFUL' in result.stdout or 'Successfully logged in' in result.stdout:
                self.cookies_loaded = self.load_cookies(self.account_name)
                if self.cookies_loaded:
                    self.login_success = True
                    return True
            return False

        except subprocess.TimeoutExpired:
            print("[ERROR] Login script timed out")
            return False
        except Exception as e:
            print(f"[ERROR] Login failed: {e}")
            traceback.print_exc()
            return False

    def load_product_list(self):
        """product_list 조회: batch_id 기준으로 제품 URL 및 기본 정보 조회"""
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT
                    account_name, page_type, retailer_sku_name,
                    number_of_units_purchased_past_month, final_sku_price, original_sku_price,
                    shipping_info, available_quantity_for_purchase, discount_type,
                    main_rank, bsr_rank, product_url, calendar_week, batch_id
                FROM amazon_hhp_product_list
                WHERE account_name = %s AND batch_id = %s AND product_url IS NOT NULL
                ORDER BY id
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            products = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'page_type': row[1],
                    'retailer_sku_name': row[2],
                    'number_of_units_purchased_past_month': row[3],
                    'final_sku_price': row[4],
                    'original_sku_price': row[5],
                    'shipping_info': row[6],
                    'available_quantity_for_purchase': row[7],
                    'discount_type': row[8],
                    'main_rank': row[9],
                    'bsr_rank': row[10],
                    'product_url': row[11],
                    'calendar_week': row[12],
                    'batch_id': row[13]
                }
                products.append(product)

            print(f"[INFO] Loaded {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

    def extract_asin_from_url(self, product_url):
        """URL에서 ASIN 추출"""
        if not product_url:
            return None

        match = re.search(r'/dp/([A-Z0-9]{10})/', product_url)
        if match:
            return match.group(1)

        match = re.search(r'%2[fF]dp%2[fF]([A-Z0-9]{10})%', product_url)
        if match:
            return match.group(1)

        return None

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
            self.setup_driver()

            # 쿠키 재로드
            if self.login_success is not False:
                self.cookies_loaded = self.load_cookies(self.account_name)

            print(f"[INFO] Accessing URL: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(8, 12))

            return True
        except Exception as e:
            print(f"[ERROR] Browser restart failed: {e}")
            return False

    def check_and_handle_throttling(self, url, max_retries=2, max_browser_restarts=3):
        """쓰로틀링 메시지 감지 및 처리"""
        # 1단계: 새로고침 재시도
        for retry in range(max_retries):
            if self.is_throttled():
                print(f"[WARNING] Throttling detected (refresh attempt {retry + 1}/{max_retries})")
                print("[INFO] Waiting before refresh...")
                time.sleep(random.uniform(15, 20))

                print("[INFO] Refreshing page...")
                self.driver.refresh()
                time.sleep(random.uniform(8, 12))
            else:
                return True

        # 2단계: URL 직접 접근 시도
        if self.is_throttled():
            print(f"[WARNING] Still throttled after {max_retries} refreshes. Trying direct URL access...")
            time.sleep(random.uniform(10, 15))

            print(f"[INFO] Accessing URL directly: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(5, 8))

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
                    print(f"[INFO] Refreshing page in 2-3 seconds...")
                    time.sleep(random.uniform(2, 3))
                    self.driver.refresh()
                    print(f"[INFO] Page refreshed, waiting for load...")
                    time.sleep(random.uniform(3, 5))
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
                    return True
                else:
                    time.sleep(60)
                    return True

            except Exception:
                time.sleep(60)
                return True

        except Exception as e:
            print(f"[ERROR] CAPTCHA handling failed: {e}")
            traceback.print_exc()
            return False

    def extract_reviews_from_detail_page(self, tree, max_reviews=10):
        """상세 페이지에서 리뷰 추출"""
        try:
            # 리뷰 컨테이너 단위로 추출 (text() 대신 element 단위)
            review_container_xpath = self.xpaths.get('review_container', {}).get('xpath')
            if not review_container_xpath:
                print("[ERROR] review_container XPath not found")
                return None
            review_containers = tree.xpath(review_container_xpath)

            if not review_containers:
                print("[ERROR] review_container not found")
                return None

            review_containers = review_containers[:max_reviews]

            cleaned_reviews = []
            for container in review_containers:
                # 각 컨테이너 내의 모든 텍스트를 합침
                review_text = container.text_content()
                if review_text:
                    cleaned = ' '.join(review_text.split())
                    if len(cleaned) > 10:
                        cleaned_reviews.append(cleaned)

            if not cleaned_reviews:
                return None

            formatted_reviews = [f"review{idx} - {review}" for idx, review in enumerate(cleaned_reviews, 1)]
            result = ' ||| '.join(formatted_reviews)
            return result

        except Exception as e:
            print(f"[ERROR] Review extraction failed: {e}")
            traceback.print_exc()
            return None

    def extract_reviews_from_review_page(self, item, max_reviews=20):
        """리뷰 페이지에서 리뷰 추출 (현재 미사용, 향후 변경 대비)"""
        try:
            if not item:
                return None

            review_url = f"https://www.amazon.com/product-reviews/{item}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"

            self.driver.get(review_url)
            time.sleep(10)

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            page_html_lower = page_html.lower()
            if "couldn't find that page" in page_html_lower or "page not found" in page_html_lower:
                return None

            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                if self.run_login_and_reload_cookies():
                    self.driver.get(review_url)
                    time.sleep(10)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                    if 'signin' in self.driver.current_url:
                        return data_extractor.get_no_reviews_text(self.account_name)
                else:
                    return data_extractor.get_no_reviews_text(self.account_name)

            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                if self.handle_captcha():
                    self.driver.get(review_url)
                    time.sleep(5)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                    if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                        return data_extractor.get_no_reviews_text(self.account_name)
                else:
                    return data_extractor.get_no_reviews_text(self.account_name)

            review_xpaths = [
                self.xpaths.get('detailed_review_content', {}).get('xpath') or '',
                self.xpaths.get('review_fallback_1', {}).get('xpath') or '',
                self.xpaths.get('review_fallback_2', {}).get('xpath') or '',
            ]

            review_texts = []
            for xpath in review_xpaths:
                if not xpath:
                    continue
                try:
                    review_texts = tree.xpath(xpath)
                    if review_texts:
                        break
                except Exception:
                    continue

            if not review_texts:
                return data_extractor.get_no_reviews_text(self.account_name)

            review_texts = review_texts[:max_reviews]

            cleaned_reviews = []
            for review in review_texts:
                if review.strip():
                    cleaned = ' '.join(review.split())
                    cleaned_reviews.append(cleaned)

            result = ' ||| '.join(cleaned_reviews)
            return result

        except Exception as e:
            print(f"[ERROR] Review page extraction failed: {e}")
            traceback.print_exc()
            return None

    def crawl_detail(self, product):
        """상세 페이지 크롤링: 페이지 로드 → 필드 추출 → 리뷰 추출 → product_list + detail 데이터 결합"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                return product

            self.driver.get(product_url)
            time.sleep(random.uniform(5, 8))

            # Sorry/Robot check 페이지 처리
            if not self.check_and_handle_sorry_page(max_retries=3):
                print(f"[SKIP] Skipping product due to persistent sorry/robot check page")
                return product

            # 쓰로틀링 처리
            if not self.check_and_handle_throttling(product_url):
                print(f"[SKIP] Skipping product due to throttling")
                return product

            # 추가 대기 (봇 감지 후 안정화)
            time.sleep(random.uniform(1, 2))

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # 로그인 체크
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                return product

            # CAPTCHA 체크 (봇 감지와 별도로 CAPTCHA 입력 필요한 경우)
            # 특정 문구로 정확하게 감지 (단순 'captcha' 키워드는 오탐지 발생)
            captcha_phrases = [
                'enter the characters you see below',
                'sorry, we just need to make sure you\'re not a robot',
                'type the characters you see in this image',
                'api-services-support@amazon.com',
            ]
            if any(phrase in page_html.lower() for phrase in captcha_phrases):
                if self.handle_captcha():
                    self.driver.get(product_url)
                    time.sleep(random.uniform(5, 8))  # CAPTCHA 후 재로드
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                else:
                    return product

            # 전체 콘텐츠 로드: 하단까지 스크롤 → 맨 위로 복귀
            self.scroll_to_bottom()
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(random.uniform(0.5, 1))

            # 기본값 초기화
            country = 'SEA'
            product_type = 'HHP'
            # 실제 로드된 URL에서 ASIN 추출 (리다이렉트 대응)
            actual_url = self.driver.current_url
            item = self.extract_asin_from_url(actual_url)

            # final_sku_price 추출 (기존 값이 빈 경우)
            final_sku_price = product.get('final_sku_price')
            if not final_sku_price:
                # current_price xpath로 추출 시도
                final_sku_price = self.safe_extract(tree, 'final_sku_price')

                # 가격 추출 실패 시 availability_status 확인
                if not final_sku_price:
                    # (No featured offers available)
                    final_sku_price = self.safe_extract(tree, 'final_sku_price_nofeatured') 
                    if not final_sku_price:
                        # (Currently unavailable)
                        final_sku_price = self.safe_extract(tree, 'final_sku_price_unavailable') 

                # product에 업데이트
                if final_sku_price:
                    product['final_sku_price'] = final_sku_price

            # bsr인 경우
            if product.get('page_type') == 'bsr':
                # number_of_units_purchased_past_month
                number_of_units_purchased_past_month_raw = self.safe_extract(tree, 'number_of_units_purchased_past_month')
                number_of_units_purchased_past_month = self.convert_units_purchased_past(number_of_units_purchased_past_month_raw)
                if number_of_units_purchased_past_month:
                    product['number_of_units_purchased_past_month'] = number_of_units_purchased_past_month

                # discount_type
                discount_type = self.safe_extract(tree, 'discount_type')
                if discount_type:
                    product['discount_type'] = discount_type
                    
                # original_sku_price
                original_sku_price = self.safe_extract(tree, 'original_sku_price')
                if original_sku_price:
                    product['original_sku_price'] = original_sku_price

                # shipping_info (여러 요소는 쉼표로 연결)
                shipping_info = self.safe_extract_join(tree, 'shipping_info', ', ')
                if shipping_info:
                    shipping_info = shipping_info.replace('Details', '').strip()
                    if shipping_info:
                        product['shipping_info'] = shipping_info

                # available_quantity_for_purchase
                available_quantity_for_purchase = self.safe_extract(tree, 'available_quantity_for_purchase')
                if available_quantity_for_purchase:
                    match = re.search(r'(\d+)', available_quantity_for_purchase)
                    if match:
                        product['available_quantity_for_purchase'] = match.group(1)

                

            # Trade-in 섹션은 JS로 늦게 로드될 수 있으므로 최신 HTML로 재파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)
            
            hhp_carrier = self.safe_extract(tree, 'hhp_carrier')
            sku_popularity = self.safe_extract(tree, 'sku_popularity')
            bundle = self.safe_extract(tree, 'bundle')
            retailer_membership_discounts_raw = self.safe_extract(tree, 'retailer_membership_discounts')
            retailer_membership_discounts = data_extractor.extract_text_before_or_after(
                retailer_membership_discounts_raw, 'Join Prime', 'before'
            )
           
            trade_in = self.safe_extract_join(tree, 'trade_in', ' ')
            if not trade_in:
                trade_in = self.safe_extract(tree, 'trade_in_fallback')

            # Additional details 버튼 클릭
            hhp_storage = None
            hhp_color = None
            rank_1 = None
            rank_2 = None
            additional_details_found = False

            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(0.5)

                additional_details_xpath = self.xpaths.get('additional_details_button', {}).get('xpath')
                if additional_details_xpath:
                    try:
                        additional_details_button = WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, additional_details_xpath))
                        )
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", additional_details_button)
                        time.sleep(0.5)
                        additional_details_button.click()
                        time.sleep(0.5)
                        additional_details_found = True

                        item_details_xpath = self.xpaths.get('item_details_button', {}).get('xpath')
                        if item_details_xpath:
                            try:
                                item_details_button = WebDriverWait(self.driver, 3).until(
                                    EC.element_to_be_clickable((By.XPATH, item_details_xpath))
                                )
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item_details_button)
                                time.sleep(0.5)
                                item_details_button.click()
                                time.sleep(0.5)
                            except Exception:
                                pass  # Item details 버튼이 없을 수 있음
                    except Exception:
                        pass  # Additional details 버튼이 없을 수 있음

                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

            except Exception as e:
                print(f"[WARNING] Additional details section failed: {e}")

            # HHP 스펙 및 랭크 추출
            if additional_details_found:
                hhp_storage = self.safe_extract(tree, 'hhp_storage')
                hhp_color = self.safe_extract(tree, 'hhp_color')
            else:
                hhp_storage = self.safe_extract(tree, 'hhp_storage_fallback')
                hhp_color = self.safe_extract(tree, 'hhp_color_fallback')

            rank_1 = self.safe_extract(tree, 'rank_1')
            rank_2 = self.safe_extract(tree, 'rank_2')

            # 리뷰 관련 필드 (최대 3회 재시도)
            count_of_reviews = None
            star_rating = None
            count_of_star_ratings = None

            # "No customer reviews" 텍스트 감지 (tree에서 검색)
            no_review_keywords = ['no customer reviews', 'no reviews', 'be the first to review']
            page_text = tree.text_content().lower() if tree is not None else ''
            is_no_reviews = any(keyword in page_text for keyword in no_review_keywords)

            if is_no_reviews:
                count_of_reviews = '0'
                star_rating = 'No customer reviews'
                count_of_star_ratings = 'No customer reviews'
            else:
                for attempt in range(1, MAX_RETRY + 1):
                    # 첫 시도는 기존 tree 사용, 재시도 시에만 재파싱
                    if attempt > 1:
                        page_html = self.driver.page_source
                        tree = html.fromstring(page_html)

                    if count_of_reviews is None:
                        count_of_reviews_raw = self.safe_extract(tree, 'count_of_reviews')
                        count_of_reviews = self.extract_review_count(count_of_reviews_raw)
                        count_of_star_ratings = count_of_reviews

                    if star_rating is None:
                        star_rating_raw = self.safe_extract(tree, 'star_rating')
                        star_rating = self.extract_rating(star_rating_raw)

                    # 필수 필드 모두 추출 성공하면 종료
                    if star_rating and count_of_reviews:
                        break

                    if attempt < MAX_RETRY:
                        time.sleep(1)
                    else:
                        # 마지막 시도에서도 실패
                        missing = []
                        if not star_rating: missing.append('star_rating')
                        if not count_of_reviews: missing.append('count_of_reviews')
                        if missing:
                            print(f"[WARNING] 리뷰 데이터 추출 실패 (시도 {attempt}/{MAX_RETRY}) - 미추출: {', '.join(missing)}")

            # 리뷰 섹션으로 이동
            summarized_review_content = None
            try:
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.5)

                review_link_xpath = self.xpaths.get('review_link', {}).get('xpath')
                if review_link_xpath:
                    review_link = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, review_link_xpath))
                    )
                    review_link.click()
                    time.sleep(1)

                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

                summarized_review_content = self.safe_extract(tree, 'summarized_review_content')
            except Exception:
                pass

            # 상세 리뷰 추출 (리뷰 없으면 건너뜀)
            if is_no_reviews:
                detailed_review_content = 'No customer reviews'
            else:
                detailed_review_content = self.extract_reviews_from_detail_page(tree, max_reviews=20)

            # 결합된 데이터
            detail_data = {
                'country': country,
                'product': product_type,
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'sku_popularity': sku_popularity,
                'bundle': bundle,
                'trade_in': trade_in,
                'retailer_membership_discounts': retailer_membership_discounts,
                'rank_1': rank_1,
                'rank_2': rank_2,
                'hhp_carrier': hhp_carrier,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'summarized_review_content': summarized_review_content,
                'detailed_review_content': detailed_review_content,
                'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            combined_data = {**product, **detail_data}
            return combined_data

        except Exception as e:
            print(f"[ERROR] Detail crawl failed: {e}")
            traceback.print_exc()
            return product

    def save_to_retail_com(self, products):
        """DB 저장: 2-tier retry (BATCH_SIZE=5 → 1개씩)"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()

            # 테스트 모드면 test_hhp_retail_com, 통합 크롤러면 hhp_retail_com
            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'

            insert_query = f"""
                INSERT INTO {table_name} (
                    country, product, item, account_name, page_type,
                    retailer_sku_name, product_url,
                    count_of_reviews, star_rating, count_of_star_ratings,
                    sku_popularity, bundle, trade_in,
                    retailer_membership_discounts,
                    rank_1, rank_2,
                    hhp_carrier, hhp_storage, hhp_color,
                    detailed_review_content, summarized_review_content,
                    final_sku_price, original_sku_price,
                    shipping_info, available_quantity_for_purchase,
                    discount_type, main_rank, bsr_rank,
                    number_of_units_purchased_past_month,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
            """

            BATCH_SIZE = 5
            saved_count = 0

            def product_to_tuple(product):
                return (
                    product.get('country'),
                    product.get('product'),
                    product.get('item'),
                    product.get('account_name'),
                    product.get('page_type'),
                    product.get('retailer_sku_name'),
                    product.get('product_url'),
                    product.get('count_of_reviews'),
                    product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('sku_popularity'),
                    product.get('bundle'),
                    product.get('trade_in'),
                    product.get('retailer_membership_discounts'),
                    product.get('rank_1'),
                    product.get('rank_2'),
                    product.get('hhp_carrier'),
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('detailed_review_content'),
                    product.get('summarized_review_content'),
                    product.get('final_sku_price'),
                    product.get('original_sku_price'),
                    product.get('shipping_info'),
                    product.get('available_quantity_for_purchase'),
                    product.get('discount_type'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('number_of_units_purchased_past_month'),
                    product.get('calendar_week'),
                    product.get('crawl_strdatetime'),
                    product.get('batch_id')
                )

            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    values_list = [product_to_tuple(p) for p in batch_products]
                    cursor.executemany(insert_query, values_list)
                    self.db_conn.commit()
                    saved_count += len(batch_products)

                except Exception:
                    self.db_conn.rollback()

                    for single_product in batch_products:
                        try:
                            cursor.execute(insert_query, product_to_tuple(single_product))
                            self.db_conn.commit()
                            saved_count += 1
                        except Exception as single_error:
                            print(f"[ERROR] DB save failed: {single_product.get('item')}: {single_error}")
                            query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                            print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                            traceback.print_exc()
                            self.db_conn.rollback()

            cursor.close()
            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return 0

    def run(self):
        """실행: initialize() → load_product_list() → 제품별 crawl_detail() → save_to_retail_com() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[ERROR] No products found")
                return False

            total_saved = 0
            crawled_products = []
            SAVE_BATCH_SIZE = 5

            for i, product in enumerate(product_list, 1):
                try:
                    sku_name = product.get('retailer_sku_name') or 'N/A'
                    print(f"[{i}/{len(product_list)}] {sku_name[:50]}...")

                    combined_data = self.crawl_detail(product)
                    if combined_data:
                        crawled_products.append(combined_data)

                    if not self.cookies_loaded and i == 1:
                        self.save_cookies(self.account_name)
                        self.cookies_loaded = True

                    if len(crawled_products) >= SAVE_BATCH_SIZE:
                        saved_count = self.save_to_retail_com(crawled_products)
                        total_saved += saved_count
                        crawled_products = []

                    time.sleep(random.uniform(3, 5))

                except Exception as e:
                    print(f"[ERROR] Product {i} failed: {e}")
                    continue

            if crawled_products:
                saved_count = self.save_to_retail_com(crawled_products)
                total_saved += saved_count

            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'
            print(f"[DONE] Processed: {len(product_list)}, Saved: {total_saved}, Table: {table_name}, batch_id: {self.batch_id}")
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
    """개별 실행 진입점 (테스트 모드, 기본 배치 ID 사용)"""
    crawler = AmazonDetailCrawler(batch_id=None, login_success=None, test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
