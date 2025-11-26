"""
Amazon Detail 페이지 크롤러
- 개별 실행: batch_id=None (하드코딩된 batch_id 사용)
- 통합 크롤러: batch_id를 파라미터로 전달
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- batch_id 기준으로 조회된 모든 제품 크롤링
"""

import sys
import os
import time
import traceback
import random
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


class AmazonDetailCrawler(BaseCrawler):
    """
    Amazon Detail 페이지 크롤러
    """

    def __init__(self, batch_id=None, login_success=None):
        """
        초기화

        Args:
            batch_id (str): 배치 ID (기본값: None)
                           - None: 하드코딩된 batch_id 사용
                           - 문자열: 통합 크롤러에서 전달된 batch_id 사용
            login_success (bool): 로그인 성공 여부 (기본값: None)
                           - None: 개별 실행 시 (쿠키 로드 시도)
                           - True: 통합 크롤러에서 로그인 성공 (쿠키 로드)
                           - False: 통합 크롤러에서 로그인 실패 (쿠키 로드 안함, 리뷰 스킵)
        """
        super().__init__()

        self.batch_id = batch_id
        self.account_name = 'Amazon'
        self.page_type = 'detail'
        self.cookies_loaded = False  # 쿠키 로드 여부 플래그
        self.login_success = login_success  # 로그인 성공 여부 (통합 크롤러에서 전달)

    def initialize(self):
        """
        크롤러 초기화 작업
        - DB 연결
        - batch_id 자동 조회 (None인 경우)
        - XPath 셀렉터 로드
        - WebDriver 설정
        - 로그 정리

        Returns:
            bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] Amazon Detail Crawler Initialization")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. 배치 ID 설정 (없으면 기본값 사용)
        if not self.batch_id:
            self.batch_id = 'a_20251125_212207'
           
        # 3. XPath 셀렉터 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        # 4. WebDriver 설정
        self.setup_driver()

        # 5. 쿠키 로드 (로그인 성공 여부에 따라 분기)
        if self.login_success is False:
            # 통합 크롤러에서 로그인 실패한 경우 -> 쿠키 로드 안함
            print("[INFO] Login failed in integrated crawler - skipping cookie load")
            print("[INFO] Reviews will be skipped if login is required")
            self.cookies_loaded = False
        else:
            # login_success가 None(개별 실행) 또는 True(로그인 성공)인 경우 -> 쿠키 로드 시도
            self.cookies_loaded = self.load_cookies(self.account_name)

        # 6. 오래된 로그 정리
        self.cleanup_old_logs()

        return True

    def run_login_and_reload_cookies(self):
        """
        로그인 요구 감지 시 로그인 스크립트 실행 후 쿠키 갱신

        Returns:
            bool: 로그인 성공 시 True, 실패 시 False
        """
        try:
            # amazon_hhp_login.py 경로
            login_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amazon_hhp_login.py')

            if not os.path.exists(login_script):
                print(f"[ERROR] Login script not found: {login_script}")
                return False

            print(f"\n[INFO] Login required - running login script...")
            print(f"[INFO] Script path: {login_script}")

            # subprocess로 로그인 스크립트 실행
            result = subprocess.run(
                ['python', login_script],
                capture_output=True,
                text=True,
                timeout=180  # 3분 타임아웃 (OTP 입력 시간 포함)
            )

            # 결과 출력
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            # 로그인 성공 여부 판단
            if result.returncode == 0 or 'LOGIN SUCCESSFUL' in result.stdout or 'Successfully logged in' in result.stdout:
                print("[OK] Login successful - reloading cookies...")

                # 쿠키 다시 로드
                self.cookies_loaded = self.load_cookies(self.account_name)

                if self.cookies_loaded:
                    print("[OK] Cookies reloaded successfully")
                    self.login_success = True
                    return True
                else:
                    print("[WARNING] Failed to reload cookies")
                    return False
            else:
                print("[WARNING] Login failed")
                return False

        except subprocess.TimeoutExpired:
            print("[ERROR] Login script timed out (180 seconds)")
            return False
        except Exception as e:
            print(f"[ERROR] Login script execution failed: {e}")
            return False

    def load_product_list(self):
        """
        amazon_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회
        - hhp_retail_com 테이블에 들어갈 필드 중 product_list에서 가져올 수 있는 것들만 조회
        - 나머지 필드(리뷰, 별점, 스펙 등)는 상세 페이지에서 추출
        - 테스트 모드: 1개만 반환

        Returns:
            list: 제품 정보 리스트 (hhp_retail_com 테이블 기준 필드 매핑)
        """
        try:
            cursor = self.db_conn.cursor()

            # PRD 기준: hhp_retail_com 테이블에 필요한 필드 중 product_list에서 가져올 것들만 SELECT
            query = """
                SELECT
                    account_name,
                    page_type,
                    retailer_sku_name,
                    number_of_units_purchased_past_month,
                    final_sku_price,
                    original_sku_price,
                    shipping_info,
                    available_quantity_for_purchase,
                    discount_type,
                    main_rank,
                    bsr_rank,
                    product_url,
                    calendar_week,
                    batch_id
                FROM amazon_hhp_product_list
                WHERE account_name = %s
                  AND batch_id = %s
                  AND product_url IS NOT NULL
                ORDER BY main_rank ASC NULLS LAST, bsr_rank ASC NULLS LAST
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            # hhp_retail_com 테이블 필드명과 일치하도록 매핑
            products = []
            for row in rows:
                product = {
                    # === product_list에서 가져오는 필드 (14개) ===
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

            # Detail 크롤러는 batch_id 기준으로 조회된 모든 제품 처리
            # (테스트/운영 모드 구분 없이 product_list에 있는 모든 제품 크롤링)

            print(f"[INFO] Loaded {len(products)} products from amazon_hhp_product_list")
            return products

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            return []

    def extract_asin_from_url(self, product_url):
        """
        product_url에서 Amazon ASIN 코드 추출

        Args: product_url (str): 제품 URL

        Returns: str: ASIN 코드 또는 None
        """
        import re

        if not product_url:
            return None

        # 패턴 1: /dp/{ASIN}/ 형식
        match = re.search(r'/dp/([A-Z0-9]{10})/', product_url)
        if match:
            return match.group(1)

        # 패턴 2: %2fdp%2F{ASIN}% 형식 (URL 인코딩)
        match = re.search(r'%2[fF]dp%2[fF]([A-Z0-9]{10})%', product_url)
        if match:
            return match.group(1)

        return None

    def handle_captcha(self):
        """CAPTCHA 자동 해결 (Selenium 기반)"""
        try:
            print("[INFO] Checking for CAPTCHA...")
            time.sleep(2)

            page_html = self.driver.page_source.lower()

            # CAPTCHA 키워드 확인
            captcha_keywords = ['captcha', 'robot', 'human verification', 'press & hold', 'press and hold']
            if not any(keyword in page_html for keyword in captcha_keywords):
                print("[INFO] No CAPTCHA detected")
                return True

            print("[WARNING] CAPTCHA detected!")

            # Amazon CAPTCHA 버튼 셀렉터
            captcha_selectors = [
                (By.XPATH, "//button[contains(text(), 'Continue shopping')]"),
                (By.XPATH, "//button[contains(@aria-label, 'CAPTCHA')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.ID, "captchacharacters"),  # 텍스트 입력형 CAPTCHA
                (By.XPATH, "//form[@action='/errors/validateCaptcha']"),
            ]

            captcha_button = None
            captcha_type = None

            # CAPTCHA 버튼 찾기
            for by, selector in captcha_selectors:
                try:
                    element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    if element.is_displayed():
                        captcha_button = element
                        captcha_type = "button" if by != By.ID else "input"
                        print(f"[OK] CAPTCHA element found: {selector}")
                        break
                except:
                    continue

            if not captcha_button:
                print("[WARNING] CAPTCHA keywords found but element not located - proceeding anyway")
                return True

            # 텍스트 입력형 CAPTCHA (수동 입력 필요)
            if captcha_type == "input":
                print("[INFO] Text CAPTCHA detected - requires manual input")
                print("[INFO] Waiting 60 seconds for you to solve it...")
                time.sleep(60)
                return True

            # 버튼 클릭형 CAPTCHA 자동 해결 시도
            print("[INFO] Attempting to solve button CAPTCHA automatically...")

            try:
                # ActionChains로 자연스러운 클릭
                actions = ActionChains(self.driver)
                actions.move_to_element(captcha_button)
                actions.pause(random.uniform(0.5, 1.0))
                actions.click()
                actions.perform()

                print("[INFO] CAPTCHA button clicked")
                time.sleep(random.uniform(3, 5))

                # 성공 확인
                new_page_html = self.driver.page_source.lower()
                if not any(keyword in new_page_html for keyword in captcha_keywords):
                    print("[OK] CAPTCHA solved successfully")
                    return True
                else:
                    print("[WARNING] CAPTCHA still present after automatic attempt")
                    print("[INFO] Waiting 60 seconds for manual intervention...")
                    time.sleep(60)
                    return True

            except Exception as e:
                print(f"[WARNING] Could not click CAPTCHA button: {e}")
                print("[INFO] Waiting 60 seconds for manual intervention...")
                time.sleep(60)
                return True

        except Exception as e:
            print(f"[ERROR] CAPTCHA handling failed: {e}")
            traceback.print_exc()
            return False

    def extract_reviews_from_detail_page(self, tree, max_reviews=10):
        """
        상세 페이지 HTML에서 리뷰 추출 (폴백용)
        - 리뷰 페이지 접근이 불가능할 때 상세 페이지에 표시된 리뷰만 수집
        - 보통 3~10개 정도의 리뷰가 표시됨

        Args:
            tree: lxml HTML 트리 (상세 페이지)
            max_reviews (int): 추출할 최대 리뷰 개수 (기본값: 10)

        Returns:
            str: 구분자(|||)로 연결된 리뷰 내용 문자열 또는 None
        """
        try:
            # 상세 페이지의 리뷰 섹션에서 리뷰 추출
            # Amazon 상세 페이지 리뷰 XPath들
            detail_page_review_xpaths = [
                "//div[@id='cm-cr-dp-review-list']//span[@data-hook='review-body']//span/text()",
                "//div[@data-hook='review']//span[@data-hook='review-body']//span/text()",
                "//div[contains(@class, 'review')]//span[@data-hook='review-body']/span/text()",
                "//div[@id='reviewsMedley']//span[@data-hook='review-body']//span/text()",
            ]

            review_texts = []
            for xpath in detail_page_review_xpaths:
                try:
                    texts = tree.xpath(xpath)
                    if texts:
                        review_texts = texts
                        break
                except:
                    continue

            if not review_texts:
                print(f"[WARNING] No reviews found on detail page (fallback)")
                return data_extractor.get_no_reviews_text(self.account_name)

            # 최대 개수만큼만 선택
            review_texts = review_texts[:max_reviews]

            # 각 리뷰 처리: 줄바꿈 공백으로 치환, 앞뒤 공백 제거
            cleaned_reviews = []
            for review in review_texts:
                if review.strip():
                    cleaned = ' '.join(review.split())
                    if len(cleaned) > 10:  # 너무 짧은 것 제외
                        cleaned_reviews.append(cleaned)

            if not cleaned_reviews:
                return data_extractor.get_no_reviews_text(self.account_name)

            # 구분자로 연결
            result = ' ||| '.join(cleaned_reviews)

            print(f"[INFO] Extracted {len(cleaned_reviews)} reviews from detail page (fallback)")
            print(f"[INFO] Total length: {len(result)} chars")
            return result

        except Exception as e:
            print(f"[ERROR] Failed to extract reviews from detail page: {e}")
            return data_extractor.get_no_reviews_text(self.account_name)

    def extract_reviews(self, item, max_reviews=20, detail_page_tree=None):
        """
        제품 리뷰 페이지에서 리뷰 내용 추출

        Args:
            item (str): Amazon ASIN 코드 (예: B0ABCD1234)
            max_reviews (int): 추출할 최대 리뷰 개수 (기본값: 20)
            detail_page_tree: 상세 페이지 HTML 트리 (폴백용, 로그인 실패 시 사용)

        Returns:
            str: 구분자(|||)로 연결된 리뷰 내용 문자열 또는 None

        Examples:
            - "Review 1 content ||| Review 2 content ||| Review 3 content"
        """
        try:
            if not item:
                print(f"[WARNING] ASIN is required for review extraction")
                return None

            # ===== 로그인 실패 상태면 상세 페이지 리뷰로 폴백 =====
            if self.login_success is False:
                print(f"[INFO] Login was not successful - trying fallback to detail page reviews")
                if detail_page_tree is not None:
                    return self.extract_reviews_from_detail_page(detail_page_tree)
                else:
                    print(f"[WARNING] No detail page tree available for fallback")
                    return data_extractor.get_no_reviews_text(self.account_name)

            # 리뷰 페이지 URL 생성
            review_url = f"https://www.amazon.com/product-reviews/{item}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"

            print(f"[INFO] Navigating to review page for ASIN: {item}")
            self.driver.get(review_url)
            time.sleep(10)  # 페이지 로딩 대기

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # ===== 페이지 오류 감지 =====
            # "Sorry, we couldn't find that page" 등 페이지 없음 오류
            page_html_lower = page_html.lower()
            if "couldn't find that page" in page_html_lower or "page not found" in page_html_lower or "sorry, we couldn" in page_html_lower:
                print(f"[WARNING] Review page not found for ASIN: {item} - skipping to next product")
                return data_extractor.get_no_reviews_text(self.account_name)

            # ===== 로그인 요구 페이지 감지 =====
            # 로그인 페이지로 리다이렉트되었는지 확인
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                print(f"[WARNING] Login required for review page")
                print(f"[INFO] Current URL: {current_url}")

                # 로그인 스크립트 실행 후 재시도
                if self.run_login_and_reload_cookies():
                    print(f"[INFO] Retrying review page after login...")
                    self.driver.get(review_url)
                    time.sleep(10)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                    # 여전히 로그인 요구 상태인지 확인
                    current_url = self.driver.current_url
                    if 'signin' in current_url or 'ap/signin' in current_url:
                        print(f"[WARNING] Still requires login after retry - using detail page fallback")
                        if detail_page_tree is not None:
                            return self.extract_reviews_from_detail_page(detail_page_tree)
                        return data_extractor.get_no_reviews_text(self.account_name)
                    # 정상 진행 (아래 리뷰 추출 로직으로 계속)
                else:
                    print(f"[WARNING] Login failed - using detail page fallback")
                    if detail_page_tree is not None:
                        return self.extract_reviews_from_detail_page(detail_page_tree)
                    return data_extractor.get_no_reviews_text(self.account_name)

            # Bot 감지 페이지 확인 (CAPTCHA 또는 "Sorry, we just need to make sure you're not a robot")
            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                print(f"[WARNING] Bot detection triggered on review page")
                print(f"[INFO] Attempting to solve CAPTCHA...")

                if self.handle_captcha():
                    # CAPTCHA 해결 후 리뷰 페이지 다시 로드
                    print(f"[OK] CAPTCHA handled, retrying review page...")
                    self.driver.get(review_url)
                    time.sleep(5)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                    # 여전히 Bot 감지 상태인지 확인
                    if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                        print(f"[WARNING] CAPTCHA still present - skipping to next product")
                        return data_extractor.get_no_reviews_text(self.account_name)
                else:
                    print(f"[WARNING] CAPTCHA handling failed - skipping to next product")
                    return data_extractor.get_no_reviews_text(self.account_name)
            # ===== 로그인 감지 끝 =====

            # 리뷰 내용 추출 (XPath - DB에서 로드)
            review_texts = tree.xpath(self.xpaths.get('detailed_review_content', {}).get('xpath') or '')

            if not review_texts:
                print(f"[WARNING] No reviews found for ASIN: {item}")
                return data_extractor.get_no_reviews_text(self.account_name)

            # 최대 개수만큼만 선택
            review_texts = review_texts[:max_reviews]

            # 각 리뷰 처리: 줄바꿈(\n, \r)을 공백 하나로 치환, 앞뒤 공백 제거
            cleaned_reviews = []
            for review in review_texts:
                if review.strip():
                    cleaned = ' '.join(review.split())
                    cleaned_reviews.append(cleaned)

            # 구분자로 연결
            result = ' ||| '.join(cleaned_reviews)

            print(f"[INFO] Extracted {len(cleaned_reviews)} reviews (Total length: {len(result)} chars)")
            return result

        except Exception as e:
            print(f"[ERROR] Failed to extract reviews: {e}")
            traceback.print_exc()
            return None

    def crawl_detail(self, product):
        """
        특정 제품의 상세 페이지 크롤링

        흐름:
        1. Detail 페이지 로드 및 모든 데이터 추출
        2. 리뷰 페이지로 이동하여 detailed_review_content 추출

        Args: product (dict): product_list 테이블에서 조회한 제품 정보

        Returns: dict: product_list 데이터 + Detail 데이터 결합
        """
        try:
            product_url = product['product_url']
            print(f"\n[INFO] Crawling detail page: {product_url}")

            # === STEP 1: Detail 페이지 로드 및 데이터 추출 ===
            self.driver.get(product_url)
            time.sleep(10)  # 페이지 로딩 대기

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # ===== Detail 페이지 로그인 감지 =====
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                print(f"[ERROR] Login required for detail page. Current URL: {current_url}")
                print(f"[INFO] Please run: python amazon_hhp_login.py to refresh cookies")
                return product  # 기본 product_list 데이터만 반환

            # Bot 감지 확인 및 CAPTCHA 자동 해결
            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                print(f"[WARNING] Bot detection triggered on detail page")
                print(f"[INFO] Attempting to solve CAPTCHA automatically...")

                if self.handle_captcha():
                    print(f"[OK] CAPTCHA handled, retrying page load...")
                    self.driver.get(product_url)
                    time.sleep(5)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                else:
                    print(f"[ERROR] CAPTCHA handling failed")
                    return product  # 기본 product_list 데이터만 반환
            # ===== 로그인 감지 끝 =====

            # === 고정값 및 URL 파싱 ===
            country = 'SEA'
            product_type = 'HHP'

            # item: product_url에서 ASIN 추출
            item = self.extract_asin_from_url(product_url)

            # === 모든 필드를 None으로 초기화 ===
            trade_in = None
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None
            count_of_reviews = None
            star_rating = None
            count_of_star_ratings = None
            summarized_review_content = None
            sku_popularity = None
            bundle = None
            retailer_membership_discounts = None
            rank_1 = None
            rank_2 = None

            # === 개별 필드 추출 (각각 try-except 처리) ===
            try:
                trade_in = self.extract_with_fallback(tree, self.xpaths.get('trade_in', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract trade_in: {e}")

            try:
                hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract hhp_carrier: {e}")

            try:
                # 기타 상세 정보
                sku_popularity = self.extract_with_fallback(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract sku_popularity: {e}")

            try:
                bundle = self.extract_with_fallback(tree, self.xpaths.get('bundle', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract bundle: {e}")

            try:
                retailer_membership_discounts = self.extract_with_fallback(tree, self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract retailer_membership_discounts: {e}")

            # === "Additional details" + "Item details" 버튼 클릭하여 정보 확장 ===
            additional_details_found = False
            try:
                # 페이지 아래로 스크롤 (Additional details 섹션까지)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(2)

                # "Additional details" 버튼 찾기
                expand_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Additional details')]/ancestor::a"))
                )

                # 버튼까지 스크롤
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", expand_button)
                time.sleep(1)

                # 클릭
                expand_button.click()
                time.sleep(1)  # 확장 애니메이션 대기
                print(f"[INFO] 'Additional details' section expanded")
                additional_details_found = True

                # "Item details" 버튼도 클릭
                try:
                    item_details_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Item details')]/ancestor::a"))
                    )
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item_details_button)
                    time.sleep(1)
                    item_details_button.click()
                    time.sleep(1)
                    print(f"[INFO] 'Item details' section expanded")
                except Exception as e:
                    print(f"[WARNING] Could not expand 'Item details': {e}")

                # 확장 후 HTML 다시 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

            except Exception as e:
                print(f"[WARNING] Could not expand 'Additional details': {e}")
                print(f"[INFO] Using fallback: static Product Information table...")

            # === hhp_storage, hhp_color, rank_1, rank_2 추출 ===
            if additional_details_found:
                # Additional details 확장된 경우: 기본 XPath로 추출
                try:
                    hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_storage: {e}")

                try:
                    hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_color: {e}")

                try:
                    # rank_1: text_content()로 자식 태그 포함 전체 텍스트 추출
                    rank1_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[1]//span[@class='a-list-item']/span")
                    if rank1_elements:
                        rank_1 = rank1_elements[0].text_content().strip()
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_1: {e}")

                try:
                    # rank_2: text_content()로 자식 태그 포함 전체 텍스트 추출
                    rank2_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[2]//span[@class='a-list-item']/span")
                    if rank2_elements:
                        rank_2 = rank2_elements[0].text_content().strip()
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_2: {e}")
            else:
                # Fallback: 정적 Product Information 테이블에서 추출
                try:
                    fallback_storage_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Memory Storage Capacity')]/following-sibling::td/text()"
                    hhp_storage = self.extract_with_fallback(tree, fallback_storage_xpath)
                    if hhp_storage:
                        print(f"[INFO] hhp_storage extracted from fallback table: {hhp_storage}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_storage (fallback): {e}")

                try:
                    fallback_color_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Color')]/following-sibling::td/text()"
                    hhp_color = self.extract_with_fallback(tree, fallback_color_xpath)
                    if hhp_color:
                        print(f"[INFO] hhp_color extracted from fallback table: {hhp_color}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract hhp_color (fallback): {e}")

                try:
                    # rank_1: text_content()로 자식 태그 포함 전체 텍스트 추출
                    rank1_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[1]//span[@class='a-list-item']/span")
                    if rank1_elements:
                        rank_1 = rank1_elements[0].text_content().strip()
                        print(f"[INFO] rank_1 extracted from fallback table: {rank_1}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_1 (fallback): {e}")

                try:
                    # rank_2: text_content()로 자식 태그 포함 전체 텍스트 추출
                    rank2_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[2]//span[@class='a-list-item']/span")
                    if rank2_elements:
                        rank_2 = rank2_elements[0].text_content().strip()
                        print(f"[INFO] rank_2 extracted from fallback table: {rank_2}")
                except Exception as e:
                    print(f"[WARNING] Failed to extract rank_2 (fallback): {e}")

            # === 리뷰 관련 필드 추출 (리뷰 섹션 이동 전에 추출) ===
            try:
                count_of_reviews_raw = self.extract_with_fallback(tree, self.xpaths.get('count_of_reviews', {}).get('xpath'))
                count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_reviews: {e}")

            try:
                star_rating_raw = self.extract_with_fallback(tree, self.xpaths.get('star_rating', {}).get('xpath'))
                star_rating = data_extractor.extract_rating(star_rating_raw, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract star_rating: {e}")

            try:
                count_of_star_ratings_xpath = self.xpaths.get('count_of_star_ratings', {}).get('xpath')
                count_of_star_ratings = data_extractor.extract_star_ratings_count(tree, count_of_reviews, count_of_star_ratings_xpath, self.account_name)
            except Exception as e:
                print(f"[WARNING] Failed to extract count_of_star_ratings: {e}")

            print(f"[INFO] Detail page extraction completed")

            # === 리뷰 섹션으로 이동 (리뷰 링크 클릭) ===
            # 상세 페이지 HTML 트리 저장 (폴백용 - 로그인 실패 시 상세 페이지 리뷰 추출)
            detail_page_tree = None
            try:
                # 페이지 맨 위로 스크롤
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                # HTML 다시 파싱 및 폴백용 저장
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                detail_page_tree = tree  # 폴백용 저장

                # 리뷰 링크 클릭
                review_link = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "acrCustomerReviewLink"))
                )
                review_link.click()
                time.sleep(2)  # 리뷰 섹션 로딩 대기

                # 리뷰 섹션 이동 후 HTML 다시 파싱
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                print(f"[INFO] Navigated to review section")
            except Exception as e:
                print(f"[WARNING] Could not navigate to review section: {e}")

            try:
                # summarized_review_content: AI 요약 리뷰 (Detail 페이지에서 추출 - 리뷰 페이지 이동 전)
                summarized_review_content = self.extract_with_fallback(tree, self.xpaths.get('summarized_review_content', {}).get('xpath'))
            except Exception as e:
                print(f"[WARNING] Failed to extract summarized_review_content: {e}")

            # === STEP 2: 리뷰 페이지로 이동하여 detailed_review_content 추출 ===
            # detail_page_tree: 로그인 실패 시 상세 페이지에서 리뷰 추출하는 폴백용
            detailed_review_content = self.extract_reviews(item, max_reviews=20, detail_page_tree=detail_page_tree) if item else None

            # Detail 페이지에서 추출할 필드 (hhp_retail_com 테이블 구조 기준)
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
            }

            # product_list 데이터 + Detail 데이터 결합
            combined_data = {**product, **detail_data}

            return combined_data

        except Exception as e:
            print(f"[ERROR] Failed to crawl detail page {product.get('product_url')}: {e}")
            traceback.print_exc()
            # 에러 발생 시 product_list 데이터만 반환 (Detail 필드는 NULL)
            return product

    def save_to_retail_com(self, products):
        """
        수집된 데이터를 hhp_retail_com 테이블에 배치 저장
        - 5개씩 배치 INSERT, 실패 시 1개씩 재시도 (2-tier retry)
        - 리뷰 데이터가 크므로 배치 크기를 5로 제한

        Args: products (list): 결합된 제품 데이터 리스트

        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        print(f"[INFO] Saving {len(products)} products to hhp_retail_com (batch mode)...\n")

        try:
            cursor = self.db_conn.cursor()

            # hhp_retail_com 테이블 구조에 맞춤
            insert_query = """
                INSERT INTO hhp_retail_com (
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

            # 배치 크기 설정 (리뷰 데이터가 크므로 5개로 제한)
            BATCH_SIZE = 5
            saved_count = 0
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            def product_to_tuple(product):
                """제품 데이터를 INSERT용 튜플로 변환"""
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
                    current_time,
                    product.get('batch_id')
                )

            # 배치 처리 (5개씩, 실패 시 1개씩 재시도)
            for batch_start in range(0, len(products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    # 1차: 5개 배치 INSERT 시도
                    values_list = [product_to_tuple(p) for p in batch_products]
                    cursor.executemany(insert_query, values_list)
                    self.db_conn.commit()
                    saved_count += len(batch_products)
                    print(f"[INFO] Inserted batch {batch_start+1}-{batch_end} ({len(batch_products)} products)")

                except Exception:
                    # 1차 실패 → 2차: 1개씩 재시도
                    print(f"[WARNING] Batch {batch_start+1}-{batch_end} failed, retrying one by one...")
                    self.db_conn.rollback()

                    for single_product in batch_products:
                        try:
                            cursor.execute(insert_query, product_to_tuple(single_product))
                            self.db_conn.commit()
                            saved_count += 1
                            print(f"[INFO] Saved individual: {single_product.get('item', 'N/A')}")
                        except Exception as single_error:
                            print(f"[ERROR] Failed to save product {single_product.get('item')}: {single_error}")
                            self.db_conn.rollback()
                            continue

            cursor.close()

            # 저장 결과 요약
            print(f"\n[SUCCESS] Saved {saved_count}/{len(products)} products to hhp_retail_com")
            for i, product in enumerate(products[:3], 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                item = product.get('item', 'N/A')
                print(f"[{i}] {sku_name[:40]}... (ASIN: {item})")
            if len(products) > 3:
                print(f"... and {len(products) - 3} more products")

            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return 0

    def run(self):
        """
        크롤러 실행

        Returns:
            bool: 성공 시 True, 실패 시 False
        """
        try:
            # 초기화
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            # product_list 테이블에서 해당 batch_id의 모든 제품 로드
            product_list = self.load_product_list()

            if not product_list:
                print(f"[WARNING] No products found for batch_id: {self.batch_id}")
                return False

            print("\n" + "="*60)
            print(f"[INFO] Starting Amazon Detail page crawling...")
            print(f"[INFO] Total products to crawl: {len(product_list)}")
            print("="*60 + "\n")

            # 모든 제품 상세 페이지 크롤링
            total_saved = 0
            crawled_products = []  # 배치 저장을 위한 버퍼
            SAVE_BATCH_SIZE = 5    # 5개씩 모아서 저장

            for i, product in enumerate(product_list, 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] Processing: {sku_name[:50]}...")

                combined_data = self.crawl_detail(product)
                crawled_products.append(combined_data)

                # 첫 제품 크롤링 후 쿠키 저장 (세션 고정, 이후 제품에서 재사용)
                if not self.cookies_loaded and i == 1:
                    self.save_cookies(self.account_name)
                    self.cookies_loaded = True

                # 5개씩 모아서 배치 저장 (2-tier retry: 5개 실패 시 1개씩)
                if len(crawled_products) >= SAVE_BATCH_SIZE:
                    saved_count = self.save_to_retail_com(crawled_products)
                    total_saved += saved_count
                    crawled_products = []  # 버퍼 초기화

                # 페이지 간 대기
                time.sleep(5)

            # 남은 제품 저장 (5개 미만)
            if crawled_products:
                saved_count = self.save_to_retail_com(crawled_products)
                total_saved += saved_count

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Amazon Detail Crawler Finished")
            print(f"[RESULT] Total products processed: {len(product_list)}")
            print(f"[RESULT] Total products saved: {total_saved}")
            print(f"[RESULT] Batch ID: {self.batch_id}")
            print("="*60 + "\n")

            return True

        except Exception as e:
            print(f"[ERROR] Crawler execution failed: {e}")
            traceback.print_exc()
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
    개별 실행 시 진입점
    - 하드코딩된 batch_id 기준으로 모든 제품 크롤링
    """
    crawler = AmazonDetailCrawler(batch_id=None)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Amazon Detail Crawler completed successfully")
    else:
        print("\n[FAILED] Amazon Detail Crawler failed")

    return success


if __name__ == '__main__':
    success = main()
    if not success:
        exit(1)