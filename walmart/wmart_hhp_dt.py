"""
Walmart Detail 페이지 크롤러 (Playwright 기반)

================================================================================
실행 모드
================================================================================
- 개별 실행: batch_id 없이 실행 시 기본값 사용
- 통합 크롤러: batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR에서 수집한 모든 제품 처리
- CAPTCHA 자동 해결 기능 포함

================================================================================
저장 테이블
================================================================================
- hhp_retail_com (상세 정보 + 리뷰)
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

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

from common.base_crawler import BaseCrawler
from common.data_extractor import extract_numeric_value


class WalmartDetailCrawler(BaseCrawler):
    """
    Walmart Detail 페이지 크롤러 (Playwright 기반)
    """

    def __init__(self, batch_id=None, test_mode=False):
        """초기화. batch_id: 통합 크롤러에서 전달, test_mode: 테스트 모드 여부"""
        super().__init__()
        self.account_name = 'Walmart'
        self.page_type = 'detail'
        self.batch_id = batch_id
        self.test_mode = test_mode
        self.standalone = batch_id is None

        # Selenium/undetected-chromedriver 객체
        self.driver = None
        self.wait = None

    def setup_browser(self):
        """undetected-chromedriver 브라우저 설정 (TV 크롤러와 동일)"""
        try:
            print("[INFO] undetected-chromedriver 설정 중 (TV 크롤러와 동일한 방식)...")

            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--start-maximized')
            options.add_argument('--disable-infobars')
            options.add_argument('--window-size=1920,1080')

            self.driver = uc.Chrome(options=options, use_subprocess=True)
            self.wait = WebDriverWait(self.driver, 20)

            print("[OK] undetected-chromedriver 설정 완료")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
            traceback.print_exc()
            return False

    def add_random_mouse_movements(self):
        """인간처럼 보이기 위한 랜덤 마우스 움직임"""
        try:
            actions = ActionChains(self.driver)
            for _ in range(random.randint(2, 4)):
                x_offset = random.randint(-100, 100)
                y_offset = random.randint(-100, 100)
                actions.move_by_offset(x_offset, y_offset)
                actions.perform()
                actions.reset_actions()
                time.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass  # 마우스 움직임 실패 시 무시

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present (TV 크롤러와 동일)"""
        try:
            print("[INFO] Checking for CAPTCHA...")

            # Check page content for CAPTCHA keywords
            page_content = self.driver.page_source.lower()
            if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha', 'human verification']):
                print("[WARNING] CAPTCHA keywords found in page")
                print("[INFO] CAPTCHA detection - waiting 60 seconds for manual intervention...")
                print("[INFO] Please solve CAPTCHA manually if present")

                # Save screenshot for debugging
                try:
                    self.driver.save_screenshot(f"captcha_screen_{int(time.time())}.png")
                    print("[INFO] Screenshot saved for debugging")
                except:
                    pass

                time.sleep(60)
                return True
            else:
                print("[INFO] No CAPTCHA detected")
                return True

        except Exception as e:
            print(f"[WARNING] CAPTCHA handling error: {e}")
            return True

    def handle_sorry_page(self, max_button_attempts=3, max_refresh_attempts=5):
        """
        Sorry 페이지 감지 및 Try Again 버튼 클릭 처리

        Args:
            max_button_attempts: Try Again 버튼 클릭 최대 시도 횟수
            max_refresh_attempts: 버튼 실패 후 새로고침 최대 시도 횟수

        Returns:
            bool: 페이지가 정상으로 복구되면 True, 실패하면 False
        """
        try:
            # Walmart Sorry 페이지 실제 문구 (정확한 매칭)
            sorry_keywords = [
                "we're having technical issues",
                "we'll be back in a flash",
                "this page isn't available right now",
                "this page isn't available",
                "return to home"
            ]

            # 1단계: Try Again 버튼 클릭 시도 (최대 max_button_attempts회)
            for attempt in range(max_button_attempts):
                page_content = self.driver.page_source.lower()

                if not any(keyword in page_content for keyword in sorry_keywords):
                    if attempt > 0:
                        print("[OK] Sorry 페이지 해결됨 (버튼 클릭)")
                    return True

                print(f"[WARNING] Sorry 페이지 감지! (버튼 시도 {attempt + 1}/{max_button_attempts})")

                # Try Again 버튼 찾기 및 클릭 시도
                try_again_clicked = False
                try_again_selectors = [
                    "//button[contains(text(), 'Try again')]",
                    "//button[contains(text(), 'try again')]",
                    "//button[contains(text(), 'Try Again')]",
                    "//a[contains(text(), 'Try again')]",
                    "//a[contains(text(), 'try again')]",
                    "//button[contains(@class, 'retry')]",
                    "//button[contains(@class, 'try-again')]",
                    "//*[contains(text(), 'Try again') and (self::button or self::a)]",
                ]

                for selector in try_again_selectors:
                    try:
                        try_again_button = self.driver.find_element(By.XPATH, selector)
                        if try_again_button.is_displayed():
                            print(f"[INFO] Try Again 버튼 발견: {selector}")
                            try_again_button.click()
                            try_again_clicked = True
                            print("[OK] Try Again 버튼 클릭 완료")
                            time.sleep(random.uniform(3, 5))
                            break
                    except:
                        continue

                # 버튼을 못 찾았으면 이 단계에서 새로고침 1회 시도
                if not try_again_clicked:
                    print("[INFO] Try Again 버튼을 찾지 못함, 새로고침 시도...")
                    self.driver.refresh()
                    time.sleep(random.uniform(5, 8))

            # 2단계: 버튼 클릭으로 해결 안 되면 새로고침 추가 시도 (최대 max_refresh_attempts회)
            page_content = self.driver.page_source.lower()
            if any(keyword in page_content for keyword in sorry_keywords):
                print(f"[WARNING] 버튼 클릭 실패, 새로고침 시도 시작 (최대 {max_refresh_attempts}회)...")

                for refresh_attempt in range(max_refresh_attempts):
                    print(f"[INFO] 새로고침 시도 {refresh_attempt + 1}/{max_refresh_attempts}...")
                    self.driver.refresh()
                    time.sleep(random.uniform(5, 8))

                    page_content = self.driver.page_source.lower()
                    if not any(keyword in page_content for keyword in sorry_keywords):
                        print(f"[OK] Sorry 페이지 해결됨 (새로고침 {refresh_attempt + 1}회)")
                        return True

            # 최종 확인
            page_content = self.driver.page_source.lower()
            if any(keyword in page_content for keyword in sorry_keywords):
                print(f"[ERROR] Sorry 페이지 해결 실패 (버튼 {max_button_attempts}회 + 새로고침 {max_refresh_attempts}회 시도 후)")

                # 최종 실패 시에만 스크린샷 저장
                try:
                    capture_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'capture')
                    os.makedirs(capture_dir, exist_ok=True)
                    screenshot_path = os.path.join(capture_dir, f"sorry_page_failed_{int(time.time())}.png")
                    self.driver.save_screenshot(screenshot_path)
                    print(f"[INFO] 스크린샷 저장됨: {screenshot_path}")
                except:
                    pass

                return False

            return True

        except Exception as e:
            print(f"[WARNING] Sorry page handling error: {e}")
            traceback.print_exc()
            return True  # 에러 발생해도 계속 진행

    def extract_rating_from_header(self, tree):
        """상단 reviews-and-ratings 영역에서 별점과 별점 수 추출
        예: '4.3 stars out of 8968 reviews' → ('4.3', '8968')
        """
        try:
            xpath = self.xpaths.get('header_rating', {}).get('xpath')
            if not xpath:
                return None, None
            results = tree.xpath(xpath)

            if results:
                text = results[0].strip()
                # 정규식: "4.3 stars out of 8968 reviews"
                match = re.match(r'([\d.]+)\s*stars?\s*out\s*of\s*([\d,]+)\s*reviews?', text, re.IGNORECASE)
                if match:
                    star_rating = match.group(1)  # "4.3"
                    try:
                        count_of_star_ratings = '{:,}'.format(int(match.group(2).replace(',', '')))  # "8,968"
                    except ValueError:
                        count_of_star_ratings = match.group(2)  # 원본 값 유지
                    return star_rating, count_of_star_ratings

            return None, None
        except Exception as e:
            print(f"[WARNING] extract_rating_from_header failed: {e}")
            traceback.print_exc()
            return None, None

    def extract_ratings_count(self, tree):
        """Walmart 별점 개수 추출 (예: '1,234 ratings' → '1,234', '12.5K ratings' → '12.5K')"""
        text = self.safe_extract(tree, 'count_of_star_ratings')
        if text:
            # 12.5K, 3.5K 등 K 포함 숫자 또는 1,234 등 쉼표 포함 숫자 추출
            match = re.search(r'([\d,]+\.?\d*K?)', text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def extract_review_count(self, tree):
        """Walmart 리뷰 개수 추출 (예: '3,572 reviews' → '3,572', '3.5K reviews' → '3.5K')"""
        text = self.safe_extract(tree, 'count_of_reviews')
        if text:
            # 3.5K, 12.5K 등 K 포함 숫자 또는 3,572 등 쉼표 포함 숫자 추출
            match = re.search(r'([\d,]+\.?\d*K?)', text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def extract_star_rating(self, tree):
        """Walmart 별점 추출 (예: '4.5 out of 5 stars' → '4.5')"""
        text = self.safe_extract(tree, 'star_rating')
        return extract_numeric_value(text, include_comma=False, include_decimal=True)

    def initialize_session(self):
        """세션 초기화: walmart.com 방문 (example.com 경유 제거)"""
        try:
            print("[INFO] 세션 초기화 중...")

            # 1단계: Walmart 메인 페이지 방문 (쿠키/세션 생성)
            print("[INFO] Step 1/2: Walmart 메인 페이지 방문...")
            self.driver.get('https://www.walmart.com')
            time.sleep(random.uniform(8, 12))

            # CAPTCHA 체크
            self.handle_captcha()

            # Sorry 페이지 체크
            self.handle_sorry_page()

            # 마우스 움직임 및 스크롤
            self.add_random_mouse_movements()
            for _ in range(random.randint(2, 4)):
                scroll_y = random.randint(200, 500)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_y})")
                time.sleep(random.uniform(1, 2))
                self.add_random_mouse_movements()

            # 위로 스크롤
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(random.uniform(2, 3))

            print("[OK] 세션 초기화 완료")
            return True

        except Exception as e:
            print(f"[WARNING] 세션 초기화 실패 (계속 진행): {e}")
            return True  # 실패해도 계속 진행

    def close_banner(self):
        """배너 감지 및 닫기 (회색 배경 div 감지 시 우측 클릭)"""
        try:
            banner_xpath = self.xpaths.get('banner', {}).get('xpath')
            if not banner_xpath:
                return
            try:
                banner = self.driver.find_element(By.XPATH, banner_xpath)
                if banner.is_displayed():
                    print("[INFO] 배너 감지됨, 닫기 시도...")
                    # 화면 우측 절반의 중앙 클릭
                    window_size = self.driver.get_window_size()
                    click_x = int(window_size['width'] * 0.75)
                    click_y = int(window_size['height'] * 0.5)
                    actions = ActionChains(self.driver)
                    actions.move_by_offset(click_x, click_y).click().perform()
                    time.sleep(random.uniform(0.5, 1))
                    print("[OK] 배너 닫기 완료")
            except:
                pass
        except Exception:
            pass

    def extract_item(self, product_url):
        """URL에서 item ID 추출"""
        if not product_url:
            return None
        try:
            # /ip/product-name/12345 패턴
            ip_match = re.search(r'/ip/[^/]+/(\d+)', product_url)
            if ip_match:
                return ip_match.group(1)
            # URL 인코딩된 패턴 %2F12345%3F
            encoded_match = re.search(r'%2F(\d+)%3F', product_url)
            if encoded_match:
                return encoded_match.group(1)
            # URL 마지막 세그먼트에서 숫자 추출
            last_segment = product_url.rstrip('/').split('/')[-1]
            item_with_params = last_segment.split('?')[0]
            number_match = re.search(r'(\d+)$', item_with_params)
            if number_match:
                return number_match.group(1)
        except Exception as e:
            print(f"[WARNING] Failed to extract item: {e}")
            traceback.print_exc()
        return None

    def scroll_to_bottom(self):
        """페이지 80% 하단까지 빠른 스크롤 (콘텐츠 로드용)"""
        try:
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            target_position = int(total_height * 0.8)  # 80%까지만
            current_position = 0

            while current_position < target_position:
                # 빠르지만 자연스러운 스크롤 (300~500px)
                scroll_step = random.randint(300, 500)
                current_position = min(current_position + scroll_step, target_position)
                self.driver.execute_script(f"window.scrollTo(0, {current_position})")
                time.sleep(random.uniform(0.3, 0.6))

                # 10% 확률로 마우스 움직임
                if random.random() < 0.1:
                    self.add_random_mouse_movements()

            time.sleep(random.uniform(0.5, 1))
        except Exception as e:
            print(f"[WARNING] Scroll failed: {e}")

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → 브라우저 설정 → batch_id 설정"""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 2. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 3. 브라우저 설정 (undetected-chromedriver)
        if not self.setup_browser():
            print("[ERROR] Initialize failed: Browser setup failed")
            return False

        # 4. 세션 초기화 (example.com → walmart.com → 카테고리)
        self.initialize_session()

        # 5. batch_id 설정
        if not self.batch_id:
            self.batch_id = 'w_20251204_015010'

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}")
        return True

    def load_product_list(self):
        """wmart_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회"""
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT 
                    retailer_sku_name, final_sku_price, original_sku_price,
                    offer, pick_up_availability, shipping_availability,
                    delivery_availability, sku_status, retailer_membership_discounts,
                    available_quantity_for_purchase, inventory_status,
                    main_rank, bsr_rank, product_url, calendar_week,
                    crawl_strdatetime, page_type
                FROM wmart_hhp_product_list
                WHERE account_name = %s AND batch_id = %s AND product_url IS NOT NULL
                ORDER BY id
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            product_list = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'retailer_sku_name': row[0],
                    'final_sku_price': row[1],
                    'original_sku_price': row[2],
                    'offer': row[3],
                    'pick_up_availability': row[4],
                    'shipping_availability': row[5],
                    'delivery_availability': row[6],
                    'sku_status': row[7],
                    'retailer_membership_discounts': row[8],
                    'available_quantity_for_purchase': row[9],
                    'inventory_status': row[10],
                    'main_rank': row[11],
                    'bsr_rank': row[12],
                    'product_url': row[13],
                    'calendar_week': row[14],
                    'crawl_strdatetime': row[15],
                    'page_type': row[16]
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

    def crawl_detail(self, product, first_product=False):
        """제품 상세 페이지 크롤링"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                print("[WARNING] Product URL is missing")
                return product

            self.driver.get(product_url)
            time.sleep(random.uniform(3, 5))

            # 마우스 움직임 추가
            self.add_random_mouse_movements()

            if first_product:
                if not self.handle_captcha():
                    print("[WARNING] CAPTCHA handling failed")
                time.sleep(random.uniform(1, 2))

            # Sorry 페이지 체크 (상세 페이지 접근 시)
            if not self.handle_sorry_page():
                print("[WARNING] 상세 페이지 Sorry 감지 - 기본 정보로 진행")

            # 전체 콘텐츠 로드: 하단까지 스크롤 → 배너 닫기 → 맨 위로 복귀
            self.scroll_to_bottom()
            self.close_banner()
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(random.uniform(0.5, 1))

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # item ID 추출 (리다이렉트된 실제 URL에서)
            actual_url = self.driver.current_url
            item = self.extract_item(actual_url)

            # "No ratings yet" 체크 (리뷰 없는 상품)
            no_ratings_yet = False
            no_ratings_text = self.safe_extract(tree, 'no_ratings_yet')
            if no_ratings_text:
                # 괄호만 제거 (괄호 안 내용은 유지)
                no_ratings_text_clean = no_ratings_text.replace('(', '').replace(')', '').strip()
                if 'No ratings yet' in no_ratings_text_clean:
                    no_ratings_yet = True
                    print("[INFO] No ratings yet 감지 - 리뷰 필드 스킵 예정")

            # 상단 별점/별점 수 추출 (No ratings yet이 아닌 경우에만)
            header_star_rating = None
            header_count_of_star_ratings = None
            if not no_ratings_yet:
                header_star_rating, header_count_of_star_ratings = self.extract_rating_from_header(tree)

            # 추가 필드 추출
            number_of_ppl_purchased_yesterday = self.safe_extract(tree, 'number_of_ppl_purchased_yesterday')
            number_of_ppl_added_to_carts = self.safe_extract(tree, 'number_of_ppl_added_to_carts')
            sku_popularity = self.safe_extract_join(tree, 'sku_popularity', separator=", ")
            savings = self.safe_extract(tree, 'savings')
            discount_type = self.safe_extract(tree, 'discount_type')

            # shipping_info 추출 (첫 번째 shipping-tile만 사용)
            shipping_info = None
            try:
                shipping_info_xpath = self.xpaths.get('shipping_info', {}).get('xpath')
                if shipping_info_xpath:
                    shipping_info_raw = tree.xpath(shipping_info_xpath)
                    if isinstance(shipping_info_raw, list):
                        # 텍스트 조합 후 중복 제거
                        texts = [text.strip() for text in shipping_info_raw if text.strip()]
                        shipping_info = ' '.join(texts)
                        # 중복 패턴 제거 (예: "Shipping Arrives tomorrow Free Shipping Arrives tomorrow Free" -> "Shipping Arrives tomorrow Free")
                        if shipping_info:
                            half_len = len(shipping_info) // 2
                            first_half = shipping_info[:half_len].strip()
                            second_half = shipping_info[half_len:].strip()
                            # 앞뒤가 동일하면 앞부분만 사용
                            if first_half and first_half == second_half:
                                shipping_info = first_half
                    else:
                        shipping_info = shipping_info_raw
            except Exception:
                pass

            # 스펙 정보 추출
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None

            try:
                spec_button_xpath = self.xpaths.get('spec_button', {}).get('xpath')
                spec_close_button_xpath = self.xpaths.get('spec_close_button', {}).get('xpath')

                if spec_button_xpath:
                    spec_button_found = False
                    spec_button = None
                    for retry in range(3):
                        # 100px씩 스크롤하며 spec_button 찾기
                        for _ in range(3):
                            try:
                                spec_button = self.driver.find_element(By.XPATH, spec_button_xpath)
                                if spec_button.is_displayed():
                                    spec_button_found = True
                                    break
                            except:
                                pass
                            self.driver.execute_script("window.scrollBy(0, 100)")
                            time.sleep(random.uniform(0.3, 0.5))

                        if spec_button_found:
                            break
                        else:
                            print(f"[WARNING] spec_button 찾기 실패 (시도 {retry + 1}/3)")
                            time.sleep(random.uniform(1, 2))
                            # HTML 다시 파싱
                            page_html = self.driver.page_source
                            tree = html.fromstring(page_html)

                    if spec_button_found:
                        # 요소를 화면 중앙에 위치시킴 (상단 헤더에 가려지지 않도록)
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", spec_button)
                        time.sleep(random.uniform(1, 2))

                        # 클릭 시도 (일반 클릭 실패 시 JavaScript로 강제 클릭)
                        try:
                            spec_button.click()
                        except Exception:
                            # ElementClickInterceptedException 발생 시 JS로 클릭
                            self.driver.execute_script("arguments[0].click();", spec_button)

                        try:
                            if spec_close_button_xpath:
                                self.wait.until(EC.visibility_of_element_located((By.XPATH, spec_close_button_xpath)))
                            time.sleep(random.uniform(0.5, 1.5))
                        except Exception:
                            time.sleep(random.uniform(1, 3))

                        modal_html = self.driver.page_source
                        modal_tree = html.fromstring(modal_html)

                        hhp_carrier = self.safe_extract(modal_tree, 'hhp_carrier')
                        hhp_storage = self.safe_extract(modal_tree, 'hhp_storage')
                        hhp_color = self.safe_extract(modal_tree, 'hhp_color')

                        if spec_close_button_xpath:
                            try:
                                close_button = self.driver.find_element(By.XPATH, spec_close_button_xpath)
                                if close_button.is_displayed():
                                    close_button.click()
                                    time.sleep(random.uniform(1, 2))
                            except:
                                pass
            except Exception as e:
                print(f"[ERROR] spec_button 처리 실패: {e}")
                traceback.print_exc()

            # 유사 제품 추출 (빠른 스크롤로 최적화)
            retailer_sku_name_similar = None
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')

            if similar_products_container_xpath:
                try:
                    # 페이지 60% 위치로 빠르게 이동 (similar products는 보통 중간~하단에 위치)
                    scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                    self.driver.execute_script(f"window.scrollTo(0, {int(scroll_height * 0.6)})")
                    time.sleep(random.uniform(0.3, 0.5))

                    # similar section 찾기 시도 (최대 3회 스크롤)
                    for _ in range(3):
                        try:
                            similar_section = self.driver.find_element(By.XPATH, similar_products_container_xpath)
                            if similar_section.is_displayed():
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", similar_section)
                                time.sleep(random.uniform(0.3, 0.5))
                                break
                        except:
                            pass
                        self.driver.execute_script("window.scrollBy(0, 400)")
                        time.sleep(random.uniform(0.2, 0.4))

                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                    product_cards = tree.xpath(similar_products_container_xpath)
                    if product_cards:
                        similar_product_names = []
                        name_xpath = self.xpaths.get('similar_product_name', {}).get('xpath')

                        for card in product_cards:
                            try:
                                if name_xpath:
                                    name_results = card.xpath(name_xpath)
                                    if name_results:
                                        similar_product_names.append(name_results[0])
                            except Exception:
                                continue

                        retailer_sku_name_similar = ' ||| '.join(similar_product_names) if similar_product_names else None
                except Exception:
                    pass

            # 리뷰 관련 필드
            count_of_reviews = None
            star_rating = None
            count_of_star_ratings = None

            if no_ratings_yet:
                # "No ratings yet" - 리뷰 없음
                count_of_reviews = '0'
                star_rating = 'No ratings yet'
                count_of_star_ratings = 'No ratings yet'
            else:
                # 1. 스크롤 전 상단에서 추출한 값 우선 사용
                star_rating = header_star_rating
                count_of_star_ratings = header_count_of_star_ratings

                for retry in range(3):
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                    # 2. 상단 추출 실패 시 기존 하단 방식으로 추출
                    if star_rating is None or count_of_star_ratings is None:
                        star_rating = self.extract_star_rating(tree)
                        count_of_star_ratings = self.extract_ratings_count(tree)

                    count_of_reviews = self.extract_review_count(tree)

                    # 3개 필드 모두 추출 성공 시 종료
                    if count_of_reviews is not None and star_rating is not None and count_of_star_ratings is not None:
                        break

                    # 실패 시 재시도 전 대기
                    if retry < 2:
                        print(f"[WARNING] 리뷰 필드 추출 실패 (시도 {retry + 1}/3) - 재시도 중...")
                        time.sleep(random.uniform(1, 2))

            # 리뷰 상세 추출
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            if reviews_button_xpath:
                review_button_found = False

                self.driver.execute_script("window.scrollTo(0, 0)")
                time.sleep(random.uniform(0.5, 1.5))

                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                current_position = 0

                # fallback XPath 로드 (|로 구분된 문자열)
                reviews_button_fallback = self.xpaths.get('reviews_button_fallback', {}).get('xpath', '')
                fallback_xpaths = reviews_button_fallback.split('|') if reviews_button_fallback else []

                reviews_button_xpaths = [reviews_button_xpath] + fallback_xpaths

                scroll_count = 0
                max_scroll_attempts = 50  # 무한 스크롤 방지
                while current_position < scroll_height and scroll_count < max_scroll_attempts:
                    scroll_count += 1

                    # 현재 위치에서 버튼 찾기 시도 (scrollIntoView 없이)
                    for xpath in reviews_button_xpaths:
                        try:
                            review_button = self.driver.find_element(By.XPATH, xpath)
                            if review_button.is_displayed():
                                # 버튼이 보이면 클릭 시도 (현재 위치에서)
                                try:
                                    # 먼저 일반 클릭 시도
                                    review_button.click()
                                    review_button_found = True
                                    time.sleep(random.uniform(3, 7))
                                    break
                                except Exception:
                                    # 클릭 실패 시 scrollIntoView 후 재시도
                                    try:
                                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", review_button)
                                        time.sleep(random.uniform(0.5, 1))
                                        review_button.click()
                                        review_button_found = True
                                        time.sleep(random.uniform(3, 7))
                                        break
                                    except Exception:
                                        # JS 클릭 시도
                                        try:
                                            self.driver.execute_script("arguments[0].click();", review_button)
                                            review_button_found = True
                                            time.sleep(random.uniform(3, 7))
                                            break
                                        except Exception:
                                            continue
                        except Exception:
                            continue

                    if review_button_found:
                        break

                    # 버튼을 못 찾았으면 스크롤
                    scroll_step = random.randint(300, 400)
                    current_position += scroll_step
                    self.driver.execute_script(f"window.scrollTo(0, {current_position})")
                    time.sleep(random.uniform(0.3, 0.5))

                if review_button_found:
                    try:
                        # 리뷰 상세 페이지 이동 후 Sorry 페이지 체크
                        if not self.handle_sorry_page():
                            print("[WARNING] 리뷰 페이지 Sorry 감지 - 리뷰 수집 중단")
                        else:
                            detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                            if detailed_review_xpath:
                                try:
                                    self.wait.until(EC.visibility_of_element_located((By.XPATH, detailed_review_xpath)))
                                    time.sleep(random.uniform(1, 3))
                                except Exception:
                                    time.sleep(random.uniform(3, 7))

                                all_reviews = []
                                current_page = 1
                                max_reviews = 20

                                while len(all_reviews) < max_reviews:
                                    if current_page > 1:
                                        time.sleep(random.uniform(1, 3))

                                    # 페이지 이동 후 Sorry 페이지 체크
                                    if not self.handle_sorry_page():
                                        print(f"[WARNING] 리뷰 페이지 {current_page} Sorry 감지 - 리뷰 수집 중단")
                                        break

                                    page_html = self.driver.page_source
                                    tree = html.fromstring(page_html)

                                    reviews_list = tree.xpath(detailed_review_xpath)

                                    if reviews_list:
                                        for review in reviews_list:
                                            if len(all_reviews) >= max_reviews:
                                                break

                                            if hasattr(review, 'text_content'):
                                                review_text = review.text_content()
                                            else:
                                                review_text = review

                                            cleaned_review = ' '.join(review_text.split())
                                            all_reviews.append(cleaned_review)

                                    if len(all_reviews) >= max_reviews:
                                        break

                                    try:
                                        next_page_num = current_page + 1
                                        review_pagination_template = self.xpaths.get('review_pagination', {}).get('xpath', '')
                                        if not review_pagination_template:
                                            break
                                        next_page_xpath = review_pagination_template.replace('{page_num}', str(next_page_num))
                                        next_page_button = self.driver.find_element(By.XPATH, next_page_xpath)

                                        if next_page_button.is_displayed():
                                            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_page_button)
                                            time.sleep(random.uniform(0.5, 1.5))
                                            next_page_button.click()
                                            time.sleep(random.uniform(2, 4))

                                            # 다음 페이지 클릭 후 Sorry 페이지 체크
                                            if not self.handle_sorry_page():
                                                print(f"[WARNING] 리뷰 페이지 {next_page_num} 이동 중 Sorry 감지 - 수집된 리뷰로 진행")
                                                break

                                            current_page = next_page_num
                                        else:
                                            break
                                    except Exception:
                                        break

                                if all_reviews:
                                    formatted_reviews = [f"review{idx} - {review}" for idx, review in enumerate(all_reviews, 1)]
                                    detailed_review_content = ' ||| '.join(formatted_reviews)
                                    print(f"[INFO] Reviews: {len(all_reviews)}")
                    except Exception as e:
                        print(f"[WARNING] Failed to extract reviews: {e}")


            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'number_of_ppl_purchased_yesterday': number_of_ppl_purchased_yesterday,
                'number_of_ppl_added_to_carts': number_of_ppl_added_to_carts,
                'sku_popularity': sku_popularity,
                'savings': savings,
                'discount_type': discount_type,
                'shipping_info': shipping_info,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'hhp_carrier': hhp_carrier,
                'retailer_sku_name_similar': retailer_sku_name_similar,
                'detailed_review_content': detailed_review_content,
                'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            return combined_data

        except Exception as e:
            print(f"[ERROR] Failed to crawl detail page: {e}")
            traceback.print_exc()
            return product

    def save_to_retail_com(self, products):
        """DB 저장: RETRY_SIZE 배치 → 1개씩 (2-tier retry)"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()

            # 테스트 모드면 test_hhp_retail_com, 통합 크롤러면 hhp_retail_com
            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'

            insert_query = f"""
                INSERT INTO {table_name} (
                    country, product, item, account_name, page_type,
                    count_of_reviews, retailer_sku_name, product_url,
                    star_rating, count_of_star_ratings,
                    number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts,
                    sku_popularity, savings, discount_type,
                    final_sku_price, original_sku_price, offer,
                    pick_up_availability, shipping_availability, delivery_availability,
                    shipping_info, available_quantity_for_purchase, inventory_status,
                    sku_status, retailer_membership_discounts,
                    hhp_storage, hhp_color, hhp_carrier,
                    retailer_sku_name_similar, detailed_review_content,
                    main_rank, bsr_rank, calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """

            RETRY_SIZE = 5
            total_saved = 0

            def product_to_tuple(product):
                return (
                    'SEA', 'HHP', product.get('item'),
                    self.account_name, product.get('page_type'),
                    product.get('count_of_reviews'), product.get('retailer_sku_name'),
                    product.get('product_url'), product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('number_of_ppl_purchased_yesterday'),
                    product.get('number_of_ppl_added_to_carts'),
                    product.get('sku_popularity'), product.get('savings'),
                    product.get('discount_type'), product.get('final_sku_price'),
                    product.get('original_sku_price'), product.get('offer'),
                    product.get('pick_up_availability'),
                    product.get('shipping_availability'),
                    product.get('delivery_availability'),
                    product.get('shipping_info'),
                    product.get('available_quantity_for_purchase'),
                    product.get('inventory_status'), product.get('sku_status'),
                    product.get('retailer_membership_discounts'),
                    product.get('hhp_storage'), product.get('hhp_color'),
                    product.get('hhp_carrier'),
                    product.get('retailer_sku_name_similar'),
                    product.get('detailed_review_content'),
                    product.get('main_rank'), product.get('bsr_rank'),
                    product.get('calendar_week'), product.get('crawl_strdatetime'), self.batch_id
                )

            def save_batch(batch_products):
                values_list = [product_to_tuple(p) for p in batch_products]
                cursor.executemany(insert_query, values_list)
                self.db_conn.commit()
                return len(batch_products)

            for batch_start in range(0, len(products), RETRY_SIZE):
                batch_end = min(batch_start + RETRY_SIZE, len(products))
                batch_products = products[batch_start:batch_end]

                try:
                    total_saved += save_batch(batch_products)

                except Exception:
                    self.db_conn.rollback()

                    for single_product in batch_products:
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
        """실행: initialize() → 제품별 crawl_detail() → save_to_retail_com() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[WARNING] No products found")
                return False

            print(f"[INFO] Total products to crawl: {len(product_list)}")

            total_saved = 0

            for i, product in enumerate(product_list, 1):
                try:
                    sku_name = product.get('retailer_sku_name') or 'N/A'
                    print(f"[{i}/{len(product_list)}] {sku_name[:50]}...")

                    first_product = (i == 1)
                    combined_data = self.crawl_detail(product, first_product=first_product)

                    if combined_data:
                        saved_count = self.save_to_retail_com([combined_data])
                        total_saved += saved_count

                    time.sleep(random.uniform(2, 3))  # 제품 간 대기

                except Exception as e:
                    print(f"[ERROR] Product {i} failed: {e}")
                    continue

            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'
            print(f"[DONE] Total: {len(product_list)}, Saved: {total_saved}, Table: {table_name}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 브라우저 리소스 정리
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            if self.db_conn:
                self.db_conn.close()
            if self.standalone:
                input("Press Enter to exit...")


def main():
    """개별 실행 진입점 (테스트 모드, 기본 배치 ID 사용)"""
    crawler = WalmartDetailCrawler(batch_id=None, test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
