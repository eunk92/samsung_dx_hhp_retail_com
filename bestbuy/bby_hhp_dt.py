"""
BestBuy Detail 페이지 크롤러
- 개별 실행: test_mode=True (기본값), batch_id 입력
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR/Trend에서 수집한 모든 제품 처리
- hhp_retail_com 및 bby_hhp_mst 테이블에 저장
"""

import sys
import os
import time
import random
import traceback
from datetime import datetime
from lxml import html
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common import data_extractor


class BestBuyDetailCrawler(BaseCrawler):
    """
    BestBuy Detail 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None):
        """
        초기화

        Args:
            test_mode (bool): 테스트 모드 (기본값: True)
                             - True: 1개 제품만 크롤링
                             - False: 전체 제품 크롤링
            batch_id (str): 배치 ID (기본값: None)
                           - None: 기본값 사용 (b_20251125_012112)
                           - 문자열: 통합 크롤러에서 전달된 배치 ID 사용
        """
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'
        self.batch_id = batch_id

        # 테스트 설정
        self.test_count = 1  # 테스트 모드에서 처리할 제품 수

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] BestBuy Detail Crawler Initialization")
        print(f"[INFO] Test Mode: {'ON (1 product)' if self.test_mode else 'OFF (all products)'}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. XPath 셀렉터 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        # 3. WebDriver 설정
        self.setup_driver()

        # 4. 배치 ID 설정 (없으면 기본값 사용)
        if not self.batch_id:
            self.batch_id = 'b_20251125_014141'
            print(f"[INFO] Using default Batch ID: {self.batch_id}")
        else:
            print(f"[INFO] Batch ID received: {self.batch_id}")

        # 5. 오래된 로그 정리
        self.cleanup_old_logs()

        return True

    def load_product_list(self):
        """
        bby_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회

        Returns: list: 제품 정보 리스트
        """
        try:
            cursor = self.db_conn.cursor()

            # product_list에서 hhp_retail_com으로 매핑할 필드 조회
            query = """
                SELECT
                    page_type,
                    retailer_sku_name,
                    final_sku_price,
                    savings,
                    comparable_pricing as original_sku_price,
                    offer,
                    pick_up_availability,
                    shipping_availability,
                    delivery_availability,
                    sku_status,
                    promotion_type,
                    main_rank,
                    bsr_rank,
                    trend_rank,
                    product_url,
                    calendar_week
                FROM bby_hhp_product_list
                WHERE account_name = %s
                  AND batch_id = %s
                  AND product_url IS NOT NULL
                ORDER BY product_url, id
            """

            # 테스트 모드일 때 LIMIT 추가
            if self.test_mode:
                query += f" LIMIT {self.test_count}"

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()

            cursor.close()

            # 딕셔너리 변환 (SELECT 컬럼 순서와 일치)
            product_list = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'page_type': row[0],              # page_type
                    'retailer_sku_name': row[1],      # retailer_sku_name
                    'final_sku_price': row[2],        # final_sku_price
                    'savings': row[3],                # savings
                    'original_sku_price': row[4],     # comparable_pricing as original_sku_price
                    'offer': row[5],                  # offer
                    'pick_up_availability': row[6],   # pick_up_availability
                    'shipping_availability': row[7],  # shipping_availability
                    'delivery_availability': row[8],  # delivery_availability
                    'sku_status': row[9],             # sku_status
                    'promotion_type': row[10],        # promotion_type
                    'main_rank': row[11],             # main_rank
                    'bsr_rank': row[12],              # bsr_rank
                    'trend_rank': row[13],            # trend_rank
                    'product_url': row[14],           # product_url
                    'calendar_week': row[15]          # calendar_week
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products from bby_hhp_product_list")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

    def extract_item_from_url(self, product_url):
        """
        BestBuy product_url에서 item (SKU ID) 추출
        Args: product_url (str): 제품 URL
        Returns: str: 추출된 SKU ID 또는 None
        """
        import re

        if not product_url:
            return None

        try:
            # Step 1: /sku/숫자 또는 /sku/숫자/openbox?... 패턴 제거
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?$', '', product_url)

            # Step 2: 쿼리 파라미터 제거 (? 이후 제거)
            cleaned_url = cleaned_url.split('?')[0]

            # Step 3: 마지막 '/' 뒷부분 추출
            parts = cleaned_url.split('/')
            if not parts:
                return None

            # 마지막 부분이 item
            item = parts[-1]

            # 빈 문자열이면 None 반환
            if not item:
                return None

            return item

        except Exception as e:
            print(f"[WARNING] Failed to extract item from URL {product_url}: {e}")
            return None

    def crawl_detail(self, product):
        """
        제품 상세 페이지 크롤링
        Args: product (dict): product_list에서 조회한 제품 정보
        Returns: dict: 결합된 제품 데이터 (product_list + Detail 필드)
        """
        try:
            product_url = product.get('product_url')
            if not product_url:
                print("[WARNING] Product URL is missing, skipping")
                return product

            # 상세 페이지 로드
            self.driver.get(product_url)

            # TV 크롤러와 동일한 간단한 대기 방식
            print("[INFO] Waiting for page to load...")
            time.sleep(random.uniform(8, 12))  # 8~12초 랜덤 대기
            print("[INFO] Page load complete")

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # item: product_url에서 추출 (우선) 또는 XPath에서 추출 (fallback)
            item = self.extract_item_from_url(product_url)

            # ========== 1단계: HHP 스펙 추출 (specs_button 클릭 후 모달에서 추출) ==========
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None

            specs_button_xpath = self.xpaths.get('specs_button', {}).get('xpath')
            if specs_button_xpath:
                specs_button_found = False

                # 최대 3번 시도
                for attempt in range(1, 4):
                    try:
                        print(f"[INFO] Attempt {attempt}/3: Trying to find specs button")

                        # 페이지 스크롤 (시도할 때마다 조금씩 다른 위치로)
                        scroll_distance = 800 + (attempt * 300)
                        self.driver.execute_script(f"window.scrollTo(0, {scroll_distance});")
                        time.sleep(1)

                        # Specs 버튼 찾기 (짧은 타임아웃)
                        specs_button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, specs_button_xpath))
                        )

                        # 버튼이 화면에 보이도록 추가 스크롤
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", specs_button)
                        time.sleep(1)

                        print(f"[INFO] Specs button found, clicking to open modal")
                        specs_button.click()
                        specs_button_found = True
                        break  # 성공하면 루프 종료

                    except Exception as e:
                        print(f"[WARNING] Attempt {attempt}/3 failed: {e}")
                        if attempt == 3:
                            print(f"[WARNING] Could not find specs button after 3 attempts, skipping HHP specs extraction")
                        else:
                            time.sleep(1)  # 다음 시도 전 대기

                # 버튼을 찾았을 때만 모달 처리
                if specs_button_found:
                    try:
                        # 모달이 완전히 로드될 때까지 대기 (carrier/storage/color 중 하나라도 나타날 때까지)
                        try:
                            WebDriverWait(self.driver, 10).until(
                                lambda driver: driver.find_elements(By.XPATH, self.xpaths.get('hhp_carrier', {}).get('xpath', '//dummy')) or
                                               driver.find_elements(By.XPATH, self.xpaths.get('hhp_storage', {}).get('xpath', '//dummy')) or
                                               driver.find_elements(By.XPATH, self.xpaths.get('hhp_color', {}).get('xpath', '//dummy'))
                            )
                            print(f"[INFO] Specs modal fully loaded")
                        except Exception:
                            print(f"[WARNING] Timeout waiting for specs modal, proceeding anyway...")
                            time.sleep(3)

                        # 모달 HTML 파싱
                        modal_html = self.driver.page_source
                        modal_tree = html.fromstring(modal_html)

                        # 모달에서 스펙 추출
                        hhp_carrier = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
                        hhp_storage = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                        hhp_color = self.extract_with_fallback(modal_tree, self.xpaths.get('hhp_color', {}).get('xpath'))

                        print(f"[INFO] Extracted specs - Carrier: {hhp_carrier}, Storage: {hhp_storage}, Color: {hhp_color}")

                        # 스펙 모달창 닫기 (닫기 버튼 클릭)
                        try:
                            # data-testid="brix-sheet-closeButton" 또는 aria-label="Close Sheet"로 닫기 버튼 찾기
                            close_button_xpath = "//button[@data-testid='brix-sheet-closeButton' or @aria-label='Close Sheet']"
                            close_button = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((By.XPATH, close_button_xpath))
                            )
                            close_button.click()
                            time.sleep(1)  # 모달이 닫힐 때까지 짧은 대기
                            print(f"[INFO] Closed specs modal")
                        except Exception as close_error:
                            print(f"[WARNING] Failed to close specs modal: {close_error}")
                            # 닫기 버튼 클릭 실패 시 ESC 키 시도
                            try:
                                from selenium.webdriver.common.keys import Keys
                                self.driver.find_element("tag name", "body").send_keys(Keys.ESCAPE)
                                time.sleep(1)
                                print(f"[INFO] Closed specs modal using ESC key")
                            except Exception:
                                print(f"[WARNING] Could not close modal, proceeding anyway...")

                    except Exception as e:
                        print(f"[WARNING] Failed to extract specs from modal: {e}")

            # ========== 2단계: 유사 제품 추출 (스크롤해서 찾기) ==========
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')

            similar_products_data = []  # [{name, pros, cons, url}, ...]
            retailer_sku_name_similar = None

            if similar_products_container_xpath:
                # 유사 제품은 페이지 중간(스펙 아래)에 있으므로 현재 위치에서 스크롤하면서 찾기
                print("[INFO] Scrolling to find similar products section...")
                similar_products_found = False

                # 현재 스크롤 위치에서 시작
                current_scroll = self.driver.execute_script("return window.pageYOffset;")
                scroll_step = 400  # 400px씩 스크롤
                page_height = self.driver.execute_script("return document.body.scrollHeight")

                print(f"[DEBUG] Starting scroll from {current_scroll}px, page height: {page_height}px")

                # 페이지 끝까지 스크롤하면서 찾기 (TV 크롤러 방식)
                while current_scroll < page_height:
                    try:
                        # 현재 위치에서 요소 찾기 시도 (타임아웃 없이)
                        similar_elements = self.driver.find_elements(By.XPATH, similar_products_container_xpath)

                        if similar_elements:
                            print(f"[INFO] Similar products section found at scroll position {current_scroll}px")
                            similar_products_found = True
                            # 요소를 찾았으면 화면에 보이도록 스크롤
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", similar_elements[0])
                            time.sleep(2)  # DOM 안정화 대기
                            break
                    except Exception as e:
                        print(f"[DEBUG] Search failed at {current_scroll}px: {e}")

                    # 못 찾았으면 계속 스크롤
                    current_scroll += scroll_step
                    self.driver.execute_script(f"window.scrollTo(0, {current_scroll});")
                    time.sleep(1)  # 스크롤 후 대기 (Lazy Loading)

                if similar_products_found:
                    # HTML 다시 파싱 (스크롤 후 업데이트된 DOM)
                    time.sleep(1)  # 최종 안정화 대기
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                    print(f"[INFO] Re-parsed HTML after finding similar products")
                else:
                    print(f"[WARNING] Could not find similar products section after scrolling entire page")

                # 각 유사 제품 컨테이너 (카드) 추출
                similar_product_containers = tree.xpath(similar_products_container_xpath)

                if similar_product_containers:
                    similar_product_names = []

                    # 1단계: 각 제품 컨테이너에서 제품명과 URL 추출
                    products_basic_info = []
                    for container in similar_product_containers:
                        # 제품명 추출
                        name_xpath = self.xpaths.get('similar_product_name', {}).get('xpath')
                        name = container.xpath(name_xpath)[0] if name_xpath and container.xpath(name_xpath) else None

                        # URL 추출
                        url_xpath = self.xpaths.get('similar_product_url', {}).get('xpath')
                        similar_product_url = container.xpath(url_xpath)[0] if url_xpath and container.xpath(url_xpath) else None

                        if name:
                            products_basic_info.append({
                                'name': name,
                                'url': similar_product_url
                            })
                            similar_product_names.append(name)

                    # 2단계: Pros/Cons 테이블에서 추출 (각 제품 위치에 맞춰)
                    # 데이터베이스에서 조회한 xpath 사용
                    pros_row_xpath = self.xpaths.get('pros', {}).get('xpath')
                    cons_row_xpath = self.xpaths.get('cons', {}).get('xpath')

                    print(f"[DEBUG] pros_row_xpath: {pros_row_xpath}")
                    print(f"[DEBUG] cons_row_xpath: {cons_row_xpath}")

                    # XPath가 유효한 경우만 실행
                    pros_cells = []
                    cons_cells = []

                    try:
                        if pros_row_xpath and pros_row_xpath.strip():
                            pros_cells = tree.xpath(pros_row_xpath)
                            print(f"[DEBUG] Found {len(pros_cells)} pros cells")
                    except Exception as e:
                        print(f"[WARNING] Failed to extract pros: {e}")

                    try:
                        if cons_row_xpath and cons_row_xpath.strip():
                            cons_cells = tree.xpath(cons_row_xpath)
                            print(f"[DEBUG] Found {len(cons_cells)} cons cells")
                    except Exception as e:
                        print(f"[WARNING] Failed to extract cons: {e}")

                    if pros_cells or cons_cells:
                        # 각 제품의 pros/cons를 리스트로 추출
                        for idx, product_info in enumerate(products_basic_info):
                            # 해당 제품 위치의 pros 셀
                            pros_list = []
                            if idx < len(pros_cells):
                                pros_cell = pros_cells[idx]
                                # 셀 안의 li 태그들 추출
                                pros_items = pros_cell.xpath('.//li/text()')
                                pros_list = [p.strip() for p in pros_items if p.strip()]

                            # 해당 제품 위치의 cons 셀
                            cons_list = []
                            if idx < len(cons_cells):
                                cons_cell = cons_cells[idx]
                                # 셀 안의 li 태그들 추출
                                cons_items = cons_cell.xpath('.//li/text()')
                                cons_list = [c.strip() for c in cons_items if c.strip()]

                            # 유사 제품 데이터 저장
                            similar_products_data.append({
                                'name': product_info['name'],
                                'pros': pros_list,  # 리스트로 저장
                                'cons': cons_list,  # 리스트로 저장
                                'url': product_info['url']
                            })
                    else:
                        # 테이블이 없으면 pros/cons 없이 저장
                        for product_info in products_basic_info:
                            similar_products_data.append({
                                'name': product_info['name'],
                                'pros': [],
                                'cons': [],
                                'url': product_info['url']
                            })

                    # 모든 유사 제품명을 ||| 구분자로 연결
                    retailer_sku_name_similar = '|||'.join(similar_product_names) if similar_product_names else None

            # ========== 3단계: 리뷰 섹션 데이터 추출 (HTML에서) ==========
            # HTML 다시 파싱 (스크롤 후 업데이트된 DOM)
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # 리뷰 관련 필드 (data_extractor 후처리)
            count_of_reviews_raw = self.extract_with_fallback(tree, self.xpaths.get('count_of_reviews', {}).get('xpath'))
            count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw, self.account_name)

            star_rating_raw = self.extract_with_fallback(tree, self.xpaths.get('star_rating', {}).get('xpath'))
            star_rating = data_extractor.extract_rating(star_rating_raw, self.account_name)

            count_of_star_ratings = data_extractor.extract_star_ratings_count(
                tree,
                count_of_reviews,
                self.xpaths.get('count_of_star_ratings', {}).get('xpath'),
                self.account_name
            )

            # 기타 필드 추출
            trade_in = self.extract_with_fallback(tree, self.xpaths.get('trade_in', {}).get('xpath'))
            top_mentions = self.extract_with_fallback(tree, self.xpaths.get('top_mentions', {}).get('xpath'))

            # recommendation_intent 추출 및 후처리
            recommendation_intent_raw = self.extract_with_fallback(tree, self.xpaths.get('recommendation_intent', {}).get('xpath'))
            recommendation_intent = recommendation_intent_raw + " would recommend to a friend"
           
            # ========== 4단계: 리뷰 더보기 버튼 클릭 및 상세 리뷰 추출 ==========
            # 리뷰 데이터 추출: "See All Customer Reviews" 버튼 클릭 후 추출
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            print(f"[DEBUG] reviews_button_xpath: {reviews_button_xpath}")

            if reviews_button_xpath:
                review_button_found = False

                # 페이지 상단으로 이동
                print(f"[INFO] Scrolling from top to find review button")
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                # 페이지 전체 높이 계산
                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                current_position = 0
                scroll_step = 400  # 400px씩 스크롤 (TV 크롤러와 동일)

                print(f"[INFO] Page height: {scroll_height}px, starting scroll search...")

                # TV 크롤러처럼 여러 XPath 시도
                reviews_button_xpaths = [
                    reviews_button_xpath,
                    '//button[contains(., "See All Customer Reviews")]',
                    '//a[contains(., "See All Customer Reviews")]',
                    '//button[contains(@class, "Op9coqeII1kYHR9Q")]',
                    '//a[contains(text(), "reviews")]'
                ]

                # 페이지 끝까지 스크롤하면서 리뷰 버튼 찾기
                while current_position < scroll_height:
                    # 각 스크롤 위치에서 여러 XPath 시도 (TV 크롤러 방식)
                    for xpath in reviews_button_xpaths:
                        try:
                            # 현재 위치에서 리뷰 버튼 찾기 시도
                            review_button = self.driver.find_element(By.XPATH, xpath)

                            # 버튼을 찾았으면 화면 중앙으로 스크롤
                            print(f"[INFO] Review button found at {current_position}px with xpath: {xpath[:50]}...")
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", review_button)
                            time.sleep(2)

                            # JavaScript로 클릭 시도
                            try:
                                self.driver.execute_script("arguments[0].click();", review_button)
                                print(f"[INFO] Review button clicked successfully (JS)")
                                review_button_found = True
                                time.sleep(5)  # 리뷰 페이지 로딩 대기
                                break
                            except Exception as click_err:
                                print(f"[WARNING] JS click failed: {click_err}, trying normal click")
                                # 일반 클릭 시도
                                review_button.click()
                                print(f"[INFO] Review button clicked successfully (normal)")
                                review_button_found = True
                                time.sleep(5)
                                break

                        except Exception as e:
                            # 이 xpath로 못 찾으면 다음 xpath 시도
                            if "no such element" not in str(e).lower():
                                print(f"[DEBUG] XPath {xpath[:30]}... failed: {e}")
                            continue

                    # 버튼을 찾았으면 전체 루프 종료
                    if review_button_found:
                        break

                    # 못 찾았으면 계속 스크롤
                    current_position += scroll_step
                    self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                    time.sleep(0.5)

                if not review_button_found:
                    print(f"[WARNING] Could not find review button after scrolling entire page, skipping review extraction")

                # 버튼을 찾았을 때만 리뷰 추출
                if review_button_found:
                    try:
                        # 리뷰 페이지가 완전히 로드될 때까지 대기
                        detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                        if detailed_review_xpath:
                            try:
                                WebDriverWait(self.driver, 30).until(
                                    lambda driver: driver.find_elements(By.XPATH, detailed_review_xpath)
                                )
                                print(f"[INFO] Review page fully loaded")
                            except Exception:
                                print(f"[WARNING] Timeout waiting for reviews page, proceeding anyway...")
                                time.sleep(5)

                            # 새 페이지의 HTML 파싱
                            page_html = self.driver.page_source
                            tree = html.fromstring(page_html)

                            # 리뷰 본문 추출 (최대 20개)
                            reviews_list = tree.xpath(detailed_review_xpath)
                            if reviews_list:
                                # 최대 20개만 추출
                                reviews_list = reviews_list[:20]
                                # 구분자로 연결
                                detailed_review_content = '|||'.join(reviews_list)
                                print(f"[INFO] Extracted {len(reviews_list)} reviews")
                            else:
                                print(f"[WARNING] No reviews found on review page")
                        else:
                            print(f"[WARNING] detailed_review_content xpath not found")
                    except Exception as e:
                        print(f"[WARNING] Failed to extract reviews from page: {e}")
 

            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'trade_in': trade_in,
                'recommendation_intent': recommendation_intent,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'hhp_carrier': hhp_carrier,
                'detailed_review_content': detailed_review_content,
                'top_mentions': top_mentions,
                'retailer_sku_name_similar': retailer_sku_name_similar,
                'similar_products_data': similar_products_data  # 각 유사 제품 객체 리스트
            })

            return combined_data

        except Exception as e:
            print(f"[ERROR] Failed to crawl detail page {product.get('product_url')}: {e}")
            traceback.print_exc()
            # 에러 발생 시 product_list 데이터만 반환
            return product

    def save_to_retail_com(self, products):
        """
        hhp_retail_com 테이블에 저장

        Args: products (list): 결합된 제품 데이터 리스트

        Returns: int: 저장된 제품 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            insert_query = """
                INSERT INTO hhp_retail_com (
                    country, product, item, account_name, page_type,
                    count_of_reviews, retailer_sku_name, product_url,
                    star_rating, count_of_star_ratings, sku_popularity,
                    final_sku_price, original_sku_price, savings, discount_type,
                    offer, bundle,
                    pick_up_availability, shipping_availability, delivery_availability,
                    inventory_status, sku_status,
                    retailer_membership_discounts, trade_in, recommendation_intent,
                    hhp_storage, hhp_color, hhp_carrier,
                    detailed_review_content, summarized_review_content, top_mentions,
                    retailer_sku_name_similar,
                    main_rank, bsr_rank, trend_rank,
                    promotion_type,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            saved_count = 0

            for product in products:
                cursor.execute(insert_query, (
                    'US',
                    'HHP',
                    product.get('item'),
                    self.account_name,
                    product.get('page_type'),
                    product.get('count_of_reviews'),
                    product.get('retailer_sku_name'),
                    product.get('product_url'),
                    product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('sku_popularity'),
                    product.get('final_sku_price'),
                    product.get('original_sku_price'),
                    product.get('savings'),
                    product.get('discount_type'),
                    product.get('offer'),
                    product.get('bundle'),
                    product.get('pick_up_availability'),
                    product.get('shipping_availability'),
                    product.get('delivery_availability'),
                    product.get('inventory_status'),
                    product.get('sku_status'),
                    product.get('retailer_membership_discounts'),
                    product.get('trade_in'),
                    product.get('recommendation_intent'),
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('hhp_carrier'),
                    product.get('detailed_review_content'),
                    product.get('summarized_review_content'),
                    product.get('top_mentions'),
                    product.get('retailer_sku_name_similar'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('trend_rank'),
                    product.get('promotion_type'),
                    product.get('calendar_week'),
                    current_time,
                    self.batch_id
                ))
                saved_count += 1

                # 제품 저장 후 바로 유사 제품 데이터를 bby_hhp_mst에 저장
                similar_products_data = product.get('similar_products_data', [])
                if similar_products_data:
                    similar_saved = self._save_similar_products_to_mst(cursor, product, current_time)

            self.db_conn.commit()
            cursor.close()

            print(f"[SUCCESS] Saved {saved_count} products to hhp_retail_com")

            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return 0

    def _save_similar_products_to_mst(self, cursor, product, current_time):
        """
        bby_hhp_mst 테이블에 유사 제품 저장 (내부 헬퍼 메서드)

        Args:
            cursor: DB cursor 객체
            product (dict): 제품 데이터
            current_time (str): 현재 시간

        Returns: int: 저장된 행 수
        """
        try:
            insert_query = """
                INSERT INTO bby_hhp_mst (
                    account_name, retailer_sku_name, item,
                    retailer_sku_name_similar, pros, cons,
                    origin_product_url, product_url,
                    calendar_week, crawl_strdatetime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            saved_count = 0
            similar_products_data = product.get('similar_products_data', [])

            # 각 유사 제품을 별도 행으로 저장
            for similar_product in similar_products_data:
                # Pros/Cons 리스트를 ||| 구분자로 연결
                pros_list = similar_product.get('pros', [])
                cons_list = similar_product.get('cons', [])

                pros_str = '|||'.join(pros_list) if pros_list else None
                cons_str = '|||'.join(cons_list) if cons_list else None

                cursor.execute(insert_query, (
                    self.account_name,
                    product.get('retailer_sku_name'),  # 원본 제품명
                    product.get('item'),                # 원본 제품 item
                    similar_product.get('name'),        # 유사 제품명
                    pros_str,                           # Pros (||| 구분자)
                    cons_str,                           # Cons (||| 구분자)
                    product.get('product_url'),         # 원본 제품 URL
                    similar_product.get('url'),         # 유사 제품 URL
                    product.get('calendar_week'),
                    current_time
                ))
                saved_count += 1

            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save similar products to bby_hhp_mst: {e}")
            traceback.print_exc()
            return 0

    def save_to_mst(self, products):
        """
        bby_hhp_mst 테이블에 저장 (Similar Products, Pros/Cons 비교 데이터) - DEPRECATED

        각 유사 제품을 별도 행으로 저장:
        - retailer_sku_name: 원본 제품명
        - retailer_sku_name_similar: 유사 제품명
        - pros: 해당 유사 제품의 Pros 리스트 (||| 구분자로 연결)
        - cons: 해당 유사 제품의 Cons 리스트 (||| 구분자로 연결)
        - similar_product_url: 유사 제품의 URL

        Args: products (list): 제품 데이터 리스트

        Returns: int: 저장된 행 수
        """
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            insert_query = """
                INSERT INTO bby_hhp_mst (
                    account_name, retailer_sku_name, item,
                    retailer_sku_name_similar, pros, cons,
                    product_url, similar_product_url,
                    calendar_week, crawl_strdatetime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            saved_count = 0

            for product in products:
                similar_products_data = product.get('similar_products_data', [])

                if similar_products_data:
                    # 각 유사 제품을 별도 행으로 저장
                    for similar_product in similar_products_data:
                        # Pros/Cons 리스트를 ||| 구분자로 연결
                        pros_list = similar_product.get('pros', [])
                        cons_list = similar_product.get('cons', [])

                        pros_str = '|||'.join(pros_list) if pros_list else None
                        cons_str = '|||'.join(cons_list) if cons_list else None

                        cursor.execute(insert_query, (
                            self.account_name,
                            product.get('retailer_sku_name'),  # 원본 제품명
                            product.get('item'),                # 원본 제품 item
                            similar_product.get('name'),        # 유사 제품명
                            pros_str,                           # Pros (||| 구분자)
                            cons_str,                           # Cons (||| 구분자)
                            product.get('product_url'),         # 원본 제품 URL
                            similar_product.get('url'),         # 유사 제품 URL
                            product.get('calendar_week'),
                            current_time
                        ))
                        saved_count += 1

            self.db_conn.commit()
            cursor.close()

            if saved_count > 0:
                print(f"[SUCCESS] Saved {saved_count} similar products to bby_hhp_mst")
            else:
                print(f"[INFO] No similar products data to save to bby_hhp_mst")

            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save to bby_hhp_mst: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
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

            # product_list 조회
            product_list = self.load_product_list()
            if not product_list:
                print("[WARNING] No products found in bby_hhp_product_list")
                return False

            print("\n" + "="*60)
            print(f"[INFO] Starting BestBuy Detail page crawling...")
            print(f"[INFO] Total products to crawl: {len(product_list)}")
            print("="*60 + "\n")

            # 모든 제품 상세 페이지 크롤링
            total_saved = 0

            for i, product in enumerate(product_list, 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] Processing: {sku_name[:50]}...")

                combined_data = self.crawl_detail(product)

                # 1개 제품마다 즉시 DB에 저장 (유사 제품도 함께 저장됨)
                saved_count = self.save_to_retail_com([combined_data])
                total_saved += saved_count

                # 페이지 간 대기
                time.sleep(5)

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] BestBuy Detail Crawler Finished")
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
    개별 실행 시 진입점 (테스트 모드 ON)
    """
    crawler = BestBuyDetailCrawler(test_mode=True)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy Detail Crawler completed successfully")
    else:
        print("\n[FAILED] BestBuy Detail Crawler failed")


if __name__ == '__main__':
    main()