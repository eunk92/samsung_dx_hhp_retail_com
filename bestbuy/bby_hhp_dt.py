"""
BestBuy Detail 페이지 크롤러

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
- Main/BSR/Trend에서 수집한 모든 제품 처리

================================================================================
저장 테이블
================================================================================
- hhp_retail_com (상세 정보 + 리뷰)
"""

import sys
import os
import time
import random
import traceback
import re
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
from common.data_extractor import extract_numeric_value


class BestBuyDetailCrawler(BaseCrawler):
    """
    BestBuy Detail 페이지 크롤러
    """

    def __init__(self, batch_id=None, test_mode=False):
        """초기화. batch_id: 통합 크롤러에서 전달, test_mode: 테스트 모드 여부"""
        super().__init__()
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'
        self.batch_id = batch_id
        self.test_mode = test_mode
        # batch_id 없으면 개별 실행
        self.standalone = batch_id is None

    def extract_rating(self, text):
        """별점 텍스트에서 숫자 추출 (소수점 포함, 쉼표 제외)"""
        return extract_numeric_value(text, include_comma=False, include_decimal=True)

    def extract_review_count(self, text):
        """리뷰 개수 텍스트에서 숫자 추출 (쉼표 포함, 소수점 제외)"""
        return extract_numeric_value(text, include_comma=True, include_decimal=False)

    def initialize(self):
        """초기화: batch_id 설정 → DB 연결 → XPath 로드 → WebDriver 설정 → 로그 정리"""
        # batch_id 없으면 기본값 사용
        if not self.batch_id:
            self.batch_id = 'b_20251207_205850'

        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        self.setup_driver()
        self.cleanup_old_logs()

        return True

    def load_product_list(self):
        """product_list 조회: batch_id 기준으로 제품 URL 및 기본 정보 조회"""
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT
                    page_type, retailer_sku_name, final_sku_price, savings,
                    comparable_pricing as original_sku_price, offer,
                    pick_up_availability, shipping_availability, delivery_availability,
                    sku_status, promotion_type, main_rank, bsr_rank, trend_rank,
                    product_url, calendar_week
                FROM bby_hhp_product_list
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
                    'page_type': row[0],
                    'retailer_sku_name': row[1],
                    'final_sku_price': row[2],
                    'savings': row[3],
                    'original_sku_price': row[4],
                    'offer': row[5],
                    'pick_up_availability': row[6],
                    'shipping_availability': row[7],
                    'delivery_availability': row[8],
                    'sku_status': row[9],
                    'promotion_type': row[10],
                    'main_rank': row[11],
                    'bsr_rank': row[12],
                    'trend_rank': row[13],
                    'product_url': row[14],
                    'calendar_week': row[15]
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            return []

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출"""
        if not product_url:
            return None

        try:
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?$', '', product_url)
            cleaned_url = cleaned_url.split('?')[0]
            parts = cleaned_url.split('/')
            if not parts:
                return None
            item = parts[-1]
            # .p 확장자 제거 (예: 6507560.p → 6507560)
            if item.endswith('.p'):
                item = item[:-2]
            return item if item else None
        except Exception:
            return None

    def crawl_detail(self, product):
        """상세 페이지 크롤링: 페이지 로드 → 스크롤 전 추출 → 스크롤(최대 3번 재시도) 후 스펙 추출 → 유사제품 추출 → 리뷰 추출 → product_list + detail 데이터 결합"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                return product

            self.driver.get(product_url)
            time.sleep(random.uniform(8, 12))

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # 실제 페이지 URL에서 item 추출 (리다이렉트 고려)
            actual_url = self.driver.current_url
            item = self.extract_item_from_url(actual_url)

            # ========== 1단계: 상단 정보 추출 (최대 3회 재시도) ==========
            top_star_rating = None
            top_count_of_reviews = None
            trade_in = None

            MAX_RETRY_TOP = 3
            for attempt in range(1, MAX_RETRY_TOP + 1):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

                if top_star_rating is None:
                    top_star_rating = self.safe_extract(tree, 'top_star_rating')

                if top_count_of_reviews is None:
                    top_count_of_reviews = self.safe_extract(tree, 'top_count_of_reviews')

                if trade_in is None:
                    trade_in = self.safe_extract(tree, 'trade_in')

                # 필수 필드 모두 추출 성공하면 종료 (Not yet reviewed인 경우도 성공으로 간주)
                if top_star_rating and top_count_of_reviews:
                    break
                if top_count_of_reviews and 'Not yet reviewed' in top_count_of_reviews:
                    break

                if attempt < MAX_RETRY_TOP:
                    time.sleep(2)
                else:
                    # 마지막 시도에서도 실패
                    missing = []
                    if not top_star_rating: missing.append('top_star_rating')
                    if not top_count_of_reviews: missing.append('top_count_of_reviews')
                    if not trade_in: missing.append('trade_in')
                    if missing:
                        print(f"[WARNING] 상단 정보 추출 실패 (시도 {attempt}/{MAX_RETRY_TOP}) - 미추출: {', '.join(missing)}")

            # ========== 1-1단계: final_sku_price 없으면 상세페이지에서 추출 ==========
            if not product.get('final_sku_price'):
                final_sku_price = self.safe_extract(tree, 'final_sku_price')
                if final_sku_price:
                    product['final_sku_price'] = final_sku_price

            # ========== 1-2단계: original_sku_price 없으면 상세페이지에서 추출 ==========
            if not product.get('original_sku_price'):
                original_sku_price = self.safe_extract(tree, 'original_sku_price')
                if original_sku_price:
                    product['original_sku_price'] = original_sku_price

            # ========== 1-3단계: SKU 추출 ==========
            sku_raw = self.safe_extract(tree, 'sku')
            sku = sku_raw.replace('Model:', '').strip() if sku_raw else None

            # ========== 1-4단계: trend인 경우 savings/pick_up/shipping availability 상세페이지에서 추출 ==========
            if product.get('page_type') == 'trend':
                if not product.get('savings'):
                    savings_raw = self.safe_extract(tree, 'savings')
                    if savings_raw:
                        product['savings'] = savings_raw.replace('Save ', '')

                if not product.get('pick_up_availability'):
                    pick_up_raw = self.safe_extract_join(tree, 'pick_up_availability', separator=' ')
                    if pick_up_raw:
                        product['pick_up_availability'] = pick_up_raw

                if not product.get('shipping_availability'):
                    shipping_raw = self.safe_extract_join(tree, 'shipping_availability', separator=' ')
                    if shipping_raw:
                        product['shipping_availability'] = shipping_raw

            # ========== 2단계: HHP 스펙 추출 (specs_button 클릭 후 모달에서 추출) ==========
            hhp_carrier = None
            hhp_storage = None
            hhp_color = None

            specs_button_xpath = self.xpaths.get('specs_button', {}).get('xpath')
            if specs_button_xpath:
                specs_button_found = False

                for _ in range(3): 
                    for attempt in range(3):  
                        try:
                            scroll_distance = 800 + (attempt * 300) 
                            self.driver.execute_script(f"window.scrollTo(0, {scroll_distance});")
                            time.sleep(1)

                            specs_button = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((By.XPATH, specs_button_xpath))
                            )
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", specs_button)
                            time.sleep(1)

                            specs_button.click()
                            specs_button_found = True
                            break
                        except Exception:
                            time.sleep(1)

                    if specs_button_found:
                        break

                    self.driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)

                if specs_button_found:
                    try:
                        try:
                            WebDriverWait(self.driver, 10).until(
                                lambda driver: driver.find_elements(By.XPATH, self.xpaths.get('hhp_carrier', {}).get('xpath', '//dummy')) or
                                               driver.find_elements(By.XPATH, self.xpaths.get('hhp_storage', {}).get('xpath', '//dummy')) or
                                               driver.find_elements(By.XPATH, self.xpaths.get('hhp_color', {}).get('xpath', '//dummy'))
                            )
                        except Exception:
                            time.sleep(3)

                        modal_html = self.driver.page_source
                        modal_tree = html.fromstring(modal_html)

                        hhp_carrier = self.safe_extract(modal_tree, 'hhp_carrier')
                        hhp_storage = self.safe_extract(modal_tree, 'hhp_storage')
                        hhp_color = self.safe_extract(modal_tree, 'hhp_color')

                        # 스펙 모달창 닫기
                        try:
                            close_button_xpath = self.xpaths.get('close_button', {}).get('xpath')
                            if close_button_xpath:
                                close_button = WebDriverWait(self.driver, 5).until(
                                    EC.element_to_be_clickable((By.XPATH, close_button_xpath))
                                )
                                close_button.click()
                                time.sleep(1)
                        except Exception:
                            try:
                                from selenium.webdriver.common.keys import Keys
                                self.driver.find_element("tag name", "body").send_keys(Keys.ESCAPE)
                                time.sleep(1)
                            except Exception:
                                pass

                    except Exception:
                        pass

            # ========== 3단계: 유사 제품 추출 ==========
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')
            retailer_sku_name_similar = None

            if similar_products_container_xpath:
                similar_products_found = False
                current_scroll = self.driver.execute_script("return window.pageYOffset;")
                page_height = self.driver.execute_script("return document.body.scrollHeight")

                while current_scroll < page_height:
                    try:
                        similar_elements = self.driver.find_elements(By.XPATH, similar_products_container_xpath)
                        if similar_elements:
                            similar_products_found = True
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", similar_elements[0])
                            time.sleep(2)
                            break
                    except Exception:
                        pass

                    scroll_step = random.randint(205, 350)
                    current_scroll += scroll_step
                    self.driver.execute_script(f"window.scrollTo(0, {current_scroll});")
                    time.sleep(random.uniform(0.5, 0.7))

                if similar_products_found:
                    time.sleep(1)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                try:
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
                    retailer_sku_name_similar = None

            # ========== 4단계: 리뷰 섹션 데이터 추출 (최대 3회 재시도) ==========
            # 리뷰 없음 텍스트 패턴
            NO_REVIEWS_TEXTS = ['not yet reviewed']
            NO_REVIEWS_VALUE = "Not yet reviewed"

            star_rating = None
            count_of_reviews = None
            count_of_star_ratings = None
            top_mentions = None
            recommendation_intent = None

            # 리뷰 없음 여부 먼저 판별
            is_no_reviews = top_count_of_reviews and any(t in top_count_of_reviews.lower() for t in NO_REVIEWS_TEXTS)

            if is_no_reviews:
                # 리뷰 없음 → 일괄 할당
                count_of_reviews = "0"
                star_rating = self.extract_rating(top_star_rating) if top_star_rating else NO_REVIEWS_VALUE
                count_of_star_ratings = NO_REVIEWS_VALUE
            else:
                # 리뷰 있음 → 추출 로직
                MAX_RETRY = 3
                for attempt in range(1, MAX_RETRY + 1):
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)

                    if count_of_reviews is None:
                        if top_count_of_reviews:
                            count_of_reviews = self.extract_review_count(top_count_of_reviews)
                        else:
                            count_of_reviews_raw = self.safe_extract(tree, 'count_of_reviews')
                            count_of_reviews = self.extract_review_count(count_of_reviews_raw)

                    if star_rating is None:
                        if top_star_rating:
                            star_rating = self.extract_rating(top_star_rating)
                        else:
                            star_rating_raw = self.safe_extract(tree, 'star_rating')
                            star_rating = self.extract_rating(star_rating_raw)

                    if count_of_star_ratings is None:
                        count_of_star_ratings = count_of_reviews

                    if top_mentions is None:
                        top_mentions_raw = self.safe_extract_join(tree, 'top_mentions', separator=', ')
                        if top_mentions_raw:
                            # 괄호와 숫자 제거: "Overall Performance (81)" → "Overall Performance"
                            top_mentions = ', '.join([re.sub(r'\s*\(\d+\)\s*', '', item).strip() for item in top_mentions_raw.split(', ')])
                        else:
                            top_mentions = None

                    if recommendation_intent is None:
                        recommendation_intent_raw = self.safe_extract(tree, 'recommendation_intent')
                        recommendation_intent = (recommendation_intent_raw + " would recommend to a friend") if recommendation_intent_raw else None

                    # 필수 필드 모두 추출 성공하면 종료
                    if star_rating and count_of_reviews and count_of_star_ratings:
                        if attempt > 1:
                            print(f"[INFO] 리뷰 데이터 추출 완료 (시도 {attempt}/{MAX_RETRY})")
                        break

                    # 재시도 필요
                    if attempt < MAX_RETRY:
                        missing = []
                        if not star_rating: missing.append('star_rating')
                        if not count_of_reviews: missing.append('count_of_reviews')
                        if not count_of_star_ratings: missing.append('count_of_star_ratings')
                        print(f"[WARNING] 리뷰 데이터 불완전 (시도 {attempt}/{MAX_RETRY}) - 미추출: {', '.join(missing)}")
                        time.sleep(2)
                    else:
                        # 마지막 시도에서도 실패
                        missing = []
                        if not star_rating: missing.append('star_rating')
                        if not count_of_reviews: missing.append('count_of_reviews')
                        if not count_of_star_ratings: missing.append('count_of_star_ratings')
                        if missing:
                            print(f"[WARNING] 리뷰 데이터 추출 실패 (시도 {attempt}/{MAX_RETRY}) - 미추출: {', '.join(missing)}")
           
            # ========== 5단계: 리뷰 더보기 버튼 클릭 및 상세 리뷰 추출 ==========
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            if reviews_button_xpath:
                review_button_found = False

                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                current_position = 0

                # reviews_button + fallback XPaths
                fallback_str = self.xpaths.get('reviews_button_fallback', {}).get('xpath') or ''
                fallback_xpaths = [x.strip() for x in fallback_str.split('|||') if x.strip()]
                reviews_button_xpaths = [reviews_button_xpath] + fallback_xpaths

                while current_position < scroll_height:
                    for xpath in reviews_button_xpaths:
                        try:
                            review_button = self.driver.find_element(By.XPATH, xpath)
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", review_button)
                            time.sleep(2)

                            try:
                                self.driver.execute_script("arguments[0].click();", review_button)
                                review_button_found = True
                                time.sleep(5)
                                break
                            except Exception:
                                review_button.click()
                                review_button_found = True
                                time.sleep(5)
                                break
                        except Exception:
                            continue

                    if review_button_found:
                        break

                    scroll_step = random.randint(205, 350)
                    current_position += scroll_step
                    self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                    time.sleep(random.uniform(0.5, 0.7))

                if review_button_found:
                    try:
                        detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                        if detailed_review_xpath:
                            try:
                                WebDriverWait(self.driver, 30).until(
                                    lambda driver: driver.find_elements(By.XPATH, detailed_review_xpath)
                                )
                            except Exception:
                                time.sleep(5)

                            page_html = self.driver.page_source
                            tree = html.fromstring(page_html)

                            reviews_list = tree.xpath(detailed_review_xpath)
                            if reviews_list:
                                reviews_list = reviews_list[:20]
                                formatted_reviews = []
                                for idx, review in enumerate(reviews_list, 1):
                                    cleaned_review = review.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                    cleaned_review = ' '.join(cleaned_review.split())
                                    formatted_reviews.append(f"review{idx} - {cleaned_review}")

                                detailed_review_content = ' ||| '.join(formatted_reviews)
                    except Exception:
                        pass

            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'sku': sku,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'trade_in': trade_in,
                'recommendation_intent': recommendation_intent,
                'hhp_storage': hhp_storage[:200] if hhp_storage else None,
                'hhp_color': hhp_color[:200] if hhp_color else None,
                'hhp_carrier': hhp_carrier[:200] if hhp_carrier else None,
                'detailed_review_content': detailed_review_content,
                'top_mentions': top_mentions,
                'retailer_sku_name_similar': retailer_sku_name_similar,
                'crawl_strdatetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            return combined_data

        except Exception as e:
            print(f"[ERROR] Detail crawl failed: {e}")
            return product

    def upsert_item_mst(self, product):
        """hhp_item_mst 테이블에 INSERT 또는 UPDATE
        - 조회 결과 없음 → INSERT (sku 없어도 빈값으로)
        - 조회 결과 있음 + 기존 sku null/빈값 + 새 sku 있음 → UPDATE
        - 조회 결과 있음 + 기존 sku null/빈값 + 새 sku도 없음 → SKIP
        """
        item = product.get('item')
        if not item:
            return

        try:
            cursor = self.db_conn.cursor()
            new_sku = product.get('sku') or ''
            product_url = product.get('product_url')

            # 기존 데이터 조회
            cursor.execute("""
                SELECT sku FROM hhp_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))

            row = cursor.fetchone()

            if row is None:
                # 조회 결과 없음 → INSERT
                cursor.execute("""
                    INSERT INTO hhp_item_mst (item, account_name, sku, product_url)
                    VALUES (%s, %s, %s, %s)
                """, (item, self.account_name, new_sku, product_url))
                self.db_conn.commit()
                print(f"  [ITEM_MST] INSERT: {item}, sku: {new_sku or '(empty)'}")
            else:
                existing_sku = row[0] or ''
                if not existing_sku and new_sku:
                    # 기존 sku 없고 새 sku 있음 → UPDATE (product_url, updated_at도 함께 업데이트)
                    cursor.execute("""
                        UPDATE hhp_item_mst SET sku = %s, product_url = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE item = %s AND account_name = %s
                    """, (new_sku, product_url, item, self.account_name))
                    self.db_conn.commit()
                    print(f"  [ITEM_MST] UPDATE: {item}, sku: {new_sku}")
                elif not existing_sku and not new_sku:
                    # 둘 다 없음 → SKIP
                    pass

            cursor.close()

        except Exception as e:
            print(f"[ERROR] upsert_item_mst failed: {item}: {e}")
            self.db_conn.rollback()

    def save_to_retail_com(self, product):
        """DB 저장: 1개씩 INSERT"""
        if not product:
            return False

        try:
            cursor = self.db_conn.cursor()

            # 테스트 모드면 test_hhp_retail_com, 통합 크롤러면 hhp_retail_com
            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'

            insert_query = f"""
                INSERT INTO {table_name} (
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

            values = (
                'SEA', 'HHP', product.get('item'), self.account_name, product.get('page_type'),
                product.get('count_of_reviews'), product.get('retailer_sku_name'), product.get('product_url'),
                product.get('star_rating'), product.get('count_of_star_ratings'), product.get('sku_popularity'),
                product.get('final_sku_price'), product.get('original_sku_price'), product.get('savings'), product.get('discount_type'),
                product.get('offer'), product.get('bundle'),
                product.get('pick_up_availability'), product.get('shipping_availability'), product.get('delivery_availability'),
                product.get('inventory_status'), product.get('sku_status'),
                product.get('retailer_membership_discounts'), product.get('trade_in'), product.get('recommendation_intent'),
                product.get('hhp_storage'), product.get('hhp_color'), product.get('hhp_carrier'),
                product.get('detailed_review_content'), product.get('summarized_review_content'), product.get('top_mentions'),
                product.get('retailer_sku_name_similar'),
                product.get('main_rank'), product.get('bsr_rank'), product.get('trend_rank'),
                product.get('promotion_type'),
                product.get('calendar_week'), product.get('crawl_strdatetime'), self.batch_id
            )

            cursor.execute(insert_query, values)
            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"[ERROR] DB save failed: {product.get('item')}: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return False

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

            for i, product in enumerate(product_list, 1):
                try:
                    sku_name = product.get('retailer_sku_name') or 'N/A'
                    print(f"[{i}/{len(product_list)}] {sku_name[:50]}...")

                    combined_data = self.crawl_detail(product)
                    if combined_data:
                        self.upsert_item_mst(combined_data)
                        if self.save_to_retail_com(combined_data):
                            total_saved += 1

                    time.sleep(5)

                except Exception as e:
                    print(f"[ERROR] Product {i} failed: {e}")
                    continue

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
    crawler = BestBuyDetailCrawler(batch_id=None, test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
