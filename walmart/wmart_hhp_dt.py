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
from playwright.sync_api import sync_playwright

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common import data_extractor


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

        # Playwright 객체
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def setup_playwright(self):
        """Playwright 브라우저 설정"""
        try:
            # Windows TEMP 폴더 문제 해결
            temp_dir = 'C:\\Temp'
            os.makedirs(temp_dir, exist_ok=True)
            os.environ['TEMP'] = temp_dir
            os.environ['TMP'] = temp_dir

            self.playwright = sync_playwright().start()

            self.browser = self.playwright.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--start-maximized',
                    '--lang=en-US'
                ]
            )

            self.context = self.browser.new_context(
                viewport=None,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US'
            )

            self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)

            self.page = self.context.new_page()
            print("[OK] Playwright browser initialized")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup Playwright: {e}")
            traceback.print_exc()
            return False

    def handle_captcha(self):
        """CAPTCHA 자동 해결"""
        try:
            time.sleep(random.uniform(1, 5))

            captcha_selectors = [
                'button:has-text("PRESS & HOLD")',
                'div:has-text("PRESS & HOLD")',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                '[class*="captcha"]',
                '[id*="captcha"]'
            ]

            button = None
            for selector in captcha_selectors:
                try:
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=5000):
                        text = temp_button.inner_text(timeout=2000).upper()
                        if ('PRESS' in text and 'HOLD' in text) or 'CAPTCHA' in text:
                            button = temp_button
                            break
                except:
                    continue

            if not button:
                return True

            box = button.bounding_box()
            if box:
                center_x = box['x'] + box['width'] / 2
                center_y = box['y'] + box['height'] / 2

                self.page.mouse.move(center_x, center_y)
                time.sleep(random.uniform(0.3, 0.6))

                self.page.mouse.down()
                hold_time = random.uniform(7, 9)
                time.sleep(hold_time)
                self.page.mouse.up()

                time.sleep(random.uniform(3, 5))

                try:
                    if not button.is_visible(timeout=3000):
                        print("[OK] CAPTCHA solved")
                        return True
                    else:
                        time.sleep(random.uniform(58, 62))
                        return True
                except Exception as e:
                    return True

            return False

        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → Playwright 설정 → batch_id 설정"""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 2. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 3. Playwright 설정
        if not self.setup_playwright():
            print("[ERROR] Initialize failed: Playwright setup failed")
            return False

        # 4. batch_id 설정
        if not self.batch_id:
            self.batch_id = 'w_20251127_123456'

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}")
        return True

    def load_product_list(self):
        """wmart_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회"""
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT DISTINCT ON (product_url)
                    retailer_sku_name, final_sku_price, original_sku_price,
                    offer, pick_up_availability, shipping_availability,
                    delivery_availability, sku_status, retailer_membership_discounts,
                    available_quantity_for_purchase, inventory_status,
                    main_rank, bsr_rank, product_url, calendar_week,
                    crawl_strdatetime, page_type
                FROM wmart_hhp_product_list
                WHERE account_name = %s AND batch_id = %s AND product_url IS NOT NULL
                ORDER BY product_url, id
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

            self.page.goto(product_url, wait_until="domcontentloaded", timeout=90000)

            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass

            time.sleep(random.uniform(2, 3))

            if first_product:
                if not self.handle_captcha():
                    print("[WARNING] CAPTCHA handling failed")
                time.sleep(random.uniform(3, 5))

            page_html = self.page.content()
            tree = html.fromstring(page_html)

            # item ID 추출
            item = None
            try:
                if product_url:
                    ip_match = re.search(r'/ip/[^/]+/(\d+)', product_url)
                    if ip_match:
                        item = ip_match.group(1)
                    else:
                        encoded_match = re.search(r'%2F(\d+)%3F', product_url)
                        if encoded_match:
                            item = encoded_match.group(1)
                        else:
                            last_segment = product_url.rstrip('/').split('/')[-1]
                            item_with_params = last_segment.split('?')[0]
                            number_match = re.search(r'(\d+)$', item_with_params)
                            if number_match:
                                item = number_match.group(1)
            except Exception as e:
                print(f"[WARNING] Failed to extract item: {e}")

            # 추가 필드 추출
            number_of_ppl_purchased_yesterday = self.safe_extract(tree, 'number_of_ppl_purchased_yesterday')
            number_of_ppl_added_to_carts = self.safe_extract(tree, 'number_of_ppl_added_to_carts')
            sku_popularity = self.safe_extract_join(tree, 'sku_popularity', separator=", ")
            savings = self.safe_extract(tree, 'savings')
            discount_type = self.safe_extract(tree, 'discount_type')

            # shipping_info 추출
            shipping_info = None
            try:
                shipping_info_xpath = self.xpaths.get('shipping_info', {}).get('xpath')
                if shipping_info_xpath:
                    shipping_info_raw = tree.xpath(shipping_info_xpath)
                    if isinstance(shipping_info_raw, list):
                        shipping_info = ' '.join([text.strip() for text in shipping_info_raw if text.strip()])
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
                    spec_button = self.page.locator(spec_button_xpath).first
                    if spec_button.is_visible(timeout=5000):
                        spec_button.scroll_into_view_if_needed()
                        time.sleep(random.uniform(1, 2))
                        spec_button.click()

                        try:
                            if spec_close_button_xpath:
                                self.page.wait_for_selector(spec_close_button_xpath, timeout=5000, state='visible')
                            time.sleep(random.uniform(0.5, 1.5))
                        except Exception:
                            time.sleep(random.uniform(1, 3))

                        modal_html = self.page.content()
                        modal_tree = html.fromstring(modal_html)

                        hhp_carrier = self.safe_extract(modal_tree, 'hhp_carrier')
                        hhp_storage = self.safe_extract(modal_tree, 'hhp_storage')
                        hhp_color = self.safe_extract(modal_tree, 'hhp_color')

                        if spec_close_button_xpath:
                            close_button = self.page.locator(spec_close_button_xpath).first
                            if close_button.is_visible(timeout=3000):
                                close_button.click()
                                time.sleep(random.uniform(1, 2))
            except Exception:
                hhp_carrier = self.safe_extract(tree, 'hhp_carrier')
                hhp_storage = self.safe_extract(tree, 'hhp_storage')
                hhp_color = self.safe_extract(tree, 'hhp_color')

            # 유사 제품 추출
            retailer_sku_name_similar = None
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')

            if similar_products_container_xpath:
                try:
                    scroll_height = self.page.evaluate("document.body.scrollHeight")
                    current_position = 0
                    scroll_step = 500

                    while current_position < scroll_height:
                        self.page.evaluate(f"window.scrollTo(0, {current_position});")
                        time.sleep(0.5)

                        try:
                            similar_section = self.page.locator(similar_products_container_xpath).first
                            if similar_section.is_visible(timeout=1000):
                                similar_section.scroll_into_view_if_needed()
                                time.sleep(random.uniform(0.5, 1.5))
                                break
                        except:
                            pass

                        current_position += scroll_step

                    page_html = self.page.content()
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
            page_html = self.page.content()
            tree = html.fromstring(page_html)

            count_of_reviews = None
            try:
                count_of_reviews_raw = self.safe_extract(tree, 'count_of_reviews')
                count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw)
            except Exception:
                pass

            star_rating = None
            try:
                star_rating_raw = self.safe_extract(tree, 'star_rating')
                star_rating = data_extractor.extract_rating(star_rating_raw)
            except Exception:
                pass

            count_of_star_ratings = None
            try:
                count_of_star_ratings = data_extractor.extract_star_ratings_count(
                    tree, count_of_reviews,
                    self.xpaths.get('count_of_star_ratings', {}).get('xpath'),
                    self.account_name
                )
            except Exception:
                pass

            # 리뷰 상세 추출
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            if reviews_button_xpath:
                review_button_found = False

                self.page.evaluate("window.scrollTo(0, 0);")
                time.sleep(random.uniform(0.5, 1.5))

                scroll_height = self.page.evaluate("document.body.scrollHeight")
                current_position = 0
                scroll_step = 400

                # fallback XPath 로드 (|로 구분된 문자열)
                reviews_button_fallback = self.xpaths.get('reviews_button_fallback', {}).get('xpath', '')
                fallback_xpaths = reviews_button_fallback.split('|') if reviews_button_fallback else []

                reviews_button_xpaths = [reviews_button_xpath] + fallback_xpaths

                while current_position < scroll_height:
                    for xpath in reviews_button_xpaths:
                        try:
                            review_button = self.page.locator(xpath).first
                            if review_button.is_visible(timeout=1000):
                                review_button.scroll_into_view_if_needed()
                                time.sleep(random.uniform(1, 3))

                                try:
                                    review_button.click()
                                    review_button_found = True
                                    time.sleep(random.uniform(3, 7))
                                    break
                                except Exception:
                                    continue
                        except Exception:
                            continue

                    if review_button_found:
                        break

                    current_position += scroll_step
                    self.page.evaluate(f"window.scrollTo(0, {current_position});")
                    time.sleep(0.5)

                if review_button_found:
                    try:
                        detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                        if detailed_review_xpath:
                            try:
                                self.page.wait_for_selector(detailed_review_xpath, timeout=30000, state='visible')
                                time.sleep(random.uniform(1, 3))
                            except Exception:
                                time.sleep(random.uniform(3, 7))

                            all_reviews = []
                            current_page = 1
                            max_reviews = 20

                            while len(all_reviews) < max_reviews:
                                if current_page > 1:
                                    try:
                                        self.page.wait_for_load_state('networkidle', timeout=10000)
                                        time.sleep(random.uniform(1, 3))
                                    except Exception:
                                        time.sleep(random.uniform(0.5, 1.5))

                                page_html = self.page.content()
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
                                    next_page_button = self.page.locator(next_page_xpath).first

                                    if next_page_button.is_visible(timeout=3000):
                                        next_page_button.scroll_into_view_if_needed()
                                        time.sleep(random.uniform(0.5, 1.5))
                                        next_page_button.click()
                                        time.sleep(random.uniform(2, 4))
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
                'detailed_review_content': detailed_review_content
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
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
                    product.get('calendar_week'), current_time, self.batch_id
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

                    time.sleep(random.uniform(3, 5))

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
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
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
