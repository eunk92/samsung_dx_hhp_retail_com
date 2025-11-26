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


class AmazonDetailCrawler(BaseCrawler):
    """
    Amazon Detail 페이지 크롤러
    """

    def __init__(self, batch_id=None, login_success=None):
        """초기화. batch_id: 통합 크롤러에서 전달, login_success: 로그인 성공 여부"""
        super().__init__()
        self.batch_id = batch_id
        self.account_name = 'Amazon'
        self.page_type = 'detail'
        self.cookies_loaded = False
        self.login_success = login_success

    def initialize(self):
        """초기화: batch_id 설정 → DB 연결 → XPath 로드 → WebDriver 설정 → 로그 정리"""
        if not self.batch_id:
            self.batch_id = 'a_20251125_212207'

        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        self.setup_driver()

        if self.login_success is False:
            self.cookies_loaded = False
        else:
            self.cookies_loaded = self.load_cookies(self.account_name)

        self.cleanup_old_logs()

        return True

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
                ORDER BY main_rank ASC NULLS LAST, bsr_rank ASC NULLS LAST
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

    def handle_captcha(self):
        """CAPTCHA 자동 해결"""
        try:
            time.sleep(2)
            page_html = self.driver.page_source.lower()

            captcha_keywords = ['captcha', 'robot', 'human verification', 'press & hold', 'press and hold']
            if not any(keyword in page_html for keyword in captcha_keywords):
                return True

            print("[WARNING] CAPTCHA detected!")

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
                print("[INFO] Text CAPTCHA - waiting 60s for manual input...")
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
                    print("[INFO] CAPTCHA still present - waiting 60s...")
                    time.sleep(60)
                    return True

            except Exception:
                time.sleep(60)
                return True

        except Exception as e:
            print(f"[ERROR] CAPTCHA handling failed: {e}")
            return False

    def extract_reviews_from_detail_page(self, tree, max_reviews=10):
        """상세 페이지에서 리뷰 추출 (하드코딩 XPath)"""
        try:
            review_xpath = "//ul[@id='cm-cr-dp-review-list']//div[@data-hook='review-collapsed']//span/text()"
            review_texts = tree.xpath(review_xpath)

            if not review_texts:
                return data_extractor.get_no_reviews_text(self.account_name)

            review_texts = review_texts[:max_reviews]

            cleaned_reviews = []
            for review in review_texts:
                if review.strip():
                    cleaned = ' '.join(review.split())
                    if len(cleaned) > 10:
                        cleaned_reviews.append(cleaned)

            if not cleaned_reviews:
                return data_extractor.get_no_reviews_text(self.account_name)

            result = ' ||| '.join(cleaned_reviews)
            print(f"[INFO] Reviews: {len(cleaned_reviews)}, Length: {len(result)}")
            return result

        except Exception as e:
            print(f"[ERROR] Review extraction failed: {e}")
            return data_extractor.get_no_reviews_text(self.account_name)

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
                return data_extractor.get_no_reviews_text(self.account_name)

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
                "//div[@data-hook='review']//span[@data-hook='review-body']//span/text()",
                "//div[@id='cm-cr-dp-review-list']//span[@data-hook='review-body']//span/text()",
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
            return None

    def crawl_detail(self, product):
        """상세 페이지 크롤링: 페이지 로드 → 필드 추출 → 리뷰 추출 → product_list + detail 데이터 결합"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                return product

            self.driver.get(product_url)
            time.sleep(10)

            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # 로그인/CAPTCHA 체크
            current_url = self.driver.current_url
            if 'signin' in current_url or 'ap/signin' in current_url:
                return product

            if 'robot' in page_html.lower() or 'captcha' in page_html.lower():
                if self.handle_captcha():
                    self.driver.get(product_url)
                    time.sleep(5)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                else:
                    return product

            # 기본값 초기화
            country = 'SEA'
            product_type = 'HHP'
            item = self.extract_asin_from_url(product_url)

            trade_in = self.extract_with_fallback(tree, self.xpaths.get('trade_in', {}).get('xpath'))
            hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
            sku_popularity = self.extract_with_fallback(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            bundle = self.extract_with_fallback(tree, self.xpaths.get('bundle', {}).get('xpath'))
            retailer_membership_discounts = self.extract_with_fallback(tree, self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))

            # Additional details 버튼 클릭
            hhp_storage = None
            hhp_color = None
            rank_1 = None
            rank_2 = None
            additional_details_found = False

            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(2)

                expand_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Additional details')]/ancestor::a"))
                )
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", expand_button)
                time.sleep(1)
                expand_button.click()
                time.sleep(1)
                additional_details_found = True

                try:
                    item_details_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Item details')]/ancestor::a"))
                    )
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item_details_button)
                    time.sleep(1)
                    item_details_button.click()
                    time.sleep(1)
                except Exception:
                    pass

                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

            except Exception:
                pass

            # HHP 스펙 및 랭크 추출
            if additional_details_found:
                hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
                hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
            else:
                fallback_storage_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Memory Storage Capacity')]/following-sibling::td/text()"
                fallback_color_xpath = "//table[@id='productDetails_detailBullets_sections1']//th[contains(text(), 'Color')]/following-sibling::td/text()"
                hhp_storage = self.extract_with_fallback(tree, fallback_storage_xpath)
                hhp_color = self.extract_with_fallback(tree, fallback_color_xpath)

            try:
                rank1_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[1]//span[@class='a-list-item']/span")
                if rank1_elements:
                    rank_1 = rank1_elements[0].text_content().strip()
            except Exception:
                pass

            try:
                rank2_elements = tree.xpath("//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td//li[2]//span[@class='a-list-item']/span")
                if rank2_elements:
                    rank_2 = rank2_elements[0].text_content().strip()
            except Exception:
                pass

            # 리뷰 관련 필드
            count_of_reviews_raw = self.extract_with_fallback(tree, self.xpaths.get('count_of_reviews', {}).get('xpath'))
            count_of_reviews = data_extractor.extract_review_count(count_of_reviews_raw, self.account_name)

            star_rating_raw = self.extract_with_fallback(tree, self.xpaths.get('star_rating', {}).get('xpath'))
            star_rating = data_extractor.extract_rating(star_rating_raw, self.account_name)

            count_of_star_ratings_xpath = self.xpaths.get('count_of_star_ratings', {}).get('xpath')
            count_of_star_ratings = data_extractor.extract_star_ratings_count(tree, count_of_reviews, count_of_star_ratings_xpath, self.account_name)

            # 리뷰 섹션으로 이동
            summarized_review_content = None
            try:
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                review_link = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "acrCustomerReviewLink"))
                )
                review_link.click()
                time.sleep(2)

                page_html = self.driver.page_source
                tree = html.fromstring(page_html)

                summarized_review_content = self.extract_with_fallback(tree, self.xpaths.get('summarized_review_content', {}).get('xpath'))
            except Exception:
                pass

            # 상세 리뷰 추출
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
            }

            combined_data = {**product, **detail_data}
            return combined_data

        except Exception as e:
            print(f"[ERROR] Detail crawl failed: {e}")
            return product

    def save_to_retail_com(self, products):
        """DB 저장: 2-tier retry (BATCH_SIZE=5 → 1개씩)"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
                    current_time,
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
                            self.db_conn.rollback()

            cursor.close()
            return saved_count

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
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
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] {sku_name[:50]}...")

                combined_data = self.crawl_detail(product)
                crawled_products.append(combined_data)

                if not self.cookies_loaded and i == 1:
                    self.save_cookies(self.account_name)
                    self.cookies_loaded = True

                if len(crawled_products) >= SAVE_BATCH_SIZE:
                    saved_count = self.save_to_retail_com(crawled_products)
                    total_saved += saved_count
                    crawled_products = []

                time.sleep(5)

            if crawled_products:
                saved_count = self.save_to_retail_com(crawled_products)
                total_saved += saved_count

            print(f"[DONE] Processed: {len(product_list)}, Saved: {total_saved}, batch_id: {self.batch_id}")
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
    """개별 실행 진입점 (기본 배치 ID 사용)"""
    crawler = AmazonDetailCrawler()
    crawler.run()


if __name__ == '__main__':
    main()
