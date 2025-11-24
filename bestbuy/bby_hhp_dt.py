"""
BestBuy Detail 페이지 크롤러
- 통합 크롤러에서만 실행 가능 (batch_id 필수)
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR/Promotion에서 수집한 모든 제품 처리
- hhp_retail_com 및 bby_hhp_mst 테이블에 저장
"""

import sys
import os
import time
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

    def __init__(self, batch_id):
        """
        초기화

        Args:
            batch_id (str): 배치 ID (필수)

        Raises: ValueError: batch_id가 제공되지 않은 경우
        """
        super().__init__()

        if not batch_id:
            raise ValueError(
                "batch_id is required. "
                "Detail crawler must be called from integrated crawler with batch_id."
            )

        self.batch_id = batch_id
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] BestBuy Detail Crawler Initialization")
        print(f"[INFO] Batch ID: {self.batch_id}")
        print("="*60 + "\n")

        # 1. DB 연결
        if not self.connect_db():
            return False

        # 2. XPath 셀렉터 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            return False

        # 3. WebDriver 설정
        self.setup_driver()

        # 4. 오래된 로그 정리
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

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()

            cursor.close()

            # 딕셔너리 변환
            product_list = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'page_type': None,  # Detail에서 결정
                    'retailer_sku_name': row[0],
                    'final_sku_price': row[1],
                    'savings': row[2],
                    'original_sku_price': row[3],
                    'offer': row[4],
                    'pick_up_availability': row[5],
                    'shipping_availability': row[6],
                    'delivery_availability': row[7],
                    'sku_status': row[8],
                    'promotion_type': row[9],
                    'main_rank': row[10],
                    'bsr_rank': row[11],
                    'promotion_rank': row[12],
                    'product_url': row[13],
                    'calendar_week': row[14]
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

            # 리뷰 수 요소가 나타날 때까지 대기 (최대 120초)
            # 리뷰 수는 동적 로딩되므로 이것이 나타나면 페이지 완전 로드됨
            try:
                print("[INFO] Waiting for page to load (checking for review count element)...")
                WebDriverWait(self.driver, 120).until(
                    lambda driver: driver.find_elements(By.XPATH, "//aside[@class='col-sm-4 col-lg-3']//div[contains(@class, 'v-text-dark-gray') and contains(@class, 'text-center') and contains(text(), 'review')]")
                )
                print("[INFO] Page fully loaded - review section found")
            except Exception as e:
                print(f"[WARNING] Timeout waiting for review section (120s): {e}")
                print("[INFO] Proceeding with data extraction anyway...")
                time.sleep(5)  # fallback 대기

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # item: product_url에서 추출 (우선) 또는 XPath에서 추출 (fallback)
            item = self.extract_item_from_url(product_url)

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

            sku_popularity = self.extract_with_fallback(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            discount_type = self.extract_with_fallback(tree, self.xpaths.get('discount_type', {}).get('xpath'))
            bundle = self.extract_with_fallback(tree, self.xpaths.get('bundle', {}).get('xpath'))
            inventory_status = self.extract_with_fallback(tree, self.xpaths.get('inventory_status', {}).get('xpath'))
            retailer_membership_discounts = self.extract_with_fallback(tree, self.xpaths.get('retailer_membership_discounts', {}).get('xpath'))
            hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
            hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
            hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))
            top_mentions = self.extract_with_fallback(tree, self.xpaths.get('top_mentions', {}).get('xpath'))
            summarized_review_content = self.extract_with_fallback(tree, self.xpaths.get('summarized_review_content', {}).get('xpath'))

            # Compare Similar Products 섹션 추출
            # XPath로 각 유사 제품 컨테이너 추출
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')

            similar_products_data = []  # [{name, pros, cons, url}, ...]
            retailer_sku_name_similar = None

            if similar_products_container_xpath:
                # 각 유사 제품 컨테이너 (카드) 추출
                similar_product_containers = tree.xpath(similar_products_container_xpath)

                if similar_product_containers:
                    similar_product_names = []

                    for container in similar_product_containers:
                        # 제품명 추출
                        name_xpath = self.xpaths.get('similar_product_name', {}).get('xpath')
                        name = container.xpath(name_xpath)[0] if name_xpath and container.xpath(name_xpath) else None

                        # Pros 추출 (리스트로 저장)
                        pros_xpath = self.xpaths.get('pros', {}).get('xpath')
                        pros_list = container.xpath(pros_xpath) if pros_xpath else []

                        # Cons 추출 (리스트로 저장)
                        cons_xpath = self.xpaths.get('cons', {}).get('xpath')
                        cons_list = container.xpath(cons_xpath) if cons_xpath else []

                        # URL 추출
                        url_xpath = self.xpaths.get('similar_product_url', {}).get('xpath')
                        similar_product_url = container.xpath(url_xpath)[0] if url_xpath and container.xpath(url_xpath) else None

                        # 유사 제품 데이터 저장
                        if name:  # 제품명이 있는 경우만
                            similar_products_data.append({
                                'name': name,
                                'pros': pros_list,  # 리스트 그대로 저장
                                'cons': cons_list,  # 리스트 그대로 저장
                                'url': similar_product_url
                            })
                            similar_product_names.append(name)

                    # 모든 유사 제품명을 ||| 구분자로 연결
                    retailer_sku_name_similar = '|||'.join(similar_product_names) if similar_product_names else None

            # 리뷰 데이터 추출: "See All Customer Reviews" 버튼 클릭 후 추출
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')

            if reviews_button_xpath:
                try:
                    # "See All Customer Reviews" 버튼 찾기 및 클릭
                    review_button = self.driver.find_element("xpath", reviews_button_xpath)
                    if review_button:
                        print(f"[INFO] Clicking 'See All Customer Reviews' button")
                        review_button.click()
                        time.sleep(30)  # 페이지 로드 대기

                        # 새 페이지의 HTML 파싱
                        page_html = self.driver.page_source
                        tree = html.fromstring(page_html)

                        # 리뷰 본문 추출 (최대 20개)
                        detailed_review_xpath = self.xpaths.get('detailed_review_content', {}).get('xpath')
                        if detailed_review_xpath:
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
                    else:
                        print(f"[WARNING] Review button not found on page")
                except Exception as e:
                    print(f"[WARNING] Failed to click review button or extract reviews: {e}")
 

            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'sku_popularity': sku_popularity,
                'discount_type': discount_type,
                'bundle': bundle,
                'inventory_status': inventory_status,
                'retailer_membership_discounts': retailer_membership_discounts,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'hhp_carrier': hhp_carrier,
                'detailed_review_content': detailed_review_content,
                'summarized_review_content': summarized_review_content,
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
                    retailer_membership_discounts,
                    hhp_storage, hhp_color, hhp_carrier,
                    detailed_review_content, summarized_review_content, top_mentions,
                    retailer_sku_name_similar,
                    main_rank, bsr_rank, promotion_rank,
                    promotion_type,
                    calendar_week, crawl_strdatetime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
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
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('hhp_carrier'),
                    product.get('detailed_review_content'),
                    product.get('summarized_review_content'),
                    product.get('top_mentions'),
                    product.get('retailer_sku_name_similar'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('promotion_rank'),
                    product.get('promotion_type'),
                    product.get('calendar_week'),
                    current_time
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
    개별 실행 시 진입점
    """
    import sys

    if len(sys.argv) < 2:
        print("[ERROR] batch_id is required")
        print("Usage: python bby_hhp_dt.py <batch_id>")
        sys.exit(1)

    batch_id = sys.argv[1]
    crawler = BestBuyDetailCrawler(batch_id=batch_id)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy Detail Crawler completed successfully")
    else:
        print("\n[FAILED] BestBuy Detail Crawler failed")


if __name__ == '__main__':
    main()