"""
Walmart Detail 페이지 크롤러
- 통합 크롤러에서만 실행 가능 (batch_id 필수)
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR에서 수집한 모든 제품 처리
"""

import sys
import os
import time
import traceback
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common import data_extractor


class WalmartDetailCrawler(BaseCrawler):
    """
    Walmart Detail 페이지 크롤러
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
        self.account_name = 'Walmart'
        self.page_type = 'detail'
        self.cookies_loaded = False

    def initialize(self):
        """
        크롤러 초기화 작업

        Returns: bool: 초기화 성공 시 True, 실패 시 False
        """
        print("\n" + "="*60)
        print(f"[INFO] Walmart Detail Crawler Initialization")
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

        # 4. 쿠키 로드
        self.cookies_loaded = self.load_cookies(self.account_name)

        # 5. 오래된 로그 정리
        self.cleanup_old_logs()

        return True

    def load_product_list(self):
        """
        wmart_hhp_product_list 테이블에서 제품 URL 및 기본 정보 조회

        Returns: list: 제품 정보 리스트
        """
        try:
            cursor = self.db_conn.cursor()

            # product_list에서 hhp_retail_com으로 매핑할 필드 조회
            query = """
                SELECT DISTINCT ON (product_url)
                    retailer_sku_name,
                    final_sku_price,
                    original_sku_price,
                    offer,
                    pick_up_availability,
                    shipping_availability,
                    delivery_availability,
                    sku_status,
                    retailer_membership_discounts,
                    available_quantity_for_purchase,
                    inventory_status,
                    main_rank,
                    bsr_rank,
                    product_url,
                    calendar_week,
                    crawl_strdatetime
                FROM wmart_hhp_product_list
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
                    'crawl_strdatetime': row[15]
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products from wmart_hhp_product_list")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

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

            print(f"[INFO] Crawling detail page: {product_url}")

            # 상세 페이지 로드
            self.driver.get(product_url)
            time.sleep(5)

            # HTML 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # Detail 필드 추출
            item = self.extract_with_fallback(tree, self.xpaths.get('item', {}).get('xpath'))

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
            shipping_info = self.extract_with_fallback(tree, self.xpaths.get('shipping_info', {}).get('xpath'))
            hhp_storage = self.extract_with_fallback(tree, self.xpaths.get('hhp_storage', {}).get('xpath'))
            hhp_color = self.extract_with_fallback(tree, self.xpaths.get('hhp_color', {}).get('xpath'))
            hhp_carrier = self.extract_with_fallback(tree, self.xpaths.get('hhp_carrier', {}).get('xpath'))

            # 리뷰 데이터 추출 (data_extractor 사용)
            detailed_review_content = data_extractor.extract_reviews(tree, self.xpaths.get('detailed_review_content', {}).get('xpath'))
            summarized_review_content = self.extract_with_fallback(tree, self.xpaths.get('summarized_review_content', {}).get('xpath'))
            top_mentions = self.extract_with_fallback(tree, self.xpaths.get('top_mentions', {}).get('xpath'))

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
                'shipping_info': shipping_info,
                'hhp_storage': hhp_storage,
                'hhp_color': hhp_color,
                'hhp_carrier': hhp_carrier,
                'detailed_review_content': detailed_review_content,
                'summarized_review_content': summarized_review_content,
                'top_mentions': top_mentions
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
                    final_sku_price, original_sku_price, discount_type,
                    offer, bundle,
                    pick_up_availability, shipping_availability, delivery_availability,
                    shipping_info,
                    available_quantity_for_purchase, inventory_status, sku_status,
                    retailer_membership_discounts,
                    hhp_storage, hhp_color, hhp_carrier,
                    detailed_review_content, summarized_review_content, top_mentions,
                    main_rank, bsr_rank,
                    calendar_week, crawl_strdatetime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """

            saved_count = 0

            for product in products:
                cursor.execute(insert_query, (
                    'US',
                    'HHP',
                    product.get('item'),
                    self.account_name,
                    'detail',
                    product.get('count_of_reviews'),
                    product.get('retailer_sku_name'),
                    product.get('product_url'),
                    product.get('star_rating'),
                    product.get('count_of_star_ratings'),
                    product.get('sku_popularity'),
                    product.get('final_sku_price'),
                    product.get('original_sku_price'),
                    product.get('discount_type'),
                    product.get('offer'),
                    product.get('bundle'),
                    product.get('pick_up_availability'),
                    product.get('shipping_availability'),
                    product.get('delivery_availability'),
                    product.get('shipping_info'),
                    product.get('available_quantity_for_purchase'),
                    product.get('inventory_status'),
                    product.get('sku_status'),
                    product.get('retailer_membership_discounts'),
                    product.get('hhp_storage'),
                    product.get('hhp_color'),
                    product.get('hhp_carrier'),
                    product.get('detailed_review_content'),
                    product.get('summarized_review_content'),
                    product.get('top_mentions'),
                    product.get('main_rank'),
                    product.get('bsr_rank'),
                    product.get('calendar_week'),
                    current_time
                ))
                saved_count += 1

            self.db_conn.commit()
            cursor.close()

            print(f"[SUCCESS] Saved {saved_count} products to hhp_retail_com")
            for i, product in enumerate(products[:3], 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                item = product.get('item', 'N/A')
                print(f"[{i}] {sku_name[:40]}... (Item: {item})")
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
                print("[WARNING] No products found in wmart_hhp_product_list")
                return False

            print("\n" + "="*60)
            print(f"[INFO] Starting Walmart Detail page crawling...")
            print(f"[INFO] Total products to crawl: {len(product_list)}")
            print("="*60 + "\n")

            # 모든 제품 상세 페이지 크롤링
            total_saved = 0

            for i, product in enumerate(product_list, 1):
                sku_name = product.get('retailer_sku_name', 'N/A')
                print(f"[{i}/{len(product_list)}] Processing: {sku_name[:50]}...")

                combined_data = self.crawl_detail(product)

                # 첫 제품 크롤링 후 쿠키 저장
                if not self.cookies_loaded and i == 1:
                    self.save_cookies(self.account_name)
                    self.cookies_loaded = True

                # 1개 제품마다 즉시 DB에 저장
                saved_count = self.save_to_retail_com([combined_data])
                total_saved += saved_count

                # 페이지 간 대기
                time.sleep(5)

            # 결과 출력
            print("\n" + "="*60)
            print(f"[COMPLETE] Walmart Detail Crawler Finished")
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
        print("Usage: python wmart_hhp_dt.py <batch_id>")
        sys.exit(1)

    batch_id = sys.argv[1]
    crawler = WalmartDetailCrawler(batch_id=batch_id)
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Walmart Detail Crawler completed successfully")
    else:
        print("\n[FAILED] Walmart Detail Crawler failed")


if __name__ == '__main__':
    main()