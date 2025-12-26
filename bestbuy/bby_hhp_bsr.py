"""
BestBuy BSR 페이지 크롤러

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
- bby_hhp_product_list (제품 목록)
"""

import sys
import os
import time
import traceback
import random
import re
from datetime import datetime
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler


class BestBuyBSRCrawler(BaseCrawler):
    """
    BestBuy BSR 페이지 크롤러
    """

    def normalize_bestbuy_url(self, url):
        """BestBuy URL에서 SKU ID 추출 후 표준 URL로 정규화 (중복 판별용, DB 저장은 원본 URL 사용)"""
        if not url:
            return None

        try:
            # /product/제품명/SKU_ID 또는 /product/제품명/SKU_ID/sku/숫자
            match = re.search(r'/product/[^/]+/([A-Z0-9]+)', url, re.IGNORECASE)
            if match:
                return f"https://www.bestbuy.com/product/{match.group(1)}"

            # SKU ID 추출 실패 시 원본 URL 반환
            return url
        except Exception:
            return url

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0
        self.db_url_map = {}       # {정규화URL: 원본URL} - Main에서 저장된 URL
        self.crawled_urls = set()  # BSR에서 수집한 정규화 URL (페이지 간 중복 방지)

        self.test_count = 1  # 테스트 모드
        self.max_products = 100  # 운영 모드
        self.max_pages = 20  # 최대 페이지 수
        self.excluded_keywords = [
            'Screen Magnifier', 'mount', 'holder', 'cable', 'adapter', 'stand', 'wallet'
        ]  # 제외할 키워드 리스트 (retailer_sku_name에 포함 시 수집 제외)

        # 통계 변수
        self.stats = {
            'collected': 0,         # 수집 진행한 갯수
            'duplicates': 0,        # 중복 URL 제거 갯수
            'keyword_filtered': 0,  # 키워드 필터링 갯수
            'updated': 0,           # UPDATE 갯수
            'inserted': 0           # INSERT 갯수
        }

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → WebDriver 설정 → batch_id 생성 → 로그 정리"""
        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type):
            return False
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            return False
        self.setup_driver()

        # batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True)

        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        # DB에서 기존 URL 캐시 로드 (Main에서 저장된 URL → 정규화 매핑)
        self.db_url_map = self.build_db_url_cache()

        return True

    def build_db_url_cache(self):
        """DB에서 현재 batch_id의 URL을 조회하여 {정규화URL: 원본URL} dict로 반환"""
        try:
            cursor = self.db_conn.cursor()
            query = """
                SELECT product_url FROM bby_hhp_product_list
                WHERE account_name = %s AND batch_id = %s
            """
            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            db_url_map = {}
            for (db_url,) in rows:
                if db_url:
                    normalized = self.normalize_bestbuy_url(db_url)
                    if normalized not in db_url_map:
                        db_url_map[normalized] = db_url

            print(f"[INFO] DB URL cache loaded: {len(db_url_map)} URLs (normalized)")
            return db_url_map

        except Exception as e:
            print(f"[WARNING] build_db_url_cache failed: {e}")
            return {}

    def scroll_to_bottom(self):
        """스크롤: 205~350px씩 점진적 스크롤 → 페이지네이션 보이면 종료"""
        try:
            current_position = 0

            for _ in range(50):
                is_pagination_visible = self.driver.execute_script("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """)

                if is_pagination_visible:
                    break

                scroll_step = random.randint(250, 350)
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.5, 0.7))

                total_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 페이지네이션까지 스크롤 → HTML 파싱 → 제품 데이터 추출
        - 0개: 리프레쉬 후 재시도 (최대 3회)
        - 1개 이상: 24개 찾을 때까지 재파싱 (최대 3회)
        """
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(10)

            self.scroll_to_bottom()
            time.sleep(30)

            base_containers = []
            expected_products = 24

            # 0개인 경우 리프레쉬 재시도 (최대 3회) - 페이지 로드 실패 상황
            for refresh_attempt in range(1, 4):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                if len(base_containers) == 0:
                    print(f"[WARNING] Page {page_number}: 0 products found, refresh attempt {refresh_attempt}/3")
                    if refresh_attempt < 3:
                        self.driver.refresh()
                        time.sleep(10)
                    continue
                break

            # 리프레쉬 3회 후에도 0개이면 빈 리스트 반환
            if len(base_containers) == 0:
                print(f"[ERROR] Page {page_number}: No products found after 3 refresh attempts")
                return []

            # 1개 이상 찾은 경우: 스크롤 후 24개 찾을 때까지 재파싱 (최대 3회)
            if len(base_containers) < expected_products:
                for scroll_attempt in range(1, 4):
                    self.scroll_to_bottom()
                    time.sleep(30)
                    page_html = self.driver.page_source
                    tree = html.fromstring(page_html)
                    base_containers = tree.xpath(base_container_xpath)
                    if len(base_containers) >= expected_products:
                        break
                    if scroll_attempt < 3:
                        time.sleep(10)

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    product_url_raw = self.safe_extract(item, 'product_url')
                    # '#'이나 유효하지 않은 URL은 None으로 처리
                    if not product_url_raw or product_url_raw == '#':
                        product_url = None
                    elif product_url_raw.startswith('/'):
                        product_url = f"https://www.bestbuy.com{product_url_raw}"
                    else:
                        product_url = product_url_raw

                    # savings 추출 후 "Save " 제거
                    savings_raw = self.safe_extract(item, 'savings')
                    savings = savings_raw.replace('Save ', '') if savings_raw else None

                    # offer 추출 후 숫자만 추출 ("+ 1 offer for you" → "1")
                    offer_raw = self.safe_extract(item, 'offer')
                    offer = None
                    if offer_raw:
                        match = re.search(r'\d+', offer_raw)
                        offer = match.group() if match else offer_raw

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'final_sku_price': self.safe_extract(item, 'final_sku_price'),
                        'savings': savings,
                        'comparable_pricing': self.safe_extract(item, 'comparable_pricing'),
                        'offer': offer,
                        'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                        'shipping_availability': self.safe_extract(item, 'shipping_availability'),
                        'delivery_availability': self.safe_extract(item, 'delivery_availability'),
                        'sku_status': self.safe_extract(item, 'sku_status'),
                        'promotion_type': self.safe_extract(item, 'promotion_type'),
                        'bsr_rank': 0,  # save_products()에서 재할당
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
        """DB 저장: bsr_rank 할당 → UPDATE 즉시 실행 / INSERT 배치 처리"""
        if not products:
            return {'insert': 0, 'update': 0}

        # 수집 갯수 통계
        self.stats['collected'] += len(products)

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0
            products_to_insert = []

            update_query = """
                UPDATE bby_hhp_product_list
                SET bsr_rank = %s, bsr_page_number = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for product in products:

                # 제외 키워드 필터링 (먼저 수행)
                retailer_sku_name = product.get('retailer_sku_name') or ''
                if self.excluded_keywords and any(keyword.lower() in retailer_sku_name.lower() for keyword in self.excluded_keywords):
                    print(f"[SKIP] 제외 키워드 포함: {retailer_sku_name[:40]}...")
                    self.stats['keyword_filtered'] += 1
                    continue

                product_url = product.get('product_url')
                normalized_url = self.normalize_bestbuy_url(product_url)

                # 1. 페이지 간 중복 체크 (이미 수집한 URL → 스킵)
                if normalized_url in self.crawled_urls:
                    self.stats['duplicates'] += 1
                    continue
                self.crawled_urls.add(normalized_url)

                # bsr_rank 할당
                self.current_rank += 1
                product['bsr_rank'] = self.current_rank

                # 2. DB 캐시에서 기존 URL 체크 → UPDATE / INSERT 분류
                matched_url = self.db_url_map.get(normalized_url)
                if matched_url:
                    try:
                        cursor.execute(update_query, (
                            product['bsr_rank'],
                            product['page_number'],
                            self.account_name,
                            product['batch_id'],
                            matched_url  # DB에 저장된 원본 URL 사용
                        ))
                        self.db_conn.commit()
                        update_count += 1
                    except Exception as e:
                        print(f"[ERROR] UPDATE failed: {product_url[:50] if product_url else 'N/A'}: {e}")
                        self.db_conn.rollback()
                else:
                    products_to_insert.append(product)

            if not products_to_insert and update_count == 0:
                print("[INFO] No products to save")
                cursor.close()
                return {'insert': 0, 'update': 0}

            # INSERT 처리 (3-tier retry: BATCH_SIZE → RETRY_SIZE → 1개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO bby_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, savings, comparable_pricing,
                        offer, pick_up_availability, shipping_availability, delivery_availability,
                        sku_status, promotion_type, bsr_rank, bsr_page_number, product_url,
                        calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                        product['savings'],
                        product['comparable_pricing'],
                        product['offer'],
                        product['pick_up_availability'],
                        product['shipping_availability'],
                        product['delivery_availability'],
                        product['sku_status'],
                        product['promotion_type'],
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
            self.stats['updated'] += update_count
            self.stats['inserted'] += insert_count
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
            self.current_rank = 0
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

                time.sleep(30)
                page_num += 1

            if page_num > self.max_pages:
                print(f"[INFO] Max pages ({self.max_pages}) reached")

            print(f"[DONE] Page: {page_num}, Update: {total_update}, Insert: {total_insert}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 통계 출력
            print(f"\n{'='*50}")
            print(f"[통계] 수집: {self.stats['collected']}, 중복제거: {self.stats['duplicates']}, 키워드필터: {self.stats['keyword_filtered']}, UPDATE: {self.stats['updated']}, INSERT: {self.stats['inserted']}")
            print(f"{'='*50}")

            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = BestBuyBSRCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
