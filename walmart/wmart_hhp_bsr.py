"""
Walmart BSR 페이지 크롤러 (Playwright 기반)

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
- CAPTCHA 자동 해결 기능 포함

================================================================================ 
저장 테이블
================================================================================
- wmart_hhp_product_list (제품 목록)
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


class WalmartBSRCrawler(BaseCrawler):
    """
    Walmart BSR 페이지 크롤러 (Playwright 기반)
    """

    def __init__(self, test_mode=True, batch_id=None):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Walmart'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.standalone = batch_id is None  # 개별 실행 여부
        self.calendar_week = None
        self.url_template = None

        # Selenium/undetected-chromedriver 객체
        self.driver = None
        self.wait = None

        self.test_count = 1  # 테스트 모드
        self.max_products = 100  # 운영 모드
        self.current_rank = 0

        # 중복 URL 추적용 (동일 batch_id 내 중복 제거)
        self.saved_urls = set()

    def format_walmart_price(self, price_result):
        """Walmart 가격 결과를 $XX.XX 형식으로 변환"""
        if not price_result:
            return None

        try:
            if isinstance(price_result, list):
                parts = [p.strip() for p in price_result if p.strip()]
                if not parts:
                    return None

                if len(parts) >= 3:
                    dollar_idx = None
                    for i, p in enumerate(parts):
                        if '$' in p:
                            dollar_idx = i
                            break

                    if dollar_idx is not None and dollar_idx + 2 < len(parts):
                        dollars = parts[dollar_idx + 1]
                        cents = parts[dollar_idx + 2]
                        if dollars.isdigit() and cents.isdigit():
                            return f"${dollars}.{cents}"

                price_result = ''.join(parts)

            if isinstance(price_result, str):
                if '$' in price_result and '.' in price_result:
                    return price_result.strip()

                match = re.search(r'\$(\d+)\.?(\d{2})?', price_result)
                if match:
                    dollars = match.group(1)
                    cents = match.group(2) if match.group(2) else '00'
                    return f"${dollars}.{cents}"

            return None

        except Exception as e:
            print(f"[WARNING] Price formatting failed: {e}")
            return None

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

    def handle_captcha(self, max_attempts=3):
        """CAPTCHA 감지 및 수동 처리"""
        try:
            time.sleep(2)

            # CAPTCHA 키워드 감지
            captcha_keywords = ['press & hold', 'press and hold', 'human verification', 'robot or human', 'verify you are human']

            for attempt in range(max_attempts):
                page_content = self.driver.page_source.lower()

                if not any(keyword in page_content for keyword in captcha_keywords):
                    if attempt > 0:
                        print("[OK] CAPTCHA 해결됨")
                    return True

                print(f"[WARNING] CAPTCHA 감지! (시도 {attempt + 1}/{max_attempts})")
                print("[INFO] 브라우저에서 CAPTCHA를 수동으로 해결한 후 엔터를 누르세요...")
                input()

            return True

        except Exception as e:
            print(f"[WARNING] CAPTCHA handling error: {e}")
            return True

    def initialize_session(self):
        """세션 초기화: example.com → walmart.com → 검색 → 카테고리 순차 접근 (TV 크롤러와 동일)"""
        try:
            print("[INFO] 세션 초기화 중...")

            # 1단계: 중립 사이트 방문 (브라우저 fingerprint 생성)
            print("[INFO] Step 1/4: 중립 사이트 방문...")
            self.driver.get('https://www.example.com')
            time.sleep(random.uniform(2, 4))
            self.add_random_mouse_movements()

            # 2단계: Walmart 메인 페이지 방문 (쿠키/세션 생성)
            print("[INFO] Step 2/4: Walmart 메인 페이지 방문...")
            self.driver.get('https://www.walmart.com')
            time.sleep(random.uniform(8, 12))

            # CAPTCHA 체크
            self.handle_captcha()

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

            # 3단계: 검색창에서 검색 시도 (TV 크롤러와 동일)
            print("[INFO] Step 3/4: 검색창에서 'phone' 검색 시도...")
            try:
                from selenium.webdriver.common.keys import Keys

                search_selectors = [
                    "input[type='search']",
                    "input[aria-label*='Search']",
                    "input[placeholder*='Search']",
                    "input[name='q']"
                ]

                search_box = None
                for selector in search_selectors:
                    try:
                        search_box = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        if search_box:
                            print(f"[OK] 검색창 발견: {selector}")
                            break
                    except:
                        continue

                if search_box:
                    # 검색창 클릭
                    search_box.click()
                    time.sleep(random.uniform(2, 3))

                    # "phone" 타이핑 (사람처럼 천천히)
                    for char in "cellphone":
                        search_box.send_keys(char)
                        time.sleep(random.uniform(0.1, 0.3))

                    time.sleep(random.uniform(3, 5))

                    # 검색 실행 (엔터)
                    search_box.send_keys(Keys.ENTER)

                    # 검색 결과 대기
                    time.sleep(random.uniform(8, 12))

                    # CAPTCHA 체크
                    self.handle_captcha()

                    # 자연스러운 스크롤
                    for _ in range(2):
                        self.driver.execute_script(f"window.scrollBy(0, {random.randint(200, 400)})")
                        time.sleep(random.uniform(1, 2))

                    print("[OK] 검색 완료")
                else:
                    print("[WARNING] 검색창을 찾지 못함, 검색 단계 건너뜀")

            except Exception as e:
                print(f"[WARNING] 검색 실패 (계속 진행): {e}")

            print("[OK] 세션 초기화 완료")
            return True

        except Exception as e:
            print(f"[WARNING] 세션 초기화 실패 (계속 진행): {e}")
            return True  # 실패해도 계속 진행

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → 브라우저 설정 → batch_id 생성 → 로그 정리"""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 2. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 3. URL 템플릿 로드
        self.url_template = self.load_page_urls(self.account_name, self.page_type)
        if not self.url_template:
            print(f"[ERROR] Initialize failed: URL template load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 4. 브라우저 설정 (undetected-chromedriver)
        if not self.setup_browser():
            print("[ERROR] Initialize failed: Browser setup failed")
            return False

        # 5. 세션 초기화 (example.com → walmart.com → 카테고리)
        self.initialize_session()

        # 6. batch_id 생성
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name)

        # 7. calendar_week 생성 및 로그 정리
        self.calendar_week = self.generate_calendar_week()
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, calendar_week={self.calendar_week}")
        return True

    def scroll_to_bottom(self):
        """스크롤: 150~300px씩 느린 점진적 스크롤 → 페이지 하단까지 진행"""
        try:
            current_position = 0

            while True:
                scroll_step = random.randint(150, 300)
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position})")
                time.sleep(random.uniform(1.5, 2.5))  # 더 느린 스크롤

                # 가끔 마우스 움직임 추가
                if random.random() < 0.3:
                    self.add_random_mouse_movements()

                total_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(random.uniform(2, 4))

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → CAPTCHA 처리 → 스크롤 → HTML 파싱(40개 검증) → 제품 데이터 추출"""
        try:
            # 첫 페이지는 &page= 파라미터 없이 URL 생성
            if page_number == 1:
                url = self.url_template.replace('&page={page}', '')
            else:
                url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.driver.get(url)
            time.sleep(random.uniform(10, 15))  # 페이지 로드 대기 시간 증가

            # 마우스 움직임 추가
            self.add_random_mouse_movements()

            if page_number == 1:
                self.handle_captcha()
                time.sleep(random.uniform(3, 5))

            # 40개 검증 (최대 3회 재시도: 파싱 → 부족하면 스크롤 → 재파싱)
            base_containers = []
            expected_products = 40

            for attempt in range(1, 4):
                page_html = self.driver.page_source
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                if len(base_containers) >= expected_products:
                    break

                if attempt < 3:
                    print(f"[WARNING] Page {page_number}: {len(base_containers)}/{expected_products} products, retrying ({attempt}/3)...")
                    self.scroll_to_bottom()
                    time.sleep(random.uniform(3, 5))

            print(f"[INFO] Page {page_number}: {len(base_containers)} products found")

            products = []
            for idx, item in enumerate(base_containers, 1):
                try:
                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.walmart.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    final_price_xpath = self.xpaths.get('final_sku_price', {}).get('xpath')
                    final_price_raw = item.xpath(final_price_xpath) if final_price_xpath else None
                    final_sku_price = self.format_walmart_price(final_price_raw)

                    membership_discounts_raw = self.safe_extract(item, 'retailer_membership_discounts')
                    retailer_membership_discounts = f"{membership_discounts_raw} W+" if membership_discounts_raw else None

                    sku_status_1 = self.safe_extract(item, 'sku_status_1')
                    sku_status_2 = self.safe_extract(item, 'sku_status_2')
                    sku_status_parts = [s for s in [sku_status_1, sku_status_2] if s]
                    sku_status = ', '.join(sku_status_parts) if sku_status_parts else None

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                        'final_sku_price': final_sku_price,
                        'original_sku_price': self.safe_extract(item, 'original_sku_price'),
                        'offer': self.safe_extract(item, 'offer'),
                        'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                        'shipping_availability': self.safe_extract(item, 'shipping_availability'),
                        'delivery_availability': self.safe_extract(item, 'delivery_availability'),
                        'sku_status': sku_status,
                        'retailer_membership_discounts': retailer_membership_discounts,
                        'available_quantity_for_purchase': self.safe_extract(item, 'available_quantity_for_purchase'),
                        'inventory_status': self.safe_extract(item, 'inventory_status'),
                        'bsr_rank': idx,  # save_products에서 중복 제거 후 재할당됨
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
        """DB 저장: 중복 제거 → rank 재할당 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
        if not products:
            return 0

        try:
            # 중복 URL 제거 및 bsr_rank 할당
            unique_products = []
            for product in products:
                product_url = product.get('product_url')
                if product_url and product_url in self.saved_urls:
                    print(f"[SKIP] 중복 URL: {product.get('retailer_sku_name', 'N/A')[:40]}...")
                    continue
                if product_url:
                    self.saved_urls.add(product_url)

                # rank 할당 (중복 제거된 제품에만 순차적으로)
                self.current_rank += 1
                product['bsr_rank'] = self.current_rank
                unique_products.append(product)

            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0

            products_to_update = []
            products_to_insert = []

            for product in unique_products:
                exists = self.check_product_exists(
                    self.account_name,
                    product['batch_id'],
                    product['product_url']
                )
                if exists:
                    products_to_update.append(product)
                else:
                    products_to_insert.append(product)

            # UPDATE 처리
            update_query = """
                UPDATE wmart_hhp_product_list
                SET bsr_rank = %s, bsr_page_number = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for product in products_to_update:
                try:
                    cursor.execute(update_query, (
                        product['bsr_rank'],
                        product['page_number'],
                        self.account_name,
                        product['batch_id'],
                        product['product_url']
                    ))
                    self.db_conn.commit()
                    update_count += 1
                except Exception:
                    self.db_conn.rollback()

            # INSERT 처리 (3-tier retry: BATCH_SIZE → RETRY_SIZE → 1개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO wmart_hhp_product_list (
                        account_name, page_type, retailer_sku_name,
                        final_sku_price, original_sku_price, offer,
                        pick_up_availability, shipping_availability, delivery_availability,
                        sku_status, retailer_membership_discounts,
                        available_quantity_for_purchase, inventory_status,
                        bsr_rank, bsr_page_number, product_url,
                        calendar_week, crawl_strdatetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                        product['original_sku_price'],
                        product['offer'],
                        product['pick_up_availability'],
                        product['shipping_availability'],
                        product['delivery_availability'],
                        product['sku_status'],
                        product['retailer_membership_discounts'],
                        product['available_quantity_for_purchase'],
                        product['inventory_status'],
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

            while (total_insert + total_update) < target_products:
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

                time.sleep(random.uniform(8, 12))  # 페이지 간 대기 시간 증가
                page_num += 1

            print(f"[DONE] Page: {page_num}, Update: {total_update}, Insert: {total_insert}, batch_id: {self.batch_id}")
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
                input("\n엔터키를 누르면 종료합니다...")


def main():
    """개별 실행 진입점 (테스트 모드)"""
    crawler = WalmartBSRCrawler(test_mode=True)
    crawler.run()


if __name__ == '__main__':
    main()
