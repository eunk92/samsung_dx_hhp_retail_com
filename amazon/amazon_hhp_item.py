"""
Amazon Item MST 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: batch_id=None (하드코딩된 batch_id 사용)
- 통합 크롤러: batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- hhp_retail_com에서 해당 batch_id의 item 조회
- hhp_item_mst에 없으면 INSERT (sku 없어도 빈값으로)
- hhp_item_mst에 있는데 sku가 null/빈값이고 추출된 sku가 있으면 UPDATE
- 추출된 sku 없으면 SKIP

================================================================================
저장 테이블
================================================================================
- hhp_item_mst (제품 마스터)
"""

import sys
import os
import time
import traceback
import random
from lxml import html

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


class AmazonItemCrawler(BaseCrawler):
    """
    Amazon Item MST 크롤러
    - hhp_retail_com에서 item 조회
    - hhp_item_mst에 INSERT 또는 UPDATE
    """

    def __init__(self, batch_id=None, test_mode=False):
        """초기화. batch_id: 통합 크롤러에서 전달, None이면 개별 실행"""
        super().__init__()
        self.batch_id = batch_id
        self.account_name = 'Amazon'
        self.page_type = 'detail'
        self.test_mode = test_mode
        self.xpaths = {}

    def initialize(self):
        """초기화: batch_id 설정 → DB 연결 → XPath 로드 → WebDriver 설정"""
        # 1. batch_id 설정
        if not self.batch_id:
            self.batch_id = 'a_20251209_224208'  # 개별 실행시 기본값

        # 2. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 3. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 4. WebDriver 설정
        try:
            self.setup_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: WebDriver setup failed - {e}")
            traceback.print_exc()
            return False

        # 5. Zipcode 설정
        if not self.set_zipcode():
            print("[WARNING] Zipcode 설정 실패, 계속 진행...")

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}")
        return True

    def set_zipcode(self, zipcode="10001", max_retries=3):
        """Amazon 배송지 Zipcode 설정 (New York)"""
        for attempt in range(max_retries):
            try:
                print(f"[INFO] Zipcode 설정 중: {zipcode} (시도 {attempt + 1}/{max_retries})")

                # Amazon 메인 페이지로 이동
                self.driver.get("https://www.amazon.com")
                time.sleep(random.uniform(3, 5))

                # Continue shopping 버튼 처리
                self.handle_continue_shopping()

                # 배송지 변경 링크 클릭
                try:
                    delivery_link = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "nav-global-location-popover-link"))
                    )
                    delivery_link.click()
                    time.sleep(random.uniform(2, 3))
                except Exception as e:
                    print(f"[WARNING] 배송지 링크 클릭 실패: {e} - 페이지 새로고침 후 재시도...")
                    self.driver.refresh()
                    time.sleep(random.uniform(3, 5))
                    continue

                # Zipcode 입력 필드 찾기 및 입력
                try:
                    zipcode_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput"))
                    )
                    zipcode_input.clear()
                    zipcode_input.send_keys(zipcode)
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    print(f"[WARNING] Zipcode 입력 실패: {e}")
                    continue

                # Apply 버튼 클릭
                try:
                    apply_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#GLUXZipUpdate input[type='submit'], #GLUXZipUpdate-announce"))
                    )
                    apply_button.click()
                    time.sleep(random.uniform(2, 3))
                except Exception as e:
                    print(f"[WARNING] Apply 버튼 클릭 실패: {e}")
                    continue

                # 모달 닫기 (Continue 또는 Done 버튼)
                try:
                    close_buttons = [
                        "//button[@name='glowDoneButton']",
                        "//button[contains(@class, 'a-popover-close')]",
                        "//input[@data-action='GLUXConfirmAction']"
                    ]
                    for xpath in close_buttons:
                        try:
                            close_btn = self.driver.find_element(By.XPATH, xpath)
                            if close_btn.is_displayed():
                                close_btn.click()
                                break
                        except:
                            continue
                    time.sleep(random.uniform(1, 2))
                except Exception:
                    pass

                print(f"[OK] Zipcode 설정 완료: {zipcode}")
                return True

            except Exception as e:
                print(f"[WARNING] Zipcode 설정 실패 (시도 {attempt + 1}): {e}")
                continue

        print(f"[ERROR] Zipcode 설정 실패 - 최대 재시도 횟수 초과")
        return False

    def load_items_from_retail_com(self):
        """hhp_retail_com에서 해당 batch_id의 item 목록 조회"""
        try:
            cursor = self.db_conn.cursor()

            # 테스트 모드면 test_hhp_retail_com, 아니면 hhp_retail_com
            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'

            query = f"""
                SELECT DISTINCT item, product_url
                FROM {table_name}
                WHERE batch_id = %s
                  AND account_name = %s
                  AND item IS NOT NULL
                  AND item != ''
                ORDER BY item
            """

            cursor.execute(query, (self.batch_id, self.account_name))
            rows = cursor.fetchall()
            cursor.close()

            items = []
            for row in rows:
                items.append({
                    'item': row[0],
                    'product_url': row[1]
                })

            print(f"[INFO] Loaded {len(items)} items from {table_name}")
            return items

        except Exception as e:
            print(f"[ERROR] Failed to load items: {e}")
            traceback.print_exc()
            return []

    def check_item_exists(self, item):
        """hhp_item_mst에서 item 존재 여부 및 기존 sku 조회"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT sku FROM hhp_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))

            row = cursor.fetchone()
            cursor.close()

            if row is None:
                return None, None  # 존재하지 않음
            else:
                return True, row[0]  # 존재함, 기존 sku 값

        except Exception as e:
            print(f"[ERROR] check_item_exists failed: {e}")
            return None, None

    def handle_continue_shopping(self):
        """Continue shopping 버튼 처리"""
        try:
            page_html = self.driver.page_source.lower()
            captcha_keywords = ['captcha', 'robot', 'human verification', 'press & hold', 'press and hold']

            if not any(keyword in page_html for keyword in captcha_keywords):
                return True

            # Continue shopping 버튼 찾기
            try:
                continue_btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Continue shopping')]"))
                )
                if continue_btn.is_displayed():
                    actions = ActionChains(self.driver)
                    actions.move_to_element(continue_btn)
                    actions.pause(random.uniform(0.5, 1.0))
                    actions.click()
                    actions.perform()
                    time.sleep(random.uniform(3, 5))
                    print("[INFO] Continue shopping 버튼 클릭 완료")
                    return True
            except:
                pass

            return True

        except Exception as e:
            print(f"[WARNING] handle_continue_shopping failed: {e}")
            return True

    def extract_sku_from_page(self, product_url):
        """상세 페이지에서 SKU(Item model number) 추출"""
        if not product_url:
            return None

        try:
            print(f"[INFO] Accessing: {product_url[:80]}...")
            self.driver.get(product_url)
            time.sleep(random.uniform(3, 5))

            # Continue shopping 버튼 처리
            self.handle_continue_shopping()

            # 페이지 소스 파싱
            page_html = self.driver.page_source
            tree = html.fromstring(page_html)

            # DB에서 로드한 XPath 사용 (sku 필드)
            sku_xpath = self.xpaths['sku']['xpath']
            results = tree.xpath(sku_xpath)

            if results:
                sku = results[0].text_content().strip()
                print(f"  [OK] SKU found: {sku}")
                return sku

            print(f"  [--] SKU not found")
            return None

        except Exception as e:
            print(f"[ERROR] extract_sku_from_page failed: {e}")
            return None

    def upsert_item_mst(self, item_data, extracted_sku):
        """hhp_item_mst 테이블에 INSERT 또는 UPDATE
        - 조회 결과 없음 → INSERT (sku 없어도 빈값으로)
        - 조회 결과 있음 + 기존 sku null/빈값 + 새 sku 있음 → UPDATE
        - 조회 결과 있음 + 기존 sku null/빈값 + 새 sku도 없음 → SKIP
        """
        item = item_data.get('item')
        product_url = item_data.get('product_url')

        if not item:
            return 'skip'

        try:
            cursor = self.db_conn.cursor()
            new_sku = extracted_sku or ''

            # 기존 데이터 조회
            exists, existing_sku = self.check_item_exists(item)

            if exists is None:
                # 조회 결과 없음 → INSERT (sku 없어도 빈값으로)
                cursor.execute("""
                    INSERT INTO hhp_item_mst (item, account_name, sku, product_url)
                    VALUES (%s, %s, %s, %s)
                """, (item, self.account_name, new_sku, product_url))
                self.db_conn.commit()
                print(f"  [ITEM_MST] INSERT: {item}, sku: {new_sku or '(empty)'}")
                cursor.close()
                return 'insert'
            else:
                existing_sku = existing_sku or ''
                if not existing_sku and new_sku:
                    # 기존 sku 없고 새 sku 있음 → UPDATE
                    cursor.execute("""
                        UPDATE hhp_item_mst SET sku = %s, product_url = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE item = %s AND account_name = %s
                    """, (new_sku, product_url, item, self.account_name))
                    self.db_conn.commit()
                    print(f"  [ITEM_MST] UPDATE: {item}, sku: {new_sku}")
                    cursor.close()
                    return 'update'
                elif not existing_sku and not new_sku:
                    # 둘 다 없음 → SKIP
                    print(f"  [ITEM_MST] SKIP: {item} (no sku)")
                    cursor.close()
                    return 'skip'
                else:
                    # 기존 sku 있음 → SKIP
                    print(f"  [ITEM_MST] SKIP: {item} (already has sku: {existing_sku})")
                    cursor.close()
                    return 'skip'

        except Exception as e:
            print(f"[ERROR] upsert_item_mst failed: {item}: {e}")
            self.db_conn.rollback()
            return 'error'

    def run(self):
        """메인 실행"""
        print("\n" + "=" * 60)
        print("Amazon Item MST Crawler")
        print("=" * 60)

        try:
            if not self.initialize():
                return {'insert': 0, 'update': 0, 'skip': 0, 'error': 0}

            # hhp_retail_com에서 item 목록 조회
            items = self.load_items_from_retail_com()

            if not items:
                print("[INFO] No items to process")
                return {'insert': 0, 'update': 0, 'skip': 0, 'error': 0}

            results = {'insert': 0, 'update': 0, 'skip': 0, 'error': 0}

            for idx, item_data in enumerate(items, 1):
                item = item_data['item']
                product_url = item_data['product_url']

                print(f"\n[{idx}/{len(items)}] Processing item: {item}")

                # 이미 sku가 있는지 먼저 확인
                exists, existing_sku = self.check_item_exists(item)

                if exists is True and existing_sku:
                    # 이미 sku가 있으면 크롤링 스킵
                    print(f"  [SKIP] Already has sku: {existing_sku}")
                    results['skip'] += 1
                    continue

                # SKU 추출 (페이지 접근 필요)
                extracted_sku = self.extract_sku_from_page(product_url)

                # INSERT 또는 UPDATE
                result = self.upsert_item_mst(item_data, extracted_sku)
                results[result] += 1

                # 요청 간격
                time.sleep(random.uniform(2, 4))

            # 결과 출력
            print("\n" + "=" * 60)
            print("Item MST Crawler 완료")
            print("=" * 60)
            print(f"batch_id: {self.batch_id}")
            print(f"INSERT: {results['insert']}건")
            print(f"UPDATE: {results['update']}건")
            print(f"SKIP: {results['skip']}건")
            print(f"ERROR: {results['error']}건")
            print(f"총계: {sum(results.values())}건")

            return results

        except Exception as e:
            print(f"[ERROR] Run failed: {e}")
            traceback.print_exc()
            return {'insert': 0, 'update': 0, 'skip': 0, 'error': 0}

        finally:
            self.cleanup()

    def cleanup(self):
        """리소스 정리"""
        try:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()
            print("[INFO] Cleanup completed")
        except Exception as e:
            print(f"[WARNING] Cleanup failed: {e}")


# ============================================================================
# 메인
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Amazon Item MST Crawler (개별 실행)")
    print("=" * 60)
    print("\n[테이블 선택]")
    print("  t: 테스트 테이블")
    print("  엔터: 운영 테이블")
    table_choice = input("선택: ").strip().lower()
    test_mode = table_choice == 't'

    crawler = AmazonItemCrawler(test_mode=test_mode)
    crawler.run()
    input("\n엔터를 눌러 종료하세요...")
