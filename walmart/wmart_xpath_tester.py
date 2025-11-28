"""
Walmart XPath 테스터

================================================================================
사용법
================================================================================
1. test_xpaths에 테스트할 XPath 추가
2. 실행: python wmart_xpath_tester.py
3. URL 입력 후 결과 확인
4. 엔터키로 종료

================================================================================
"""

import sys
import os
import time
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from lxml import html
from playwright.sync_api import sync_playwright

# ============================================================================
# 설정
# ============================================================================

# 테스트할 XPath 목록 (필드명: [xpath 후보들])
TEST_XPATHS = {
    'example_field': [
        # 테스트할 XPath를 여기에 추가
        "//div[@class='example']//span/text()",
    ],
}

# 컨테이너 XPath (리스트 페이지용)
CONTAINER_XPATH = "//div[contains(@data-testid, 'list-view')]//div[contains(@class, 'product-card')]"

# ============================================================================
# 테스터 클래스
# ============================================================================

class WalmartXPathTester:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def setup_playwright(self):
        """Playwright 브라우저 설정 (Walmart용)"""
        print("[INFO] Playwright 브라우저 설정 중...")

        # Windows TEMP 폴더 문제 해결
        temp_dir = 'C:\\Temp'
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        os.environ['TEMP'] = temp_dir
        os.environ['TMP'] = temp_dir

        self.playwright = sync_playwright().start()

        # Chromium 브라우저 실행
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--lang=en-US',
                '--disable-infobars',
                '--disable-extensions',
                '--start-maximized',
            ]
        )

        # 컨텍스트 생성
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York'
        )

        # 자동화 감지 방지 스크립트
        self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        self.page = self.context.new_page()
        self.page.set_default_timeout(60000)

        print("[INFO] Playwright 브라우저 설정 완료")

    def handle_captcha(self):
        """CAPTCHA 감지 및 처리"""
        try:
            time.sleep(3)

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
                        if 'PRESS' in text or 'HOLD' in text or 'CAPTCHA' in text:
                            button = temp_button
                            print(f"[WARNING] CAPTCHA detected")
                            break
                except:
                    continue

            if not button:
                page_content = self.page.content().lower()
                if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha']):
                    print("[WARNING] CAPTCHA keywords found - waiting for manual input...")
                    print("[INFO] 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
                    input()
                    return True
                return True

            print("[INFO] Attempting to solve CAPTCHA...")
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
                        print("[WARNING] CAPTCHA still visible - waiting for manual input...")
                        print("[INFO] 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
                        input()
                        return True
                except:
                    return True

            return True

        except Exception as e:
            print(f"[WARNING] CAPTCHA handling error: {e}")
            return True

    def load_page(self, url):
        """페이지 로드"""
        print(f"[INFO] 페이지 로딩: {url}")
        self.page.goto(url, wait_until='domcontentloaded')

        # 랜덤 대기 시간
        wait_time = random.uniform(5, 8)
        print(f"[INFO] 대기 중... ({wait_time:.1f}초)")
        time.sleep(wait_time)

        # CAPTCHA 처리
        self.handle_captcha()

        # 페이지 상태 확인
        page_html = self.page.content().lower()

        # Walmart 차단/에러 감지
        block_phrases = [
            'access denied',
            'blocked',
            'robot check',
            'unusual activity',
        ]

        is_blocked = any(phrase in page_html for phrase in block_phrases)

        if is_blocked:
            print("[WARNING] 차단/에러 감지!")
            print("[INFO] 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
            print("[TIP] 페이지가 정상이면 그냥 엔터를 누르세요.")
            input()
            time.sleep(2)

        print("[INFO] 페이지 로딩 완료")
        return html.fromstring(self.page.content())

    def extract_text(self, element):
        """요소에서 텍스트 추출"""
        return element.text_content().strip()

    def test_xpaths(self, tree, field_name, xpath_list):
        """XPath 목록 테스트 (상세 페이지용)"""
        print(f"\n{'='*80}")
        print(f"필드: {field_name}")
        print('='*80)

        for i, xpath in enumerate(xpath_list, 1):
            print(f"\n[{i}] XPath: {xpath}")
            try:
                elements = tree.xpath(xpath)
                if elements:
                    print(f"    ✓ 성공! 요소 {len(elements)}개 발견")
                    # 모든 요소의 전체 텍스트 출력
                    for j, elem in enumerate(elements):
                        # /text() XPath는 문자열을 반환하므로 타입 체크
                        if isinstance(elem, str):
                            text = elem.strip()
                        else:
                            text = self.extract_text(elem)
                        print(f"\n    --- 요소 {j+1} 전체 텍스트 ---")
                        print(f"    {text}")
                        print(f"    --- 끝 (길이: {len(text)}자) ---")
                else:
                    print(f"    ✗ 실패 - 요소 없음")
            except Exception as e:
                print(f"    ✗ 오류: {e}")

    def test_xpaths_with_container(self, tree, field_name, xpath_list, max_items=50):
        """XPath 목록 테스트 (리스트 페이지용 - 컨테이너 기반)"""
        print(f"\n{'='*80}")
        print(f"필드: {field_name} (컨테이너 기반)")
        print('='*80)

        # 컨테이너 찾기
        containers = tree.xpath(CONTAINER_XPATH)
        print(f"[INFO] 컨테이너 {len(containers)}개 발견")

        if not containers:
            print("    ✗ 컨테이너 없음")
            return

        for i, xpath in enumerate(xpath_list, 1):
            print(f"\n[{i}] XPath: {xpath}")
            success_count = 0

            for j, container in enumerate(containers[:max_items]):
                try:
                    elements = container.xpath(xpath)
                    if elements:
                        elem = elements[0]
                        if isinstance(elem, str):
                            text = elem.strip()
                        else:
                            text = self.extract_text(elem)
                        print(f"    컨테이너 {j+1}: {text}")
                        success_count += 1
                    else:
                        print(f"    컨테이너 {j+1}: (없음)")
                except Exception as e:
                    print(f"    컨테이너 {j+1}: 오류 - {e}")

            print(f"    → 성공률: {success_count}/{min(len(containers), max_items)}")

    def run(self):
        """테스터 실행"""
        # URL 입력받기
        url = input("테스트할 URL을 입력하세요: ").strip()
        if not url:
            print("URL이 입력되지 않아 프로그램을 종료합니다.")
            return

        # 페이지 타입 선택
        page_type = input("페이지 타입 (1=상세, 2=리스트, 기본값=1): ").strip()
        use_container = page_type == '2'

        try:
            self.setup_playwright()

            tree = self.load_page(url)

            print("\n" + "="*60)
            print("XPath 테스트 결과")
            print("="*60)
            print(f"URL: {url}")
            print(f"모드: {'컨테이너 기반 (리스트)' if use_container else '상세 페이지'}")

            for field_name, xpath_list in TEST_XPATHS.items():
                if use_container:
                    self.test_xpaths_with_container(tree, field_name, xpath_list)
                else:
                    self.test_xpaths(tree, field_name, xpath_list)

            print("\n" + "="*60)
            print("테스트 완료")
            print("="*60)

        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
        finally:
            input("\n엔터키를 누르면 종료합니다...")
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()


if __name__ == "__main__":
    tester = WalmartXPathTester()
    tester.run()
