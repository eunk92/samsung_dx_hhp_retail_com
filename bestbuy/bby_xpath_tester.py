"""
BestBuy XPath 테스터

================================================================================
사용법
================================================================================
1. test_xpaths에 테스트할 XPath 추가
2. 실행: python xpath_tester.py
3. URL 입력 후 결과 확인
4. 엔터키로 종료

================================================================================
"""

import sys
import os
from copy import deepcopy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

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
CONTAINER_XPATH = "//li[contains(@class, 'sku-item')]"

# ============================================================================
# 테스터 클래스
# ============================================================================

class XPathTester:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Chrome 드라이버 설정 (BestBuy용 - 기본 설정)"""
        print("[INFO] Chrome 드라이버 설정 중...")
        options = Options()

        # Page Load Strategy 설정
        options.page_load_strategy = 'none'

        # 자동화 감지 방지
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # User-Agent 설정
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        # 전체화면으로 시작
        options.add_argument('--start-maximized')

        # 안정화 옵션
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--lang=en-US')

        # 추가 옵션
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # 페이지 로드 타임아웃 설정
        self.driver.set_page_load_timeout(120)

        # 자동화 감지 방지 스크립트
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })

        print("[INFO] Chrome 드라이버 설정 완료")

    def load_page(self, url):
        """페이지 로드"""
        import time
        import random

        print(f"[INFO] 페이지 로딩: {url}")
        self.driver.get(url)

        # 랜덤 대기 시간
        wait_time = random.uniform(8, 12)
        print(f"[INFO] 대기 중... ({wait_time:.1f}초)")
        time.sleep(wait_time)

        # 페이지 상태 확인
        page_html = self.driver.page_source.lower()

        # BestBuy 차단/에러 감지
        block_phrases = [
            'access denied',
            'blocked',
            'captcha',
            'robot',
            'unusual activity',
        ]

        is_blocked = any(phrase in page_html for phrase in block_phrases)

        # 상품 페이지인지 확인
        if not is_blocked and 'bestbuy.com' in url:
            if '/site/' in url and 'sku-header' not in page_html and 'product-title' not in page_html:
                is_blocked = True
                print("[WARNING] 상품 페이지 내용이 비정상적입니다")

        if is_blocked:
            print("[WARNING] 차단/에러 감지!")
            print("[INFO] 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
            print("[TIP] 페이지가 정상이면 그냥 엔터를 누르세요.")
            input()
            time.sleep(2)

        print("[INFO] 페이지 로딩 완료")
        return html.fromstring(self.driver.page_source)

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
            self.setup_driver()

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
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    tester = XPathTester()
    tester.run()
