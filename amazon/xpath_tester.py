"""
Amazon XPath 테스터

================================================================================
사용법
================================================================================
1. product_url 설정
2. test_xpaths에 테스트할 XPath 추가
3. 실행: python xpath_tester.py
4. 결과 확인 후 엔터키로 종료

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

# 테스트할 상품 URL (실행 시 입력받음)
PRODUCT_URL = ""

# 테스트할 XPath 목록 (필드명: [xpath 후보들])
TEST_XPATHS = {
    'trade_in': [
        # "Trade-in and save" 텍스트 기준으로 찾기
        '//*[contains(text(), "Trade-in and save")]/following::*[contains(text(), "Save up to")][1]',
        '//*[contains(text(), "Trade-in and save")]/ancestor::div[1]/following-sibling::div[1]//*[contains(text(), "Save")]',
        '//*[contains(text(), "Trade-in and save")]/../..//*[contains(text(), "Save up to")]',
        # 기존 방법
        '//div[@id="NO_INTENT_DOM_RENDER"]//div[@class="utxDynamicLongMessage"]',
        '//div[@id="tradeInOfferViewModel_0"]//div[@class="utxLongMessage"]',
        '//span[contains(text(), "Trade-in and save")]/ancestor::div[contains(@class, "Expander")]//div[@class="utxDynamicLongMessage"]',
        '//div[contains(@class, "unifiedTradeInIngress")]//div[@class="utxDynamicLongMessage"]',
    ],
    # 다른 필드 추가 가능
    # 'hhp_carrier': [
    #     '//xpath1',
    #     '//xpath2',
    # ],
}

# a-offscreen 제거 여부 (True: 중복 텍스트 제거)
REMOVE_OFFSCREEN = True

# ============================================================================
# 테스터 클래스
# ============================================================================

class XPathTester:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        print("[INFO] Chrome 드라이버 설정 중...")
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # CDP 명령으로 webdriver 속성 숨기기
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
            '''
        })
        print("[INFO] Chrome 드라이버 설정 완료")

    def load_page(self, url):
        """페이지 로드"""
        print(f"[INFO] 페이지 로딩: {url}")
        self.driver.get(url)
        import time
        time.sleep(3)  # 페이지 로딩 대기
        print("[INFO] 페이지 로딩 완료")
        return html.fromstring(self.driver.page_source)

    def extract_text(self, element):
        """요소에서 텍스트 추출 (a-offscreen 제거 옵션)"""
        if REMOVE_OFFSCREEN:
            elem_copy = deepcopy(element)
            for offscreen in elem_copy.xpath('.//span[@class="a-offscreen"]'):
                offscreen.getparent().remove(offscreen)
            return elem_copy.text_content().strip()
        else:
            return element.text_content().strip()

    def test_xpaths(self, tree, field_name, xpath_list):
        """XPath 목록 테스트"""
        print(f"\n{'='*60}")
        print(f"필드: {field_name}")
        print('='*60)

        for i, xpath in enumerate(xpath_list, 1):
            print(f"\n[{i}] XPath: {xpath}")
            try:
                elements = tree.xpath(xpath)
                if elements:
                    text = self.extract_text(elements[0])
                    print(f"    ✓ 성공! 요소 {len(elements)}개 발견")
                    print(f"    추출값: {text}")
                else:
                    print(f"    ✗ 실패 - 요소 없음")
            except Exception as e:
                print(f"    ✗ 오류: {e}")

    def run(self):
        """테스터 실행"""
        # URL 입력받기
        url = input("테스트할 상품 URL을 입력하세요: ").strip()
        if not url:
            print("URL이 입력되지 않아 프로그램을 종료합니다.")
            return

        try:
            self.setup_driver()
            tree = self.load_page(url)

            print("\n" + "="*60)
            print("XPath 테스트 결과")
            print("="*60)
            print(f"URL: {url}")

            for field_name, xpath_list in TEST_XPATHS.items():
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
