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
    'shipping_info': [
        # 현재 저장된 XPath
        ".//div[@data-cy='delivery-recipe']//div[contains(@class, 'udm-primary-delivery-message')]",
        # 대안 후보들
        ".//div[@data-cy='delivery-recipe']//span[contains(@class, 'a-text-bold')]",
        ".//div[@data-cy='delivery-recipe']//div[contains(@class, 'a-section')]//span",
        ".//div[contains(@class, 'delivery-message')]//span[contains(@class, 'a-text-bold')]",
        ".//span[@data-csa-c-delivery-price]",
        ".//div[@id='mir-layout-DELIVERY_BLOCK']//span[contains(@class, 'a-text-bold')]",
        ".//div[@id='deliveryBlockMessage']//span",
    ],
}

# a-offscreen 제거 여부 (True: 중복 텍스트 제거)
REMOVE_OFFSCREEN = True

# 컨테이너 XPath (메인/BSR 페이지용)
CONTAINER_XPATH = "//div[@data-component-type='s-search-result']"

# ============================================================================
# 테스터 클래스
# ============================================================================

class XPathTester:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Chrome 드라이버 설정 (강화된 봇 감지 회피)"""
        print("[INFO] Chrome 드라이버 설정 중...")
        options = Options()

        # 기본 옵션
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')

        # 봇 감지 회피 옵션
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')

        # 창 크기 설정 (봇처럼 보이지 않게)
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')

        # User-Agent 설정 (최신 Chrome 버전)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

        # 자동화 흔적 숨기기
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)

        # 언어 설정
        options.add_argument('--lang=en-US')
        prefs = {
            'intl.accept_languages': 'en-US,en',
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False
        }
        options.add_experimental_option('prefs', prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # CDP 명령으로 webdriver 속성 및 기타 자동화 흔적 숨기기
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                // webdriver 속성 숨기기
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // chrome 객체 정상화
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };

                // permissions 쿼리 오버라이드
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );

                // plugins 배열 정상화 (빈 배열이면 봇으로 탐지됨)
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // languages 정상화
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });

                // platform 정상화
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32'
                });

                // hardware concurrency (CPU 코어 수)
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: () => 8
                });

                // device memory
                Object.defineProperty(navigator, 'deviceMemory', {
                    get: () => 8
                });

                // WebGL 벤더/렌더러 정상화
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) {
                        return 'Intel Inc.';
                    }
                    if (parameter === 37446) {
                        return 'Intel Iris OpenGL Engine';
                    }
                    return getParameter.apply(this, arguments);
                };
            '''
        })

        # User-Agent 클라이언트 힌트 설정
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            "platform": "Windows",
            "acceptLanguage": "en-US,en;q=0.9"
        })

        print("[INFO] Chrome 드라이버 설정 완료 (강화된 봇 감지 회피 적용)")

    def load_cookies(self):
        """크롤러와 동일한 쿠키 로드"""
        import pickle
        cookie_file = os.path.join(os.path.dirname(__file__), '..', 'cookies', 'amazon_cookies.pkl')
        if os.path.exists(cookie_file):
            try:
                # 먼저 Amazon 도메인 접속
                self.driver.get("https://www.amazon.com")
                import time
                time.sleep(2)

                with open(cookie_file, 'rb') as f:
                    cookies = pickle.load(f)
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except:
                        pass
                print("[INFO] 쿠키 로드 완료")
                return True
            except Exception as e:
                print(f"[WARNING] 쿠키 로드 실패: {e}")
                return False
        else:
            print("[INFO] 쿠키 파일 없음 - 비로그인 상태로 테스트")
            return False

    def load_page(self, url):
        """페이지 로드 (CAPTCHA/차단 감지 시 수동 대기)"""
        import time
        import random

        print(f"[INFO] 페이지 로딩: {url}")
        self.driver.get(url)

        # 랜덤 대기 시간 (봇처럼 보이지 않게)
        wait_time = random.uniform(3, 6)
        print(f"[INFO] 대기 중... ({wait_time:.1f}초)")
        time.sleep(wait_time)

        # CAPTCHA/차단 감지 (정확한 문구로 체크)
        page_html = self.driver.page_source.lower()
        block_phrases = [
            'enter the characters you see below',  # CAPTCHA 페이지
            'sorry, we just need to make sure you\'re not a robot',  # 로봇 확인
            'automated access to amazon',  # 자동화 접근 차단
            'unusual traffic from your computer',  # 비정상 트래픽
            'api-services-support@amazon.com',  # 차단 안내 이메일
            'to discuss automated access',  # 자동화 접근 문의
        ]

        is_blocked = any(phrase in page_html for phrase in block_phrases)

        # 추가로 검색 결과가 없는지 확인 (컨테이너 없음 = 차단 가능성)
        if not is_blocked and 's-search-result' not in page_html and 'dp/' not in page_html:
            is_blocked = True
            print("[WARNING] 페이지 내용이 비정상적입니다 (상품 없음)")

        if is_blocked:
            print("[WARNING] 차단/CAPTCHA 감지!")
            print("[INFO] 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
            print("[TIP] 페이지가 정상이면 그냥 엔터를 누르세요.")
            input()
            time.sleep(2)

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
                        text = self.extract_text(elem)
                        print(f"\n    --- 요소 {j+1} 전체 텍스트 ---")
                        print(f"    {text}")
                        print(f"    --- 끝 (길이: {len(text)}자) ---")
                else:
                    print(f"    ✗ 실패 - 요소 없음")
            except Exception as e:
                print(f"    ✗ 오류: {e}")

    def test_xpaths_with_container(self, tree, field_name, xpath_list, max_items=5):
        """XPath 목록 테스트 (메인/BSR 페이지용 - 컨테이너 기반)"""
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
                        text = self.extract_text(elements[0])
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
        page_type = input("페이지 타입 (1=상세, 2=메인/BSR, 기본값=1): ").strip()
        use_container = page_type == '2'

        # 쿠키 사용 여부 (메인/BSR은 쿠키 없이, 상세는 쿠키 로드)
        if use_container:
            use_cookies = input("쿠키 로드할까요? (y/n, 기본값=n): ").strip().lower() == 'y'
        else:
            use_cookies = input("쿠키 로드할까요? (y/n, 기본값=y): ").strip().lower() != 'n'

        try:
            self.setup_driver()

            if use_cookies:
                self.load_cookies()

            tree = self.load_page(url)

            print("\n" + "="*60)
            print("XPath 테스트 결과")
            print("="*60)
            print(f"URL: {url}")
            print(f"모드: {'컨테이너 기반 (메인/BSR)' if use_container else '상세 페이지'}")

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
