"""
Walmart Main 페이지 크롤러 (Playwright 기반)

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Main 페이지에서 제품 리스트 수집 (main_rank 자동 계산)
- main_rank는 페이지 관계없이 1부터 순차 증가
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


class WalmartMainCrawler(BaseCrawler):
    """
    Walmart Main 페이지 크롤러 (undetected-chromedriver 기반)
    """

    def __init__(self, test_mode=True, batch_id=None, stealth_mode='full'):
        """
        초기화.

        Args:
            test_mode: 테스트(True)/운영 모드(False)
            batch_id: 통합 크롤러에서 전달
            stealth_mode: 'simple' (기본, TV 크롤러 수준) 또는 'full' (강화된 PerimeterX 대응)
        """
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Walmart'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.standalone = batch_id is None  # 개별 실행 여부
        self.calendar_week = None
        self.url_template = None
        self.stealth_mode = stealth_mode  # 스텔스 모드: 'simple' 또는 'full'

        # Selenium/undetected-chromedriver 객체
        self.driver = None
        self.wait = None

        self.test_count = 1  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.current_rank = 0
        self.browser_restart_interval = 5  # N페이지마다 브라우저 재시작
        self.saved_urls = set()  # 중복 URL 체크용

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
        """undetected-chromedriver 브라우저 설정"""
        try:
            print(f"[INFO] undetected-chromedriver 설정 중 (stealth_mode={self.stealth_mode})...")

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

            # 스텔스 스크립트 주입
            stealth_script = self.get_stealth_script(mode=self.stealth_mode)
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_script})
            print(f"[OK] 스텔스 스크립트 주입 완료 (mode={self.stealth_mode})")

            print("[OK] undetected-chromedriver 설정 완료")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
            traceback.print_exc()
            return False

    def restart_browser(self):
        """브라우저 재시작"""
        try:
            print("\n" + "="*60)
            print("[INFO] 브라우저 재시작 중...")
            print("="*60)

            # 기존 브라우저 종료
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None

            print("[INFO] 브라우저 종료 완료, 잠시 대기...")
            time.sleep(random.uniform(3, 5))

            # 브라우저 재시작
            if not self.setup_browser():
                print("[ERROR] 브라우저 재시작 실패")
                return False

            # 자연스러운 세션 초기화
            self.initialize_session()

            print("[OK] 브라우저 재시작 완료\n")
            return True

        except Exception as e:
            print(f"[ERROR] 브라우저 재시작 실패: {e}")
            traceback.print_exc()
            return False

    def get_stealth_script(self, mode='simple'):
        """
        봇 탐지 우회를 위한 스텔스 스크립트

        Args:
            mode: 'simple' (기본, TV 크롤러 수준) 또는 'full' (강화된 PerimeterX 대응)
        """
        if mode == 'simple':
            return self._get_simple_stealth_script()
        else:
            return self._get_full_stealth_script()

    def _get_simple_stealth_script(self):
        """단순화된 스텔스 스크립트 (TV 크롤러 수준) - 기본값"""
        return """
            // ==================== navigator.webdriver 제거 ====================
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            delete navigator.__proto__.webdriver;

            // ==================== Chrome 객체 기본 설정 ====================
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };

            // ==================== Plugins 기본 설정 ====================
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                ]
            });

            // ==================== Languages ====================
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // ==================== Permissions ====================
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            // ==================== 화면 정보 ====================
            Object.defineProperty(screen, 'width', { get: () => 1920 });
            Object.defineProperty(screen, 'height', { get: () => 1080 });

            // ==================== 하드웨어 정보 ====================
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });

            console.log('[Stealth-Simple] Anti-detection script loaded');
        """

    def _get_full_stealth_script(self):
        """강화된 스텔스 스크립트 (PerimeterX 대응)"""
        return """
            // ==================== 1. navigator.webdriver 완전 제거 ====================
            // 프로토타입 체인까지 완전히 숨기기
            delete Object.getPrototypeOf(navigator).webdriver;
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
                configurable: true
            });

            // Reflect API로도 숨기기
            const originalReflectGet = Reflect.get;
            Reflect.get = function(target, prop, receiver) {
                if (prop === 'webdriver' && target === navigator) {
                    return undefined;
                }
                return originalReflectGet.apply(this, arguments);
            };

            // ==================== 2. Chrome Runtime 완벽 모방 ====================
            window.chrome = {
                app: {
                    isInstalled: false,
                    InstallState: {DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed'},
                    RunningState: {CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running'}
                },
                runtime: {
                    OnInstalledReason: {CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update'},
                    OnRestartRequiredReason: {APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic'},
                    PlatformArch: {ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64'},
                    PlatformNaclArch: {ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64'},
                    PlatformOs: {ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win'},
                    RequestUpdateCheckStatus: {NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available'},
                    connect: function() { return { onDisconnect: { addListener: function() {} }, onMessage: { addListener: function() {} }, postMessage: function() {} }; },
                    sendMessage: function() {}
                },
                csi: function() { return {}; },
                loadTimes: function() {
                    return {
                        commitLoadTime: Date.now() / 1000 - Math.random() * 2,
                        connectionInfo: 'h2',
                        finishDocumentLoadTime: Date.now() / 1000 - Math.random(),
                        finishLoadTime: Date.now() / 1000 - Math.random() * 0.5,
                        firstPaintAfterLoadTime: 0,
                        firstPaintTime: Date.now() / 1000 - Math.random() * 1.5,
                        navigationType: 'Other',
                        npnNegotiatedProtocol: 'h2',
                        requestTime: Date.now() / 1000 - Math.random() * 3,
                        startLoadTime: Date.now() / 1000 - Math.random() * 2.5,
                        wasAlternateProtocolAvailable: false,
                        wasFetchedViaSpdy: true,
                        wasNpnNegotiated: true
                    };
                }
            };

            // ==================== 3. Plugins & MimeTypes 완벽 모방 ====================
            const pluginData = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
            ];

            const pluginArray = pluginData.map((p, i) => {
                const plugin = Object.create(Plugin.prototype);
                Object.defineProperties(plugin, {
                    name: { value: p.name, enumerable: true },
                    filename: { value: p.filename, enumerable: true },
                    description: { value: p.description, enumerable: true },
                    length: { value: 1, enumerable: true }
                });
                return plugin;
            });

            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const arr = Object.create(PluginArray.prototype);
                    pluginArray.forEach((p, i) => { arr[i] = p; });
                    Object.defineProperty(arr, 'length', { value: pluginArray.length });
                    arr.item = (i) => arr[i];
                    arr.namedItem = (name) => pluginArray.find(p => p.name === name);
                    arr.refresh = () => {};
                    return arr;
                }
            });

            // ==================== 4. Languages ====================
            Object.defineProperty(navigator, 'languages', {
                get: () => Object.freeze(['en-US', 'en'])
            });
            Object.defineProperty(navigator, 'language', {
                get: () => 'en-US'
            });

            // ==================== 5. Permissions API ====================
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => {
                if (parameters.name === 'notifications') {
                    return Promise.resolve({ state: Notification.permission, onchange: null });
                }
                return originalQuery.call(navigator.permissions, parameters);
            };

            // ==================== 6. WebGL Fingerprint ====================
            const getParameterProxyHandler = {
                apply: function(target, thisArg, args) {
                    const param = args[0];
                    // UNMASKED_VENDOR_WEBGL
                    if (param === 37445) return 'Google Inc. (Intel)';
                    // UNMASKED_RENDERER_WEBGL
                    if (param === 37446) return 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)';
                    return target.apply(thisArg, args);
                }
            };
            WebGLRenderingContext.prototype.getParameter = new Proxy(WebGLRenderingContext.prototype.getParameter, getParameterProxyHandler);
            if (typeof WebGL2RenderingContext !== 'undefined') {
                WebGL2RenderingContext.prototype.getParameter = new Proxy(WebGL2RenderingContext.prototype.getParameter, getParameterProxyHandler);
            }

            // ==================== 7. Canvas Fingerprint 노이즈 ====================
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type) {
                if (type === 'image/png' && this.width > 16 && this.height > 16) {
                    const context = this.getContext('2d');
                    if (context) {
                        const imageData = context.getImageData(0, 0, this.width, this.height);
                        for (let i = 0; i < imageData.data.length; i += 4) {
                            // 아주 미세한 노이즈 추가 (눈에 안 보임)
                            imageData.data[i] = imageData.data[i] ^ (Math.random() > 0.99 ? 1 : 0);
                        }
                        context.putImageData(imageData, 0, 0);
                    }
                }
                return originalToDataURL.apply(this, arguments);
            };

            // ==================== 8. AudioContext Fingerprint ====================
            if (typeof AudioContext !== 'undefined') {
                const originalCreateOscillator = AudioContext.prototype.createOscillator;
                AudioContext.prototype.createOscillator = function() {
                    const oscillator = originalCreateOscillator.apply(this, arguments);
                    oscillator.frequency.value = oscillator.frequency.value + (Math.random() * 0.0001 - 0.00005);
                    return oscillator;
                };
            }

            // ==================== 9. 화면/하드웨어 정보 ====================
            Object.defineProperty(screen, 'width', { get: () => 1920 });
            Object.defineProperty(screen, 'height', { get: () => 1080 });
            Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
            Object.defineProperty(screen, 'availHeight', { get: () => 1040 });
            Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
            Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });

            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
            Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 0 });

            // ==================== 10. 연결 정보 ====================
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    effectiveType: '4g',
                    rtt: 50,
                    downlink: 10,
                    saveData: false,
                    onchange: null
                })
            });

            // ==================== 11. Battery API (있으면) ====================
            if ('getBattery' in navigator) {
                navigator.getBattery = () => Promise.resolve({
                    charging: true,
                    chargingTime: 0,
                    dischargingTime: Infinity,
                    level: 1,
                    onchargingchange: null,
                    onchargingtimechange: null,
                    ondischargingtimechange: null,
                    onlevelchange: null
                });
            }

            // ==================== 12. Notification ====================
            Object.defineProperty(Notification, 'permission', { get: () => 'default' });

            // ==================== 13. 자동화 도구 감지 변수들 제거 ====================
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            delete window.__nightmare;
            delete window._phantom;
            delete window.callPhantom;
            delete window._selenium;
            delete window.__webdriver_script_fn;
            delete window.__driver_evaluate;
            delete window.__webdriver_evaluate;
            delete window.__selenium_evaluate;
            delete window.__fxdriver_evaluate;
            delete window.__driver_unwrapped;
            delete window.__webdriver_unwrapped;
            delete window.__selenium_unwrapped;
            delete window.__fxdriver_unwrapped;
            delete window._Selenium_IDE_Recorder;
            delete window._WEBDRIVER_ELEM_CACHE;
            delete window.ChromeDriverw;
            delete document.__webdriver_script_fn;
            delete document.__driver_evaluate;
            delete document.__webdriver_evaluate;
            delete document.__selenium_evaluate;
            delete document.__fxdriver_evaluate;

            // ==================== 14. iframe contentWindow 보호 ====================
            const originalContentWindow = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
            Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
                get: function() {
                    const win = originalContentWindow.get.call(this);
                    if (win) {
                        try {
                            Object.defineProperty(win.navigator, 'webdriver', { get: () => undefined });
                        } catch (e) {}
                    }
                    return win;
                }
            });

            // ==================== 15. Error Stack 정리 ====================
            const originalError = Error;
            Error = function(...args) {
                const error = new originalError(...args);
                if (error.stack) {
                    error.stack = error.stack.replace(/playwright|puppeteer|selenium|webdriver/gi, 'chrome');
                }
                return error;
            };
            Error.prototype = originalError.prototype;

            console.log('[Stealth] Anti-detection script loaded');
        """

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

    def handle_captcha(self, max_attempts=3, wait_seconds=60):
        """CAPTCHA 감지 및 자동 대기 처리 (TV 크롤러 방식)"""
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

                # 자동 대기 (input() 대신 time.sleep 사용)
                print(f"[INFO] CAPTCHA 해결 대기 중... ({wait_seconds}초)")
                time.sleep(wait_seconds)

            # 최종 실패 시에만 스크린샷 저장
            page_content = self.driver.page_source.lower()
            if any(keyword in page_content for keyword in captcha_keywords):
                try:
                    capture_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'capture')
                    os.makedirs(capture_dir, exist_ok=True)
                    screenshot_path = os.path.join(capture_dir, f"captcha_failed_{int(time.time())}.png")
                    self.driver.save_screenshot(screenshot_path)
                    print(f"[INFO] 스크린샷 저장됨: {screenshot_path}")
                except:
                    pass

            return True

        except Exception as e:
            print(f"[WARNING] CAPTCHA handling error: {e}")
            return True

    def initialize_session(self):
        """세션 초기화: walmart.com → 검색 (example.com 경유 제거)"""
        try:
            print("[INFO] 세션 초기화 중...")

            # 1단계: Walmart 메인 페이지 방문 (쿠키/세션 생성)
            print("[INFO] Step 1/3: Walmart 메인 페이지 방문...")
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

            # 2단계: 검색창에서 검색 시도
            print("[INFO] Step 2/3: 검색창에서 'cellphone' 검색 시도...")
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

                    # "cellphone" 타이핑 (사람처럼 천천히)
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
        """스크롤: 사람처럼 자연스러운 스크롤 패턴"""
        try:
            current_position = 0
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            viewport_height = self.driver.execute_script("return window.innerHeight")

            scroll_count = 0

            while current_position < total_height - viewport_height:
                scroll_count += 1

                # 1. 스크롤 거리 변화 (사람은 일정하게 스크롤하지 않음)
                if random.random() < 0.7:
                    # 70%: 보통 스크롤 (200~400px)
                    scroll_step = random.randint(200, 400)
                elif random.random() < 0.5:
                    # 15%: 빠른 스크롤 (400~700px) - 빨리 넘기고 싶을 때
                    scroll_step = random.randint(400, 700)
                else:
                    # 15%: 느린 스크롤 (80~150px) - 뭔가 보면서 천천히
                    scroll_step = random.randint(80, 150)

                current_position += scroll_step

                # 스크롤 실행
                self.driver.execute_script(f"window.scrollTo(0, {current_position})")

                # 대기 시간 변화 (읽는 시간 시뮬레이션)
                if random.random() < 0.15:
                    # 15%: 긴 멈춤 (뭔가 읽거나 보는 중)
                    time.sleep(random.uniform(2.5, 4.5))
                elif random.random() < 0.25:
                    # 25%: 짧은 멈춤
                    time.sleep(random.uniform(0.5, 1.0))
                else:
                    # 60%: 보통 멈춤
                    time.sleep(random.uniform(1.0, 2.0))

                # 마우스 움직임 (상품 호버하는 것처럼)
                if random.random() < 0.25:
                    self.add_random_mouse_movements()

                # 가끔 위로 살짝 스크롤 (놓친 거 다시 보기)
                if random.random() < 0.08 and current_position > 500:
                    scroll_back = random.randint(100, 250)
                    current_position -= scroll_back
                    self.driver.execute_script(f"window.scrollTo(0, {current_position})")
                    time.sleep(random.uniform(1.0, 2.0))

                # 가끔 잠깐 멈춤 (다른 일 하는 것처럼)
                if random.random() < 0.05:
                    time.sleep(random.uniform(3, 6))

                # 페이지 높이 다시 확인 (lazy loading 대응)
                total_height = self.driver.execute_script("return document.body.scrollHeight")

            # 마지막에 완전히 하단으로
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(random.uniform(1.5, 3.0))

            # 가끔 다시 위로 좀 올라감
            if random.random() < 0.3:
                scroll_up = random.randint(300, 800)
                final_pos = total_height - scroll_up
                self.driver.execute_script(f"window.scrollTo(0, {final_pos})")
                time.sleep(random.uniform(1, 2))

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → CAPTCHA 처리 → 스크롤 → HTML 파싱(50개 검증) → 제품 데이터 추출"""
        try:
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            # 첫 페이지는 세션 초기화에서 이미 검색 결과 페이지에 있음 (URL 로드 스킵)
            if page_number == 1:
                print(f"[INFO] Page 1: 검색 결과 페이지에서 바로 추출 시작")
            else:
                url = self.url_template.replace('{page}', str(page_number))
                self.driver.get(url)
                time.sleep(random.uniform(10, 15))
                self.add_random_mouse_movements()

            # 50개 검증 (최대 3회 재시도: 파싱 → 부족하면 스크롤 → 재파싱)
            base_containers = []
            expected_products = 50

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
                        'main_rank': 0,  # save_products()에서 재할당
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
        """DB 저장: BATCH_SIZE 배치 → RETRY_SIZE 배치 → 1개씩 (3-tier retry)"""
        if not products:
            return 0

        # 중복 제거 및 rank 할당
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
            product['main_rank'] = self.current_rank
            unique_products.append(product)

        if not unique_products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_query = """
                INSERT INTO wmart_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    final_sku_price, original_sku_price, offer,
                    pick_up_availability, shipping_availability, delivery_availability,
                    sku_status, retailer_membership_discounts,
                    available_quantity_for_purchase, inventory_status,
                    main_rank, main_page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            BATCH_SIZE = 20
            RETRY_SIZE = 5
            total_saved = 0

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
                    product['main_rank'],
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

            for batch_start in range(0, len(unique_products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(unique_products))
                batch_products = unique_products[batch_start:batch_end]

                try:
                    total_saved += save_batch(batch_products)

                except Exception:
                    self.db_conn.rollback()

                    for sub_start in range(0, len(batch_products), RETRY_SIZE):
                        sub_end = min(sub_start + RETRY_SIZE, len(batch_products))
                        sub_batch = batch_products[sub_start:sub_end]

                        try:
                            total_saved += save_batch(sub_batch)

                        except Exception:
                            self.db_conn.rollback()

                            for single_product in sub_batch:
                                try:
                                    cursor.execute(insert_query, product_to_tuple(single_product))
                                    self.db_conn.commit()
                                    total_saved += 1
                                except Exception as single_error:
                                    print(f"[ERROR] DB save failed: {(single_product.get('retailer_sku_name') or 'N/A')[:30]}: {single_error}")
                                    query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                                    print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                                    traceback.print_exc()
                                    self.db_conn.rollback()

            cursor.close()
            return total_saved

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return 0

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_products = 0
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            self.saved_urls = set()  # 중복 URL 체크용 초기화
            page_num = 1

            while total_products < target_products:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    remaining = target_products - total_products
                    products_to_save = products[:remaining]
                    saved_count = self.save_products(products_to_save)
                    total_products += saved_count

                    if total_products >= target_products:
                        break

                # N페이지마다 브라우저 재시작 (CAPTCHA 우회)
                if page_num % self.browser_restart_interval == 0:
                    print(f"[INFO] {page_num}페이지 완료, 브라우저 재시작...")
                    if not self.restart_browser():
                        print("[WARNING] 브라우저 재시작 실패, 계속 진행...")
                    time.sleep(random.uniform(5, 8))

                time.sleep(random.uniform(8, 12))  # 페이지 간 대기 시간 증가
                page_num += 1

            print(f"[DONE] Page: {page_num}, Saved: {total_products}, batch_id: {self.batch_id}")
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
    """
    개별 실행 진입점 (테스트 모드)

    사용법:
        python wmart_hhp_main.py                    # 기본 (simple 스텔스)
        python wmart_hhp_main.py --stealth simple  # simple 스텔스 (TV 크롤러 수준)
        python wmart_hhp_main.py --stealth full    # full 스텔스 (PerimeterX 대응)
    """
    import argparse
    parser = argparse.ArgumentParser(description='Walmart HHP Main Crawler')
    parser.add_argument('--stealth', type=str, choices=['simple', 'full'], default='full',
                        help='스텔스 모드: simple (TV 크롤러 수준) 또는 full (기본, 강화된 PerimeterX 대응)')
    args = parser.parse_args()

    crawler = WalmartMainCrawler(test_mode=True, stealth_mode=args.stealth)
    crawler.run()


if __name__ == '__main__':
    main()
