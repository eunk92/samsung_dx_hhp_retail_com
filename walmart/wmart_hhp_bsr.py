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
from playwright.sync_api import sync_playwright

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler

# 쿠키/세션 저장 파일 (walmart 폴더 내, Main과 공유)
STORAGE_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "walmart_hhp_storage_state.json")


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

        # Playwright 객체
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        self.test_count = 3  # 테스트 모드
        self.max_products = 100  # 운영 모드
        self.current_rank = 0

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

    def setup_playwright(self):
        """Playwright 브라우저 설정 (쿠키 저장/로드 지원)"""
        try:
            self.playwright = sync_playwright().start()

            # 설치된 Chrome 사용
            self.browser = self.playwright.chromium.launch(
                headless=False,
                channel="chrome",
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-infobars',
                    '--window-size=1920,1080',
                    '--start-maximized',
                    '--lang=en-US,en;q=0.9'
                ]
            )

            # 저장된 쿠키/세션 확인
            storage_state = None
            if os.path.exists(STORAGE_STATE_FILE):
                print(f"[INFO] 저장된 세션 발견: {STORAGE_STATE_FILE}")
                print("[INFO] 이전 세션의 쿠키를 로드합니다...")
                storage_state = STORAGE_STATE_FILE
            else:
                print("[INFO] 저장된 세션 없음, 새 세션 시작")

            # 컨텍스트 옵션
            context_options = {
                'viewport': {'width': 1920, 'height': 1080},
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'locale': 'en-US',
                'timezone_id': 'America/New_York',
                'permissions': ['geolocation', 'notifications'],
                'geolocation': {'longitude': -74.006, 'latitude': 40.7128},
                'color_scheme': 'light',
                'extra_http_headers': {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0'
                }
            }

            # 저장된 세션이 있으면 로드
            if storage_state:
                context_options['storage_state'] = storage_state

            # 컨텍스트 생성
            self.context = self.browser.new_context(**context_options)

            # 스텔스 스크립트 주입
            self.context.add_init_script(self.get_stealth_script())

            self.page = self.context.new_page()
            self.page.set_default_timeout(60000)

            print("[OK] Playwright initialized (with stealth)")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup Playwright: {e}")
            traceback.print_exc()
            return False

    def save_storage_state(self):
        """현재 세션 상태(쿠키 + localStorage) 저장"""
        try:
            if self.context:
                self.context.storage_state(path=STORAGE_STATE_FILE)
                print(f"[OK] 세션 저장됨: {STORAGE_STATE_FILE}")
                return True
        except Exception as e:
            print(f"[WARNING] 세션 저장 실패: {e}")
            return False

    def get_stealth_script(self):
        """봇 탐지 우회를 위한 스텔스 스크립트"""
        return """
            // navigator.webdriver 숨기기
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            delete navigator.__proto__.webdriver;

            // plugins 오버라이드
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    },
                    {
                        0: {type: "application/pdf", suffixes: "pdf", description: ""},
                        description: "",
                        filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                        length: 1,
                        name: "Chrome PDF Viewer"
                    }
                ]
            });

            // languages 오버라이드
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // chrome 객체 추가
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };

            // permissions 오버라이드
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            // WebGL vendor 오버라이드
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                return getParameter.apply(this, arguments);
            };

            // 화면 해상도 일관성
            Object.defineProperty(screen, 'width', { get: () => 1920 });
            Object.defineProperty(screen, 'height', { get: () => 1080 });
            Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
            Object.defineProperty(screen, 'availHeight', { get: () => 1040 });

            // 하드웨어 정보
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });

            // 연결 정보
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    effectiveType: '4g',
                    rtt: 50,
                    downlink: 10,
                    saveData: false
                })
            });

            // Notification permission
            Object.defineProperty(Notification, 'permission', {
                get: () => 'default'
            });
        """

    def add_random_mouse_movements(self):
        """인간처럼 보이기 위한 랜덤 마우스 움직임"""
        try:
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, 1800)
                y = random.randint(100, 900)
                self.page.mouse.move(x, y)
                time.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass  # 마우스 움직임 실패 시 무시

    def handle_captcha(self, max_attempts=3):
        """CAPTCHA 감지 및 자동/수동 처리 (iframe 지원)"""
        try:
            time.sleep(2)

            # CAPTCHA 키워드 감지
            captcha_keywords = ['press & hold', 'press and hold', 'human verification', 'robot or human', 'verify you are human']

            for attempt in range(max_attempts):
                page_content = self.page.content().lower()

                if not any(keyword in page_content for keyword in captcha_keywords):
                    if attempt > 0:
                        print("[OK] CAPTCHA 해결됨")
                    return True

                print(f"[WARNING] CAPTCHA 감지! (시도 {attempt + 1}/{max_attempts})")

                # Press & Hold 버튼 자동 시도
                try:
                    button = None
                    target_frame = self.page

                    # 1. 먼저 iframe 안에서 버튼 찾기 (Walmart CAPTCHA는 보통 iframe 내부)
                    frames = self.page.frames
                    for frame in frames:
                        if frame == self.page.main_frame:
                            continue
                        try:
                            frame_content = frame.content().lower()
                            if any(keyword in frame_content for keyword in captcha_keywords):
                                print(f"[INFO] CAPTCHA iframe 발견")
                                target_frame = frame
                                break
                        except:
                            continue

                    # 2. 텍스트 기반으로 Press & Hold 버튼 찾기 (가장 우선)
                    print("[INFO] Press & Hold 버튼 검색 중...")

                    # 방법 1: get_by_text로 텍스트 직접 검색
                    text_patterns = [
                        "Press & Hold",
                        "PRESS & HOLD",
                        "press & hold",
                        "Press and Hold",
                        "PRESS AND HOLD",
                    ]

                    for text_pattern in text_patterns:
                        try:
                            temp_button = target_frame.get_by_text(text_pattern, exact=False).first
                            if temp_button.is_visible(timeout=1000):
                                button = temp_button
                                print(f"[OK] CAPTCHA 버튼 발견 (텍스트): '{text_pattern}'")
                                break
                        except:
                            continue

                    # 방법 2: role="button" 요소 중 텍스트 포함된 것 찾기
                    if not button:
                        try:
                            role_buttons = target_frame.locator('div[role="button"]')
                            count = role_buttons.count()
                            print(f"[DEBUG] role=button 요소 {count}개 발견")
                            for i in range(count):
                                try:
                                    btn = role_buttons.nth(i)
                                    if btn.is_visible(timeout=500):
                                        btn_text = btn.text_content() or ""
                                        print(f"[DEBUG] 버튼 {i}: '{btn_text[:50]}'")
                                        if "hold" in btn_text.lower() or "press" in btn_text.lower():
                                            button = btn
                                            print(f"[OK] CAPTCHA 버튼 발견 (role=button): '{btn_text[:30]}'")
                                            break
                                except:
                                    continue
                        except Exception as e:
                            print(f"[DEBUG] role=button 검색 실패: {e}")

                    # 방법 3: 기존 셀렉터들
                    if not button:
                        button_selectors = [
                            '[aria-label*="Press & Hold"]',
                            '[aria-label*="Human Challenge"]',
                            '#px-captcha',
                            '[id*="captcha"]',
                            '[class*="captcha"]',
                        ]

                        for selector in button_selectors:
                            try:
                                temp_button = target_frame.locator(selector).first
                                if temp_button.is_visible(timeout=1000):
                                    button = temp_button
                                    print(f"[OK] CAPTCHA 버튼 발견 (셀렉터): {selector}")
                                    break
                            except:
                                continue

                    if button:
                        print("[INFO] Press & Hold 버튼 자동 시도 중...")

                        # 버튼 위치로 천천히 이동
                        box = button.bounding_box()
                        if box:
                            center_x = box['x'] + box['width'] / 2
                            center_y = box['y'] + box['height'] / 2

                            # 인간처럼 커서를 천천히 이동
                            current_x, current_y = random.randint(100, 300), random.randint(100, 300)
                            steps = random.randint(5, 10)
                            for i in range(steps):
                                progress = (i + 1) / steps
                                new_x = current_x + (center_x - current_x) * progress
                                new_y = current_y + (center_y - current_y) * progress
                                self.page.mouse.move(new_x, new_y)
                                time.sleep(random.uniform(0.02, 0.05))

                            time.sleep(random.uniform(0.3, 0.7))

                            # 버튼 누르고 10~13초 유지 (더 길게)
                            self.page.mouse.down()
                            hold_time = random.uniform(10, 13)
                            print(f"[INFO] {hold_time:.1f}초 동안 유지 중...")
                            time.sleep(hold_time)
                            self.page.mouse.up()

                            print("[INFO] CAPTCHA 자동 시도 완료, 결과 확인 중...")
                            time.sleep(random.uniform(3, 5))
                            continue  # 다음 attempt에서 확인

                    else:
                        print("[WARNING] CAPTCHA 버튼을 찾을 수 없음")

                        # 스크린샷 저장 (디버깅용)
                        try:
                            screenshot_path = f"captcha_debug_{attempt}.png"
                            self.page.screenshot(path=screenshot_path)
                            print(f"[DEBUG] 스크린샷 저장: {screenshot_path}")
                        except:
                            pass

                except Exception as e:
                    print(f"[WARNING] 자동 CAPTCHA 시도 실패: {e}")

            # 모든 자동 시도 실패 시 수동 처리
            print("[INFO] 자동 해결 실패. 브라우저에서 수동으로 해결 후 엔터를 누르세요...")
            input()
            return True

        except Exception as e:
            print(f"[WARNING] CAPTCHA handling error: {e}")
            return True

    def initialize_session(self):
        """세션 초기화: example.com → walmart.com → 검색 페이지 순차 접근"""
        try:
            print("[INFO] 세션 초기화 중...")

            # 1단계: 중립 사이트 방문 (브라우저 fingerprint 생성)
            print("[INFO] Step 1/3: 중립 사이트 방문...")
            self.page.goto('https://www.example.com', wait_until='domcontentloaded')
            time.sleep(random.uniform(2, 4))
            self.add_random_mouse_movements()

            # 2단계: Walmart 메인 페이지 방문 (쿠키/세션 생성)
            print("[INFO] Step 2/3: Walmart 메인 페이지 방문...")
            self.page.goto('https://www.walmart.com', wait_until='domcontentloaded')
            time.sleep(random.uniform(5, 8))

            # CAPTCHA 체크
            self.handle_captcha()

            # 마우스 움직임 및 스크롤
            self.add_random_mouse_movements()
            for _ in range(3):
                scroll_y = random.randint(200, 500)
                self.page.evaluate(f"window.scrollBy(0, {scroll_y})")
                time.sleep(random.uniform(1, 2))

            # 3단계: 카테고리 페이지 방문 (자연스러운 네비게이션)
            print("[INFO] Step 3/3: Electronics 카테고리 방문...")
            self.page.goto('https://www.walmart.com/cp/electronics/3944', wait_until='domcontentloaded')
            time.sleep(random.uniform(4, 6))
            self.add_random_mouse_movements()

            print("[OK] 세션 초기화 완료")
            return True

        except Exception as e:
            print(f"[WARNING] 세션 초기화 실패 (계속 진행): {e}")
            return True  # 실패해도 계속 진행

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → Playwright 설정 → batch_id 생성 → 로그 정리"""
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

        # 4. Playwright 설정
        if not self.setup_playwright():
            print("[ERROR] Initialize failed: Playwright setup failed")
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
                self.page.evaluate(f"window.scrollTo(0, {current_position})")
                time.sleep(random.uniform(1.5, 2.5))  # 더 느린 스크롤

                # 가끔 마우스 움직임 추가
                if random.random() < 0.3:
                    self.add_random_mouse_movements()

                total_height = self.page.evaluate("document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(random.uniform(2, 4))

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → CAPTCHA 처리 → 스크롤 → HTML 파싱(40개 검증) → 제품 데이터 추출"""
        try:
            url = self.url_template.replace('{page}', str(page_number))

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.page.goto(url, wait_until='domcontentloaded')
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
                page_html = self.page.content()
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
                    self.current_rank += 1

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
                        'bsr_rank': self.current_rank,
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
        """DB 저장: 중복 확인 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
        if not products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0

            products_to_update = []
            products_to_insert = []

            for product in products:
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
            # 세션 저장 (CAPTCHA 통과 후 쿠키 유지)
            self.save_storage_state()

            # Playwright 리소스 정리
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
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
