"""
BaseCrawler - HHP 크롤러 공통 기능 제공
모든 개별 크롤러(Main, BSR, Promotion, Detail)가 상속받는 베이스 클래스
"""

import psycopg2
import time
import glob
import os
import sys
import pickle
import traceback
from datetime import datetime, timedelta
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

from config import DB_CONFIG


class TeeLogger:
    """
    stdout/stderr를 콘솔과 파일 양쪽에 출력하는 클래스
    통합 크롤러에서 모든 print() 출력과 traceback을 로그 파일에 저장
    """

    def __init__(self, log_file_path):
        self.terminal_stdout = sys.stdout
        self.terminal_stderr = sys.stderr
        self.log_file = open(log_file_path, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal_stdout.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal_stdout.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


class TeeLoggerStderr:
    """
    stderr를 콘솔과 파일 양쪽에 출력하는 클래스
    traceback.print_exc() 등 stderr 출력을 로그 파일에 저장
    """

    def __init__(self, tee_logger):
        self.tee_logger = tee_logger
        self.terminal_stderr = sys.stderr

    def write(self, message):
        self.terminal_stderr.write(message)
        self.tee_logger.log_file.write(message)
        self.tee_logger.log_file.flush()

    def flush(self):
        self.terminal_stderr.flush()
        self.tee_logger.log_file.flush()


class BaseCrawler:
    """
    HHP 크롤러 베이스 클래스
    공통 메서드를 제공하여 코드 중복 방지 및 유지보수성 향상
    """

    def __init__(self):
        """초기화"""
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        self.tee_logger = None
        self.tee_logger_stderr = None
        self.original_stdout = None
        self.original_stderr = None

    def start_logging(self, batch_id):
        """
        콘솔 출력을 파일에도 저장하기 시작

        쓰임새:
        - 통합 크롤러 시작 시 호출
        - 이후 모든 print() 출력이 콘솔과 파일 양쪽에 기록됨

        Args:
            batch_id (str): 배치 ID

        Returns:
            str: 로그 파일 경로
        """
        try:
            # logs 폴더 경로 (프로젝트 루트/logs/)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logs_dir = os.path.join(project_root, 'logs')
            os.makedirs(logs_dir, exist_ok=True)

            # 로그 파일명: {batch_id}_{생성시간}.txt
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            log_file_path = os.path.join(logs_dir, f"{batch_id}_{timestamp}.txt")

            # TeeLogger 시작 (stdout + stderr)
            self.original_stdout = sys.stdout
            self.original_stderr = sys.stderr
            self.tee_logger = TeeLogger(log_file_path)
            self.tee_logger_stderr = TeeLoggerStderr(self.tee_logger)
            sys.stdout = self.tee_logger
            sys.stderr = self.tee_logger_stderr

            return log_file_path

        except Exception as e:
            print(f"[WARNING] Failed to start logging: {e}")
            return None

    def stop_logging(self):
        """
        콘솔 출력 파일 저장 종료

        쓰임새:
        - 통합 크롤러 종료 시 호출
        - stdout/stderr를 원래대로 복원하고 로그 파일 닫기
        """
        try:
            if self.tee_logger and self.original_stdout:
                sys.stdout = self.original_stdout
                sys.stderr = self.original_stderr
                self.tee_logger.close()
                self.tee_logger = None
                self.tee_logger_stderr = None
                self.original_stdout = None
        except Exception as e:
            print(f"[WARNING] Failed to stop logging: {e}")

    def connect_db(self):
        """
        PostgreSQL 데이터베이스 연결

        쓰임새:
        - 크롤러 시작 시 DB 연결 설정
        - config.py의 DB_CONFIG 정보 사용
        - 트랜잭션 모드로 동작 (commit/rollback 지원)

        Returns:
            bool: 연결 성공 시 True, 실패 시 False
        """
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG, database='postgres')
            print("[SUCCESS] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            traceback.print_exc()
            return False

    def load_xpaths(self, account_name, page_type):
        """
        hhp_xpath_selectors 테이블에서 XPath/CSS 셀렉터 조회

        쓰임새:
        - 크롤러 시작 시 해당 쇼핑몰/페이지 타입의 셀렉터를 미리 로드
        - 데이터 필드별 XPath/CSS 셀렉터를 딕셔너리로 저장
        - is_active=TRUE인 셀렉터만 로드

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)
            page_type (str): 페이지 타입 (main, bsr, promotion, detail)

        Returns:
            bool: 로드 성공 시 True, 실패 시 False
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM hhp_xpath_selectors
                WHERE account_name = %s AND page_type = %s AND is_active = TRUE
            """, (account_name, page_type))

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[SUCCESS] Loaded {len(self.xpaths)} XPath selectors for {account_name}/{page_type}")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            traceback.print_exc()
            return False

    def load_page_urls(self, account_name, page_type):
        """
        hhp_target_page_url 테이블에서 크롤링 대상 URL 템플릿 조회

        쓰임새:
        - Main/BSR/Promotion 크롤러가 크롤링할 URL 템플릿을 가져옴
        - URL 템플릿의 {page} 플레이스홀더를 페이지 번호로 치환하여 사용

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)
            page_type (str): 페이지 타입 (main, bsr, promotion)

        Returns:
            str or None: URL 템플릿 문자열, 실패 시 None
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT url_template
                FROM hhp_target_page_url
                WHERE account_name = %s AND page_type = %s
            """, (account_name, page_type))

            result = cursor.fetchone()
            cursor.close()

            if result:
                print(f"[SUCCESS] Loaded URL template for {account_name}/{page_type}")
                return result[0]
            else:
                print(f"[WARNING] No URL template found for {account_name}/{page_type}")
                return None

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            traceback.print_exc()
            return None

    def setup_driver(self):
        """
        Chrome WebDriver 설정 및 초기화

        쓰임새:
        - Selenium을 사용한 동적 웹 크롤링을 위한 WebDriver 설정
        - 자동화 감지 방지 옵션 적용
        - User-Agent 설정으로 일반 브라우저처럼 동작
        - 일관된 결과를 위한 세션 및 쿠키 관리

        Returns:
            None
        """
        chrome_options = Options()

        # Page Load Strategy 설정 (동적 페이지 로딩 최적화)
        chrome_options.page_load_strategy = 'none'  # 전체 페이지 로드를 기다리지 않음

        # 자동화 감지 방지
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # User-Agent 고정 (일관된 결과를 위해)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        # 전체화면으로 시작
        chrome_options.add_argument('--start-maximized')

        # 추가 안정화 옵션
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--lang=ko-KR')  # 언어 고정

        # 쿠키 및 세션 유지를 위한 프로필 디렉토리 설정 (선택적)
        # chrome_options.add_argument('--user-data-dir=./chrome_profile')

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # 페이지 로드 타임아웃 설정 (120초)
        self.driver.set_page_load_timeout(120)

        # 자동화 감지 방지 스크립트 실행
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })

        print("[SUCCESS] WebDriver setup complete")

    def setup_driver_stealth(self, account_name='Amazon'):
        """
        강화된 봇 감지 회피 Chrome WebDriver 설정

        쓰임새:
        - Main/BSR 크롤러에서 쿠키 없이 크롤링할 때 사용
        - Amazon의 봇 감지를 회피하기 위한 강화된 설정 적용
        - navigator.webdriver, plugins, WebGL 등 다양한 속성 위장

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)

        Returns:
            None
        """
        # Amazon만 강화된 봇 감지 회피 적용, 다른 쇼핑몰은 기본 setup_driver 사용
        if account_name != 'Amazon':
            self.setup_driver()
            return

        chrome_options = Options()

        # 기본 옵션
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')

        # 봇 감지 회피 옵션
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--allow-running-insecure-content')

        # 창 크기 설정 (봇처럼 보이지 않게)
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')

        # User-Agent 설정 (최신 Chrome 버전)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

        # 자동화 흔적 숨기기
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # 언어 설정
        chrome_options.add_argument('--lang=en-US')
        prefs = {
            'intl.accept_languages': 'en-US,en',
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False
        }
        chrome_options.add_experimental_option('prefs', prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # 페이지 로드 타임아웃 설정 (120초)
        self.driver.set_page_load_timeout(120)

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

        print("[SUCCESS] WebDriver setup complete (stealth mode)")

        # Amazon인 경우 뉴욕 ZIP 코드 자동 설정
        if account_name == 'Amazon':
            self.set_amazon_zip_code('10001')

    def set_amazon_zip_code(self, zip_code='10001', max_retries=3):
        """
        Amazon 배송 지역 ZIP 코드 설정 (뉴욕: 10001)

        쓰임새:
        - 크롤러 시작 시 배송 지역을 뉴욕으로 고정
        - 일관된 가격/재고 정보 수집을 위해 필요

        Args:
            zip_code (str): ZIP 코드 (기본: 10001 - 맨해튼)
            max_retries (int): 최대 재시도 횟수 (기본: 3)

        Returns:
            bool: 설정 성공 시 True, 실패 시 False
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import random

        try:
            print(f"[INFO] Setting Amazon ZIP code to {zip_code}...")

            # Amazon 메인 페이지 접속 (1회만)
            self.driver.get('https://www.amazon.com')
            time.sleep(random.uniform(5, 8))

            # "Deliver to" 버튼 클릭 (재시도 포함)
            for attempt in range(1, max_retries + 1):
                try:
                    deliver_to_btn = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, 'nav-global-location-popover-link'))
                    )
                    deliver_to_btn.click()
                    time.sleep(random.uniform(2, 3))
                    break
                except Exception as e:
                    print(f"[WARNING] Deliver to button not found (attempt {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        print(f"[INFO] Waiting 3 seconds before retry...")
                        time.sleep(3)
                    else:
                        raise

            # ZIP 코드 입력 필드 찾기 (재시도 포함)
            for attempt in range(1, max_retries + 1):
                try:
                    zip_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, 'GLUXZipUpdateInput'))
                    )
                    zip_input.clear()
                    zip_input.send_keys(zip_code)
                    time.sleep(random.uniform(1, 2))
                    break
                except Exception as e:
                    print(f"[WARNING] ZIP input field not found (attempt {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        print(f"[INFO] Waiting 3 seconds before retry...")
                        time.sleep(3)
                    else:
                        raise

            # Apply 버튼 클릭 (재시도 포함)
            for attempt in range(1, max_retries + 1):
                try:
                    apply_btn = self.driver.find_element(By.CSS_SELECTOR, '#GLUXZipUpdate input[type="submit"], #GLUXZipUpdate .a-button-input')
                    apply_btn.click()
                    time.sleep(random.uniform(3, 5))
                    break
                except Exception as e:
                    print(f"[WARNING] Apply button not found (attempt {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        print(f"[INFO] Waiting 3 seconds before retry...")
                        time.sleep(3)
                    else:
                        raise

            # 팝업 닫기 (있는 경우)
            try:
                close_btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.a-popover-footer button, #GLUXConfirmClose'))
                )
                close_btn.click()
                time.sleep(1)
            except:
                pass  # 팝업이 없을 수 있음

            print(f"[SUCCESS] Amazon ZIP code set to {zip_code} (New York)")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to set Amazon ZIP code: {e}")
            print(f"[INFO] Continuing without ZIP code setting...")
            return False

    def extract_text_safe(self, element, xpath):
        """
        XPath를 사용하여 안전하게 텍스트 추출

        쓰임새:
        - lxml element에서 XPath로 데이터 추출 시 에러 방지
        - 속성 추출 (예: @href)과 텍스트 추출 모두 지원
        - 값이 없거나 에러 발생 시 None 반환

        Args:
            element: lxml HTML element
            xpath (str): XPath 표현식

        Returns:
            str or None: 추출된 텍스트, 실패 시 None
        """
        try:
            result = element.xpath(xpath)
            if result:
                # 속성 추출인 경우 (예: @href)
                if isinstance(result[0], str):
                    return result[0].strip()
                # 요소 텍스트 추출인 경우
                else:
                    return result[0].text_content().strip()
            return None
        except Exception:
            return None

    def extract_with_fallback(self, element, xpath, default=None):
        """
        파싱 실패 시 기본값을 반환하는 안전한 추출

        쓰임새:
        - extract_text_safe의 래퍼 함수
        - 추출 실패 시 사용자 지정 기본값 반환
        - NULL 값 대신 특정 문자열을 저장하고 싶을 때 사용

        Args:
            element: lxml HTML element
            xpath (str): XPath 표현식
            default: 추출 실패 시 반환할 기본값

        Returns:
            str or default: 추출된 텍스트 또는 기본값
        """
        result = self.extract_text_safe(element, xpath)
        return result if result is not None else default

    def safe_extract(self, element, field_name):
        """필드 추출 시 예외 발생하면 None 반환 후 다음 필드로 진행"""
        try:
            return self.extract_with_fallback(element, self.xpaths.get(field_name, {}).get('xpath'))
        except Exception as e:
            print(f"[WARNING] Failed to extract {field_name}: {e}")
            return None

    def safe_extract_join(self, element, field_name, separator=" / "):
        """
        여러 요소를 추출하여 구분자로 결합

        쓰임새:
        - Union XPath (|)로 여러 요소를 선택하는 경우
        - 예: shipping_info에서 primary + secondary delivery message를 결합
        - separator로 결합 구분자 지정 가능

        Args:
            element: lxml HTML element
            field_name (str): XPath 필드명 (xpaths 딕셔너리 키)
            separator (str): 요소 결합 구분자 (기본: " / ")

        Returns:
            str or None: 결합된 텍스트, 요소 없으면 None
        """
        try:
            xpath = self.xpaths.get(field_name, {}).get('xpath')
            if not xpath:
                return None

            elements = element.xpath(xpath)
            if not elements:
                return None

            # 각 요소에서 텍스트 추출
            texts = []
            for elem in elements:
                if isinstance(elem, str):
                    text = elem.strip()
                else:
                    text = elem.text_content().strip()
                if text:
                    texts.append(text)

            if not texts:
                return None

            # 요소가 1개면 그대로, 2개 이상이면 구분자로 결합
            return separator.join(texts) if len(texts) > 1 else texts[0]

        except Exception as e:
            print(f"[WARNING] Failed to extract_join {field_name}: {e}")
            return None

    def generate_batch_id(self, account_name):
        """
        배치 ID 생성 (쇼핑몰 prefix + 타임스탬프)

        쓰임새:
        - 크롤링 세션을 구분하기 위한 고유 ID 생성
        - 같은 배치에서 수집된 데이터를 그룹화
        - 중복 방지 로직에서 사용 (batch_id + product_url 조합)

        형식:
        - Amazon: a_20231120_143045
        - Bestbuy: b_20231120_143045
        - Walmart: w_20231120_143045

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)

        Returns:
            str: 생성된 배치 ID
        """
        # 쇼핑몰별 prefix 매핑
        prefix_map = {
            'Amazon': 'a_',
            'Bestbuy': 'b_',
            'Walmart': 'w_'
        }

        prefix = prefix_map.get(account_name, 'x_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        return f"{prefix}{timestamp}"

    def generate_calendar_week(self):
        """
        캘린더 주차 생성 (예: w47)

        쓰임새:
        - 데이터 수집 시점의 주차 정보 기록
        - 주별 데이터 분석 및 리포트에 사용
        - ISO 8601 주차 계산 방식 사용

        Returns:
            str: 캘린더 주차 (예: 'w47')
        """
        now = datetime.now()
        week_number = now.isocalendar()[1]  # ISO week number
        return f"w{week_number}"

    def cleanup_old_logs(self, days=30):
        """
        오래된 로그 파일 자동 삭제

        쓰임새:
        - 크롤러 시작 시 호출하여 오래된 로그 자동 정리
        - 디스크 공간 절약
        - 기본 30일 이상 된 로그 파일 삭제

        Args:
            days (int): 보관 일수 (기본: 30일)

        Returns:
            None
        """
        try:
            # logs 폴더 경로 (프로젝트 루트/logs/)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logs_dir = os.path.join(project_root, 'logs')

            if not os.path.exists(logs_dir):
                return

            # 삭제 기준 시간 계산
            cutoff_time = datetime.now() - timedelta(days=days)

            # *.txt 패턴 파일 검색 (logs/ 하단의 모든 txt 파일)
            log_pattern = os.path.join(logs_dir, '*.txt')
            log_files = glob.glob(log_pattern)

            deleted_count = 0
            for log_file in log_files:
                # 파일 수정 시간 확인
                file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))

                # 기준 시간보다 오래된 파일 삭제
                if file_mtime < cutoff_time:
                    os.remove(log_file)
                    deleted_count += 1

            if deleted_count > 0:
                print(f"[INFO] Deleted {deleted_count} old log files (older than {days} days)")

        except Exception as e:
            print(f"[WARNING] Failed to cleanup old logs: {e}")

    def save_cookies(self, account_name):
        """
        현재 세션의 쿠키를 파일로 저장

        쓰임새:
        - 첫 페이지 로드 후 쿠키를 저장하여 세션 유지
        - 다음 크롤링 시 같은 쿠키를 사용하여 일관된 결과 확보

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)

        Returns:
            None
        """
        try:
            # account_name 기반으로 쿠키 파일 경로 생성
            cookie_file = f'cookies/{account_name.lower()}_cookies.pkl'

            # cookies 디렉토리 생성
            os.makedirs('cookies', exist_ok=True)

            # 쿠키 저장
            cookies = self.driver.get_cookies()
            with open(cookie_file, 'wb') as f:
                pickle.dump(cookies, f)

            print(f"[INFO] Cookies saved to {cookie_file}")

        except Exception as e:
            print(f"[WARNING] Failed to save cookies: {e}")

    def load_cookies(self, account_name):
        """
        저장된 쿠키를 로드하여 세션 복원

        쓰임새:
        - 이전에 저장한 쿠키를 로드하여 같은 세션으로 크롤링
        - 일관된 제품 목록 순서 유지
        - Amazon Detail 크롤러: 로그인 세션 복원 (리뷰 수집용)

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)

        Returns:
            bool: 쿠키 로드 성공 시 True, 실패 시 False
        """
        try:
            # account_name 기반으로 쿠키 파일 경로 생성
            cookie_file = f'cookies/{account_name.lower()}_cookies.pkl'

            if not os.path.exists(cookie_file):
                print(f"[INFO] No saved cookies found at {cookie_file}")
                if account_name == 'Amazon':
                    print(f"[WARNING] Amazon login cookies not found")
                    print(f"[WARNING] Review collection may fail without login")
                    print(f"[INFO] To create cookies, run: python amazon_login.py")
                return False

            # 쿠키 로드
            with open(cookie_file, 'rb') as f:
                cookies = pickle.load(f)

            # 도메인에 먼저 접속 (쿠키를 추가하기 전에 필요)
            if account_name == 'Amazon':
                self.driver.get('https://www.amazon.com')
            elif account_name == 'Bestbuy':
                self.driver.get('https://www.bestbuy.com')
            elif account_name == 'Walmart':
                self.driver.get('https://www.walmart.com')
            time.sleep(2)

            # 쿠키 추가
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception:
                    pass  # 일부 쿠키는 추가 실패할 수 있음

            print(f"[INFO] Cookies loaded from {cookie_file}")

            # Amazon인 경우 로그인 확인
            if account_name == 'Amazon':
                self.driver.refresh()
                time.sleep(2)
                try:
                    from selenium.webdriver.common.by import By
                    account_element = self.driver.find_element(By.ID, "nav-link-accountList")
                    account_text = account_element.text.lower()

                    if "hello" in account_text and "sign in" not in account_text:
                        print(f"[SUCCESS] Amazon login verified with cookies")
                    else:
                        print(f"[WARNING] Amazon cookies may be expired")
                        print(f"[INFO] If review collection fails, run: python amazon_login.py")
                except:
                    print(f"[INFO] Could not verify Amazon login status")

            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            return False

    def check_product_exists(self, account_name, batch_id, product_url):
        """
        product_list 테이블에서 제품 존재 여부 확인

        쓰임새:
        - BSR/Promotion 크롤러에서 중복 확인에 사용
        - 같은 batch_id + product_url 조합이 이미 있는지 확인
        - 존재하면 UPDATE, 없으면 INSERT 결정

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)
            batch_id (str): 배치 ID
            product_url (str): 제품 URL

        Returns:
            bool: 제품이 존재하면 True, 없으면 False
        """
        try:
            # 테이블명 매핑
            table_map = {
                'Amazon': 'amazon_hhp_product_list',
                'Bestbuy': 'bby_hhp_product_list',
                'Walmart': 'wmart_hhp_product_list'
            }

            table_name = table_map.get(account_name)
            if not table_name:
                return False

            cursor = self.db_conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE batch_id = %s AND product_url = %s
            """, (batch_id, product_url))

            count = cursor.fetchone()[0]
            cursor.close()

            return count > 0

        except Exception as e:
            print(f"[ERROR] Failed to check product existence: {e}")
            traceback.print_exc()
            return False

    def update_product_rank(self, account_name, batch_id, product_url, rank_type, rank_value, additional_fields=None):
        """
        기존 제품에 rank 정보 업데이트

        쓰임새:
        - BSR/Promotion 크롤러에서 이미 존재하는 제품에 순위 정보 추가
        - Main에서 수집된 제품이 BSR에도 있을 때 bsr_rank 업데이트
        - 추가 필드도 함께 업데이트 가능

        Args:
            account_name (str): 쇼핑몰명 (Amazon, Bestbuy, Walmart)
            batch_id (str): 배치 ID
            product_url (str): 제품 URL
            rank_type (str): 순위 타입 (main_rank, bsr_rank, trend_rank)
            rank_value (int): 순위 값
            additional_fields (dict): 추가로 업데이트할 필드 딕셔너리

        Returns:
            bool: 업데이트 성공 시 True, 실패 시 False
        """
        try:
            # 테이블명 매핑
            table_map = {
                'Amazon': 'amazon_hhp_product_list',
                'Bestbuy': 'bby_hhp_product_list',
                'Walmart': 'wmart_hhp_product_list'
            }

            table_name = table_map.get(account_name)
            if not table_name:
                return False

            # UPDATE 쿼리 구성
            update_fields = [f"{rank_type} = %s"]
            values = [rank_value]

            # 추가 필드가 있으면 포함
            if additional_fields:
                for field, value in additional_fields.items():
                    update_fields.append(f"{field} = %s")
                    values.append(value)

            # WHERE 조건 값 추가
            values.extend([batch_id, product_url])

            query = f"""
                UPDATE {table_name}
                SET {', '.join(update_fields)}
                WHERE batch_id = %s AND product_url = %s
            """

            cursor = self.db_conn.cursor()
            cursor.execute(query, values)
            cursor.close()

            print(f"[INFO] Updated {rank_type} for existing product")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to update product rank: {e}")
            traceback.print_exc()
            return False

    def retry_on_network_error(self, func, max_retries=3, delay=5):
        """
        네트워크 에러 발생 시 재시도 데코레이터

        쓰임새:
        - 네트워크 불안정으로 인한 일시적 오류 대응
        - 최대 3회까지 재시도 (재시도 간격 5초)
        - TimeoutException, ConnectionError 등에 대응

        Args:
            func: 재시도할 함수
            max_retries (int): 최대 재시도 횟수 (기본: 3)
            delay (int): 재시도 간격 초 (기본: 5초)

        Returns:
            function result or None: 함수 실행 결과 또는 실패 시 None
        """
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[WARNING] Attempt {attempt + 1} failed: {e}")
                    print(f"[INFO] Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"[ERROR] All {max_retries} attempts failed")
                    return None