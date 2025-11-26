"""
Amazon 로그인 스크립트

================================================================================
주요 기능
================================================================================
- Amazon 로그인 후 세션 쿠키 저장
- Detail 크롤러에서 리뷰 수집 시 필요

================================================================================
사용법
================================================================================
python amazon/amazon_hhp_login.py

================================================================================
주의사항
================================================================================
- config.py에 Amazon 계정 정보 설정 필요
- 최초 1회 실행하여 쿠키 저장
- 쿠키 만료 시 재실행 필요
- CAPTCHA/OTP 발생 시 수동으로 60초 내 입력
"""

import sys
import os
import time
import pickle
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Amazon 계정 정보 로드
try:
    from config import AMAZON_LOGIN
    AMAZON_EMAIL = AMAZON_LOGIN['email']
    AMAZON_PASSWORD = AMAZON_LOGIN['password']
except ImportError:
    print("[ERROR] config.py not found - Please create from config.example.py")
    sys.exit(1)
except KeyError:
    print("[ERROR] AMAZON_LOGIN not found in config.py")
    sys.exit(1)

COOKIE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cookies', 'amazon_cookies.pkl')


def setup_driver():
    """Chrome WebDriver 설정"""
    chrome_options = Options()
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        '''
    })

    return driver


def save_cookies(driver, filepath):
    """쿠키 저장"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        pickle.dump(driver.get_cookies(), f)
    print(f"[OK] Cookies saved: {filepath}")


def load_cookies(driver, filepath):
    """쿠키 로드"""
    if not os.path.exists(filepath):
        return False
    with open(filepath, 'rb') as f:
        cookies = pickle.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)
    print(f"[OK] Cookies loaded: {filepath}")
    return True


def login_to_amazon(driver, email, password):
    """Amazon 로그인 수행"""
    try:
        print("\n" + "="*60)
        print("Amazon Login")
        print("="*60)

        # [1] Amazon 접속
        print("\n[1] Accessing Amazon.com...")
        driver.get("https://www.amazon.com")
        time.sleep(3)

        # [2] Sign in 버튼 클릭
        print("[2] Clicking Sign in...")
        sign_in_selectors = [
            (By.ID, "nav-link-accountList"),
            (By.CSS_SELECTOR, "a[data-nav-role='signin']"),
            (By.XPATH, "//a[contains(@href, 'ap/signin')]")
        ]

        signed_in = False
        for by, selector in sign_in_selectors:
            try:
                sign_in = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, selector)))
                sign_in.click()
                signed_in = True
                print("    [OK] Sign in button clicked")
                break
            except:
                continue

        if not signed_in:
            print("    [WARNING] Sign-in button not found")

        time.sleep(2)

        # [3] 계정 선택 화면 확인
        print("[3] Checking account selection...")
        account_button_selectors = [
            (By.CSS_SELECTOR, "div[data-a-input-name='accountSelectionSelect'] span.a-button-text"),
            # (By.XPATH, "//div[@data-a-input-name='accountSelectionSelect']//span[contains(@class, 'a-button-text')]"),
            (By.XPATH, "//div[contains(@class, 'cvf-account-switcher-account')]"),
            # (By.XPATH, "//div[contains(@data-testid, 'account-list-item')]"),
            # (By.CSS_SELECTOR, "div[data-testid*='account-list-item']"),
            (By.XPATH, f"//span[contains(text(), '{email}')]"),
            # (By.XPATH, f"//div[contains(text(), '{email}')]"),
            (By.XPATH, "//span[contains(text(), '@')]"),
            # (By.XPATH, "//div[contains(text(), '@')]"),
            (By.CSS_SELECTOR, "div.cvf-account-switcher-account"),
            # (By.CSS_SELECTOR, "div[class*='account']"),
        ]

        account_found = False
        for by, selector in account_button_selectors:
            try:
                account_button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, selector)))
                account_button.click()
                account_found = True
                print("    [OK] Existing account selected")
                time.sleep(2)
                break
            except:
                continue

        if not account_found:
            # 이메일 입력
            print("    [INFO] Entering email...")
            email_selectors = [
                (By.ID, "ap_email"),
                (By.NAME, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                # (By.XPATH, "//input[@id='ap_email']"),
                # (By.XPATH, "//input[@name='email']"),
            ]

            email_input = None
            for by, selector in email_selectors:
                try:
                    email_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((by, selector)))
                    break
                except:
                    continue

            if not email_input:
                print("    [ERROR] Email input not found")
                return False

            email_input.clear()
            email_input.send_keys(email)
            time.sleep(1)

            # Continue 버튼 클릭
            print("[4] Clicking Continue...")
            continue_selectors = [
                (By.ID, "continue"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                # (By.XPATH, "//input[@id='continue']"),
                # (By.CSS_SELECTOR, "input#continue"),
            ]

            for by, selector in continue_selectors:
                try:
                    driver.find_element(by, selector).click()
                    print("    [OK] Continue clicked")
                    break
                except:
                    continue

            time.sleep(3)

        # [5] 비밀번호 입력
        print("[5] Entering password...")
        password_selectors = [
            (By.ID, "ap_password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
            # (By.XPATH, "//input[@id='ap_password']"),
            # (By.XPATH, "//input[@name='password']"),
        ]

        password_input = None
        for by, selector in password_selectors:
            try:
                password_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((by, selector)))
                break
            except:
                continue

        if not password_input:
            print("    [ERROR] Password input not found")
            return False

        password_input.clear()
        password_input.send_keys(password)
        time.sleep(1)

        # [6] Sign-In 버튼 클릭
        print("[6] Clicking Sign-In...")
        signin_selectors = [
            (By.ID, "signInSubmit"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            # (By.XPATH, "//input[@id='signInSubmit']"),
            # (By.CSS_SELECTOR, "input#signInSubmit"),
        ]

        for by, selector in signin_selectors:
            try:
                driver.find_element(by, selector).click()
                print("    [OK] Sign-In clicked")
                break
            except:
                continue

        time.sleep(5)

        # [7] CAPTCHA/OTP 확인
        print("[7] Checking CAPTCHA/OTP...")
        current_url = driver.current_url

        if "ap/cvf" in current_url or "ap/mfa" in current_url:
            print("    [WARNING] OTP required - waiting 60s for manual input...")
            time.sleep(60)
        elif "captcha" in current_url or "captcha" in driver.page_source.lower():
            print("    [WARNING] CAPTCHA detected - waiting 60s for manual input...")
            time.sleep(60)

        # [8] 로그인 확인
        print("[8] Verifying login...")
        driver.get("https://www.amazon.com")
        time.sleep(3)

        try:
            account_element = driver.find_element(By.ID, "nav-link-accountList")
            account_text = account_element.text.lower()

            if "hello" in account_text and "sign in" not in account_text:
                print("\n[OK] LOGIN SUCCESSFUL!")
                return True
            else:
                print("\n[FAIL] LOGIN FAILED")
                return False
        except Exception as e:
            print(f"    [WARNING] Could not verify: {e}")
            return True

    except Exception as e:
        print(f"[ERROR] Login failed: {e}")
        traceback.print_exc()
        return False


def test_login_with_cookies():
    """쿠키 로그인 테스트 또는 새 로그인"""
    driver = setup_driver()

    try:
        # 저장된 쿠키로 시도
        if os.path.exists(COOKIE_FILE):
            print(f"[INFO] Found cookies: {COOKIE_FILE}")

            driver.get("https://www.amazon.com")
            time.sleep(2)
            load_cookies(driver, COOKIE_FILE)
            driver.refresh()
            time.sleep(3)

            try:
                account_element = driver.find_element(By.ID, "nav-link-accountList")
                account_text = account_element.text.lower()

                if "hello" in account_text and "sign in" not in account_text:
                    print("[OK] Cookie login successful!")
                    return driver
                else:
                    print("[WARNING] Cookies expired, need fresh login")
            except:
                print("[WARNING] Cookie verification failed")

        # 새 로그인
        print("[INFO] Starting fresh login...")

        if AMAZON_EMAIL == 'your-email@example.com':
            print("[ERROR] Please set Amazon credentials in config.py")
            return None

        if login_to_amazon(driver, AMAZON_EMAIL, AMAZON_PASSWORD):
            save_cookies(driver, COOKIE_FILE)
            return driver
        else:
            print("[ERROR] Login failed!")
            return None

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("="*60)
    print("Amazon Login Script")
    print("="*60)

    driver = test_login_with_cookies()

    if driver:
        print("\n" + "="*60)
        print("[DONE] Login completed")
        print(f"Cookie: {COOKIE_FILE}")
        print("="*60)
        driver.quit()
    else:
        print("\n[FAILED] Login failed")
