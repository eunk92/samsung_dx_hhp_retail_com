"""
Amazon Login Script for HHP Crawler
- 아마존에 로그인하여 세션 쿠키를 저장
- Detail 크롤러에서 리뷰 수집을 위해 필요

사용법:
    python amazon/amazon_login.py

주의사항:
    - config.py에 Amazon 계정 정보 설정 필요
    - 최초 1회 실행하여 쿠키 저장
    - 쿠키 만료 시 재실행 필요
    - CAPTCHA/OTP 발생 시 수동으로 60초 내 입력
"""

import sys
import os
import time
import pickle

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Import Amazon credentials from config
try:
    from config import AMAZON_LOGIN
    AMAZON_EMAIL = AMAZON_LOGIN['email']
    AMAZON_PASSWORD = AMAZON_LOGIN['password']
except ImportError:
    print("\n" + "!" * 80)
    print("! ERROR: config.py not found!")
    print("! Please create config.py from config.example.py")
    print("!" * 80)
    sys.exit(1)
except KeyError:
    print("\n" + "!" * 80)
    print("! ERROR: AMAZON_LOGIN not found in config.py!")
    print("! Please add AMAZON_LOGIN configuration to config.py")
    print("!" * 80)
    sys.exit(1)

# Cookie file path (저장 위치: 프로젝트 루트의 cookies 폴더)
COOKIE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cookies', 'amazon_cookies.pkl')

def setup_driver():
    """Setup Chrome WebDriver"""
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

    # Anti-detection
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        '''
    })

    return driver

def save_cookies(driver, filepath):
    """Save cookies to file"""
    # cookies 디렉토리 생성
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'wb') as f:
        pickle.dump(driver.get_cookies(), f)
    print(f"[OK] Cookies saved to {filepath}")

def load_cookies(driver, filepath):
    """Load cookies from file"""
    if not os.path.exists(filepath):
        return False

    with open(filepath, 'rb') as f:
        cookies = pickle.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)
    print(f"[OK] Cookies loaded from {filepath}")
    return True

def login_to_amazon(driver, email, password):
    """Login to Amazon"""
    try:
        print("\n" + "=" * 80)
        print("Amazon Login Process")
        print("=" * 80)

        # Go to Amazon
        print("\n[1] Accessing Amazon.com...")
        driver.get("https://www.amazon.com")
        time.sleep(3)

        # Click "Sign in" button
        print("\n[2] Clicking 'Sign in' button...")
        try:
            # Try multiple selectors
            sign_in_selectors = [
                (By.ID, "nav-link-accountList"),
                (By.CSS_SELECTOR, "a[data-nav-role='signin']"),
                (By.XPATH, "//a[contains(@href, 'ap/signin')]")
            ]

            signed_in = False
            for by, selector in sign_in_selectors:
                try:
                    sign_in = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((by, selector))
                    )
                    sign_in.click()
                    signed_in = True
                    print("    [OK] Clicked sign in button")
                    break
                except:
                    continue

            if not signed_in:
                print("    [WARNING] Could not find sign-in button, might already be on sign-in page")

            time.sleep(2)

        except Exception as e:
            print(f"    [WARNING] Error clicking sign-in: {e}")

        # Check for account selection screen first
        print("\n[3] Checking for account selection screen...")
        print(f"    Current URL: {driver.current_url}")

        # Save page source for debugging
        debug_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'login_page_debug.html')
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"    [DEBUG] Saved page source to {debug_file}")

        # Try to find existing account button
        account_button_selectors = [
            # Strategy 1: Account selection dropdown
            (By.CSS_SELECTOR, "div[data-a-input-name='accountSelectionSelect'] span.a-button-text"),
            (By.XPATH, "//div[@data-a-input-name='accountSelectionSelect']//span[contains(@class, 'a-button-text')]"),

            # Strategy 2: "Switch accounts" section
            (By.XPATH, "//div[contains(@class, 'cvf-account-switcher-account')]"),
            (By.XPATH, "//div[contains(@data-testid, 'account-list-item')]"),
            (By.CSS_SELECTOR, "div[data-testid*='account-list-item']"),

            # Strategy 3: Find by email text
            (By.XPATH, f"//span[contains(text(), '{email}')]"),
            (By.XPATH, f"//div[contains(text(), '{email}')]"),

            # Strategy 4: Generic - any element with @ symbol
            (By.XPATH, "//span[contains(text(), '@')]"),
            (By.XPATH, "//div[contains(text(), '@')]"),

            # Strategy 5: Account card/container
            (By.CSS_SELECTOR, "div.cvf-account-switcher-account"),
            (By.CSS_SELECTOR, "div[class*='account']"),
        ]

        account_found = False
        for idx, (by, selector) in enumerate(account_button_selectors, 1):
            try:
                account_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((by, selector))
                )
                print(f"    [OK] Found existing account button using selector #{idx}")
                button_text = account_button.text[:80] if account_button.text else "[No text]"
                print(f"    [OK] Clicking account: {button_text}...")
                account_button.click()
                account_found = True
                time.sleep(2)
                break
            except:
                continue

        if account_found:
            print("    [OK] Selected existing account, skipping email entry")
        else:
            # No account selection, proceed with email entry
            print("    [X] No account selection screen, entering email...")

            # Try multiple selectors for email field
            email_selectors = [
                (By.ID, "ap_email"),
                (By.NAME, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[name='email']"),
                (By.XPATH, "//input[@type='email']")
            ]

            email_input = None
            for idx, (by, selector) in enumerate(email_selectors, 1):
                try:
                    email_input = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    print(f"    [OK] Found email input using selector #{idx}: {selector}")
                    break
                except:
                    print(f"    [X] Not found using selector #{idx}: {selector}")
                    continue

            if not email_input:
                print("\n[ERROR] Could not find email input field!")
                print(f"Please check {debug_file} for page structure")
                raise Exception("Email input field not found")

            email_input.clear()
            email_input.send_keys(email)
            print("    [OK] Email entered")
            time.sleep(1)

            # Click Continue
            print("\n[4] Clicking 'Continue' button...")
            continue_selectors = [
                (By.ID, "continue"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                (By.CSS_SELECTOR, "input.a-button-input"),
                (By.XPATH, "//input[@id='continue']")
            ]

            for idx, (by, selector) in enumerate(continue_selectors, 1):
                try:
                    continue_button = driver.find_element(by, selector)
                    continue_button.click()
                    print(f"    [OK] Clicked continue button using selector #{idx}")
                    break
                except:
                    continue

            time.sleep(3)

        # Enter password
        print("\n[5] Entering password...")
        password_selectors = [
            (By.ID, "ap_password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[name='password']"),
            (By.XPATH, "//input[@type='password']")
        ]

        password_input = None
        for idx, (by, selector) in enumerate(password_selectors, 1):
            try:
                password_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((by, selector))
                )
                print(f"    [OK] Found password input using selector #{idx}: {selector}")
                break
            except:
                print(f"    [X] Not found using selector #{idx}: {selector}")
                continue

        if not password_input:
            print("\n[ERROR] Could not find password input field!")
            raise Exception("Password input field not found")

        password_input.clear()
        password_input.send_keys(password)
        print("    [OK] Password entered")
        time.sleep(1)

        # Click Sign-In
        print("\n[6] Clicking 'Sign-In' button...")
        signin_selectors = [
            (By.ID, "signInSubmit"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.CSS_SELECTOR, "input.a-button-input"),
            (By.XPATH, "//input[@id='signInSubmit']")
        ]

        for idx, (by, selector) in enumerate(signin_selectors, 1):
            try:
                sign_in_button = driver.find_element(by, selector)
                sign_in_button.click()
                print(f"    [OK] Clicked sign-in button using selector #{idx}")
                break
            except:
                continue

        time.sleep(5)

        # Check for CAPTCHA or OTP
        print("\n[7] Checking for CAPTCHA or OTP...")
        current_url = driver.current_url

        if "ap/cvf" in current_url or "ap/mfa" in current_url:
            print("\n" + "!" * 80)
            print("! OTP / 2-Factor Authentication Required!")
            print("! Please enter the OTP code manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        elif "ap/captcha" in current_url or "captcha" in driver.page_source.lower():
            print("\n" + "!" * 80)
            print("! CAPTCHA Detected!")
            print("! Please solve the CAPTCHA manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        # Verify login success
        print("\n[8] Verifying login status...")
        driver.get("https://www.amazon.com")
        time.sleep(3)

        # Check if logged in
        try:
            account_element = driver.find_element(By.ID, "nav-link-accountList")
            account_text = account_element.text.lower()

            if "hello" in account_text and "sign in" not in account_text:
                print("\n" + "=" * 80)
                print("[OK] LOGIN SUCCESSFUL!")
                print("=" * 80)
                return True
            else:
                print("\n" + "=" * 80)
                print("[X] LOGIN FAILED - Still showing 'Sign in'")
                print("=" * 80)
                return False

        except Exception as e:
            print(f"\n[WARNING] Could not verify login status: {e}")
            print("Assuming login successful...")
            return True

    except Exception as e:
        print(f"\n[ERROR] Login failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_login_with_cookies():
    """Test login using saved cookies or fresh login"""
    driver = setup_driver()

    try:
        # Try to load cookies first
        if os.path.exists(COOKIE_FILE):
            print(f"\n[INFO] Found saved cookies: {COOKIE_FILE}")
            print("[INFO] Trying to login with saved cookies...")

            driver.get("https://www.amazon.com")
            time.sleep(2)

            load_cookies(driver, COOKIE_FILE)
            driver.refresh()
            time.sleep(3)

            # Check if logged in
            try:
                account_element = driver.find_element(By.ID, "nav-link-accountList")
                account_text = account_element.text.lower()

                if "hello" in account_text and "sign in" not in account_text:
                    print("\n[OK] Successfully logged in using saved cookies!")
                    return driver
                else:
                    print("\n[WARNING] Saved cookies expired, need fresh login")
            except:
                print("\n[WARNING] Could not verify cookie login, need fresh login")

        # Fresh login
        print("\n[INFO] Starting fresh login...")
        print(f"[INFO] Email: {AMAZON_EMAIL}")
        print(f"[INFO] Password: {'*' * len(AMAZON_PASSWORD)}")

        if AMAZON_EMAIL == 'your-email@example.com':
            print("\n" + "!" * 80)
            print("! ERROR: Please set Amazon credentials in config.py!")
            print("! Edit config.py and update AMAZON_LOGIN settings")
            print("!" * 80)
            return None

        if login_to_amazon(driver, AMAZON_EMAIL, AMAZON_PASSWORD):
            save_cookies(driver, COOKIE_FILE)
            return driver
        else:
            print("\n[ERROR] Login failed!")
            return None

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("="*80)
    print("Amazon Login Script for HHP Crawler")
    print("="*80)

    try:
        driver = test_login_with_cookies()

        if driver:
            print("\n" + "=" * 80)
            print("Login test completed successfully!")
            print("You can now use the saved cookies for crawling")
            print(f"Cookie file: {COOKIE_FILE}")
            print("=" * 80)

            input("\nPress Enter to close browser...")
            driver.quit()
        else:
            print("\n[FAILED] Login test failed")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")