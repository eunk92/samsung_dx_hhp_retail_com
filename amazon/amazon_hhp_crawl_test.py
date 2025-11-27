"""
Amazon HHP 통합 크롤러 (테스트용)

================================================================================
실행 흐름: Main → BSR → Login → Detail
================================================================================
STEP 1. Main   - 검색 결과 페이지에서 제품 목록 수집 (test_count 설정값)
STEP 2. BSR    - Best Seller 페이지에서 제품 목록 수집 (test_count 설정값)
STEP 3. Login  - Amazon 로그인 (쿠키 갱신)
STEP 4. Detail - 수집된 모든 제품의 상세 페이지 크롤링

================================================================================
주요 특징
================================================================================
- 동일한 batch_id로 전체 파이프라인 실행
- 각 크롤러 실패 시에도 다음 단계 계속 진행
- --resume-from 옵션으로 특정 단계부터 재개 가능
- 로그인 실패 시에도 Detail 크롤러 진행

================================================================================
사용법
================================================================================
# 처음부터 실행
python amazon_hhp_crawl_test.py

# 특정 단계부터 재시작
python amazon_hhp_crawl_test.py --resume-from detail --batch-id a_20250123_143045
python amazon_hhp_crawl_test.py --resume-from bsr --batch-id a_20250123_143045

================================================================================
저장 테이블
================================================================================
- Main/BSR     → amazon_hhp_product_list (제품 목록)
- Detail       → hhp_retail_com (상세 정보 + 리뷰)
"""

import sys
import os
import argparse
import subprocess
from datetime import datetime

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from amazon.amazon_hhp_main import AmazonMainCrawler
from amazon.amazon_hhp_bsr import AmazonBSRCrawler
from amazon.amazon_hhp_dt import AmazonDetailCrawler
from common.base_crawler import BaseCrawler


class AmazonIntegratedCrawlerTest:
    """Amazon 통합 크롤러 (테스트용)"""

    def __init__(self, resume_from=None, batch_id=None):
        """
        Args:
            resume_from: 재시작 단계 ('main'/'bsr'/'login'/'detail'/None)
            batch_id: 재시작 시 사용할 배치 ID
        """
        self.account_name = 'Amazon'
        self.batch_id = batch_id
        self.start_time = None
        self.end_time = None
        self.resume_from = resume_from
        self.login_success = False
        self.base_crawler = BaseCrawler()

    def run_login(self):
        """Amazon 로그인 스크립트 실행. Returns: bool"""
        try:
            login_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amazon_hhp_login.py')

            if not os.path.exists(login_script):
                print(f"[ERROR] Login script not found: {login_script}")
                return False

            print(f"[INFO] Running login script...")

            result = subprocess.run(
                ['python', login_script],
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            if result.returncode == 0 or 'LOGIN SUCCESSFUL' in result.stdout or 'Successfully logged in' in result.stdout:
                print("[OK] Login successful")
                return True
            else:
                print("[WARNING] Login may have failed, but continuing...")
                return False

        except subprocess.TimeoutExpired:
            print("[ERROR] Login script timed out (120 seconds)")
            return False
        except Exception as e:
            print(f"[ERROR] Login script execution failed: {e}")
            return False

    def run(self):
        """통합 크롤러 실행 (테스트 모드). Returns: bool"""
        self.start_time = datetime.now()

        # batch_id 생성 또는 재사용
        if not self.batch_id:
            self.batch_id = self.base_crawler.generate_batch_id(self.account_name)

        # 로깅 시작 (콘솔 출력을 파일에도 저장)
        log_file = self.base_crawler.start_logging(self.batch_id)

        print("\n" + "="*60)
        print("Amazon Integrated Crawler (Test)")
        print("="*60)
        print(f"batch_id: {self.batch_id}")
        if log_file:
            print(f"log_file: {log_file}")
        if self.resume_from:
            print(f"resume_from: {self.resume_from}")

        try:
            crawl_results = {'main': None, 'bsr': None, 'login': None, 'detail': None}

            # STEP 1: Main
            if not self.resume_from or self.resume_from == 'main':
                print(f"\n[STEP 1/4] Main Crawler...")
                try:
                    crawl_results['main'] = AmazonMainCrawler(test_mode=True, batch_id=self.batch_id).run()
                except Exception as e:
                    print(f"[ERROR] Main: {e}")
                    crawl_results['main'] = False
            else:
                crawl_results['main'] = 'skipped'

            # STEP 2: BSR
            if not self.resume_from or self.resume_from in ['main', 'bsr']:
                print(f"\n[STEP 2/4] BSR Crawler...")
                try:
                    crawl_results['bsr'] = AmazonBSRCrawler(test_mode=True, batch_id=self.batch_id).run()
                except Exception as e:
                    print(f"[ERROR] BSR: {e}")
                    crawl_results['bsr'] = False
            else:
                crawl_results['bsr'] = 'skipped'

            # STEP 3: Login
            if not self.resume_from or self.resume_from in ['main', 'bsr', 'login']:
                print(f"\n[STEP 3/4] Login...")
                try:
                    self.login_success = self.run_login()
                    crawl_results['login'] = self.login_success
                except Exception as e:
                    print(f"[ERROR] Login: {e}")
                    crawl_results['login'] = False
            else:
                crawl_results['login'] = 'skipped'
                self.login_success = True  # resume_from=detail인 경우 기존 쿠키 사용

            # STEP 4: Detail
            print(f"\n[STEP 4/4] Detail Crawler...")
            try:
                crawl_results['detail'] = AmazonDetailCrawler(batch_id=self.batch_id, login_success=self.login_success).run()
            except Exception as e:
                print(f"[ERROR] Detail: {e}")
                crawl_results['detail'] = False

            # 결과 출력
            self.end_time = datetime.now()
            elapsed = (self.end_time - self.start_time).total_seconds()

            print("\n" + "="*60)
            print(f"완료 ({elapsed/60:.1f}분)")
            for step, result in crawl_results.items():
                status = "SKIP" if result == 'skipped' else "OK" if result else "FAIL"
                print(f"  {step}: {status}")
            print("="*60)

            # 로깅 종료
            self.base_crawler.stop_logging()

            success_count = sum(1 for r in crawl_results.values() if r is True)
            return success_count > 0

        except Exception as e:
            print(f"\n[ERROR] Amazon HHP Integrated Crawler (Test Mode) failed: {e}")
            import traceback
            traceback.print_exc()
            # 예외 발생 시에도 로깅 종료
            self.base_crawler.stop_logging()
            return False


def main():
    """테스트용 통합 크롤러 진입점"""
    parser = argparse.ArgumentParser(description='Amazon HHP Integrated Crawler (Test Mode)')
    parser.add_argument('--resume-from', type=str, choices=['main', 'bsr', 'login', 'detail'])
    parser.add_argument('--batch-id', type=str)
    args = parser.parse_args()

    if args.resume_from and not args.batch_id:
        print("[ERROR] --batch-id is required when using --resume-from")
        exit(1)

    crawler = AmazonIntegratedCrawlerTest(resume_from=args.resume_from, batch_id=args.batch_id)
    success = crawler.run()
    exit(0 if success else 1)


if __name__ == '__main__':
    main()
