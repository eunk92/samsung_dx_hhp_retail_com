"""
Amazon HHP 통합 크롤러 (테스트용)

================================================================================
실행 흐름: Main → BSR → Login → Detail → Item
================================================================================
STEP 1. Main   - 검색 결과 페이지에서 제품 목록 수집 (test_count 설정값)
STEP 2. BSR    - Best Seller 페이지에서 제품 목록 수집 (test_count 설정값)
STEP 3. Login  - Amazon 로그인 (쿠키 갱신)
STEP 4. Detail - 수집된 모든 제품의 상세 페이지 크롤링
STEP 5. Item   - hhp_item_mst에 SKU 추출 및 저장

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
python amazon_hhp_crawl_test.py --resume-from item --batch-id a_20250123_143045

================================================================================
저장 테이블
================================================================================
- Main/BSR     → amazon_hhp_product_list (제품 목록)
- Detail       → hhp_retail_com (상세 정보 + 리뷰)
- Item         → hhp_item_mst (제품 마스터)
"""

import sys
import os
import argparse
import subprocess
import traceback
import time
from datetime import datetime
import pytz

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from amazon.amazon_hhp_main import AmazonMainCrawler
from amazon.amazon_hhp_bsr import AmazonBSRCrawler
from amazon.amazon_hhp_dt import AmazonDetailCrawler
from amazon.amazon_hhp_item import AmazonItemCrawler
from common.base_crawler import BaseCrawler
from common.alert_hhp_monitor import send_crawl_alert


class AmazonIntegratedCrawlerTest:
    """Amazon 통합 크롤러 (테스트용)"""

    def __init__(self, resume_from=None, batch_id=None):
        """
        Args:
            resume_from: 재시작 단계 ('main'/'bsr'/'login'/'detail'/'item'/None)
            batch_id: 재시작 시 사용할 배치 ID
        """
        self.account_name = 'Amazon'
        self.batch_id = batch_id
        self.start_time_kst = None
        self.start_time_server = None
        self.end_time = None
        self.resume_from = resume_from
        self.login_success = False
        self.base_crawler = BaseCrawler()
        self.korea_tz = pytz.timezone('Asia/Seoul')

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
        self.start_time_kst = datetime.now(self.korea_tz)
        self.start_time_server = datetime.now()

        # batch_id 생성 또는 재사용
        if not self.batch_id:
            self.batch_id = self.base_crawler.generate_batch_id(self.account_name, test_mode=True)

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
            # 결과: {'stage': {'success': bool, 'duration': float}} 형태로 저장
            crawl_results = {'main': None, 'bsr': None, 'login': None, 'detail': None, 'item': None}

            # STEP 1: Main
            if not self.resume_from or self.resume_from == 'main':
                print(f"\n[STEP 1/5] Main Crawler...")
                stage_start = time.time()
                try:
                    success = AmazonMainCrawler(test_mode=True, batch_id=self.batch_id).run()
                    crawl_results['main'] = {'success': success, 'duration': time.time() - stage_start}
                except Exception as e:
                    print(f"[ERROR] Main: {e}")
                    crawl_results['main'] = {'success': False, 'duration': time.time() - stage_start}
            else:
                crawl_results['main'] = 'skipped'

            # STEP 2: BSR
            if not self.resume_from or self.resume_from in ['main', 'bsr']:
                print(f"\n[STEP 2/5] BSR Crawler...")
                stage_start = time.time()
                try:
                    success = AmazonBSRCrawler(test_mode=True, batch_id=self.batch_id).run()
                    crawl_results['bsr'] = {'success': success, 'duration': time.time() - stage_start}
                except Exception as e:
                    print(f"[ERROR] BSR: {e}")
                    crawl_results['bsr'] = {'success': False, 'duration': time.time() - stage_start}
            else:
                crawl_results['bsr'] = 'skipped'

            # STEP 3: Login
            if not self.resume_from or self.resume_from in ['main', 'bsr', 'login']:
                print(f"\n[STEP 3/5] Login...")
                stage_start = time.time()
                try:
                    self.login_success = self.run_login()
                    crawl_results['login'] = {'success': self.login_success, 'duration': time.time() - stage_start}
                except Exception as e:
                    print(f"[ERROR] Login: {e}")
                    crawl_results['login'] = {'success': False, 'duration': time.time() - stage_start}
            else:
                crawl_results['login'] = 'skipped'
                self.login_success = True  # resume_from=detail인 경우 기존 쿠키 사용

            # STEP 4: Detail
            if not self.resume_from or self.resume_from in ['main', 'bsr', 'login', 'detail']:
                print(f"\n[STEP 4/5] Detail Crawler...")
                stage_start = time.time()
                try:
                    success = AmazonDetailCrawler(batch_id=self.batch_id, login_success=self.login_success, test_mode=True).run()
                    crawl_results['detail'] = {'success': success, 'duration': time.time() - stage_start}
                except Exception as e:
                    print(f"[ERROR] Detail: {e}")
                    crawl_results['detail'] = {'success': False, 'duration': time.time() - stage_start}
            else:
                crawl_results['detail'] = 'skipped'

            # STEP 5: Item
            print(f"\n[STEP 5/5] Item Crawler...")
            stage_start = time.time()
            try:
                success = AmazonItemCrawler(batch_id=self.batch_id, test_mode=True).run()
                crawl_results['item'] = {'success': success, 'duration': time.time() - stage_start}
            except Exception as e:
                print(f"[ERROR] Item: {e}")
                crawl_results['item'] = {'success': False, 'duration': time.time() - stage_start}

            # 결과 출력
            self.end_time = datetime.now()
            elapsed = (self.end_time - self.start_time_server).total_seconds()

            print("\n" + "="*60)
            print(f"완료 ({elapsed/60:.1f}분)")
            for step, result in crawl_results.items():
                if result == 'skipped':
                    status = "SKIP"
                elif isinstance(result, dict):
                    status = "OK" if result.get('success') else "FAIL"
                else:
                    status = "FAIL"
                print(f"  {step}: {status}")
            print("="*60)

            # 이메일 알림 발송
            failed_stages = [
                k for k, v in crawl_results.items()
                if isinstance(v, dict) and v.get('success') is False
            ]
            send_crawl_alert(
                retailer='USA Amazon HHP',
                results=crawl_results,
                failed_stages=failed_stages,
                elapsed_time=elapsed,
                resume_from=self.resume_from,
                test_mode=True,
                start_time_kst=self.start_time_kst,
                start_time_server=self.start_time_server
            )

            # 로깅 종료
            self.base_crawler.stop_logging()

            success_count = sum(
                1 for r in crawl_results.values()
                if isinstance(r, dict) and r.get('success') is True
            )
            return success_count > 0

        except Exception as e:
            print(f"\n[ERROR] Amazon HHP Integrated Crawler (Test Mode) failed: {e}")
            traceback.print_exc()

            # 예외 발생 시에도 이메일 알림 발송
            self.end_time = datetime.now()
            elapsed = (self.end_time - self.start_time_server).total_seconds() if self.start_time_server else 0
            send_crawl_alert(
                retailer='USA Amazon HHP',
                results=crawl_results,
                failed_stages=['Fatal error'],
                elapsed_time=elapsed,
                error_message=str(e),
                resume_from=self.resume_from,
                test_mode=True,
                start_time_kst=self.start_time_kst,
                start_time_server=self.start_time_server
            )

            # 예외 발생 시에도 로깅 종료
            self.base_crawler.stop_logging()
            return False


def main():
    """테스트용 통합 크롤러 진입점"""
    parser = argparse.ArgumentParser(description='Amazon HHP Integrated Crawler (Test Mode)')
    parser.add_argument('--resume-from', type=str, choices=['main', 'bsr', 'login', 'detail', 'item'])
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
