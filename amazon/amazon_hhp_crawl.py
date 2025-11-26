"""
Amazon 통합 크롤러 (운영용)
- Main → BSR → Login → Detail 순차 실행
- 동일한 batch_id로 전체 파이프라인 실행
- 운영 모드: Main(최대 400개) + BSR(2페이지) + Detail(전체)
- 재시작 기능: --resume-from 옵션으로 특정 단계부터 재개 가능
- 로그인 실패 시에도 Detail 크롤러 진행 (리뷰만 스킵)
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


class AmazonIntegratedCrawler:
    """
    Amazon 통합 크롤러 (운영용)
    Main → BSR → Login → Detail 순차 실행
    """

    def __init__(self, resume_from=None, batch_id=None):
        """
        초기화

        Args:
            resume_from (str): 재시작할 단계 ('main', 'bsr', 'login', 'detail', None)
            batch_id (str): 재시작 시 사용할 배치 ID (resume_from 사용 시 필수)
        """
        self.account_name = 'Amazon'
        self.batch_id = batch_id
        self.start_time = None
        self.end_time = None
        self.resume_from = resume_from
        self.login_success = False  # 로그인 성공 여부
        # batch_id 생성을 위한 임시 BaseCrawler 인스턴스
        self.base_crawler = BaseCrawler()

    def run_login(self):
        """
        Amazon 로그인 스크립트 실행

        Returns:
            bool: 로그인 성공 시 True, 실패 시 False
        """
        try:
            # amazon_login.py 경로
            login_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amazon_login.py')

            if not os.path.exists(login_script):
                print(f"[ERROR] Login script not found: {login_script}")
                return False

            print(f"[INFO] Running login script: {login_script}")

            # subprocess로 로그인 스크립트 실행
            result = subprocess.run(
                ['python', login_script],
                capture_output=True,
                text=True,
                timeout=120  # 2분 타임아웃 (OTP 입력 시간 포함)
            )

            # 결과 출력
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            # 로그인 성공 여부 판단
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
        """
        통합 크롤러 실행

        실행 순서:
        0. 통합 크롤러에서 batch_id 생성 (또는 기존 batch_id 사용)
        1. Main 크롤러 (운영 모드: 최대 400개 제품)
        2. BSR 크롤러 (운영 모드: 2페이지)
        3. Login 실행 (쿠키 갱신)
        4. Detail 크롤러 (Main + BSR에서 수집한 모든 제품)

        Returns:
            bool: 성공 시 True, 실패 시 False
        """
        self.start_time = datetime.now()

        # batch_id 생성 또는 재사용
        if not self.batch_id:
            self.batch_id = self.base_crawler.generate_batch_id(self.account_name)
            print(f"[INFO] New Batch ID generated: {self.batch_id}")
        else:
            print(f"[INFO] Resuming with Batch ID: {self.batch_id}")

        print("\n" + "="*80)
        print("Amazon Integrated Crawler (Production Mode)")
        if self.resume_from:
            print(f"[RESUME MODE] Starting from: {self.resume_from.upper()}")
        print("="*80)
        print(f"[INFO] Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Batch ID: {self.batch_id}")
        print("="*80 + "\n")

        try:
            # ========================================
            # STEP 1: Main 크롤러 실행 (운영 모드)
            # ========================================
            if not self.resume_from or self.resume_from == 'main':
                print("\n" + "="*80)
                print("STEP 1: Main Crawler (Production Mode)")
                print("="*80 + "\n")

                main_crawler = AmazonMainCrawler(test_mode=False, batch_id=self.batch_id)
                main_success = main_crawler.run()

                if not main_success:
                    print("\n[ERROR] Main crawler failed. Stopping integrated crawler.")
                    return False
            else:
                print(f"\n[SKIP] Main Crawler (resume_from={self.resume_from})\n")

            # ========================================
            # STEP 2: BSR 크롤러 실행 (운영 모드)
            # ========================================
            if not self.resume_from or self.resume_from in ['main', 'bsr']:
                print("\n" + "="*80)
                print("STEP 2: BSR Crawler (Production Mode)")
                print("="*80 + "\n")

                bsr_crawler = AmazonBSRCrawler(test_mode=False, batch_id=self.batch_id)
                bsr_success = bsr_crawler.run()

                if not bsr_success:
                    print("\n[ERROR] BSR crawler failed. Stopping integrated crawler.")
                    return False
            else:
                print(f"\n[SKIP] BSR Crawler (resume_from={self.resume_from})\n")

            # ========================================
            # STEP 3: Login 실행 (쿠키 갱신)
            # ========================================
            if not self.resume_from or self.resume_from in ['main', 'bsr', 'login']:
                print("\n" + "="*80)
                print("STEP 3: Amazon Login (Cookie Refresh)")
                print("="*80 + "\n")

                self.login_success = self.run_login()

                if self.login_success:
                    print("[OK] Login successful - Detail crawler will load cookies")
                else:
                    print("[WARNING] Login failed - Detail crawler will skip review collection")
            else:
                print(f"\n[SKIP] Login (resume_from={self.resume_from})\n")
                # resume_from=detail인 경우, 기존 쿠키 사용 시도
                self.login_success = True

            # ========================================
            # STEP 4: Detail 크롤러 실행
            # ========================================
            print("\n" + "="*80)
            print("STEP 4: Detail Crawler (All Products)")
            print("="*80 + "\n")

            detail_crawler = AmazonDetailCrawler(test_mode=False, batch_id=self.batch_id, login_success=self.login_success)
            detail_success = detail_crawler.run()

            if not detail_success:
                print("\n[ERROR] Detail crawler failed.")
                return False

            # ========================================
            # 최종 결과 출력
            # ========================================
            self.end_time = datetime.now()
            elapsed_time = (self.end_time - self.start_time).total_seconds()

            print("\n" + "="*80)
            print("Amazon Integrated Crawler - COMPLETED")
            print("="*80)
            print(f"[INFO] Batch ID: {self.batch_id}")
            print(f"[INFO] Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[INFO] End Time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[INFO] Total Elapsed Time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
            print("="*80 + "\n")

            return True

        except Exception as e:
            print(f"\n[ERROR] Integrated crawler failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """
    운영용 통합 크롤러 진입점

    사용법:
        # 처음부터 실행
        python amazon_hhp_crawl.py

        # Detail부터 재시작
        python amazon_hhp_crawl.py --resume-from detail --batch-id a_20250123_143045

        # BSR부터 재시작
        python amazon_hhp_crawl.py --resume-from bsr --batch-id a_20250123_143045
    """
    parser = argparse.ArgumentParser(
        description='Amazon HHP Integrated Crawler (Production Mode)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--resume-from',
        type=str,
        choices=['main', 'bsr', 'login', 'detail'],
        help='재시작할 단계 (main, bsr, login, detail)'
    )

    parser.add_argument(
        '--batch-id',
        type=str,
        help='재시작 시 사용할 배치 ID (resume-from 사용 시 필수)'
    )

    args = parser.parse_args()

    # 검증: resume-from 사용 시 batch-id 필수
    if args.resume_from and not args.batch_id:
        print("[ERROR] --batch-id is required when using --resume-from")
        print("Example: python amazon_hhp_crawl.py --resume-from detail --batch-id a_20250123_143045")
        exit(1)

    # 크롤러 실행
    crawler = AmazonIntegratedCrawler(
        resume_from=args.resume_from,
        batch_id=args.batch_id
    )
    success = crawler.run()

    if success:
        print("\n[SUCCESS] Amazon Integrated Crawler completed successfully")
        exit(0)
    else:
        print("\n[FAILED] Amazon Integrated Crawler failed")
        exit(1)


if __name__ == '__main__':
    main()