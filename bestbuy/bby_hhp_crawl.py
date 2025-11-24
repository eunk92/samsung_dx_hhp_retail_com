"""
BestBuy 통합 크롤러 (운영용)
- Main → BSR → Promotion → Detail 순차 실행
- 동일한 batch_id로 전체 파이프라인 실행
- 운영 모드: Main(최대 400개) + BSR(2페이지) + Promotion(2페이지) + Detail(전체)
- 재시작 기능: --resume-from 옵션으로 특정 단계부터 재개 가능
"""

import sys
import os
import argparse
from datetime import datetime

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from bestbuy.bby_hhp_main import BestBuyMainCrawler
from bestbuy.bby_hhp_bsr import BestBuyBSRCrawler
from bestbuy.bby_hhp_pmt import BestBuyPromotionCrawler
from bestbuy.bby_hhp_dt import BestBuyDetailCrawler
from common.base_crawler import BaseCrawler


class BestBuyIntegratedCrawler:
    """
    BestBuy 통합 크롤러 (운영용)
    Main → BSR → Promotion → Detail 순차 실행
    """

    def __init__(self, resume_from=None, batch_id=None):
        """
        초기화

        Args:
            resume_from (str): 재시작할 단계 ('main', 'bsr', 'promotion', 'detail', None)
            batch_id (str): 재시작 시 사용할 배치 ID (resume_from 사용 시 필수)
        """
        self.account_name = 'Bestbuy'
        self.batch_id = batch_id
        self.start_time = None
        self.end_time = None
        self.resume_from = resume_from
        self.base_crawler = BaseCrawler()

    def run(self):
        """
        통합 크롤러 실행

        실행 순서:
        0. 통합 크롤러에서 batch_id 생성 (또는 기존 batch_id 사용)
        1. Main 크롤러 (운영 모드: 최대 400개 제품)
        2. BSR 크롤러 (운영 모드: 2페이지)
        3. Promotion 크롤러 (운영 모드: 2페이지)
        4. Detail 크롤러 (Main + BSR + Promotion에서 수집한 모든 제품)

        Returns: bool: 성공 시 True, 실패 시 False
        """
        self.start_time = datetime.now()

        # batch_id 생성 또는 재사용
        if not self.batch_id:
            self.batch_id = self.base_crawler.generate_batch_id(self.account_name)
            print(f"[INFO] New Batch ID generated: {self.batch_id}")
        else:
            print(f"[INFO] Resuming with Batch ID: {self.batch_id}")

        print("\n" + "="*80)
        print("BestBuy Integrated Crawler (Production Mode)")
        if self.resume_from:
            print(f"[RESUME MODE] Starting from: {self.resume_from.upper()}")
        print("="*80)
        print(f"[INFO] Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Batch ID: {self.batch_id}")
        print("="*80 + "\n")

        try:
            # 크롤러 결과 추적
            crawl_results = {
                'main': None,
                'bsr': None,
                'promotion': None,
                'detail': None
            }

            # ========================================
            # STEP 1: Main 크롤러 실행 (운영 모드)
            # ========================================
            if not self.resume_from or self.resume_from == 'main':
                print("\n" + "="*80)
                print("STEP 1: Main Crawler (Production Mode)")
                print("="*80 + "\n")

                try:
                    main_crawler = BestBuyMainCrawler(test_mode=False, batch_id=self.batch_id)
                    main_success = main_crawler.run()
                    crawl_results['main'] = main_success

                    if not main_success:
                        print("\n[WARNING] Main crawler failed. Continuing to next step...")
                except Exception as e:
                    print(f"\n[ERROR] Main crawler exception: {e}")
                    crawl_results['main'] = False
            else:
                print(f"\n[SKIP] Main Crawler (resume_from={self.resume_from})\n")
                crawl_results['main'] = 'skipped'

            # ========================================
            # STEP 2: BSR 크롤러 실행 (운영 모드)
            # ========================================
            if not self.resume_from or self.resume_from in ['main', 'bsr']:
                print("\n" + "="*80)
                print("STEP 2: BSR Crawler (Production Mode)")
                print("="*80 + "\n")

                try:
                    bsr_crawler = BestBuyBSRCrawler(test_mode=False, batch_id=self.batch_id)
                    bsr_success = bsr_crawler.run()
                    crawl_results['bsr'] = bsr_success

                    if not bsr_success:
                        print("\n[WARNING] BSR crawler failed. Continuing to next step...")
                except Exception as e:
                    print(f"\n[ERROR] BSR crawler exception: {e}")
                    crawl_results['bsr'] = False
            else:
                print(f"\n[SKIP] BSR Crawler (resume_from={self.resume_from})\n")
                crawl_results['bsr'] = 'skipped'

            # ========================================
            # STEP 3: Promotion 크롤러 실행 (운영 모드)
            # ========================================
            if not self.resume_from or self.resume_from in ['main', 'bsr', 'promotion']:
                print("\n" + "="*80)
                print("STEP 3: Promotion Crawler (Production Mode)")
                print("="*80 + "\n")

                try:
                    pmt_crawler = BestBuyPromotionCrawler(test_mode=False, batch_id=self.batch_id)
                    pmt_success = pmt_crawler.run()
                    crawl_results['promotion'] = pmt_success

                    if not pmt_success:
                        print("\n[WARNING] Promotion crawler failed. Continuing to next step...")
                except Exception as e:
                    print(f"\n[ERROR] Promotion crawler exception: {e}")
                    crawl_results['promotion'] = False
            else:
                print(f"\n[SKIP] Promotion Crawler (resume_from={self.resume_from})\n")
                crawl_results['promotion'] = 'skipped'

            # ========================================
            # STEP 4: Detail 크롤러 실행
            # ========================================
            print("\n" + "="*80)
            print("STEP 4: Detail Crawler (All Products)")
            print("="*80 + "\n")

            try:
                detail_crawler = BestBuyDetailCrawler(batch_id=self.batch_id)
                detail_success = detail_crawler.run()
                crawl_results['detail'] = detail_success

                if not detail_success:
                    print("\n[WARNING] Detail crawler failed.")
            except Exception as e:
                print(f"\n[ERROR] Detail crawler exception: {e}")
                crawl_results['detail'] = False

            # ========================================
            # 최종 결과 출력
            # ========================================
            self.end_time = datetime.now()
            elapsed_time = (self.end_time - self.start_time).total_seconds()

            print("\n" + "="*80)
            print("BestBuy Integrated Crawler - COMPLETED")
            print("="*80)
            print(f"[INFO] Batch ID: {self.batch_id}")
            print(f"[INFO] Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[INFO] End Time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[INFO] Total Elapsed Time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
            print("\n[INFO] Crawler Results:")
            for step, result in crawl_results.items():
                if result == 'skipped':
                    status = "SKIPPED"
                elif result is True:
                    status = "SUCCESS"
                elif result is False:
                    status = "FAILED"
                else:
                    status = "NOT RUN"
                print(f"  - {step.upper()}: {status}")
            print("="*80 + "\n")

            # 최소 1개 이상 성공했는지 체크
            success_count = sum(1 for result in crawl_results.values() if result is True)
            if success_count > 0:
                print(f"[SUCCESS] {success_count}/4 crawlers succeeded")
                return True
            else:
                print("[FAILED] All crawlers failed")
                return False

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
        python bby_hhp_crawl.py

        # Detail부터 재시작
        python bby_hhp_crawl.py --resume-from detail --batch-id b_20250123_143045

        # Promotion부터 재시작
        python bby_hhp_crawl.py --resume-from promotion --batch-id b_20250123_143045
    """
    parser = argparse.ArgumentParser(
        description='BestBuy HHP Integrated Crawler (Production Mode)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--resume-from',
        type=str,
        choices=['main', 'bsr', 'promotion', 'detail'],
        help='재시작할 단계 (main, bsr, promotion, detail)'
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
        print("Example: python bby_hhp_crawl.py --resume-from detail --batch-id b_20250123_143045")
        exit(1)

    # 크롤러 실행
    crawler = BestBuyIntegratedCrawler(
        resume_from=args.resume_from,
        batch_id=args.batch_id
    )
    success = crawler.run()

    if success:
        print("\n[SUCCESS] BestBuy Integrated Crawler completed successfully")
        exit(0)
    else:
        print("\n[FAILED] BestBuy Integrated Crawler failed")
        exit(1)


if __name__ == '__main__':
    main()