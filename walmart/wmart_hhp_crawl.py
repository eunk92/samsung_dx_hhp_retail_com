"""
Walmart HHP 통합 크롤러 (운영용)

================================================================================
실행 흐름: Main → BSR → Detail
================================================================================
STEP 1. Main   - 검색 결과 페이지에서 제품 목록 수집 (최대 300개)
STEP 2. BSR    - Best Seller 페이지에서 제품 목록 수집 (최대 100개)
STEP 3. Detail - 수집된 모든 제품의 상세 페이지 크롤링

================================================================================
주요 특징
================================================================================
- 동일한 batch_id로 전체 파이프라인 실행
- 각 크롤러 실패 시에도 다음 단계 계속 진행
- --resume-from 옵션으로 특정 단계부터 재개 가능

================================================================================
사용법
================================================================================
# 처음부터 실행
python wmart_hhp_crawl.py

# 특정 단계부터 재시작
python wmart_hhp_crawl.py --resume-from detail --batch-id w_20250123_143045
python wmart_hhp_crawl.py --resume-from bsr --batch-id w_20250123_143045

================================================================================
저장 테이블
================================================================================
- Main/BSR     → wmart_hhp_product_list (제품 목록)
- Detail       → hhp_retail_com (상세 정보 + 리뷰)
"""

import sys
import os
import argparse
import traceback
from datetime import datetime

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.setup import setup_environment
setup_environment(__file__)

from walmart.wmart_hhp_main import WalmartMainCrawler
from walmart.wmart_hhp_bsr import WalmartBSRCrawler
from walmart.wmart_hhp_dt import WalmartDetailCrawler
from common.base_crawler import BaseCrawler


class WalmartIntegratedCrawler:
    """Walmart 통합 크롤러 (운영용)"""

    def __init__(self, resume_from=None, batch_id=None):
        """
        Args:
            resume_from: 재시작 단계 ('main'/'bsr'/'detail'/None)
            batch_id: 재시작 시 사용할 배치 ID
        """
        self.account_name = 'Walmart'
        self.batch_id = batch_id
        self.start_time = None
        self.end_time = None
        self.resume_from = resume_from
        self.base_crawler = BaseCrawler()

    def run(self):
        """통합 크롤러 실행. Returns: bool"""
        self.start_time = datetime.now()

        # batch_id 생성 또는 재사용
        if not self.batch_id:
            self.batch_id = self.base_crawler.generate_batch_id(self.account_name)

        # 로깅 시작 (콘솔 출력을 파일에도 저장)
        log_file = self.base_crawler.start_logging(self.account_name, self.batch_id)

        print("\n" + "="*60)
        print("Walmart Integrated Crawler (Production)")
        print("="*60)
        print(f"batch_id: {self.batch_id}")
        if log_file:
            print(f"log_file: {log_file}")
        if self.resume_from:
            print(f"resume_from: {self.resume_from}")

        try:
            crawl_results = {'main': None, 'bsr': None, 'detail': None}

            # STEP 1: Main
            if not self.resume_from or self.resume_from == 'main':
                print(f"\n[STEP 1/3] Main Crawler...")
                try:
                    crawl_results['main'] = WalmartMainCrawler(test_mode=False, batch_id=self.batch_id).run()
                except Exception as e:
                    print(f"[ERROR] Main: {e}")
                    traceback.print_exc()
                    crawl_results['main'] = False
            else:
                crawl_results['main'] = 'skipped'

            # STEP 2: BSR
            if not self.resume_from or self.resume_from in ['main', 'bsr']:
                print(f"\n[STEP 2/3] BSR Crawler...")
                try:
                    crawl_results['bsr'] = WalmartBSRCrawler(test_mode=False, batch_id=self.batch_id).run()
                except Exception as e:
                    print(f"[ERROR] BSR: {e}")
                    traceback.print_exc()
                    crawl_results['bsr'] = False
            else:
                crawl_results['bsr'] = 'skipped'

            # STEP 3: Detail
            print(f"\n[STEP 3/3] Detail Crawler...")
            try:
                crawl_results['detail'] = WalmartDetailCrawler(batch_id=self.batch_id).run()
            except Exception as e:
                print(f"[ERROR] Detail: {e}")
                traceback.print_exc()
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
            print(f"\n[ERROR] Integrated crawler failed: {e}")
            traceback.print_exc()
            # 예외 발생 시에도 로깅 종료
            self.base_crawler.stop_logging()
            return False


def main():
    """운영용 통합 크롤러 진입점"""
    parser = argparse.ArgumentParser(description='Walmart HHP Integrated Crawler (Production)')
    parser.add_argument('--resume-from', type=str, choices=['main', 'bsr', 'detail'])
    parser.add_argument('--batch-id', type=str)
    args = parser.parse_args()

    if args.resume_from and not args.batch_id:
        print("[ERROR] --batch-id is required when using --resume-from")
        exit(1)

    crawler = WalmartIntegratedCrawler(resume_from=args.resume_from, batch_id=args.batch_id)
    success = crawler.run()
    exit(0 if success else 1)


if __name__ == '__main__':
    main()
