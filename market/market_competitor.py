"""
Market Competitor Analyzer - OpenAI 기반 경쟁제품 분석

OpenAI API를 활용한 경쟁사 제품 분석 크롤러
키워드 기반 경쟁제품 매칭 및 분석 결과 DB 저장

================================================================================
실행 모드
================================================================================
- 운영 모드: 10초 내 입력 없으면 자동 실행 (market_competitor 테이블에 저장)
- 테스트 모드: 't' 입력 시 실행 (test_market_competitor 테이블에 저장)

================================================================================
필요 패키지
================================================================================
pip install openai psycopg2-binary

================================================================================
"""

import os
import sys
import time
import json
import traceback
import logging
import glob
import psycopg2
import msvcrt
from datetime import datetime
from openai import OpenAI

# 상위 디렉토리의 config.py 참조
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, OPENAI_API_KEY

# ============================================================================
# 로그 설정
# ============================================================================

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(ROOT_DIR, 'logs')
LOG_FILE = None
logger = None


def setup_logger():
    """로거 설정 (파일 + 콘솔 출력)"""
    global LOG_FILE, logger

    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    LOG_FILE = os.path.join(LOG_DIR, f'market_competitor_{timestamp}.log')

    logger = logging.getLogger('market_competitor')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)

    return LOG_FILE


def cleanup_old_logs(days=30):
    """오래된 로그 파일 정리"""
    try:
        log_pattern = os.path.join(LOG_DIR, 'market_competitor_*.log')
        log_files = glob.glob(log_pattern)
        now = datetime.now()

        for log_file in log_files:
            file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
            age_days = (now - file_mtime).days

            if age_days > days:
                os.remove(log_file)
                print_log("INFO", f"오래된 로그 삭제: {os.path.basename(log_file)} ({age_days}일 전)")
    except Exception as e:
        print_log("WARNING", f"로그 정리 실패: {e}")


def get_timestamp():
    """현재 시간 반환"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def print_log(level, message):
    """로그 출력"""
    if logger:
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)
    else:
        timestamp = get_timestamp()
        print(f"[{timestamp}] [{level}] {message}")


def get_input_with_timeout(prompt, timeout=10):
    """타임아웃이 있는 입력 받기 (Windows용)"""
    sys.stdout.write(prompt)
    sys.stdout.flush()

    start_time = time.time()
    input_chars = []

    while True:
        elapsed = time.time() - start_time
        remaining = timeout - elapsed

        if remaining <= 0:
            print()
            return None

        if msvcrt.kbhit():
            char = msvcrt.getwch()
            if char == '\r':
                print()
                return ''.join(input_chars)
            elif char == '\b':
                if input_chars:
                    input_chars.pop()
                    print('\b \b', end='', flush=True)
            else:
                input_chars.append(char)
                print(char, end='', flush=True)

        time.sleep(0.1)


# ============================================================================
# 데이터베이스 클래스
# ============================================================================

class DatabaseManager:
    """데이터베이스 연결 및 쿼리 관리"""

    def __init__(self, test_mode=False):
        self.conn = None
        self.cursor = None
        self.test_mode = test_mode
        self.table_name = 'test_market_competitor' if test_mode else 'market_competitor'

    def connect(self):
        """DB 연결"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG, database='postgres')
            self.cursor = self.conn.cursor()
            print_log("INFO", f"DB 연결 완료 (테이블: {self.table_name})")
            return True
        except Exception as e:
            print_log("ERROR", f"DB 연결 실패: {e}")
            return False

    def disconnect(self):
        """DB 연결 해제"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print_log("INFO", "DB 연결 해제")

    def execute(self, query, params=None):
        """쿼리 실행"""
        try:
            self.cursor.execute(query, params)
            return True
        except Exception as e:
            print_log("ERROR", f"쿼리 실행 실패: {e}")
            return False

    def fetchall(self):
        """모든 결과 반환"""
        return self.cursor.fetchall()

    def fetchone(self):
        """단일 결과 반환"""
        return self.cursor.fetchone()

    def commit(self):
        """커밋"""
        self.conn.commit()

    def rollback(self):
        """롤백"""
        self.conn.rollback()

    def get_samsung_products(self, product_line=None, limit=None):
        """
        Samsung 제품 키워드 조회 (content_type = 'samsung')

        Returns:
            list: [(id, product_line, keyword), ...]
        """
        query = """
            SELECT id, product_line, keyword
            FROM market_mst
            WHERE is_active = true
              AND analysis_type = 'competitor'
              AND content_type = 'samsung'
        """
        params = []

        if product_line:
            query += " AND product_line = %s"
            params.append(product_line)

        query += " ORDER BY id"

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        self.execute(query, params if params else None)
        return self.fetchall()

    def get_competitor_brands(self, product_line=None, limit=None):
        """
        경쟁사 브랜드 조회 (content_type = 'comp')

        Returns:
            list: [(id, product_line, keyword), ...]
        """
        query = """
            SELECT id, product_line, keyword
            FROM market_mst
            WHERE is_active = true
              AND analysis_type = 'competitor'
              AND content_type = 'comp'
        """
        params = []

        if product_line:
            query += " AND product_line = %s"
            params.append(product_line)

        query += " ORDER BY id"

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        self.execute(query, params if params else None)
        return self.fetchall()

    def save_analysis_result(self, samsung_product, comp_brand, comp_sku_name, expected_release, comment, calendar_week):
        """분석 결과 저장

        Args:
            samsung_product: 삼성 제품명 (samsung_series_name)
            comp_brand: 경쟁사 브랜드
            comp_sku_name: 경쟁사 제품명
            expected_release: 예상 출시일
            comment: 코멘트
            calendar_week: 캘린더 주차 (예: w49)
        """
        query = f"""
            INSERT INTO {self.table_name} (
                country, samsung_series_name, comp_brand, comp_series_name,
                expected_release, comment, calender_week
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        return self.execute(query, (
            'North America', samsung_product, comp_brand, comp_sku_name,
            expected_release, comment, calendar_week
        ))

    def save_batch_with_retry(self, results, calendar_week):
        """배치 단위로 저장 (20 → 5 → 1 재시도)"""
        insert_query = f"""
            INSERT INTO {self.table_name} (
                country, samsung_series_name, comp_brand, comp_series_name,
                expected_release, comment, calender_week, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        def result_to_tuple(r):
            return (
                'North America', r['samsung_product'], r['comp_brand'], r['comp_sku_name'],
                r.get('expected_release', ''), r.get('comment', ''), calendar_week,
                r.get('created_at')
            )

        total_success = 0
        total_fail = 0
        BATCH_SIZE = 20
        SUB_BATCH_SIZE = 5

        for batch_start in range(0, len(results), BATCH_SIZE):
            batch = results[batch_start:batch_start + BATCH_SIZE]

            # 1차: 20개 배치 저장
            try:
                values_list = [result_to_tuple(r) for r in batch]
                self.cursor.executemany(insert_query, values_list)
                self.commit()
                total_success += len(batch)
                continue
            except Exception:
                self.rollback()

            # 2차: 5개씩 분할 저장
            for sub_start in range(0, len(batch), SUB_BATCH_SIZE):
                sub_batch = batch[sub_start:sub_start + SUB_BATCH_SIZE]

                try:
                    values_list = [result_to_tuple(r) for r in sub_batch]
                    self.cursor.executemany(insert_query, values_list)
                    self.commit()
                    total_success += len(sub_batch)
                except Exception:
                    self.rollback()

                    # 3차: 1개씩 개별 저장
                    for result in sub_batch:
                        try:
                            self.cursor.execute(insert_query, result_to_tuple(result))
                            self.commit()
                            total_success += 1
                        except Exception as e:
                            print_log("ERROR", f"저장 실패: {result['samsung_product']} vs {result['comp_brand']}: {e}")
                            self.rollback()
                            total_fail += 1

        return total_success, total_fail


# ============================================================================
# OpenAI API 클래스
# ============================================================================

class OpenAIClient:
    """OpenAI API 클라이언트"""

    def __init__(self, api_key):
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o"  # 또는 "gpt-4-turbo", "gpt-3.5-turbo"

    def generate_prompt(self, category, samsung_product, competitor_brands):
        """프롬프트 생성 (Samsung 제품 1개 vs 경쟁사 N개)"""
        today_date = datetime.now().strftime('%Y-%m-%d')

        prompt = f"""Today is {today_date}. Do NOT assume any other date. Use only the provided date.

You are given:
• A list of our upcoming {category} products: {samsung_product}
• A list of competitor brands to analyze: {competitor_brands}

Rules you MUST follow:
1. Consider only products from the competitor brands provided in the input.
2. Include only upcoming, unreleased, leaked, rumored, expected, or officially announced future models in North America.
3. Exclude all products that are already released as of the provided "Today is {today_date}" date.
4. If the competitor product name is not known or cannot be verified, return:
   • "comp_sku_name": "info_not_available"
5. Do NOT invent or create new product names.
6. For each competitor model, label its release_status as:
   • "announced", "expected", "leaked", "rumored", "info_not_available"
7. Provide a short explanation ("comment") describing why this competitor model is relevant.
8. Present results in the following JSON structure:

{{
  "analysis_date": "{today_date}",
  "samsung_product": "{samsung_product}",
  "category": "{category}",
  "competitor_analysis": [
    {{
      "brand": "<competitor brand>",
      "comp_sku_name": "<competitor product name or info_not_available>",
      "release_status": "<announced|expected|leaked|rumored|info_not_available>",
      "expected_release": "<expected release date or quarter, e.g., Q1 2025>",
      "comment": "<short explanation of relevance>"
    }}
  ]
}}

If no competitor products are found, return:
{{
  "analysis_date": "{today_date}",
  "samsung_product": "{samsung_product}",
  "category": "{category}",
  "competitor_analysis": []
}}
"""
        return prompt

    def analyze(self, category, samsung_product, competitor_brands):
        """OpenAI API 호출하여 경쟁제품 분석 (Samsung 1개 vs 경쟁사 N개)"""
        prompt = self.generate_prompt(category, samsung_product, competitor_brands)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a market analyst specializing in consumer electronics. Provide accurate, factual information about competitor products. Always respond with valid JSON format."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )

            response_text = response.choices[0].message.content
            response_time = datetime.now()  # 응답 받은 시점

            # JSON 파싱 검증
            try:
                json.loads(response_text)
            except json.JSONDecodeError:
                print_log("WARNING", "응답이 유효한 JSON이 아닙니다. 원본 텍스트 저장")

            return {
                'success': True,
                'prompt': prompt,
                'response': response_text,
                'tokens_used': response.usage.total_tokens if response.usage else 0,
                'response_time': response_time
            }

        except Exception as e:
            print_log("ERROR", f"OpenAI API 호출 실패: {e}")
            return {
                'success': False,
                'prompt': prompt,
                'response': None,
                'error': str(e)
            }


# ============================================================================
# 경쟁제품 분석기
# ============================================================================

class CompetitorAnalyzer:
    """경쟁제품 분석기"""

    def __init__(self, test_mode=False, test_product_line=None, test_count=None, dry_run=False):
        self.test_mode = test_mode
        self.test_product_line = test_product_line
        self.test_count = test_count
        self.dry_run = dry_run
        self.db = DatabaseManager(test_mode=test_mode)
        self.openai = None

    def setup(self):
        """초기화"""
        if not self.db.connect():
            return False

        # OpenAI 클라이언트 초기화
        try:
            self.openai = OpenAIClient(OPENAI_API_KEY)
            print_log("INFO", "OpenAI 클라이언트 초기화 완료")
        except Exception as e:
            print_log("ERROR", f"OpenAI 클라이언트 초기화 실패: {e}")
            return False

        return True

    def cleanup(self):
        """정리"""
        self.db.disconnect()

    def generate_calendar_week(self):
        """캘린더 주차 생성 (예: w48)"""
        now = datetime.now()
        week_number = now.isocalendar()[1]
        return f"w{week_number}"

    def analyze_single(self, product_line, samsung_product, competitor_brands_list):
        """단일 분석 (Samsung 제품 1개 vs 경쟁사 N개)

        Args:
            product_line: 제품 라인 (TV/HHP)
            samsung_product: 삼성 제품 키워드
            competitor_brands_list: 경쟁사 브랜드 리스트 ['LG', 'Sony', 'TCL']

        Returns:
            list: 각 경쟁사별 결과 리스트
        """
        # 경쟁사 브랜드를 콤마로 연결
        competitor_brands_str = ', '.join(competitor_brands_list)

        try:
            print_log("INFO", f"분석 중: {samsung_product} vs [{competitor_brands_str}]")

            result = self.openai.analyze(product_line, samsung_product, competitor_brands_str)

            if result['success']:
                print_log("INFO", f"  -> 분석 완료 (토큰: {result['tokens_used']})")

                # OpenAI 응답에서 competitor_analysis 배열 추출
                results = []
                try:
                    response_data = json.loads(result['response'])
                    competitor_analysis = response_data.get('competitor_analysis', [])

                    for comp in competitor_analysis:
                        results.append({
                            'samsung_product': samsung_product,
                            'comp_brand': comp.get('brand', 'unknown'),
                            'comp_sku_name': comp.get('comp_sku_name', 'info_not_available'),
                            'expected_release': comp.get('expected_release', ''),
                            'comment': comp.get('comment', ''),
                            'product_line': product_line,
                            'response_json': result['response'],
                            'success': True,
                            'created_at': result.get('response_time')
                        })

                except (json.JSONDecodeError, TypeError) as e:
                    print_log("WARNING", f"JSON 파싱 실패: {e}")
                    # 파싱 실패 시 각 브랜드별로 info_not_available 반환
                    for brand in competitor_brands_list:
                        results.append({
                            'samsung_product': samsung_product,
                            'comp_brand': brand,
                            'comp_sku_name': 'info_not_available',
                            'expected_release': '',
                            'comment': '',
                            'product_line': product_line,
                            'response_json': result['response'],
                            'success': True,
                            'created_at': result.get('response_time')
                        })

                return results, result['response']

            else:
                print_log("WARNING", f"  -> 분석 실패: {result.get('error', 'Unknown error')}")
                # 실패 시 각 브랜드별로 info_not_available 반환
                results = []
                for brand in competitor_brands_list:
                    results.append({
                        'samsung_product': samsung_product,
                        'comp_brand': brand,
                        'comp_sku_name': 'info_not_available',
                        'expected_release': '',
                        'comment': '',
                        'product_line': product_line,
                        'response_json': None,
                        'success': False
                    })
                return results, None

        except Exception as e:
            print_log("ERROR", f"분석 실패 ({samsung_product}): {e}")
            traceback.print_exc()
            results = []
            for brand in competitor_brands_list:
                results.append({
                    'samsung_product': samsung_product,
                    'comp_brand': brand,
                    'comp_sku_name': 'info_not_available',
                    'expected_release': '',
                    'comment': '',
                    'product_line': product_line,
                    'response_json': None,
                    'success': False
                })
            return results, None

    def analyze_all_products(self, samsung_products, competitor_brands, calendar_week, dry_run=False):
        """모든 Samsung 제품 분석 (카테고리별 → 제품별)

        Args:
            samsung_products: [(id, product_line, keyword), ...]
            competitor_brands: [(id, product_line, keyword), ...]
            calendar_week: 캘린더 주차
            dry_run: True이면 DB 저장 없이 OpenAI 응답만 로그에 출력
        """
        BATCH_SIZE = 10
        total_success = 0
        total_fail = 0
        analysis_results = []

        # 카테고리별 Samsung 제품 그룹화
        samsung_by_category = {}
        for item in samsung_products:
            _, pl, keyword = item
            if pl not in samsung_by_category:
                samsung_by_category[pl] = []
            samsung_by_category[pl].append(keyword)

        # 카테고리별 경쟁사 브랜드 그룹화
        comp_by_category = {}
        for _, pl, keyword in competitor_brands:
            if pl not in comp_by_category:
                comp_by_category[pl] = []
            comp_by_category[pl].append(keyword)

        # 고정 카테고리 목록
        CATEGORIES = ['TV', 'HHP']
        total_products = len(samsung_products)
        current_idx = 0

        # 카테고리별로 처리
        for category in CATEGORIES:
            samsung_list = samsung_by_category.get(category, [])
            comp_brands = comp_by_category.get(category, [])

            if not samsung_list:
                print_log("INFO", f"[{category}] Samsung 제품 없음, 스킵")
                continue

            if not comp_brands:
                print_log("WARNING", f"[{category}] 경쟁사 브랜드 없음, 스킵")
                current_idx += len(samsung_list)
                continue

            print_log("INFO", f"\n{'='*60}")
            print_log("INFO", f"[{category}] 분석 시작 - Samsung {len(samsung_list)}개 vs 경쟁사 {len(comp_brands)}개")
            print_log("INFO", f"[{category}] 경쟁사: {', '.join(comp_brands)}")
            print_log("INFO", f"{'='*60}")

            # 해당 카테고리의 Samsung 제품별로 분석
            for samsung_keyword in samsung_list:
                current_idx += 1
                print(f"\n[{current_idx}/{total_products}] ", end="")

                # Samsung 1개 vs 경쟁사 N개 분석
                results, response_json = self.analyze_single(category, samsung_keyword, comp_brands)

                # 결과 처리
                success_count = sum(1 for r in results if r['success'])
                fail_count = len(results) - success_count

                if dry_run:
                    print_log("INFO", f"=" * 50)
                    print_log("INFO", f"[DRY RUN] {samsung_keyword} vs [{', '.join(comp_brands)}]")
                    for r in results:
                        print_log("INFO", f"[DRY RUN] {r['comp_brand']}: {r['comp_sku_name']}")
                    print_log("INFO", f"[DRY RUN] OpenAI 응답:")
                    print_log("INFO", response_json)
                    print_log("INFO", f"=" * 50)
                    total_success += success_count
                else:
                    # 성공한 결과만 저장 대상에 추가
                    for r in results:
                        if r['success']:
                            analysis_results.append(r)

                total_fail += fail_count

                # dry_run이 아닐 때만 배치 저장
                if not dry_run and len(analysis_results) >= BATCH_SIZE:
                    success, fail = self.save_batch(analysis_results, calendar_week)
                    total_success += success
                    total_fail += fail
                    analysis_results = []

                # API 요청 간격 (Rate limit 방지)
                time.sleep(1)

        # dry_run이 아닐 때만 남은 결과 저장
        if not dry_run and analysis_results:
            success, fail = self.save_batch(analysis_results, calendar_week)
            total_success += success
            total_fail += fail

        return total_success, total_fail

    def save_batch(self, results, calendar_week):
        """배치 저장"""
        print_log("INFO", f"배치 저장 ({len(results)}건)")
        return self.db.save_batch_with_retry(results, calendar_week)

    def print_summary(self, success_count, fail_count, total_count):
        """결과 요약 출력"""
        mode_str = "테스트 모드" if self.test_mode else "운영 모드"
        print("\n" + "=" * 60)
        print("분석 완료")
        print("=" * 60)
        print(f"모드: {mode_str}")
        print(f"테이블: {self.db.table_name}")
        print(f"성공: {success_count}건")
        print(f"실패: {fail_count}건")
        print(f"총계: {total_count}건")

    def run(self):
        """메인 실행"""
        log_file = setup_logger()
        cleanup_old_logs()

        mode_str = "테스트 모드" if self.test_mode else "운영 모드"
        dry_run_str = " [DRY RUN - DB 저장 안함]" if self.dry_run else ""

        print("\n" + "=" * 60)
        print(f"Market Competitor Analyzer ({mode_str}){dry_run_str}")
        print(f"저장 테이블: {self.db.table_name}")
        if self.dry_run:
            print("*** DRY RUN 모드: OpenAI 응답만 로그에 출력, DB 저장 안함 ***")
        print(f"로그 파일: {log_file}")
        print("=" * 60)

        try:
            if not self.setup():
                return

            # Samsung 제품 조회 (content_type = 'samsung')
            samsung_products = self.db.get_samsung_products(
                product_line=self.test_product_line,
                limit=self.test_count if self.test_mode else None
            )

            # 경쟁사 브랜드 조회 (content_type = 'comp')
            competitor_brands = self.db.get_competitor_brands(
                product_line=self.test_product_line
            )

            # 필터 정보 출력
            if self.test_mode:
                filter_info = []
                if self.test_product_line:
                    filter_info.append(f"product_line={self.test_product_line}")
                if self.test_count:
                    filter_info.append(f"samsung_limit={self.test_count}")
                if filter_info:
                    print_log("INFO", f"테스트 필터: {', '.join(filter_info)}")

            if not samsung_products:
                print_log("INFO", "Samsung 제품이 없습니다. (content_type='samsung' 확인)")
                return

            if not competitor_brands:
                print_log("INFO", "경쟁사 브랜드가 없습니다. (content_type='comp' 확인)")
                return

            # 조합 수 계산 (같은 product_line만)
            total_combinations = sum(
                1 for _, pl, _ in samsung_products
                for _, cpl, _ in competitor_brands
                if pl == cpl
            )

            print_log("INFO", f"Samsung 제품: {len(samsung_products)}개")
            print_log("INFO", f"경쟁사 브랜드: {len(competitor_brands)}개")
            print_log("INFO", f"분석 조합 수: {total_combinations}개")

            calendar_week = self.generate_calendar_week()
            success, fail = self.analyze_all_products(
                samsung_products, competitor_brands, calendar_week, dry_run=self.dry_run
            )
            self.print_summary(success, fail, total_combinations)

        except Exception as e:
            print_log("ERROR", f"실행 오류: {e}")
            traceback.print_exc()

        finally:
            if self.test_mode:
                input("\n엔터키를 누르면 종료합니다...")
            self.cleanup()


# ============================================================================
# 메인
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Market Competitor Analyzer (OpenAI)")
    print("=" * 60)
    print("\n[모드 선택]")
    print("  - 't' 입력: 테스트 모드 (test_market_competitor 테이블)")
    print("  - 'd' 입력: DRY RUN 모드 (OpenAI 응답만 로그에 출력, DB 저장 안함)")
    print("  - 10초 내 입력 없음: 운영 모드 (market_competitor 테이블)")
    print()

    user_input = get_input_with_timeout("모드 선택 (t=테스트, d=DRY RUN, 10초 후 자동 운영모드): ", timeout=10)

    if user_input and user_input.lower().strip() == 'd':
        # DRY RUN 모드
        print_log("INFO", "DRY RUN 모드로 실행합니다. (DB 저장 안함)")

        print("\n[DRY RUN 필터 설정] (엔터: 전체)")
        test_product_line = input("  product_line (TV/HHP): ").strip() or None
        test_count_input = input("  test_count: ").strip()
        test_count = int(test_count_input) if test_count_input else None

        analyzer = CompetitorAnalyzer(
            test_mode=True,
            test_product_line=test_product_line,
            test_count=test_count,
            dry_run=True
        )
    elif user_input and user_input.lower().strip() == 't':
        test_mode = True
        print_log("INFO", "테스트 모드로 실행합니다.")

        print("\n[테스트 필터 설정] (엔터: 전체)")
        test_product_line = input("  product_line (TV/HHP): ").strip() or None
        test_count_input = input("  test_count: ").strip()
        test_count = int(test_count_input) if test_count_input else None

        analyzer = CompetitorAnalyzer(
            test_mode=test_mode,
            test_product_line=test_product_line,
            test_count=test_count
        )
    else:
        test_mode = False
        print_log("INFO", "운영 모드로 실행합니다.")
        analyzer = CompetitorAnalyzer(test_mode=test_mode)

    analyzer.run()
