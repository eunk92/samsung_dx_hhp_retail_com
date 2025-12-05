"""
Market Promotion Analyzer - OpenAI 기반 프로모션 일정 분석

OpenAI API를 활용한 미국 리테일러 프로모션 일정 수집
9주 이내 쇼핑 이벤트의 프로모션 시작/종료일 분석

================================================================================
실행 모드
================================================================================
- 운영 모드: 10초 내 입력 없으면 자동 실행 (market_promotion 테이블에 저장)
- 테스트 모드: 't' 입력 시 실행 (test_market_promotion 테이블에 저장)
- DRY RUN 모드: 'd' 입력 시 실행 (OpenAI 응답만 로그에 출력, DB 저장 안함)

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
from datetime import datetime, timedelta

# 상위 디렉토리의 config.py 참조
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, OPENAI_API_KEY

# ============================================================================
# 상수 정의
# ============================================================================

RETAILERS = ['Amazon', 'Best Buy', 'Walmart', "Sam's Club", 'Home Depot', "Lowe's", 'Costco']

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
    LOG_FILE = os.path.join(LOG_DIR, f'market_promotion_{timestamp}.log')

    logger = logging.getLogger('market_promotion')
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
        log_pattern = os.path.join(LOG_DIR, 'market_promotion_*.log')
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
        self.table_name = 'test_market_promotion' if test_mode else 'market_promotion'

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

    def get_shopping_events(self, weeks=9):
        """
        9주 이내 쇼핑 이벤트 조회 (market_mst 테이블)

        Returns:
            list: [(id, event_name), ...]
        """
        query = """
            SELECT id, keyword
            FROM market_mst
            WHERE is_active = true
              AND analysis_type = 'promotion'
            ORDER BY id
        """
        self.execute(query)
        return self.fetchall()

    def save_batch_with_retry(self, results, calendar_week):
        """배치 단위로 저장 (20 → 5 → 1 재시도)"""
        insert_query = f"""
            INSERT INTO {self.table_name} (
                event_channel, event_name, event_start_date, event_end_date,
                source, calender_week, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        def result_to_tuple(r):
            return (
                r['event_channel'],
                r['event_name'],
                r.get('event_start_date'),
                r.get('event_end_date'),
                r.get('source', ''),
                calendar_week,
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
                            print_log("ERROR", f"저장 실패: {result['event_channel']} - {result['event_name']}: {e}")
                            self.rollback()
                            total_fail += 1

        return total_success, total_fail


# ============================================================================
# OpenAI API 클래스
# ============================================================================

class OpenAIClient:
    """OpenAI API 클라이언트"""

    def __init__(self, api_key):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o"

    def generate_prompt(self, event_name, analysis_date=None):
        """프로모션 일정 조회 프롬프트 생성"""
        target_date = analysis_date if analysis_date else datetime.now().strftime('%Y-%m-%d')

        prompt = f"""Today's date is {target_date}.

Please provide the UPCOMING promotion start and end dates {{YYYY-MM-DD}} for the following U.S. retailers:
Amazon, Best Buy, Walmart, Sam's Club, Home Depot, Lowe's, Costco
for the following U.S. shopping events in 9 weeks: {event_name}
Present the result in the form of a JSON with the following structure:
{{
  "event_name": "{event_name}",
  "analysis_date": "{target_date}",
  "promotions": [
    {{
      "retailer": "<retailer name>",
      "event": "{event_name}",
      "start_date": "<YYYY-MM-DD or 'Not available'>",
      "end_date": "<YYYY-MM-DD or 'Not available'>",
      "source": "<URL source or 'Not available'>"
    }}
  ]
}}
"""
        return prompt

    def analyze(self, event_name, analysis_date=None):
        """OpenAI API 호출하여 프로모션 일정 분석"""
        prompt = self.generate_prompt(event_name, analysis_date)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a retail market analyst specializing in U.S. retail promotions and shopping events. Provide accurate, factual information about retailer promotion schedules. Always respond with valid JSON format."
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
            response_time = datetime.now()

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
# 프로모션 분석기
# ============================================================================

class PromotionAnalyzer:
    """프로모션 일정 분석기"""

    def __init__(self, test_mode=False, dry_run=False, test_count=None, analysis_date=None):
        self.test_mode = test_mode
        self.dry_run = dry_run
        self.test_count = test_count
        self.analysis_date = analysis_date  # None이면 오늘 날짜 사용
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

    def analyze_event(self, event_name):
        """단일 이벤트 프로모션 분석

        Args:
            event_name: 쇼핑 이벤트명

        Returns:
            tuple: (results list, response_json)
        """
        try:
            print_log("INFO", f"분석 중: {event_name}")

            result = self.openai.analyze(event_name, self.analysis_date)

            if result['success']:
                print_log("INFO", f"  -> 분석 완료 (토큰: {result['tokens_used']})")

                results = []
                try:
                    response_data = json.loads(result['response'])
                    promotions = response_data.get('promotions', [])

                    for promo in promotions:
                        results.append({
                            'event_channel': promo.get('retailer', 'unknown'),
                            'event_name': event_name,
                            'event_start_date': promo.get('start_date'),
                            'event_end_date': promo.get('end_date'),
                            'source': promo.get('source', ''),
                            'success': True,
                            'created_at': result.get('response_time')
                        })

                except (json.JSONDecodeError, TypeError) as e:
                    print_log("WARNING", f"JSON 파싱 실패: {e}")
                    # 파싱 실패 시 각 리테일러별로 빈 결과 반환
                    for retailer in RETAILERS:
                        results.append({
                            'event_channel': retailer,
                            'event_name': event_name,
                            'event_start_date': None,
                            'event_end_date': None,
                            'source': '',
                            'success': False,
                            'created_at': result.get('response_time')
                        })

                return results, result['response']

            else:
                print_log("WARNING", f"  -> 분석 실패: {result.get('error', 'Unknown error')}")
                results = []
                for retailer in RETAILERS:
                    results.append({
                        'event_channel': retailer,
                        'event_name': event_name,
                        'event_start_date': None,
                        'event_end_date': None,
                        'source': '',
                        'success': False,
                        'created_at': None
                    })
                return results, None

        except Exception as e:
            print_log("ERROR", f"분석 실패 ({event_name}): {e}")
            traceback.print_exc()
            results = []
            for retailer in RETAILERS:
                results.append({
                    'event_channel': retailer,
                    'event_name': event_name,
                    'event_start_date': None,
                    'event_end_date': None,
                    'source': '',
                    'success': False,
                    'created_at': None
                })
            return results, None

    def analyze_all_events(self, events, calendar_week):
        """모든 이벤트 분석

        Args:
            events: [(id, event_name), ...]
            calendar_week: 캘린더 주차
        """
        total_success = 0
        total_fail = 0
        analysis_results = []

        total_events = len(events)

        for idx, (event_id, event_name) in enumerate(events, 1):
            print(f"\n[{idx}/{total_events}] ", end="")

            results, response_json = self.analyze_event(event_name)

            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count

            if self.dry_run:
                print_log("INFO", f"=" * 50)
                print_log("INFO", f"[DRY RUN] Event: {event_name}")
                for r in results:
                    print_log("INFO", f"[DRY RUN] {r['event_channel']}: {r['event_start_date']} ~ {r['event_end_date']}")
                print_log("INFO", f"[DRY RUN] OpenAI 응답:")
                print_log("INFO", response_json)
                print_log("INFO", f"=" * 50)
                total_success += success_count
            else:
                for r in results:
                    if r['success']:
                        analysis_results.append(r)

            total_fail += fail_count

            # API 요청 간격
            time.sleep(1)

        # dry_run이 아닐 때만 결과 저장
        if not self.dry_run and analysis_results:
            success, fail = self.db.save_batch_with_retry(analysis_results, calendar_week)
            total_success += success
            total_fail += fail

        return total_success, total_fail

    def print_summary(self, success_count, fail_count, total_count):
        """결과 요약 출력"""
        if self.dry_run:
            mode_str = "DRY RUN 모드"
        elif self.test_mode:
            mode_str = "테스트 모드"
        else:
            mode_str = "운영 모드"
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

        if self.dry_run:
            mode_str = "DRY RUN 모드"
        elif self.test_mode:
            mode_str = "테스트 모드"
        else:
            mode_str = "운영 모드"

        print("\n" + "=" * 60)
        print(f"Market Promotion Analyzer ({mode_str})")
        print(f"저장 테이블: {self.db.table_name}")
        if self.dry_run:
            print("*** DRY RUN 모드: OpenAI 응답만 로그에 출력, DB 저장 안함 ***")
        print(f"로그 파일: {log_file}")
        print("=" * 60)

        try:
            if not self.setup():
                return

            # 쇼핑 이벤트 조회
            events = self.db.get_shopping_events()

            if not events:
                print_log("INFO", "쇼핑 이벤트가 없습니다. (analysis_type='promotion' 확인)")
                return

            # test_count 적용
            if self.test_count and self.test_count < len(events):
                events = events[:self.test_count]
                print_log("INFO", f"테스트 모드: {self.test_count}개 이벤트만 조회")

            # 예상 결과 수 (이벤트 수 x 리테일러 수)
            total_combinations = len(events) * len(RETAILERS)

            print_log("INFO", f"쇼핑 이벤트: {len(events)}개")
            print_log("INFO", f"리테일러: {len(RETAILERS)}개 ({', '.join(RETAILERS)})")
            print_log("INFO", f"예상 결과 수: {total_combinations}개")

            calendar_week = self.generate_calendar_week()
            success, fail = self.analyze_all_events(events, calendar_week)
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
    print("Market Promotion Analyzer (OpenAI)")
    print("=" * 60)
    print("\n[모드 선택]")
    print("  - 't' 입력: 테스트 모드 (test_market_promotion 테이블)")
    print("  - 'd' 입력: DRY RUN 모드 (OpenAI 응답만 로그에 출력, DB 저장 안함)")
    print("  - 10초 내 입력 없음: 운영 모드 (market_promotion 테이블)")
    print()

    user_input = get_input_with_timeout("모드 선택 (t=테스트, d=DRY RUN, 10초 후 자동 운영모드): ", timeout=10)

    if user_input and user_input.lower().strip() == 'd':
        # DRY RUN 모드
        print_log("INFO", "DRY RUN 모드로 실행합니다. (DB 저장 안함)")

        print("\n[DRY RUN 필터 설정] (엔터: 기본값)")
        test_count_input = input("  조회할 이벤트 수: ").strip()
        test_count = int(test_count_input) if test_count_input else None
        analysis_date_input = input("  분석 날짜 (YYYY-MM-DD, 엔터=오늘): ").strip()
        analysis_date = analysis_date_input if analysis_date_input else None

        analyzer = PromotionAnalyzer(test_mode=True, dry_run=True, test_count=test_count, analysis_date=analysis_date)

    elif user_input and user_input.lower().strip() == 't':
        # 테스트 모드
        print_log("INFO", "테스트 모드로 실행합니다.")

        print("\n[테스트 필터 설정] (엔터: 기본값)")
        test_count_input = input("  조회할 이벤트 수: ").strip()
        test_count = int(test_count_input) if test_count_input else None
        analysis_date_input = input("  분석 날짜 (YYYY-MM-DD, 엔터=오늘): ").strip()
        analysis_date = analysis_date_input if analysis_date_input else None

        analyzer = PromotionAnalyzer(test_mode=True, dry_run=False, test_count=test_count, analysis_date=analysis_date)

    else:
        # 운영 모드
        print_log("INFO", "운영 모드로 실행합니다.")
        analyzer = PromotionAnalyzer(test_mode=False, dry_run=False)

    analyzer.run()