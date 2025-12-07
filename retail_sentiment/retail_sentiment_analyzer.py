"""
Sentiment Analyzer - OpenAI 기반 리뷰 감성 분석

OpenAI API를 활용한 제품 리뷰 데이터 감성 분석
TV: tv_retail_com / tv_item_mst 테이블
HHP: hhp_retail_com / hhp_item_mst 테이블

================================================================================
실행 모드
================================================================================
- 운영 모드: 10초 내 입력 없으면 자동 실행
- DRY RUN 모드: 'd' 입력 시 실행 (OpenAI 응답만 로그에 출력, DB 저장 안함)
- 테스트 모드: 't' 입력 시 실행 (테스트 테이블에 저장)

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
    LOG_FILE = os.path.join(LOG_DIR, f'sentiment_analyzer_{timestamp}.log')

    logger = logging.getLogger('sentiment_analyzer')
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
        log_pattern = os.path.join(LOG_DIR, 'sentiment_analyzer_*.log')
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

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        """DB 연결"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG, database='postgres')
            self.cursor = self.conn.cursor()
            print_log("INFO", "DB 연결 완료")
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


# ============================================================================
# OpenAI API 클래스
# ============================================================================

class OpenAIClient:
    """OpenAI API 클라이언트"""

    def __init__(self, api_key):
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o"

    def generate_prompt(self, product_data):
        """프롬프트 생성"""
        # 변수 맵핑
        retailer_sku_name = product_data.get('Retailer_SKU_Name', '')
        item = product_data.get('Item', '')
        detailed_review_content = product_data.get('detailed_review_content', '')
        top_mentions = product_data.get('top_mentions', '')
        recommendation_intent = product_data.get('recommendation_intent')
        star_ratings = product_data.get('star_ratings')
        count_of_star_ratings = product_data.get('count_of_star_ratings')
        bsr_rank = product_data.get('bsr_rank')

        prompt = f"""Analyze structured product review data from Retail.com and output a strict JSON object only (no extra text).

Each product (SKU) contains detailed reviews and ranking data.

You must:
- Infer the overall sentiment toward each product ({retailer_sku_name}, {item}).
- Use both textual signals ({detailed_review_content}, {top_mentions}) and quantitative indicators ({recommendation_intent}, {star_ratings}, {count_of_star_ratings}, {bsr_rank}).
- Base your sentiment primarily on review text polarity and intensity, but adjust for quantitative indicators (higher ratings, more reviews, better rank increase positivity).
- Sentiment scale: -5 (most negative), 0 (neutral), +5 (most positive).
- If information is missing, set the JSON field to null and explain in `"notes"`.
- Output **only valid JSON** (no markdown formatting, no additional commentary).
- Include short evidence snippets and a human-readable comment to justify the sentiment score.

----------------------------------------
OUTPUT JSON FORMAT (STRICT REQUIREMENT)

The response must be a JSON object with the following structure:

{{{{
  "product_name": "<Retailer_SKU_Name or Item>",
  "sentiment_score": <numeric between -5 and +5>,
  "confidence_level": "<high | medium | low>",
  "inputs_used": {{{{
    "star_rating": <numeric or null>,
    "review_count": <numeric or null>,
    "recommendation_intent": <numeric % or null>,
    "bsr_rank": <numeric or null>,
    "textual_sentiment_summary": "<short polarity interpretation>"
  }}}},
  "evidence": {{{{
    "top_positive_phrases": ["...", "..."],
    "top_negative_phrases": ["...", "..."],
    "representative_review_snippet": "..."
  }}}},
  "final_interpretation": "<1–2 sentence reasoning combining text and quantitative factors>",
  "notes": "<state missing data, ambiguity, confidence rationale, or assumptions>"
}}}}
"""
        return prompt

    def analyze(self, product_data):
        """OpenAI API 호출하여 감성 분석"""
        prompt = self.generate_prompt(product_data)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a sentiment analysis expert. Analyze product review data and provide accurate sentiment scores. Always respond with valid JSON format only."
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
# TV 감성 분석기
# ============================================================================

class TVSentimentAnalyzer:
    """TV 제품 리뷰 감성 분석기"""

    def __init__(self, limit=None, dry_run=False, target_date=None, test_mode=False):
        self.limit = limit
        self.dry_run = dry_run
        self.target_date = target_date
        self.test_mode = test_mode
        self.db = DatabaseManager()
        self.openai = None
        self.source_table = 'tv_retail_com'
        self.master_table = 'tv_item_mst'
        self.target_table = 'test_tv_retail_sentiment' if test_mode else 'tv_retail_sentiment'

    def setup(self):
        """초기화"""
        if not self.db.connect():
            return False

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

    def get_review_data(self):
        """TV 리뷰 데이터 조회"""
        if self.target_date:
            date_condition = f"DATE(r.crawl_datetime) = '{self.target_date}'"
            print_log("INFO", f"[TV] 조회 날짜: {self.target_date} (지정)")
        else:
            date_condition = "DATE(r.crawl_datetime) = CURRENT_DATE - INTERVAL '1 day'"
            print_log("INFO", "[TV] 조회 날짜: 어제 (기본값)")

        query = f"""
            WITH latest_data AS (
                SELECT
                    r.id,
                    r.retailer_sku_name,
                    m.sku,
                    r.detailed_review_content,
                    r.summarized_review_content,
                    r.recommendation_intent,
                    r.star_rating,
                    r.count_of_star_ratings,
                    r.bsr_rank,
                    r.item,
                    r.account_name,
                    r.crawl_datetime,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.item, r.account_name
                        ORDER BY r.crawl_datetime DESC
                    ) AS rn
                FROM {self.source_table} r
                INNER JOIN {self.master_table} m ON r.item = m.item AND r.account_name = m.account_name
                WHERE r.detailed_review_content IS NOT NULL
                  AND r.detailed_review_content != ''
                  AND m.sku IS NOT NULL
                  AND m.sku != ''
                  AND {date_condition}
            )
            SELECT
                id,
                retailer_sku_name,
                sku,
                detailed_review_content,
                summarized_review_content,
                recommendation_intent,
                star_rating,
                count_of_star_ratings,
                bsr_rank
            FROM latest_data
            WHERE rn = 1
            ORDER BY account_name, id
        """

        if self.limit:
            query += f" LIMIT {self.limit}"

        self.db.execute(query)
        return self.db.fetchall()

    def prepare_product_data(self, row):
        """DB 조회 결과를 분석용 딕셔너리로 변환"""
        return {
            'id': row[0],
            'Retailer_SKU_Name': row[1],
            'Item': row[2],
            'detailed_review_content': row[3],
            'top_mentions': row[4],
            'recommendation_intent': row[5],
            'star_ratings': row[6],
            'count_of_star_ratings': row[7],
            'bsr_rank': row[8]
        }

    def save_sentiment(self, retail_com_id, response_text):
        """감성 분석 결과 저장"""
        try:
            response_data = json.loads(response_text)
            sentiment_score = response_data.get('sentiment_score')
            final_interpretation = response_data.get('final_interpretation')

            query = f"""
                INSERT INTO {self.target_table} (retail_com_id, sentiment_score, final_interpretation)
                VALUES (%s, %s, %s)
            """
            self.db.execute(query, (retail_com_id, str(sentiment_score), final_interpretation))
            self.db.commit()
            print_log("INFO", f"  -> 저장 완료 (테이블: {self.target_table})")
        except Exception as e:
            print_log("ERROR", f"저장 실패: {e}")
            self.db.rollback()

    def analyze_single(self, product_data):
        """단일 제품 감성 분석"""
        try:
            sku_name = product_data.get('Retailer_SKU_Name', 'Unknown')
            print_log("INFO", f"분석 중: {sku_name[:50]}...")

            result = self.openai.analyze(product_data)

            if result['success']:
                print_log("INFO", f"  -> 분석 완료 (토큰: {result['tokens_used']})")
                return {
                    'success': True,
                    'sku_name': sku_name,
                    'response': result['response'],
                    'tokens_used': result['tokens_used'],
                    'response_time': result['response_time']
                }
            else:
                print_log("WARNING", f"  -> 분석 실패: {result.get('error', 'Unknown error')}")
                return {
                    'success': False,
                    'sku_name': sku_name,
                    'error': result.get('error')
                }

        except Exception as e:
            print_log("ERROR", f"분석 실패: {e}")
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def run(self):
        """메인 실행"""
        dry_run_str = " [DRY RUN]" if self.dry_run else ""
        test_mode_str = " [TEST MODE]" if self.test_mode else ""

        print_log("INFO", f"{'=' * 60}")
        print_log("INFO", f"[TV] Sentiment Analyzer 시작{dry_run_str}{test_mode_str}")
        print_log("INFO", f"[TV] 소스 테이블: {self.source_table}")
        print_log("INFO", f"[TV] 저장 테이블: {self.target_table}")
        print_log("INFO", f"{'=' * 60}")

        try:
            if not self.setup():
                return 0, 0

            review_data = self.get_review_data()

            if not review_data:
                print_log("INFO", "[TV] 분석할 리뷰 데이터가 없습니다.")
                return 0, 0

            print_log("INFO", f"[TV] 분석 대상 제품: {len(review_data)}개")

            total_success = 0
            total_fail = 0

            for idx, row in enumerate(review_data, 1):
                print_log("INFO", f"[TV] [{idx}/{len(review_data)}]")

                product_data = self.prepare_product_data(row)
                result = self.analyze_single(product_data)

                if result['success']:
                    if self.dry_run:
                        print_log("INFO", f"{'=' * 50}")
                        print_log("INFO", f"[DRY RUN] SKU: {result['sku_name']}")
                        print_log("INFO", f"[DRY RUN] 응답:")
                        print_log("INFO", result['response'])
                        print_log("INFO", f"{'=' * 50}")
                    else:
                        # 저장 로직
                        self.save_sentiment(product_data['id'], result['response'])
                    total_success += 1
                else:
                    total_fail += 1

                time.sleep(1)

            print_log("INFO", f"{'=' * 60}")
            print_log("INFO", f"[TV] 분석 완료 - 성공: {total_success}건, 실패: {total_fail}건")
            print_log("INFO", f"{'=' * 60}")

            return total_success, total_fail

        except Exception as e:
            print_log("ERROR", f"[TV] 실행 오류: {e}")
            traceback.print_exc()
            return 0, 0

        finally:
            self.cleanup()


# ============================================================================
# HHP 감성 분석기
# ============================================================================

class HHPSentimentAnalyzer:
    """HHP 제품 리뷰 감성 분석기"""

    def __init__(self, limit=None, dry_run=False, target_date=None, test_mode=False):
        self.limit = limit
        self.dry_run = dry_run
        self.target_date = target_date
        self.test_mode = test_mode
        self.db = DatabaseManager()
        self.openai = None
        self.source_table = 'hhp_retail_com'
        self.master_table = 'hhp_item_mst'
        self.target_table = 'test_hhp_retail_sentiment' if test_mode else 'hhp_retail_sentiment'

    def setup(self):
        """초기화"""
        if not self.db.connect():
            return False

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

    def get_review_data(self):
        """HHP 리뷰 데이터 조회"""
        if self.target_date:
            date_condition = f"DATE(r.crawl_strdatetime) = '{self.target_date}'"
            print_log("INFO", f"[HHP] 조회 날짜: {self.target_date} (지정)")
        else:
            date_condition = "DATE(r.crawl_strdatetime) = CURRENT_DATE - INTERVAL '1 day'"
            print_log("INFO", "[HHP] 조회 날짜: 어제 (기본값)")

        query = f"""
            WITH latest_data AS (
                SELECT
                    r.id,
                    r.retailer_sku_name,
                    m.sku,
                    r.detailed_review_content,
                    r.summarized_review_content,
                    r.recommendation_intent,
                    r.star_rating,
                    r.count_of_star_ratings,
                    r.bsr_rank,
                    r.item,
                    r.account_name,
                    r.crawl_strdatetime,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.item, r.account_name
                        ORDER BY r.crawl_strdatetime DESC
                    ) AS rn
                FROM {self.source_table} r
                INNER JOIN {self.master_table} m ON r.item = m.item AND r.account_name = m.account_name
                WHERE r.detailed_review_content IS NOT NULL
                  AND r.detailed_review_content != ''
                  AND m.sku IS NOT NULL
                  AND m.sku != ''
                  AND {date_condition}
            )
            SELECT
                id,
                retailer_sku_name,
                sku,
                detailed_review_content,
                summarized_review_content,
                recommendation_intent,
                star_rating,
                count_of_star_ratings,
                bsr_rank
            FROM latest_data
            WHERE rn = 1
            ORDER BY account_name, id
        """

        if self.limit:
            query += f" LIMIT {self.limit}"

        self.db.execute(query)
        return self.db.fetchall()

    def prepare_product_data(self, row):
        """DB 조회 결과를 분석용 딕셔너리로 변환"""
        return {
            'id': row[0],
            'Retailer_SKU_Name': row[1],
            'Item': row[2],
            'detailed_review_content': row[3],
            'top_mentions': row[4],
            'recommendation_intent': row[5],
            'star_ratings': row[6],
            'count_of_star_ratings': row[7],
            'bsr_rank': row[8]
        }

    def save_sentiment(self, retail_com_id, response_text):
        """감성 분석 결과 저장"""
        try:
            response_data = json.loads(response_text)
            sentiment_score = response_data.get('sentiment_score')
            final_interpretation = response_data.get('final_interpretation')

            query = f"""
                INSERT INTO {self.target_table} (retail_com_id, sentiment_score, final_interpretation)
                VALUES (%s, %s, %s)
            """
            self.db.execute(query, (retail_com_id, str(sentiment_score), final_interpretation))
            self.db.commit()
            print_log("INFO", f"  -> 저장 완료 (테이블: {self.target_table})")
        except Exception as e:
            print_log("ERROR", f"저장 실패: {e}")
            self.db.rollback()

    def analyze_single(self, product_data):
        """단일 제품 감성 분석"""
        try:
            sku_name = product_data.get('Retailer_SKU_Name', 'Unknown')
            print_log("INFO", f"분석 중: {sku_name[:50]}...")

            result = self.openai.analyze(product_data)

            if result['success']:
                print_log("INFO", f"  -> 분석 완료 (토큰: {result['tokens_used']})")
                return {
                    'success': True,
                    'sku_name': sku_name,
                    'response': result['response'],
                    'tokens_used': result['tokens_used'],
                    'response_time': result['response_time']
                }
            else:
                print_log("WARNING", f"  -> 분석 실패: {result.get('error', 'Unknown error')}")
                return {
                    'success': False,
                    'sku_name': sku_name,
                    'error': result.get('error')
                }

        except Exception as e:
            print_log("ERROR", f"분석 실패: {e}")
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def run(self):
        """메인 실행"""
        dry_run_str = " [DRY RUN]" if self.dry_run else ""
        test_mode_str = " [TEST MODE]" if self.test_mode else ""

        print_log("INFO", f"{'=' * 60}")
        print_log("INFO", f"[HHP] Sentiment Analyzer 시작{dry_run_str}{test_mode_str}")
        print_log("INFO", f"[HHP] 소스 테이블: {self.source_table}")
        print_log("INFO", f"[HHP] 저장 테이블: {self.target_table}")
        print_log("INFO", f"{'=' * 60}")

        try:
            if not self.setup():
                return 0, 0

            review_data = self.get_review_data()

            if not review_data:
                print_log("INFO", "[HHP] 분석할 리뷰 데이터가 없습니다.")
                return 0, 0

            print_log("INFO", f"[HHP] 분석 대상 제품: {len(review_data)}개")

            total_success = 0
            total_fail = 0

            for idx, row in enumerate(review_data, 1):
                print_log("INFO", f"[HHP] [{idx}/{len(review_data)}]")

                product_data = self.prepare_product_data(row)
                result = self.analyze_single(product_data)

                if result['success']:
                    if self.dry_run:
                        print_log("INFO", f"{'=' * 50}")
                        print_log("INFO", f"[DRY RUN] SKU: {result['sku_name']}")
                        print_log("INFO", f"[DRY RUN] 응답:")
                        print_log("INFO", result['response'])
                        print_log("INFO", f"{'=' * 50}")
                    else:
                        # 저장 로직
                        self.save_sentiment(product_data['id'], result['response'])
                    total_success += 1
                else:
                    total_fail += 1

                time.sleep(1)

            print_log("INFO", f"{'=' * 60}")
            print_log("INFO", f"[HHP] 분석 완료 - 성공: {total_success}건, 실패: {total_fail}건")
            print_log("INFO", f"{'=' * 60}")

            return total_success, total_fail

        except Exception as e:
            print_log("ERROR", f"[HHP] 실행 오류: {e}")
            traceback.print_exc()
            return 0, 0

        finally:
            self.cleanup()


# ============================================================================
# 메인
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Sentiment Analyzer (OpenAI)")
    print("=" * 60)
    print("\n[모드 선택]")
    print("  - 'd' 입력: DRY RUN 모드 (OpenAI 응답만 로그에 출력, DB 저장 안함)")
    print("  - 't' 입력: 테스트 모드 (테스트 테이블에 저장)")
    print("  - 10초 내 입력 없음: 운영 모드")
    print()

    user_input = get_input_with_timeout("모드 선택 (d=DRY RUN, t=테스트, 10초 후 자동 운영모드): ", timeout=10)

    if user_input and user_input.lower().strip() == 'd':
        # DRY RUN 모드
        log_file = setup_logger()
        cleanup_old_logs()

        print_log("INFO", "DRY RUN 모드로 실행합니다. (DB 저장 안함)")
        print(f"로그 파일: {log_file}")

        print("\n[DRY RUN 필터 설정]")
        target_date_input = input("  조회 날짜 (YYYY-MM-DD, 엔터: 어제): ").strip()
        target_date = target_date_input if target_date_input else None
        test_count_input = input("  test_count (엔터: 전체): ").strip()
        test_count = int(test_count_input) if test_count_input else None

        # TV 분석
        print("\n" + "=" * 60)
        print("[1/2] TV 감성 분석 시작")
        print("=" * 60)
        tv_analyzer = TVSentimentAnalyzer(
            limit=test_count,
            dry_run=True,
            target_date=target_date
        )
        tv_success, tv_fail = tv_analyzer.run()

        # HHP 분석
        print("\n" + "=" * 60)
        print("[2/2] HHP 감성 분석 시작")
        print("=" * 60)
        hhp_analyzer = HHPSentimentAnalyzer(
            limit=test_count,
            dry_run=True,
            target_date=target_date
        )
        hhp_success, hhp_fail = hhp_analyzer.run()

        # 최종 결과
        print("\n" + "=" * 60)
        print("전체 분석 완료")
        print("=" * 60)
        print(f"TV  - 성공: {tv_success}건, 실패: {tv_fail}건")
        print(f"HHP - 성공: {hhp_success}건, 실패: {hhp_fail}건")
        print(f"총계 - 성공: {tv_success + hhp_success}건, 실패: {tv_fail + hhp_fail}건")

        input("\n엔터키를 누르면 종료합니다...")

    elif user_input and user_input.lower().strip() == 't':
        # 테스트 모드
        log_file = setup_logger()
        cleanup_old_logs()

        print_log("INFO", "테스트 모드로 실행합니다. (테스트 테이블에 저장)")
        print(f"로그 파일: {log_file}")

        print("\n[테스트 필터 설정]")
        target_date_input = input("  조회 날짜 (YYYY-MM-DD, 엔터: 어제): ").strip()
        target_date = target_date_input if target_date_input else None
        test_count_input = input("  test_count (엔터: 전체): ").strip()
        test_count = int(test_count_input) if test_count_input else None

        # TV 분석
        print("\n" + "=" * 60)
        print("[1/2] TV 감성 분석 시작 (테스트)")
        print("=" * 60)
        tv_analyzer = TVSentimentAnalyzer(
            limit=test_count,
            dry_run=False,
            target_date=target_date,
            test_mode=True
        )
        tv_success, tv_fail = tv_analyzer.run()

        # HHP 분석
        print("\n" + "=" * 60)
        print("[2/2] HHP 감성 분석 시작 (테스트)")
        print("=" * 60)
        hhp_analyzer = HHPSentimentAnalyzer(
            limit=test_count,
            dry_run=False,
            target_date=target_date,
            test_mode=True
        )
        hhp_success, hhp_fail = hhp_analyzer.run()

        # 최종 결과
        print("\n" + "=" * 60)
        print("전체 분석 완료 (테스트)")
        print("=" * 60)
        print(f"TV  - 성공: {tv_success}건, 실패: {tv_fail}건")
        print(f"HHP - 성공: {hhp_success}건, 실패: {hhp_fail}건")
        print(f"총계 - 성공: {tv_success + hhp_success}건, 실패: {tv_fail + hhp_fail}건")

        input("\n엔터키를 누르면 종료합니다...")

    else:
        # 운영 모드
        log_file = setup_logger()
        cleanup_old_logs()

        print_log("INFO", "운영 모드로 실행합니다.")
        print(f"로그 파일: {log_file}")

        # TV 분석
        print("\n" + "=" * 60)
        print("[1/2] TV 감성 분석 시작")
        print("=" * 60)
        tv_analyzer = TVSentimentAnalyzer()
        tv_success, tv_fail = tv_analyzer.run()

        # HHP 분석
        print("\n" + "=" * 60)
        print("[2/2] HHP 감성 분석 시작")
        print("=" * 60)
        hhp_analyzer = HHPSentimentAnalyzer()
        hhp_success, hhp_fail = hhp_analyzer.run()

        # 최종 결과
        print("\n" + "=" * 60)
        print("전체 분석 완료")
        print("=" * 60)
        print(f"TV  - 성공: {tv_success}건, 실패: {tv_fail}건")
        print(f"HHP - 성공: {hhp_success}건, 실패: {hhp_fail}건")
        print(f"총계 - 성공: {tv_success + hhp_success}건, 실패: {tv_fail + hhp_fail}건")
