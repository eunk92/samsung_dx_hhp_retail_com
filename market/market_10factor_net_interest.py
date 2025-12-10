"""
Market 10대 인자 - 금융기관 순 이자수입 (Net Interest Income)

IMF FSI (Financial Soundness Indicators) - FSIBSIS 데이터셋
- NINTINC_USD: Net Interest Income (순 이자수입, USD)
- 부문: S12CFSI (Other Depository Corporations)

================================================================================
API 엔드포인트 (SDMX 3.0)
================================================================================
Base URL: https://api.imf.org/external/sdmx/3.0
Data: /data/dataflow/IMF.STA/FSIBSIS/+/{key}

Key 구조: COUNTRY.SECTOR.INDICATOR.FREQUENCY
- COUNTRY: USA, KOR 등
- SECTOR: S12CFSI
- INDICATOR: NINTINC_USD
- FREQUENCY: Q (분기), A (연간)

예시:
https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/FSIBSIS/+/USA.S12CFSI.NINTINC_USD.Q

================================================================================
"""

import os
import sys
import logging
import traceback
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================================
# 로깅 설정
# ============================================================================

logger = None


def setup_logger():
    """로거 설정"""
    global logger
    logger = logging.getLogger('market_net_interest')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)


def print_log(level, message):
    """로그 출력"""
    if logger:
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)
    else:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {message}")


# ============================================================================
# IMF SDMX 3.0 API 클라이언트
# ============================================================================

class IMFNetInterestClient:
    """IMF FSIBSIS SDMX 3.0 API 클라이언트 - 순 이자수입 데이터 조회"""

    BASE_URL = "https://api.imf.org/external/sdmx/3.0"
    DATAFLOW = "IMF.STA/FSIBSIS"
    SECTOR = "S12CFSI"
    INDICATOR = "NINTINC_USD"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0'
        })

    def get_data(self, country_code=None, frequency="Q", period=None):
        """
        순 이자수입 데이터 조회

        Args:
            country_code: 국가 코드 (예: 'USA', 'KOR', None/'all'=전체)
            frequency: 'Q' (분기) 또는 'A' (연간)
            period: 기간 (예: '2024-Q1', 'all', None)
                    - None 또는 'all': 전체 데이터
                    - '2024-Q1': 해당 기간만

        Returns:
            tuple: (request_url, response_json)
        """
        # Key 구조: COUNTRY.SECTOR.INDICATOR.FREQUENCY
        # 국가 미지정 또는 'all'이면 와일드카드(*) 사용
        if not country_code or country_code.lower() == 'all':
            country_key = '*'
        else:
            country_key = country_code.upper()

        key = f"{country_key}.{self.SECTOR}.{self.INDICATOR}.{frequency}"
        url = f"{self.BASE_URL}/data/dataflow/{self.DATAFLOW}/+/{key}"

        params = {
            'dimensionAtObservation': 'TIME_PERIOD',
            'attributes': 'dsd',
            'measures': 'all',
            'includeHistory': 'false'
        }

        # 기간 설정 (c 파라미터로 TIME_PERIOD 필터링)
        if period and period.lower() != 'all':
            params['c[TIME_PERIOD]'] = period
            print_log("INFO", f"기간 필터: {period}")

        print_log("INFO", f"API 요청: {url}")
        print_log("INFO", f"파라미터: {params}")

        try:
            resp = self.session.get(url, params=params, timeout=60)
            # 실제 요청 URL (파라미터 포함)
            request_url = resp.url
            print_log("INFO", f"응답 상태: {resp.status_code}")

            if resp.status_code == 200:
                return request_url, resp.json()
            else:
                print_log("WARNING", f"응답 코드: {resp.status_code}")
                print_log("WARNING", f"응답 내용: {resp.text[:500]}")
                return request_url, None

        except Exception as e:
            print_log("ERROR", f"API 요청 실패: {e}")
            traceback.print_exc()
            return url, None

    def parse_response(self, data):
        """
        JSON 응답 파싱

        Args:
            data: API JSON 응답

        Returns:
            list: [{'country_code': 'USA', 'sector': 'S12CFSI', 'indicator': 'NINTINC_USD',
                    'frequency': 'Q', 'period': '2024-Q1', 'value': 123456789}, ...]
        """
        if not data:
            return []

        results = []

        try:
            datasets = data.get('data', {}).get('dataSets', [])
            structures = data.get('data', {}).get('structures', [])

            if not datasets or not structures:
                print_log("WARNING", "데이터셋 또는 구조 정보 없음")
                return []

            # 구조 정보에서 차원 값들 추출
            country_codes = []
            sector_codes = []
            indicator_codes = []
            frequency_codes = []
            time_periods = []

            for struct in structures:
                # series dimensions에서 추출
                series_dims = struct.get('dimensions', {}).get('series', [])
                for dim in series_dims:
                    dim_id = dim.get('id')
                    values = dim.get('values', [])
                    if dim_id == 'COUNTRY':
                        country_codes = [v.get('id') for v in values]
                    elif dim_id == 'SECTOR':
                        sector_codes = [v.get('id') for v in values]
                    elif dim_id == 'INDICATOR':
                        indicator_codes = [v.get('id') for v in values]
                    elif dim_id == 'FREQ':
                        frequency_codes = [v.get('id') for v in values]

                # observation dimensions에서 시간 기간 추출
                obs_dims = struct.get('dimensions', {}).get('observation', [])
                for dim in obs_dims:
                    if dim.get('id') == 'TIME_PERIOD':
                        time_periods = [v.get('value') for v in dim.get('values', [])]
                        break

            # 관측값 추출
            for dataset in datasets:
                series = dataset.get('series', {})
                for series_key, series_data in series.items():
                    # series_key에서 인덱스 추출 (예: "0:0:0:0" -> country:sector:indicator:freq)
                    key_parts = series_key.split(':')
                    country_idx = int(key_parts[0]) if len(key_parts) > 0 else 0
                    sector_idx = int(key_parts[1]) if len(key_parts) > 1 else 0
                    indicator_idx = int(key_parts[2]) if len(key_parts) > 2 else 0
                    freq_idx = int(key_parts[3]) if len(key_parts) > 3 else 0

                    country_code = country_codes[country_idx] if country_idx < len(country_codes) else 'UNKNOWN'
                    sector = sector_codes[sector_idx] if sector_idx < len(sector_codes) else self.SECTOR
                    indicator = indicator_codes[indicator_idx] if indicator_idx < len(indicator_codes) else self.INDICATOR
                    frequency = frequency_codes[freq_idx] if freq_idx < len(frequency_codes) else 'Q'

                    observations = series_data.get('observations', {})
                    for idx_str, value_list in observations.items():
                        idx = int(idx_str)
                        if idx < len(time_periods) and value_list:
                            results.append({
                                'country_code': country_code,
                                'sector': sector,
                                'indicator': indicator,
                                'frequency': frequency,
                                'period': time_periods[idx],
                                'value': float(value_list[0])
                            })

            # 국가, 기간 순 정렬
            results.sort(key=lambda x: (x['country_code'], x['period']))
            print_log("INFO", f"파싱 완료: {len(results)}건")

        except Exception as e:
            print_log("ERROR", f"파싱 오류: {e}")
            traceback.print_exc()

        return results

    def get_net_interest_income(self, country_code=None, frequency="Q", period=None):
        """
        순 이자수입 데이터 조회 및 파싱

        Args:
            country_code: 국가 코드 (None/'all'=전체)
            frequency: 'Q' (분기) 또는 'A' (연간)
            period: 기간 (예: '2024-Q1', 'all', None)

        Returns:
            tuple: (request_url, response_json, results)
        """
        request_url, response_json = self.get_data(country_code, frequency, period)
        results = self.parse_response(response_json)

        return request_url, response_json, results


# ============================================================================
# DB 저장
# ============================================================================

def save_to_db(results, batch_id, table_name='market_net_interest'):
    """DB 저장 (period + country_code 중복 시 skip)"""
    if not results:
        print_log("WARNING", "저장할 데이터 없음")
        return False

    try:
        import psycopg2
        from config import DB_CONFIG

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        created_at = datetime.now()

        inserted = 0
        skipped = 0

        for row in results:
            # 중복 체크
            cursor.execute(f"""
                SELECT 1 FROM {table_name}
                WHERE period = %s AND country_code = %s
            """, (row['period'], row['country_code']))

            if cursor.fetchone():
                skipped += 1
                continue

            # INSERT
            cursor.execute(f"""
                INSERT INTO {table_name}
                    (period, country_code, sector, indicator, frequency, net_interest_income, batch_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['period'],
                row['country_code'],
                row.get('sector'),
                row.get('indicator'),
                row.get('frequency'),
                row['value'],
                batch_id,
                created_at
            ))
            inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        print_log("INFO", f"DB 저장 완료 ({table_name}): INSERT {inserted}건, SKIP {skipped}건")
        return True

    except Exception as e:
        print_log("ERROR", f"DB 저장 실패: {e}")
        traceback.print_exc()
        return False


def save_api_request(api_name, batch_id, request_url, response_json):
    """API 요청/응답 저장"""
    try:
        import psycopg2
        import json
        from config import DB_CONFIG

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO market_10factor_api_request
                (api_name, batch_id, request_url, response_json, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            api_name,
            batch_id,
            request_url,
            json.dumps(response_json) if response_json else None,
            datetime.now()
        ))

        conn.commit()
        cursor.close()
        conn.close()

        print_log("INFO", f"API 요청 저장 완료: {api_name}")
        return True

    except Exception as e:
        print_log("ERROR", f"API 요청 저장 실패: {e}")
        traceback.print_exc()
        return False


# ============================================================================
# 유틸리티 함수
# ============================================================================

def get_current_quarter():
    """현재 시점의 분기 반환 (예: 2025-Q4)"""
    now = datetime.now()
    quarter = (now.month - 1) // 3 + 1
    return f"{now.year}-Q{quarter}"


def input_with_timeout(prompt, timeout=10):
    """타임아웃 지원 입력"""
    print(f"{prompt}: ", end='', flush=True)

    value = ''
    start_time = time.time()
    while time.time() - start_time < timeout:
        if msvcrt.kbhit():
            char = msvcrt.getwch()
            if char == '\r':
                print()
                break
            elif char == '\b':  # 백스페이스
                if value:
                    value = value[:-1]
                    print('\b \b', end='', flush=True)
            else:
                value += char
                print(char, end='', flush=True)
        time.sleep(0.1)
    else:
        print("\n시간 초과")
        return None

    return value.strip() if value.strip() else None


# ============================================================================
# 메인 실행
# ============================================================================

def dry_run():
    """드라이 모드 - API 응답 확인"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 순 이자수입 [DRY RUN]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")
    print("데이터: IMF FSIBSIS (Financial Soundness Indicators)")
    print("지표: NINTINC_USD (Net Interest Income, USD)")
    print("부문: S12CFSI (Other Depository Corporations)")
    print()

    # 국가 선택
    print("  all 또는 엔터: 전체 국가")
    print("  USA, KOR 등: 특정 국가")
    country = input_with_timeout("국가 입력 (예: USA, all)", timeout=30)
    if not country:
        country = 'USA'
    print(f"선택된 국가: {country}")

    # 기간 선택
    period = input_with_timeout("기간 입력 (예: 2024-Q1, all)", timeout=30)
    if not period:
        period = 'all'
    print(f"선택된 기간: {period}")

    client = IMFNetInterestClient()
    print("\n조회 중...")
    _, _, results = client.get_net_interest_income(country, 'Q', period)

    if results:
        print(f"\n조회 결과: {len(results)}건")
        print("-" * 50)
        for row in results:
            print(f"  [{row['country_code']}] {row['period']}: {row['value']}")
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[DRY RUN] 완료 - DB 저장 없음")
    print("=" * 60)


def test_mode():
    """테스트 모드 - DB 저장"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 순 이자수입 [TEST MODE]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")
    print()

    # 국가 선택
    print("  all 또는 엔터: 전체 국가")
    print("  USA, KOR 등: 특정 국가")
    country = input_with_timeout("국가 입력 (예: USA, all)", timeout=30)
    if not country:
        country = 'USA'
    print(f"선택된 국가: {country}")

    # 기간 선택
    period = input_with_timeout("기간 입력 (예: 2024-Q1, all)", timeout=30)
    if not period:
        period = 'all'
    print(f"선택된 기간: {period}")

    client = IMFNetInterestClient()
    print("\n조회 중...")
    request_url, response_json, results = client.get_net_interest_income(country, 'Q', period)

    # API 요청/응답 저장
    save_api_request('net_interest_income', batch_id, request_url, response_json)

    if results:
        print(f"\n조회 결과: {len(results)}건")
        save_to_db(results, batch_id, table_name='test_market_net_interest')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print(f"[TEST MODE] 완료 - {len(results) if results else 0}건 저장")
    print("=" * 60)


def main(country=None, period=None):
    """운영 모드"""
    setup_logger()
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 순 이자수입 [운영 모드]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 국가 설정 (기본값: 전체)
    if not country:
        country = 'all'
    print(f"국가: {country.upper() if country.lower() != 'all' else '전체'}")

    # 기간 설정 (기본값: 현재 분기)
    if not period:
        period = get_current_quarter()
        print(f"기간: {period} (현재 분기)")
    elif period.lower() == 'all':
        print("기간: 전체")
    else:
        print(f"기간: {period}")
    print()

    client = IMFNetInterestClient()
    print("조회 중...")
    request_url, response_json, results = client.get_net_interest_income(country, 'Q', period)

    # API 요청/응답 저장
    save_api_request('net_interest_income', batch_id, request_url, response_json)

    if results:
        print(f"\n조회 결과: {len(results)}건")
        print("-" * 50)
        for row in results:
            print(f"  [{row['country_code']}] {row['period']}: {row['value']}")
        save_to_db(results, batch_id, table_name='market_net_interest')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print(f"[운영 모드] 완료 - {len(results) if results else 0}건 저장")
    print("=" * 60)


if __name__ == "__main__":
    import msvcrt
    import time

    print("\n[모드 선택] (10초 후 자동으로 운영모드 실행)")
    print("  d: DRY RUN (API 응답 확인)")
    print("  t: TEST MODE (test_market_net_interest)")
    print("  엔터: 운영 모드 (market_net_interest)")
    print()

    mode = ''
    print("모드 선택 (10초 대기): ", end='', flush=True)
    start_time = time.time()
    while time.time() - start_time < 10:
        if msvcrt.kbhit():
            char = msvcrt.getwch()
            if char == '\r':
                print()
                break
            mode = char.lower()
            print(char)
            break
        time.sleep(0.1)
    else:
        print("\n시간 초과 - 운영 모드 자동 실행")

    try:
        if mode == 'd':
            dry_run()
            input("\n엔터키를 누르면 종료합니다...")
        elif mode == 't':
            test_mode()
            input("\n엔터키를 누르면 종료합니다...")
        else:
            # 운영 모드: 국가/기간 입력 (타임아웃 시 전체 국가, 현재 분기)
            print("\n[국가 선택] (10초 후 전체 국가로 자동 실행)")
            print("  all 또는 엔터: 전체 국가")
            print("  USA, KOR 등: 특정 국가")
            country = input_with_timeout("국가 입력 (예: USA, all)", timeout=10)

            print("\n[기간 선택] (10초 후 현재 분기로 자동 실행)")
            print("  all: 전체 데이터")
            print("  2024-Q1: 특정 분기")
            print("  엔터: 현재 분기")
            period = input_with_timeout("기간 입력 (예: 2024-Q1, all)", timeout=10)

            main(country, period)
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        traceback.print_exc()
