"""
Market 10대 인자 - 금융기관 순 이자수입 (Net Interest Income)

================================================================================
데이터셋 정보
================================================================================
Dataset name: Financial Soundness Indicators (FSI), Balance Sheet, Income Statement and Memorandum Series
ID: FSIBSIS
Agency: IMF.STA
Version: 18.0.0

- NINTINC_XDC: Net Interest Income (순 이자수입, Domestic Currency)
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
- INDICATOR: NINTINC_XDC, NINTINC_USD
- FREQUENCY: Q (분기), A (연간)

예시 (두 지표 동시 조회, +로 연결):
https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/FSIBSIS/+/USA.S12CFSI.NINTINC_XDC+NINTINC_USD.Q

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


def setup_logger(log_file=None):
    """로거 설정

    Args:
        log_file: 로그 파일명 (None이면 콘솔만 출력)

    Returns:
        str: 로그 파일 경로 (파일 로깅 시)
    """
    global logger
    logger = logging.getLogger('market_net_interest')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러 (log_file이 지정된 경우)
    if log_file:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, log_file)

        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return log_path

    return None


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
    INDICATORS = ["NINTINC_XDC", "NINTINC_USD"]  # Domestic Currency, USD

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0'
        })

    def get_data(self, country_code=None, frequency="Q", period=None):
        """
        순 이자수입 데이터 조회 (NINTINC_XDC, NINTINC_USD 동시 조회)

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

        # 여러 지표를 +로 연결하여 한 번에 조회 (URL path에서 +는 %2B로 인코딩)
        indicator_key = '%2B'.join(self.INDICATORS)

        key = f"{country_key}.{self.SECTOR}.{indicator_key}.{frequency}"
        url = f"{self.BASE_URL}/data/dataflow/{self.DATAFLOW}/+/{key}"

        params = {
            'dimensionAtObservation': 'TIME_PERIOD',
            'attributes': 'dsd',
            'measures': 'all',
            'includeHistory': 'false'
        }

        # 기간 설정 (c 파라미터로 TIME_PERIOD 필터링)
        # 입력 형식: 2023 (연도) / 2023-Q1 (단일 분기) / 2023-Q1~2023-Q4 (범위)
        if period and period.lower() != 'all':
            if '~' in period:
                # 범위: 2023-Q1~2023-Q4 → ge:2023-Q1+le:2023-Q4
                start_period, end_period = period.split('~')
                params['c[TIME_PERIOD]'] = f'ge:{start_period.strip()}+le:{end_period.strip()}'
                print_log("INFO", f"기간 필터: {start_period.strip()} ~ {end_period.strip()}")
            elif period.isdigit() and len(period) == 4:
                # 연도만: 2023 → ge:2023-Q1+le:2023-Q4
                params['c[TIME_PERIOD]'] = f'ge:{period}-Q1+le:{period}-Q4'
                print_log("INFO", f"기간 필터: {period}-Q1 ~ {period}-Q4")
            else:
                # 단일 분기: 2023-Q1
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
                    elif dim_id in ('FREQ', 'FREQUENCY'):
                        frequency_codes = [v.get('id') for v in values]

                # observation dimensions에서 시간 기간 추출
                obs_dims = struct.get('dimensions', {}).get('observation', [])
                for dim in obs_dims:
                    if dim.get('id') == 'TIME_PERIOD':
                        time_periods = [v.get('value') for v in dim.get('values', [])]
                        break

            # 관측값 추출 (최적화: 리스트 컴프리헨션 + 캐싱)
            country_len = len(country_codes)
            sector_len = len(sector_codes)
            indicator_len = len(indicator_codes)
            freq_len = len(frequency_codes)
            period_len = len(time_periods)

            for dataset in datasets:
                series = dataset.get('series', {})
                for series_key, series_data in series.items():
                    # series_key에서 인덱스 추출
                    key_parts = series_key.split(':')
                    country_idx = int(key_parts[0]) if key_parts else 0
                    sector_idx = int(key_parts[1]) if len(key_parts) > 1 else 0
                    indicator_idx = int(key_parts[2]) if len(key_parts) > 2 else 0
                    freq_idx = int(key_parts[3]) if len(key_parts) > 3 else 0

                    # 시리즈 공통 값 캐싱
                    country_code = country_codes[country_idx] if country_idx < country_len else 'UNKNOWN'
                    sector = sector_codes[sector_idx] if sector_idx < sector_len else self.SECTOR
                    indicator = indicator_codes[indicator_idx] if indicator_idx < indicator_len else self.INDICATOR
                    frequency = frequency_codes[freq_idx] if freq_idx < freq_len else 'Q'

                    # 관측값 일괄 처리
                    observations = series_data.get('observations', {})
                    for idx_str, value_list in observations.items():
                        idx = int(idx_str)
                        if idx < period_len and value_list:
                            results.append({
                                'country_code': country_code,
                                'sector': sector,
                                'indicator': indicator,
                                'frequency': frequency,
                                'period': time_periods[idx],
                                'value': float(value_list[0])
                            })

            # 국가, indicator, 기간 순 정렬
            results.sort(key=lambda x: (x['country_code'], x['indicator'], x['period']))
            print_log("INFO", f"파싱 완료: {len(results)}건")

        except Exception as e:
            print_log("ERROR", f"파싱 오류: {e}")
            traceback.print_exc()

        return results

    def get_net_interest_income(self, country_code=None, frequency="Q", period=None):
        """
        순 이자수입 데이터 조회 및 파싱 (NINTINC_XDC, NINTINC_USD 동시 조회)

        Args:
            country_code: 국가 코드 (None/'all'=전체)
            frequency: 'Q' (분기) 또는 'A' (연간)
            period: 기간 (예: '2024-Q1', 'all', None)

        Returns:
            tuple: (request_url, response_json, results)
        """
        print_log("INFO", f"지표 조회: {', '.join(self.INDICATORS)}")
        request_url, response_json = self.get_data(country_code, frequency, period)
        results = self.parse_response(response_json)

        # 국가, indicator, 기간 순 정렬
        results.sort(key=lambda x: (x['country_code'], x['indicator'], x['period']))
        print_log("INFO", f"총 {len(results)}건 조회 완료")

        return request_url, response_json, results


# ============================================================================
# DB 저장
# ============================================================================

def save_to_db(results, batch_id, table_name='market_net_interest'):
    """DB 저장 (period + country_code + indicator 중복 시 skip)"""
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
            # 중복 체크 (period + country_code + indicator)
            cursor.execute(f"""
                SELECT 1 FROM {table_name}
                WHERE period = %s AND country_code = %s AND indicator = %s
            """, (row['period'], row['country_code'], row.get('indicator')))

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


def get_previous_quarters():
    """전전분기 ~ 전분기 기간 반환 (예: 2024-Q3~2024-Q4)"""
    now = datetime.now()
    year = now.year
    quarter = (now.month - 1) // 3 + 1

    # 전분기 계산
    prev_q = quarter - 1
    prev_year = year
    if prev_q <= 0:
        prev_q = 4
        prev_year = year - 1

    # 전전분기 계산
    prev_prev_q = prev_q - 1
    prev_prev_year = prev_year
    if prev_prev_q <= 0:
        prev_prev_q = 4
        prev_prev_year = prev_year - 1

    return f"{prev_prev_year}-Q{prev_prev_q}~{prev_year}-Q{prev_q}"


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

# 모드 설정
MODE_CONFIG = {
    'dry': {
        'name': 'DRY RUN',
        'batch_prefix': 't_',
        'table_name': None,  # DB 저장 안함
        'save_log': False,
        'save_api': False
    },
    'test': {
        'name': 'TEST MODE',
        'batch_prefix': 't_',
        'table_name': 'test_market_net_interest',
        'save_log': True,
        'save_api': True
    },
    'prod': {
        'name': '운영 모드',
        'batch_prefix': '',
        'table_name': 'market_net_interest',
        'save_log': True,
        'save_api': True
    }
}


def run(mode='prod'):
    """
    통합 실행 함수

    Args:
        mode: 'dry' (DRY RUN), 'test' (TEST MODE), 'prod' (운영 모드)
    """
    config = MODE_CONFIG.get(mode, MODE_CONFIG['prod'])

    # batch_id 생성
    batch_id = config['batch_prefix'] + datetime.now().strftime('%Y%m%d_%H%M%S')

    # 로거 설정
    log_path = None
    if config['save_log']:
        log_file = f"net_interest_{batch_id}.log"
        log_path = setup_logger(log_file)
    else:
        setup_logger()

    # 헤더 출력
    print("\n" + "=" * 60)
    print(f"Market 10대 인자 - 순 이자수입 [{config['name']}]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")
    if log_path:
        print(f"로그 파일: {log_path}")
    print()

    # 국가 선택
    default_period = get_previous_quarters()
    print("[국가 선택] (10초 후 전체 국가로 자동 실행)")
    print("  all 또는 엔터: 전체 국가")
    print("  USA, KOR 등: 특정 국가")
    country = input_with_timeout("국가 입력", timeout=10)
    if not country:
        country = 'all'
    print(f"선택된 국가: {country}")

    # 기간 선택
    print(f"\n[기간 선택] (10초 후 {default_period} 자동 실행)")
    print("  all: 전체 기간")
    print("  2024-Q1~2024-Q4: 범위 조회")
    print("  2024-Q1: 특정 분기")
    print(f"  엔터: {default_period}")
    period = input_with_timeout("기간 입력", timeout=10)
    if not period:
        period = default_period
    print(f"선택된 기간: {period}")

    # 지표 선택
    print("\n[지표 선택] (10초 후 전체 지표로 자동 실행)")
    print("  1: NINTINC_XDC (Domestic Currency)")
    print("  2: NINTINC_USD (USD)")
    print("  엔터: 전체 지표")
    indicator_choice = input_with_timeout("지표 입력 (1/2/엔터)", timeout=10)

    client = IMFNetInterestClient()
    if indicator_choice == '1':
        client.INDICATORS = ['NINTINC_XDC']
        print("선택된 지표: NINTINC_XDC")
    elif indicator_choice == '2':
        client.INDICATORS = ['NINTINC_USD']
        print("선택된 지표: NINTINC_USD")
    else:
        print(f"선택된 지표: {', '.join(client.INDICATORS)} (전체)")

    # API 조회
    print("\n조회 중...")
    request_url, response_json, results = client.get_net_interest_income(country, 'Q', period)

    # API 요청/응답 저장
    if config['save_api']:
        save_api_request('net_interest_income', batch_id, request_url, response_json)

    # 결과 출력 및 DB 저장
    if results:
        print_log("INFO", f"조회 결과: {len(results)}건")
        print_log("INFO", "-" * 50)
        for idx, row in enumerate(results, 1):
            print_log("INFO", f"  [{idx}/{len(results)}] [{row['country_code']}] [{row['indicator']}] {row['period']}: {row['value']}")

        if config['table_name']:
            save_to_db(results, batch_id, table_name=config['table_name'])
    else:
        print_log("WARNING", "데이터 없음")

    # 완료 메시지
    print_log("INFO", "=" * 60)
    if config['table_name']:
        print_log("INFO", f"[{config['name']}] 완료 - {len(results) if results else 0}건 저장")
    else:
        print_log("INFO", f"[{config['name']}] 완료 - DB 저장 없음")
    print_log("INFO", "=" * 60)


if __name__ == "__main__":
    import msvcrt
    import time

    print("\n[모드 선택] (10초 후 자동으로 운영모드 실행)")
    print("  d: DRY RUN (API 응답 확인)")
    print("  t: TEST MODE (test_market_net_interest)")
    print("  엔터: 운영 모드 (market_net_interest)")
    print()

    mode = ''
    print("모드 입력: ", end='', flush=True)
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
            run('dry')
        elif mode == 't':
            run('test')
        else:
            run('prod')

        input("\n엔터키를 누르면 종료합니다...")
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        traceback.print_exc()
