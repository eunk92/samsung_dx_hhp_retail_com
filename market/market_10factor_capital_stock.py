"""
Market 10대 인자 - 자본스톡 (Capital Stock)

IMF Investment and Capital Stock Dataset (ICSD) 데이터 수집

================================================================================
데이터 소스
================================================================================
- IMF ICSD (Investment and Capital Stock Dataset)
- 지표: CAPSTCK_PS_V_XDC (Capital stock, private sector, current prices, domestic currency)

================================================================================
API 엔드포인트 (SDMX 3.0)
================================================================================
IMF SDMX 3.0 REST API:
- Base URL: https://api.imf.org/external/sdmx/3.0
- 데이터 조회: /data/{context}/{agencyID}/{resourceID}/{version}/{key}

참고: https://portal.api.imf.org/apis#tags=iData

================================================================================
필요 패키지
================================================================================
pip install requests

================================================================================
"""

import os
import sys
import logging
import traceback
import requests
from datetime import datetime

# 상위 디렉토리의 config.py 참조
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================================
# 설정
# ============================================================================

# 로그 설정
logger = None


def setup_logger():
    """로거 설정 (콘솔 출력만)"""
    global logger

    logger = logging.getLogger('market_capital_stock')
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
# IMF SDMX 3.0 REST API 클라이언트
# ============================================================================

class IMFCapitalStockClient:
    """IMF ICSD SDMX 3.0 REST API 클라이언트 - 자본스톡 데이터 조회

    IMF SDMX 3.0 API:
    https://api.imf.org/external/sdmx/3.0
    """

    BASE_URL = "https://api.imf.org/external/sdmx/3.0"
    DATAFLOW = "IMF.FAD/ICSD"  # Investment and Capital Stock Dataset

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_data(self, key="*", start_year=None, end_year=None):
        """
        ICSD 데이터 조회 (SDMX 3.0 REST API)

        Args:
            key: 데이터 키 (예: 'USA', 'USA+KOR', '*'=전체)
            start_year: 시작 연도 (예: 2015)
            end_year: 종료 연도 (예: 2020)

        Returns:
            tuple: (request_url, response_json)
        """
        try:
            # SDMX 3.0 엔드포인트: /data/dataflow/{agencyID}/{resourceID}/+/{key}
            base_url = f"{self.BASE_URL}/data/dataflow/{self.DATAFLOW}/+/{key}"

            # 쿼리 파라미터 (이미 인코딩된 형태로 직접 구성)
            query_parts = [
                'dimensionAtObservation=TIME_PERIOD',
                'attributes=dsd',
                'measures=all',
                'includeHistory=false'
            ]

            # 기간 필터링: c[TIME_PERIOD] 파라미터
            # URL 인코딩: [ → %5B, ] → %5D, + → %2B
            if start_year and end_year:
                query_parts.append(f'c%5BTIME_PERIOD%5D=ge:{start_year}-01%2Ble:{end_year}-12')

            url = f"{base_url}?{'&'.join(query_parts)}"
            print_log("INFO", f"데이터 요청: {url}")

            resp = self.session.get(url, timeout=120)
            request_url = resp.url
            print_log("INFO", f"응답 상태: {resp.status_code}")

            if resp.status_code == 200:
                return request_url, resp.json()
            else:
                print_log("WARNING", f"응답 코드: {resp.status_code}")
                print_log("WARNING", f"응답 내용: {resp.text[:1000]}")
                return request_url, None

        except Exception as e:
            print_log("ERROR", f"데이터 조회 실패: {e}")
            traceback.print_exc()
            return url, None

    INDICATOR = "CAPSTCK_PS_V_XDC"  # 민간 자본스톡 (현재 가격, 자국 통화)
    FREQUENCY = "A"  # 연간

    def get_capital_stock_data(self, country_codes, start_year=None, end_year=None):
        """
        자본스톡 데이터 조회

        Args:
            country_codes: 국가 코드 리스트 또는 문자열 (예: ['USA', 'KOR'] 또는 'USA' 또는 'all')
            start_year: 시작 연도
            end_year: 종료 연도

        Returns:
            tuple: (request_url, response_json, data_list)

        Note:
            indicator: CAPSTCK_PS_V_XDC (민간 자본스톡, 현재 가격, 자국 통화)
        """
        try:
            # 국가 코드 처리
            if not country_codes or country_codes == 'all':
                country_key = '*'
            elif isinstance(country_codes, list):
                country_key = '+'.join(country_codes)
            else:
                country_key = country_codes

            # 키 구성: 국가.지표.빈도
            key = f"{country_key}.{self.INDICATOR}.{self.FREQUENCY}"

            print_log("INFO", f"자본스톡 조회: key={key}")

            # JSON 형식으로 데이터 요청
            request_url, response_json = self.get_data(
                key=key,
                start_year=start_year,
                end_year=end_year
            )

            if not response_json:
                return request_url, response_json, None

            # JSON 응답 파싱
            results = self._parse_json_response(response_json)

            if not results:
                print_log("WARNING", "파싱된 데이터 없음")
                return request_url, response_json, None

            print_log("INFO", f"데이터 {len(results)}건 조회 완료")

            # 데이터 변환 (리스트 형태)
            data_list = []
            for row in results:
                data_list.append({
                    'year': int(row['year']),
                    'country_code': row['country'],
                    'indicator': self.INDICATOR,
                    'frequency': self.FREQUENCY,
                    'capital_stock': row['value']
                })

            # 정렬 (country_code, year)
            data_list.sort(key=lambda x: (x['country_code'], x['year']))

            print_log("INFO", f"데이터 변환 완료: {len(data_list)}건")
            return request_url, response_json, data_list

        except Exception as e:
            print_log("ERROR", f"자본스톡 조회 실패: {e}")
            traceback.print_exc()
            return None, None, None

    def _parse_json_response(self, data):
        """JSON 응답 파싱 (SDMX 3.0)"""
        results = []
        try:
            datasets = data.get('data', {}).get('dataSets', [])
            structures = data.get('data', {}).get('structures', [])

            if not datasets or not structures:
                print_log("WARNING", "데이터셋 또는 구조 정보 없음")
                return []

            # 구조 정보에서 차원 값들 추출
            country_codes = []
            time_periods = []

            for struct in structures:
                # series dimensions에서 국가 코드 추출
                series_dims = struct.get('dimensions', {}).get('series', [])
                for dim in series_dims:
                    if dim.get('id') in ('COUNTRY', 'REF_AREA'):
                        country_codes = [v.get('id') for v in dim.get('values', [])]

                # observation dimensions에서 시간 기간 추출
                obs_dims = struct.get('dimensions', {}).get('observation', [])
                for dim in obs_dims:
                    if dim.get('id') == 'TIME_PERIOD':
                        time_periods = [v.get('value') for v in dim.get('values', [])]
                        break

            print_log("DEBUG", f"국가 수: {len(country_codes)}, 기간 수: {len(time_periods)}")

            # 관측값 추출
            for dataset in datasets:
                series = dataset.get('series', {})
                for series_key, series_data in series.items():
                    # series_key에서 국가 인덱스 추출
                    key_parts = series_key.split(':')
                    country_idx = int(key_parts[0]) if key_parts else 0
                    country = country_codes[country_idx] if country_idx < len(country_codes) else 'UNKNOWN'

                    # 관측값 처리
                    observations = series_data.get('observations', {})
                    for idx_str, value_list in observations.items():
                        idx = int(idx_str)
                        if idx < len(time_periods) and value_list:
                            results.append({
                                'country': country,
                                'year': time_periods[idx],
                                'value': float(value_list[0])
                            })

            print_log("DEBUG", f"파싱 결과: {len(results)}건")

        except Exception as e:
            print_log("ERROR", f"JSON 파싱 오류: {e}")
            traceback.print_exc()

        return results

    def extract_unit_info(self, data):
        """API 응답에서 단위 정보 추출"""
        unit_info = {}
        try:
            structures = data.get('data', {}).get('structures', [])

            for struct in structures:
                # attributes에서 UNIT_MEASURE, UNIT_MULT 등 추출
                attrs = struct.get('attributes', {})

                # series attributes
                series_attrs = attrs.get('series', [])
                for attr in series_attrs:
                    attr_id = attr.get('id')
                    values = attr.get('values', [])
                    if attr_id == 'UNIT_MEASURE' and values:
                        unit_info['unit_measure'] = values[0].get('name', values[0].get('id', ''))
                    elif attr_id == 'UNIT_MULT' and values:
                        unit_info['unit_mult'] = values[0].get('name', values[0].get('id', ''))

                # observation attributes
                obs_attrs = attrs.get('observation', [])
                for attr in obs_attrs:
                    attr_id = attr.get('id')
                    values = attr.get('values', [])
                    if attr_id == 'UNIT_MEASURE' and values:
                        unit_info['unit_measure'] = values[0].get('name', values[0].get('id', ''))
                    elif attr_id == 'UNIT_MULT' and values:
                        unit_info['unit_mult'] = values[0].get('name', values[0].get('id', ''))

                # dimensions에서 indicator 이름 추출
                series_dims = struct.get('dimensions', {}).get('series', [])
                for dim in series_dims:
                    if dim.get('id') == 'INDICATOR':
                        values = dim.get('values', [])
                        if values:
                            unit_info['indicator_name'] = values[0].get('name', '')

        except Exception as e:
            print_log("WARNING", f"단위 정보 추출 실패: {e}")

        return unit_info


# ============================================================================
# API 요청 저장
# ============================================================================

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
# 메인 실행
# ============================================================================

def input_with_timeout(prompt, timeout=10):
    """타임아웃 지원 입력 (Windows/Linux 호환)"""
    import sys
    import time

    if sys.platform == 'win32':
        # Windows
        import msvcrt
        print(f"{prompt}: ", end='', flush=True)

        value = ''
        start_time = time.time()
        while time.time() - start_time < timeout:
            if msvcrt.kbhit():
                char = msvcrt.getwch()
                if char == '\r':
                    print()
                    break
                elif char == '\b':
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
    else:
        # Linux/Unix
        import select
        print(f"{prompt} ({timeout}초 대기): ", end='', flush=True)

        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if ready:
            value = sys.stdin.readline().strip()
            return value if value else None
        else:
            print("\n시간 초과")
            return None


def get_previous_year():
    """전년도 반환"""
    return datetime.now().year - 1


def parse_year_input(year_input):
    """연도 입력 파싱

    Args:
        year_input: 연도 입력값
            - None/빈값: 전년도
            - 'all': 전체 연도
            - '2023': 단일 연도
            - '2015-2020': 범위

    Returns:
        tuple: (start_year, end_year, display_text)
    """
    if not year_input:
        prev_year = get_previous_year()
        return prev_year, prev_year, f"{prev_year}년 (전년도)"

    if year_input.lower() == 'all':
        return None, None, "전체 연도"

    if '-' in year_input:
        # 범위: 2015-2020
        parts = year_input.split('-')
        start_year = int(parts[0].strip())
        end_year = int(parts[1].strip())
        return start_year, end_year, f"{start_year}~{end_year}년"

    # 단일 연도
    year = int(year_input)
    return year, year, f"{year}년"


def parse_country_input(country_input):
    """국가 입력 파싱

    Args:
        country_input: 국가 입력값
            - None/빈값: 전체 국가
            - 'USA': 단일 국가
            - 'USA,KOR,JPN': 여러 국가 (쉼표 구분)

    Returns:
        tuple: (country_codes, display_text)
            - country_codes: 'all' 또는 리스트
    """
    if not country_input:
        return 'all', "전체 국가"

    # 쉼표로 구분된 국가 코드
    codes = [c.strip().upper() for c in country_input.split(',')]
    codes = [c for c in codes if c]  # 빈 값 제거

    if not codes:
        return 'all', "전체 국가"

    if len(codes) == 1:
        return codes, codes[0]

    return codes, ','.join(codes)


def dry_run(year=None, country=None):
    """드라이 모드 - API 응답값 확인 - DB 저장 없음"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [DRY RUN]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    print(f"대상: {country_text}, {year_text}")
    print()

    client = IMFCapitalStockClient()

    print_log("INFO", f"자본스톡 데이터 조회...")

    _, response_json, data_list = client.get_capital_stock_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year
    )

    if data_list and len(data_list) > 0:
        # 지표 정보 출력 (API 응답에서 추출)
        print("\n[지표 정보]")
        print(f"  지표: {client.INDICATOR}")
        print(f"  설명: Capital stock, private sector, current prices, domestic currency")

        # API 응답에서 단위 정보 추출
        if response_json:
            unit_info = client.extract_unit_info(response_json)
            unit_measure = unit_info.get('unit_measure', 'Domestic currency')
            unit_mult = unit_info.get('unit_mult', '')
            print(f"  단위: {unit_measure}")
            if unit_mult:
                print(f"  스케일: {unit_mult}")
        else:
            print(f"  단위: Domestic currency (자국 통화)")

        print(f"  빈도: Annual (연간)")

        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 70)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'indicator':>20}  {'capital_stock':>20}")
        print("-" * 70)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['indicator']:>20}  {row['capital_stock']:>20,}")
    else:
        print("\n데이터 없음")

    # DRY RUN: API 요청 저장 안함
    print("\n" + "=" * 60)
    print("[DRY RUN] 완료 - DB 저장 없음")
    print("=" * 60)

    return data_list


def test_mode(year=None, country=None):
    """테스트 모드 - DB 저장 (test_market_capital_stock)"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [TEST MODE]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    print(f"대상: {country_text}, {year_text}")
    print()

    client = IMFCapitalStockClient()

    print_log("INFO", f"자본스톡 데이터 조회...")

    request_url, response_json, data_list = client.get_capital_stock_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year
    )

    # API 요청/응답 저장
    save_api_request('capital_stock', batch_id, request_url, response_json)

    if data_list and len(data_list) > 0:
        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 70)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'indicator':>20}  {'capital_stock':>20}")
        print("-" * 70)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['indicator']:>20}  {row['capital_stock']:>20,}")

        # DB 저장 (테스트 테이블)
        save_to_db(data_list, batch_id, table_name='test_market_capital_stock')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[TEST MODE] 완료")
    print("=" * 60)

    return data_list


def main(year=None, country=None):
    """운영 모드 - DB 저장 (market_capital_stock)"""
    setup_logger()
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [운영 모드]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    print(f"대상: {country_text}, {year_text}")
    print()

    client = IMFCapitalStockClient()

    print_log("INFO", f"자본스톡 데이터 조회...")

    request_url, response_json, data_list = client.get_capital_stock_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year
    )

    # API 요청/응답 저장
    save_api_request('capital_stock', batch_id, request_url, response_json)

    if data_list and len(data_list) > 0:
        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 70)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'indicator':>20}  {'capital_stock':>20}")
        print("-" * 70)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['indicator']:>20}  {row['capital_stock']:>20,}")

        # DB 저장 (운영 테이블)
        save_to_db(data_list, batch_id, table_name='market_capital_stock')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[운영 모드] 완료")
    print("=" * 60)

    return data_list


def save_to_db(data_list, batch_id, table_name='market_capital_stock'):
    """데이터 리스트를 DB에 저장

    Args:
        data_list: 저장할 데이터 리스트
        batch_id: 배치 ID
        table_name: 테이블명 (기본값: market_capital_stock)

    테이블 컬럼: year, country_code, indicator, frequency, capital_stock, batch_id, created_at

    Note:
        year + country_code + indicator 기준 중복 체크, 존재하면 SKIP
    """
    try:
        import psycopg2
        from config import DB_CONFIG

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 실행 서버 시간 (API 응답 수신 시점)
        created_at = datetime.now()

        insert_count = 0
        skip_count = 0

        for row in data_list:
            year_val = row['year']
            country_code = row['country_code']
            indicator = row['indicator']

            # 중복 체크 (year + country_code + indicator)
            cursor.execute(f"""
                SELECT 1 FROM {table_name}
                WHERE year = %s AND country_code = %s AND indicator = %s
                LIMIT 1
            """, (year_val, country_code, indicator))

            if cursor.fetchone():
                skip_count += 1
                continue

            # INSERT
            query = f"""
                INSERT INTO {table_name}
                    (year, country_code, indicator, frequency, capital_stock, batch_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(query, (
                year_val,
                country_code,
                indicator,
                row['frequency'],
                row['capital_stock'],
                batch_id,
                created_at
            ))
            insert_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        print_log("INFO", f"DB 저장 완료 ({table_name}): INSERT {insert_count}건, SKIP {skip_count}건")
        return True

    except Exception as e:
        print_log("ERROR", f"DB 저장 실패: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import time

    def select_mode_with_timeout(timeout=10):
        """모드 선택 (타임아웃 지원, Windows/Linux 호환)"""
        if sys.platform == 'win32':
            import msvcrt
            print("모드 선택 (10초 대기): ", end='', flush=True)
            start_time = time.time()
            while time.time() - start_time < timeout:
                if msvcrt.kbhit():
                    char = msvcrt.getwch()
                    if char == '\r':
                        print()
                        return ''
                    print(char)
                    return char.lower()
                time.sleep(0.1)
            print("\n시간 초과 - 운영 모드 자동 실행")
            return ''
        else:
            import select
            print(f"모드 선택 ({timeout}초 대기): ", end='', flush=True)
            ready, _, _ = select.select([sys.stdin], [], [], timeout)
            if ready:
                value = sys.stdin.readline().strip().lower()
                return value if value else ''
            else:
                print("\n시간 초과 - 운영 모드 자동 실행")
                return ''

    default_year = get_previous_year()

    print("\n[모드 선택] (10초 후 자동으로 운영모드 실행)")
    print("  d: DRY RUN (API 응답 확인, DB 저장 없음)")
    print("  t: TEST MODE (test_market_capital_stock)")
    print("  엔터: 운영 모드 (전체 국가, market_capital_stock)")
    print()

    # 10초 타임아웃으로 입력 대기
    mode = select_mode_with_timeout(10)

    try:
        if mode == 'd':
            # 국가 선택
            print(f"\n[국가 선택] (10초 후 전체 국가)")
            print(f"  USA: 단일 국가")
            print(f"  USA,KOR,JPN: 여러 국가 (쉼표 구분)")
            print(f"  엔터: 전체 국가")
            country = input_with_timeout("국가 입력", timeout=10)
            # 연도 선택
            print(f"\n[연도 선택] (10초 후 {default_year}년 자동 실행)")
            print(f"  2023: 단일 연도 조회")
            print(f"  2015-2020: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: {default_year}년 (전년도)")
            year = input_with_timeout("연도 입력", timeout=10)
            dry_run(year, country)
            input("\n엔터키를 누르면 종료합니다...")
        elif mode == 't':
            # 국가 선택
            print(f"\n[국가 선택] (10초 후 전체 국가)")
            print(f"  USA: 단일 국가")
            print(f"  USA,KOR,JPN: 여러 국가 (쉼표 구분)")
            print(f"  엔터: 전체 국가")
            country = input_with_timeout("국가 입력", timeout=10)
            # 연도 선택
            print(f"\n[연도 선택] (10초 후 {default_year}년 자동 실행)")
            print(f"  2023: 단일 연도 조회")
            print(f"  2015-2020: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: {default_year}년 (전년도)")
            year = input_with_timeout("연도 입력", timeout=10)
            test_mode(year, country)
            input("\n엔터키를 누르면 종료합니다...")
        else:
            # 국가 선택
            print(f"\n[국가 선택] (10초 후 전체 국가)")
            print(f"  USA: 단일 국가")
            print(f"  USA,KOR,JPN: 여러 국가 (쉼표 구분)")
            print(f"  엔터: 전체 국가")
            country = input_with_timeout("국가 입력", timeout=10)
            # 연도 선택
            print(f"\n[연도 선택] (10초 후 {default_year}년 자동 실행)")
            print(f"  2023: 단일 연도 조회")
            print(f"  2015-2020: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: {default_year}년 (전년도)")
            year = input_with_timeout("연도 입력", timeout=10)
            results = main(year, country)
            # 운영모드는 자동 종료 (input 없음)
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        import traceback
        traceback.print_exc()
