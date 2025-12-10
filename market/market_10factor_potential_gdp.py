"""
Market 10대 인자 - 잠재적 산출량 (Potential GDP)

OECD Economic Outlook Long-Term Baseline 데이터 수집

================================================================================
데이터 소스
================================================================================
- OECD Economic Outlook 117 (Long-Term Baseline)
- 지표: GDPVTRD (Potential gross domestic product, USD PPP)
- Measure: Potential gross domestic product, volume, USD at 2021 Purchasing Power Parities
- Scenario: All
    - Business-as-usual, median climate damage, no carbon mitigation (BAU1)
    - Business-as-usual, high damage curve, no carbon mitigation (BAU2)
    - Accelerated transition, median damage, slow carbon reduction (ET1)
    - Accelerated transition, median damage, fast carbon reduction (ET2)
    - Accelerated transition, high damage, slow carbon reduction (ET3)
    - Accelerated transition, high damage, fast carbon reduction (ET4)

================================================================================
API 엔드포인트 (SDMX)
================================================================================
- Base URL: https://sdmx.oecd.org/public/rest
- 데이터 조회: /data/{agencyID},{dataflowID},{version}/{key}
- 참고: https://data-explorer.oecd.org
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

    logger = logging.getLogger('market_potential_gdp')
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
# OECD SDMX REST API 클라이언트
# ============================================================================

class OECDPotentialGDPClient:
    """OECD Economic Outlook SDMX REST API 클라이언트 - 잠재적 산출량 데이터 조회

    OECD SDMX API:
    https://sdmx.oecd.org/public/rest
    """

    BASE_URL = "https://sdmx.oecd.org/public/rest/data"

    # 데이터플로우: Economic Outlook Long-Term Baseline
    # 형식: {Agency},{DataflowID},{Version}
    DEFAULT_DATAFLOW = "OECD.ECO.MAD,DSD_EO_LTB@DF_EO_LTB,1.0"

    INDICATOR = "GDPVTRD"  # Potential GDP (USD PPP)
    FREQUENCY = "A"  # 연간

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/vnd.sdmx.data+json; charset=utf-8; version=1.0',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_potential_gdp_data(self, country_codes=None, start_year=None, end_year=None, scenarios=None):
        """
        잠재적 산출량 데이터 조회

        Args:
            country_codes: 국가 코드 리스트 또는 문자열 (예: ['USA', 'KOR'] 또는 'USA' 또는 None=전체)
            start_year: 시작 연도
            end_year: 종료 연도
            scenarios: 시나리오 코드 리스트 (예: ['BAU1', 'ET1'] 또는 None=전체)

        Returns:
            tuple: (request_url, response_text, data_list)
        """
        try:
            # 국가 코드 처리
            if not country_codes or country_codes == 'all':
                country_key = ''  # 전체 국가
            elif isinstance(country_codes, list):
                country_key = '+'.join(country_codes)
            else:
                country_key = country_codes

            # 시나리오 처리
            if scenarios and isinstance(scenarios, list):
                scenario_key = '+'.join(scenarios)
            else:
                scenario_key = ''  # 전체 시나리오

            # 키 구성: 국가.지표.시나리오.빈도
            key = f"{country_key}.{self.INDICATOR}.{scenario_key}.{self.FREQUENCY}"

            # URL 구성
            url = f"{self.BASE_URL}/{self.DEFAULT_DATAFLOW}/{key}"

            # 쿼리 파라미터
            params = [
                'dimensionAtObservation=AllDimensions'
            ]
            if start_year:
                params.append(f'startPeriod={start_year}')
            if end_year:
                params.append(f'endPeriod={end_year}')

            url = f"{url}?{'&'.join(params)}"

            print_log("INFO", f"데이터 요청: {url}")

            resp = self.session.get(url, timeout=120)
            request_url = resp.url
            print_log("INFO", f"응답 상태: {resp.status_code}")

            if resp.status_code == 200:
                # JSON 응답 파싱
                data_list = self._parse_json_response(resp.json())
                print_log("INFO", f"데이터 {len(data_list)}건 조회 완료")
                return request_url, resp.json(), data_list
            else:
                print_log("WARNING", f"응답 코드: {resp.status_code}")
                print_log("WARNING", f"응답 내용: {resp.text[:1000]}")
                return request_url, None, None

        except Exception as e:
            print_log("ERROR", f"데이터 조회 실패: {e}")
            traceback.print_exc()
            return None, None, None

    def _parse_json_response(self, data):
        """JSON 응답 파싱 (SDMX-JSON)"""
        results = []
        try:
            # SDMX-JSON v2: data.dataSets, data.structures
            data_obj = data.get('data', data)  # 'data' 키가 있으면 사용, 없으면 root
            datasets = data_obj.get('dataSets', [])
            structures = data_obj.get('structures', [{}])[0] if data_obj.get('structures') else data_obj.get('structure', {})

            if not datasets:
                print_log("WARNING", "dataSets not found in response")
                return []

            # 차원 정보 추출
            dimensions = structures.get('dimensions', {}).get('observation', [])

            # 차원 인덱스 매핑
            dim_map = {}  # {dimension_id: {index: value}}
            dim_positions = {}  # {dimension_id: position_in_key}

            for pos, dim in enumerate(dimensions):
                dim_id = dim.get('id')
                dim_positions[dim_id] = pos
                dim_map[dim_id] = {}
                for idx, val in enumerate(dim.get('values', [])):
                    dim_map[dim_id][idx] = val.get('id')

            # observations 파싱
            for dataset in datasets:
                observations = dataset.get('observations', {})

                for obs_key, obs_values in observations.items():
                    # obs_key: "0:0:0:0:0" 형식
                    key_parts = [int(k) for k in obs_key.split(':')]

                    # 각 차원값 추출
                    ref_area = dim_map.get('REF_AREA', {}).get(key_parts[dim_positions.get('REF_AREA', 0)], '')
                    scenario = dim_map.get('SCENARIO', {}).get(key_parts[dim_positions.get('SCENARIO', 2)], '')
                    time_period = dim_map.get('TIME_PERIOD', {}).get(key_parts[dim_positions.get('TIME_PERIOD', 4)], '')

                    # 값 추출 (첫 번째 요소가 실제 값)
                    value = obs_values[0] if obs_values else None

                    if ref_area and time_period and value is not None:
                        results.append({
                            'year': int(time_period),
                            'country_code': ref_area,
                            'indicator': self.INDICATOR,
                            'frequency': self.FREQUENCY,
                            'scenario': scenario,
                            'potential_gdp': value
                        })

            # 정렬 (country_code, scenario, year)
            results.sort(key=lambda x: (x['country_code'], x['scenario'], x['year']))

            print_log("DEBUG", f"파싱 결과: {len(results)}건")

        except Exception as e:
            print_log("ERROR", f"JSON 파싱 오류: {e}")
            traceback.print_exc()

        return results


# ============================================================================
# API 요청 저장
# ============================================================================

def save_api_request(api_name, batch_id, request_url, response_json):
    """API 요청/응답 저장"""
    try:
        import psycopg2
        from psycopg2.extras import Json
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
            Json(response_json) if response_json else None,
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
    """타임아웃 지원 입력 (Windows)"""
    import msvcrt
    import time

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


def parse_year_input(year_input):
    """연도 입력 파싱

    Args:
        year_input: 연도 입력값
            - None/빈값: 실행시점 기준 미래 데이터 (내년~)
            - '2027': 단일 연도
            - '2025-2030': 범위

    Returns:
        tuple: (start_year, end_year, display_text)
    """
    if not year_input:
        # 기본값: 실행시점 기준 내년부터 5년치
        next_year = datetime.now().year + 1
        end_year = next_year + 4
        return next_year, end_year, f"{next_year}~{end_year}년 (미래 전망치)"

    if '-' in year_input:
        # 범위: 2025-2030
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


def parse_scenario_input(scenario_input):
    """시나리오 입력 파싱

    Args:
        scenario_input: 시나리오 입력값
            - None/빈값: 전체 시나리오
            - 'BAU1': 단일 시나리오
            - 'BAU1,ET1': 여러 시나리오 (쉼표 구분)

    Returns:
        tuple: (scenarios, display_text)
            - scenarios: None (전체) 또는 리스트
    """
    if not scenario_input:
        return None, "전체 시나리오"

    # 쉼표로 구분된 시나리오 코드
    codes = [c.strip().upper() for c in scenario_input.split(',')]
    codes = [c for c in codes if c]  # 빈 값 제거

    if not codes:
        return None, "전체 시나리오"

    if len(codes) == 1:
        return codes, codes[0]

    return codes, ','.join(codes)


def dry_run(year=None, country=None, scenario=None):
    """드라이 모드 - API 응답값 확인 - DB 저장 없음"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 잠재적 산출량 [DRY RUN]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    # 시나리오 설정
    scenarios, scenario_text = parse_scenario_input(scenario)
    print(f"대상: {country_text}, {year_text}, {scenario_text}")
    print()

    client = OECDPotentialGDPClient()

    print_log("INFO", f"잠재적 산출량 데이터 조회...")

    _, _, data_list = client.get_potential_gdp_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year,
        scenarios=scenarios
    )

    if data_list and len(data_list) > 0:
        # 지표 정보 출력
        print("\n[지표 정보]")
        print(f"  지표: {client.INDICATOR}")
        print(f"  설명(measure): Potential gross domestic product, volume, USD at 2021 Purchasing Power Parities")
        print(f"  단위: USD PPP (구매력평가 기준 미달러)")
        print(f"  빈도: Annual (연간)")

        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        scenarios = set(row['scenario'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"시나리오 수: {len(scenarios)} ({', '.join(sorted(scenarios))})")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 95)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'scenario':>8}  {'indicator':>12}  {'potential_gdp':>25}")
        print("-" * 95)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['scenario']:>8}  {row['indicator']:>12}  {row['potential_gdp']:>25,}")
    else:
        print("\n데이터 없음")

    # DRY RUN: API 요청 저장 안함
    print("\n" + "=" * 60)
    print("[DRY RUN] 완료 - DB 저장 없음")
    print("=" * 60)

    return data_list


def test_mode(year=None, country=None, scenario=None):
    """테스트 모드 - DB 저장 (test_market_potential_gdp)"""
    setup_logger()
    batch_id = "t_" + datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 잠재적 산출량 [TEST MODE]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    # 시나리오 설정
    scenarios, scenario_text = parse_scenario_input(scenario)
    print(f"대상: {country_text}, {year_text}, {scenario_text}")
    print()

    client = OECDPotentialGDPClient()

    print_log("INFO", f"잠재적 산출량 데이터 조회...")

    request_url, response_text, data_list = client.get_potential_gdp_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year,
        scenarios=scenarios
    )

    # API 요청/응답 저장
    save_api_request('potential_gdp', batch_id, request_url, response_text)

    if data_list and len(data_list) > 0:
        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        scenarios = set(row['scenario'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"시나리오 수: {len(scenarios)} ({', '.join(sorted(scenarios))})")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 95)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'scenario':>8}  {'indicator':>12}  {'potential_gdp':>25}")
        print("-" * 95)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['scenario']:>8}  {row['indicator']:>12}  {row['potential_gdp']:>25,}")

        # DB 저장 (테스트 테이블)
        save_to_db(data_list, batch_id, table_name='test_market_potential_gdp')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[TEST MODE] 완료")
    print("=" * 60)

    return data_list


def main(year=None, country=None, scenario=None):
    """운영 모드 - DB 저장 (market_potential_gdp)"""
    setup_logger()
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 잠재적 산출량 [운영 모드]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")

    # 연도 설정
    start_year, end_year, year_text = parse_year_input(year)
    # 국가 설정
    country_codes, country_text = parse_country_input(country)
    # 시나리오 설정
    scenarios, scenario_text = parse_scenario_input(scenario)
    print(f"대상: {country_text}, {year_text}, {scenario_text}")
    print()

    client = OECDPotentialGDPClient()

    print_log("INFO", f"잠재적 산출량 데이터 조회...")

    request_url, response_text, data_list = client.get_potential_gdp_data(
        country_codes=country_codes,
        start_year=start_year,
        end_year=end_year,
        scenarios=scenarios
    )

    # API 요청/응답 저장
    save_api_request('potential_gdp', batch_id, request_url, response_text)

    if data_list and len(data_list) > 0:
        # 데이터 통계
        countries = set(row['country_code'] for row in data_list)
        scenarios = set(row['scenario'] for row in data_list)
        years = [row['year'] for row in data_list]
        print(f"\n조회 결과: {len(data_list)}건")
        print(f"국가 수: {len(countries)}")
        print(f"시나리오 수: {len(scenarios)} ({', '.join(sorted(scenarios))})")
        print(f"연도 범위: {min(years)} ~ {max(years)}")

        # 테이블 형식 출력
        print("\n" + "-" * 95)
        print(f"{'no':>5}  {'year':>6}  {'country_code':>12}  {'scenario':>8}  {'indicator':>12}  {'potential_gdp':>25}")
        print("-" * 95)
        for idx, row in enumerate(data_list, 1):
            print(f"{idx:>5}  {row['year']:>6}  {row['country_code']:>12}  {row['scenario']:>8}  {row['indicator']:>12}  {row['potential_gdp']:>25,}")

        # DB 저장 (운영 테이블)
        save_to_db(data_list, batch_id, table_name='market_potential_gdp')
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[운영 모드] 완료")
    print("=" * 60)

    return data_list


def save_to_db(data_list, batch_id, table_name='market_potential_gdp'):
    """데이터 리스트를 DB에 저장

    Args:
        data_list: 저장할 데이터 리스트
        batch_id: 배치 ID
        table_name: 테이블명 (기본값: market_potential_gdp)

    테이블 컬럼: year, country_code, indicator, frequency, scenario, potential_gdp, batch_id, created_at

    Note:
        전망치 데이터는 매번 변경될 수 있으므로 중복 체크 없이 모두 저장
        batch_id로 수집 시점 구분
    """
    try:
        import psycopg2
        from config import DB_CONFIG

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 실행 서버 시간 (API 응답 수신 시점)
        created_at = datetime.now()

        insert_count = 0

        for row in data_list:
            # INSERT (중복 체크 없이 모두 저장)
            query = f"""
                INSERT INTO {table_name}
                    (year, country_code, indicator, frequency, scenario, potential_gdp, batch_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(query, (
                row['year'],
                row['country_code'],
                row['indicator'],
                row['frequency'],
                row['scenario'],
                row['potential_gdp'],
                batch_id,
                created_at
            ))
            insert_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        print_log("INFO", f"DB 저장 완료 ({table_name}): INSERT {insert_count}건")
        return True

    except Exception as e:
        print_log("ERROR", f"DB 저장 실패: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import time

    def select_mode_with_timeout(timeout=10):
        """모드 선택 (타임아웃 지원, Windows)"""
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

    print("\n[모드 선택] (10초 후 자동으로 운영모드 실행)")
    print("  d: DRY RUN (API 응답 확인, DB 저장 없음)")
    print("  t: TEST MODE (test_market_potential_gdp)")
    print("  엔터: 운영 모드 (전체 국가, market_potential_gdp)")
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
            print(f"\n[연도 선택] (10초 후 2026-2030 자동 실행)")
            print(f"  2027: 단일 연도 조회")
            print(f"  2026-2035: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: 2026~2030년 (기본값)")
            year = input_with_timeout("연도 입력", timeout=10)
            # 시나리오 선택
            print(f"\n[시나리오 선택] (10초 후 전체 시나리오)")
            print(f"  BAU1: 단일 시나리오")
            print(f"  BAU1,BAU2,ET1,ET2,ET3,ET4: 여러 시나리오 (쉼표 구분)")
            print(f"  엔터: 전체 시나리오")
            scenario = input_with_timeout("시나리오 입력", timeout=10)
            dry_run(year, country, scenario)
            input("\n엔터키를 누르면 종료합니다...")
        elif mode == 't':
            # 국가 선택
            print(f"\n[국가 선택] (10초 후 전체 국가)")
            print(f"  USA: 단일 국가")
            print(f"  USA,KOR,JPN: 여러 국가 (쉼표 구분)")
            print(f"  엔터: 전체 국가")
            country = input_with_timeout("국가 입력", timeout=10)
            # 연도 선택
            print(f"\n[연도 선택] (10초 후 2026-2030 자동 실행)")
            print(f"  2027: 단일 연도 조회")
            print(f"  2026-2035: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: 2026~2030년 (기본값)")
            year = input_with_timeout("연도 입력", timeout=10)
            # 시나리오 선택
            print(f"\n[시나리오 선택] (10초 후 전체 시나리오)")
            print(f"  BAU1: 단일 시나리오")
            print(f"  BAU1,BAU2,ET1: 여러 시나리오 (쉼표 구분)")
            print(f"  엔터: 전체 시나리오")
            scenario = input_with_timeout("시나리오 입력", timeout=10)
            test_mode(year, country, scenario)
            input("\n엔터키를 누르면 종료합니다...")
        else:
            # 국가 선택
            print(f"\n[국가 선택] (10초 후 전체 국가)")
            print(f"  USA: 단일 국가")
            print(f"  USA,KOR,JPN: 여러 국가 (쉼표 구분)")
            print(f"  엔터: 전체 국가")
            country = input_with_timeout("국가 입력", timeout=10)
            # 연도 선택
            print(f"\n[연도 선택] (10초 후 2026-2030 자동 실행)")
            print(f"  2027: 단일 연도 조회")
            print(f"  2026-2035: 범위 조회")
            print(f"  all: 전체 연도 조회")
            print(f"  엔터: 2026~2030년 (기본값)")
            year = input_with_timeout("연도 입력", timeout=10)
            # 시나리오 선택
            print(f"\n[시나리오 선택] (10초 후 전체 시나리오)")
            print(f"  BAU1: 단일 시나리오")
            print(f"  BAU1,BAU2,ET1: 여러 시나리오 (쉼표 구분)")
            print(f"  엔터: 전체 시나리오")
            scenario = input_with_timeout("시나리오 입력", timeout=10)
            results = main(year, country, scenario)
            # 운영모드는 자동 종료 (input 없음)
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        import traceback
        traceback.print_exc()
