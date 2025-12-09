"""
Market 10대 인자 - 자본스톡 (Capital Stock)

IMF Investment and Capital Stock Dataset (ICSD) 데이터 수집
Penn World Table 기준 실질 PPP 자본스톡 데이터 추출

================================================================================
데이터 소스
================================================================================
- IMF ICSD (Investment and Capital Stock Dataset)
- 데이터: 자본 스톡 (실질, PPP 기준)

================================================================================
API 엔드포인트 (2025-12-09 확인)
================================================================================
IMF SDMX 2.1 REST API:
- Base URL: https://api.imf.org/external/sdmx/2.1/
- 데이터 조회: /data/{flowRef}/{key}
- 가용성 조회: /availableconstraint/{flowRef}/{key}/{providerRef}/{componentID}

참고: https://portal.api.imf.org/apis#tags=iData

================================================================================
필요 패키지
================================================================================
pip install pandas requests

================================================================================
"""

import os
import sys
import logging
import traceback
import requests
import pandas as pd
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
# IMF SDMX 2.1 REST API 클라이언트
# ============================================================================

class IMFCapitalStockClient:
    """IMF ICSD SDMX 2.1 REST API 클라이언트 - 자본스톡 데이터 조회

    IMF SDMX 2.1 API:
    https://api.imf.org/external/sdmx/2.1/
    """

    BASE_URL = "https://api.imf.org/external/sdmx/2.1"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.database_id = "ICSD"  # Investment and Capital Stock Dataset

        # XML 저장 폴더
        self.xml_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'xml')
        os.makedirs(self.xml_dir, exist_ok=True)

    def _save_xml_response(self, xml_text, key):
        """XML 응답을 파일로 저장"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # key에서 파일명 생성 (특수문자 제거)
            safe_key = key.replace('+', '_').replace('.', '_')[:50]
            filename = f"capital_stock_{safe_key}_{timestamp}.xml"
            filepath = os.path.join(self.xml_dir, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(xml_text)

            print_log("INFO", f"XML 저장: {filepath}")
        except Exception as e:
            print_log("WARNING", f"XML 저장 실패: {e}")

    def get_data(self, key="all", start_period=None, end_period=None, format_type="json"):
        """
        ICSD 데이터 조회 (SDMX 2.1 REST API)

        Args:
            key: 데이터 키 (예: 'US', 'US+KR', 'US.KN_PPP_XDC')
            start_period: 시작 기간 (예: '2010')
            end_period: 종료 기간 (예: '2023')
            format_type: 응답 형식 ('json' 또는 'xml')

        Returns:
            dict/str: API 응답 데이터
        """
        try:
            # 데이터 엔드포인트: /data/{flowRef}/{key}
            url = f"{self.BASE_URL}/data/IMF.FAD,{self.database_id}/{key}"

            params = {}
            if start_period:
                params['startPeriod'] = start_period
            if end_period:
                params['endPeriod'] = end_period

            print_log("INFO", f"데이터 요청: {url}")
            if params:
                print_log("INFO", f"파라미터: {params}")

            # Accept 헤더 설정
            headers = {'Accept': 'application/json' if format_type == 'json' else 'application/xml'}

            resp = self.session.get(url, params=params, headers=headers, timeout=120)
            print_log("INFO", f"응답 상태: {resp.status_code}")

            if resp.status_code == 200:
                if format_type == 'json':
                    return resp.json()
                else:
                    # XML 응답을 파일로 저장
                    self._save_xml_response(resp.text, key)
                    return resp.text
            else:
                print_log("WARNING", f"응답 코드: {resp.status_code}")
                print_log("WARNING", f"응답 내용: {resp.text[:1000]}")
                return None

        except Exception as e:
            print_log("ERROR", f"데이터 조회 실패: {e}")
            traceback.print_exc()
            return None

    def get_capital_stock_data(self, country_codes, start_year=None, end_year=None):
        """
        자본스톡 데이터 조회 (민간+정부 합산)

        Args:
            country_codes: 국가 코드 리스트 또는 문자열 (예: ['USA', 'KOR'] 또는 'USA')
            start_year: 시작 연도
            end_year: 종료 연도

        Returns:
            pd.DataFrame: 자본스톡 데이터 (year, country, capital_stock)

        Note:
            capital_stock = 민간(CAPSTCK_PS_Q_PU_RY2017) + 정부(CAPSTCK_S13_Q_PU_RY2017)
            단위: Billions of 2017 PPP international dollars (10억 달러)
        """
        try:
            # 국가 코드 처리 (리스트면 + 로 연결)
            if isinstance(country_codes, list):
                country_key = '+'.join(country_codes)
            else:
                country_key = country_codes

            # 민간 + 정부 지표 조회
            indicators = [
                'CAPSTCK_PS_Q_PU_RY2017',   # 민간 자본스톡
                'CAPSTCK_S13_Q_PU_RY2017',  # 정부 자본스톡
            ]
            indicator_key = '+'.join(indicators)

            # 키 구성: 국가.지표.빈도
            key = f"{country_key}.{indicator_key}.A"

            print_log("INFO", f"자본스톡 조회: key={key}")

            # XML 형식으로 데이터 요청
            data = self.get_data(
                key=key,
                start_period=str(start_year) if start_year else None,
                end_period=str(end_year) if end_year else None,
                format_type="xml"
            )

            if not data:
                return None

            # XML 응답 파싱
            results = self._parse_xml_response(data)

            if not results:
                print_log("WARNING", "파싱된 데이터 없음")
                return None

            df = pd.DataFrame(results)
            print_log("INFO", f"데이터 {len(df)}건 조회 완료")

            if len(df) == 0:
                return None

            # 피벗 테이블로 변환 (연도, 국가별 민간/정부 합산)
            df_pivot = df.pivot_table(
                index=['year', 'country'],
                columns='indicator',
                values='value',
                aggfunc='first'
            ).reset_index()

            df_pivot.columns.name = None

            # 민간 + 정부 합산하여 capital_stock 컬럼 생성
            # Indicator: CAPSTCK_PS_Q_PU_RY2017 (민간) + CAPSTCK_S13_Q_PU_RY2017 (정부)
            private_col = 'CAPSTCK_PS_Q_PU_RY2017'
            gov_col = 'CAPSTCK_S13_Q_PU_RY2017'

            if private_col in df_pivot.columns and gov_col in df_pivot.columns:
                df_pivot['capital_stock'] = df_pivot[private_col] + df_pivot[gov_col]
            elif private_col in df_pivot.columns:
                df_pivot['capital_stock'] = df_pivot[private_col]
            elif gov_col in df_pivot.columns:
                df_pivot['capital_stock'] = df_pivot[gov_col]
            else:
                print_log("WARNING", "민간/정부 자본스톡 컬럼 없음")
                return None

            # 필요한 컬럼만 선택
            df_result = df_pivot[['year', 'country', 'capital_stock']].copy()

            # 정렬
            df_result = df_result.sort_values(['country', 'year']).reset_index(drop=True)

            print_log("INFO", f"데이터 변환 완료: {len(df_result)}건")
            return df_result

        except Exception as e:
            print_log("ERROR", f"자본스톡 조회 실패: {e}")
            traceback.print_exc()
            return None

    def _parse_xml_response(self, xml_data):
        """XML 응답 파싱"""
        results = []
        try:
            from lxml import etree
            root = etree.fromstring(xml_data.encode('utf-8'))

            # Series 요소 찾기 (태그명 끝이 Series인 모든 요소)
            series_list = [elem for elem in root.iter() if elem.tag.endswith('Series')]
            print_log("DEBUG", f"Series 수: {len(series_list)}")

            for series in series_list:
                # 시리즈 속성에서 국가, 지표 추출
                attrs = dict(series.attrib)
                country = attrs.get('COUNTRY', attrs.get('REF_AREA', ''))
                indicator = attrs.get('INDICATOR', '')

                # Obs (관측치) 찾기
                obs_list = [elem for elem in series.iter() if elem.tag.endswith('Obs')]

                for obs in obs_list:
                    obs_attrs = dict(obs.attrib)
                    year = obs_attrs.get('TIME_PERIOD', '')
                    value_str = obs_attrs.get('OBS_VALUE', '')

                    if value_str:
                        try:
                            value = float(value_str)
                            results.append({
                                'country': country,
                                'indicator': indicator,
                                'year': year,
                                'value': value
                            })
                        except ValueError:
                            pass

            print_log("DEBUG", f"파싱 결과: {len(results)}건")

        except Exception as e:
            print_log("ERROR", f"XML 파싱 오류: {e}")
            traceback.print_exc()

        return results

# ============================================================================
# 메인 실행
# ============================================================================

def dry_run():
    """드라이 모드 - API 응답값 확인 (미국, 2015년 이후)"""
    setup_logger()

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [DRY RUN]")
    print("=" * 60)
    print("대상: 미국(USA), 2015년 이후")
    print()

    client = IMFCapitalStockClient()

    # 자본스톡 데이터 조회 (미국, 2015년 이후)
    print_log("INFO", "자본스톡 데이터 조회 (미국, 2015~최신)...")

    df = client.get_capital_stock_data(
        country_codes=['USA'],
        start_year=2015,
        end_year=None  # 최신까지
    )

    if df is not None and len(df) > 0:
        print(f"\nDataFrame shape: {df.shape}")
        print(f"컬럼: {list(df.columns)}")
        print()
        print(df.to_string())
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[DRY RUN] 완료 - DB 저장 없음")
    print("=" * 60)

    return df


def test_mode():
    """테스트 모드 - 미국 데이터만 DB 저장 (2015년 이후)"""
    setup_logger()
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [TEST MODE]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")
    print("대상: 미국(USA), 2015년 이후")
    print()

    client = IMFCapitalStockClient()

    # 자본스톡 데이터 조회 (미국, 2015년 이후)
    print_log("INFO", "자본스톡 데이터 조회 (미국, 2015~최신)...")

    df = client.get_capital_stock_data(
        country_codes=['USA'],
        start_year=2015,
        end_year=None
    )

    if df is not None and len(df) > 0:
        print(f"\nDataFrame shape: {df.shape}")
        print(f"컬럼: {list(df.columns)}")
        print()
        print(df.to_string())

        # DB 저장 (테스트 테이블)
        saved = save_to_db(df, batch_id, table_name='test_market_capital_stock')
        if saved:
            print_log("INFO", f"DB 저장 완료: {len(df)}건")
        else:
            print_log("ERROR", "DB 저장 실패")
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[TEST MODE] 완료")
    print("=" * 60)

    return df


def main():
    """운영 모드 - 모든 국가 전체 연도"""
    setup_logger()
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("\n" + "=" * 60)
    print("Market 10대 인자 - 자본스톡 [운영 모드]")
    print("=" * 60)
    print(f"배치 ID: {batch_id}")
    print("대상: 전체 국가, 전체 연도")
    print()

    client = IMFCapitalStockClient()

    # 전체 국가 자본스톡 데이터 조회 (전체 연도)
    print_log("INFO", "자본스톡 데이터 조회 (전체 국가, 전체 연도)...")

    df = client.get_capital_stock_data(
        country_codes='all',  # 전체 국가
        start_year=None,      # 전체 연도
        end_year=None
    )

    if df is not None and len(df) > 0:
        print(f"\nDataFrame shape: {df.shape}")
        print(f"컬럼: {list(df.columns)}")
        print(f"\n국가 수: {df['country'].nunique()}")
        print(f"연도 범위: {df['year'].min()} ~ {df['year'].max()}")

        # 샘플 출력 (처음 20건)
        print("\n[샘플 데이터 - 처음 20건]")
        print(df.head(20).to_string())

        # DB 저장 (운영 테이블)
        saved = save_to_db(df, batch_id, table_name='market_capital_stock')
        if saved:
            print_log("INFO", f"DB 저장 완료: {len(df)}건")
        else:
            print_log("ERROR", "DB 저장 실패")
    else:
        print("\n데이터 없음")

    print("\n" + "=" * 60)
    print("[운영 모드] 완료")
    print("=" * 60)

    return df


def save_to_db(df, batch_id, table_name='market_capital_stock'):
    """DataFrame을 DB에 저장

    Args:
        df: 저장할 DataFrame
        batch_id: 배치 ID
        table_name: 테이블명 (기본값: market_capital_stock)

    테이블 컬럼: year, country_code, capital_stock, batch_id, created_at

    Note:
        capital_stock = 민간(CAPSTCK_PS_Q_PU_RY2017) + 정부(CAPSTCK_S13_Q_PU_RY2017)
        단위: Billions of 2017 PPP international dollars (10억 달러)
        국가명은 market_capital_stock_country 테이블에서 조인하여 조회

        매년 전체 데이터를 새로 저장하는 방식 (중복 허용)
    """
    try:
        import psycopg2
        from config import DB_CONFIG

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 실행 서버 시간 (API 응답 수신 시점)
        created_at = datetime.now()

        for _, row in df.iterrows():
            country_code = row['country']

            # 단순 INSERT (매년 전체 데이터 새로 저장)
            query = f"""
                INSERT INTO {table_name}
                    (year, country_code, capital_stock, batch_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """

            cursor.execute(query, (
                int(row['year']),
                country_code,
                row.get('capital_stock'),
                batch_id,
                created_at
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print_log("INFO", f"DB 저장 완료 ({table_name}): {len(df)}건")
        return True

    except Exception as e:
        print_log("ERROR", f"DB 저장 실패: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import msvcrt
    import time

    print("\n[모드 선택] (10초 후 자동으로 운영모드 실행)")
    print("  d: DRY RUN (API 응답 확인, DB 저장 없음)")
    print("  t: TEST MODE (미국만, 2015년 이후, test_market_capital_stock)")
    print("  엔터: 운영 모드 (전체 국가, 전체 연도, market_capital_stock)")
    print()

    # 10초 타임아웃으로 입력 대기
    mode = ''
    print("모드 선택 (10초 대기): ", end='', flush=True)
    start_time = time.time()
    while time.time() - start_time < 10:
        if msvcrt.kbhit():
            char = msvcrt.getwch()
            if char == '\r':  # Enter
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
            results = main()
            # 운영모드는 자동 종료 (input 없음)
    except Exception as e:
        print(f"\n[ERROR] 예외 발생: {e}")
        import traceback
        traceback.print_exc()
