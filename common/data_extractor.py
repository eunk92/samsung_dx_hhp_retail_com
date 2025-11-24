"""
DataExtractor - 데이터 추출 유틸리티

크롤링된 텍스트에서 필요한 데이터를 추출하고 변환하는 유틸리티 함수 모음

주요 기능:
- 숫자 추출: 텍스트에서 가격, 별점, 리뷰 개수 등의 숫자 추출
- 데이터 변환: 추출된 값을 표준 형식으로 변환
- 별점 분포 계산: HTML에서 별점 통계 추출 및 포맷팅

사용법:
    from common import data_extractor

    price = data_extractor.extract_price("$1,234.56")  # "1,234.56"
    rating = data_extractor.extract_rating("4.5 out of 5")  # "4.5"
    count = data_extractor.extract_review_count("3,572 reviews")  # "3,572"
    no_reviews = data_extractor.get_no_reviews_text("Amazon")  # "No customer reviews"

모든 크롤러에서 독립적으로 import하여 사용 가능
"""

import re


def extract_numeric_value(text, include_comma=True, include_decimal=True):
    """
    텍스트에서 숫자 추출 (쉼표, 소수점 옵션 가능)

    Args:
        text (str): 원본 텍스트
        include_comma (bool): 쉼표 포함 여부 (기본값: True)
        include_decimal (bool): 소수점 포함 여부 (기본값: True)

    Returns:
        str: 숫자만 추출된 문자열 또는 None

    Examples:
        - extract_numeric_value("$1,234.56") → "1,234.56"
        - extract_numeric_value("3,572등급", include_decimal=False) → "3,572"
        - extract_numeric_value("4.5 out of 5", include_comma=False) → "4.5"
    """
    if not text:
        return None

    if include_comma and include_decimal:
        pattern = r'[\d,.]+'
    elif include_comma:
        pattern = r'[\d,]+'
    elif include_decimal:
        pattern = r'\d+\.?\d*'
    else:
        pattern = r'\d+'

    match = re.search(pattern, text)
    return match.group(0) if match else None



def extract_rating(text, account_name=None):
    """
    별점 텍스트에서 숫자 추출 (소수점 포함, 쉼표 제외)

    쓰임새:
    - 별점 데이터 후처리

    Args:
        text (str): 원본 텍스트
        account_name (str, optional): 쇼핑몰 계정명 (None 반환 시 쇼핑몰별 텍스트 사용)

    Returns:
        str: 별점 숫자 또는 쇼핑몰별 리뷰 없음 텍스트

    Examples:
        - "4.5 out of 5 stars" → "4.5"
        - "3.8 out of 5" → "3.8"
        - "5.0" → "5.0"
        - None (with account_name="Amazon") → "No customer reviews"
    """
    result = extract_numeric_value(text, include_comma=False, include_decimal=True)
    if result is None and account_name:
        return get_no_reviews_text(account_name)
    return result


def extract_review_count(text, account_name=None):
    """
    리뷰 개수 텍스트에서 숫자 추출 (쉼표 포함, 소수점 제외)

    쓰임새:
    - 리뷰 개수, 판매량 등 정수 데이터 후처리

    Args:
        text (str): 원본 텍스트
        account_name (str, optional): 쇼핑몰 계정명 (None 반환 시 쇼핑몰별 텍스트 사용)

    Returns:
        str: 리뷰 개수 또는 쇼핑몰별 리뷰 없음 텍스트

    Examples:
        - "3,572등급 글로벌 평점" → "3,572"
        - "1234 reviews" → "1234"
        - None (with account_name="Amazon") → "No customer reviews"
    """
    result = extract_numeric_value(text, include_comma=True, include_decimal=False)
    if result is None and account_name:
        return get_no_reviews_text(account_name)
    return result


def get_no_reviews_text(account_name):
    """
    쇼핑몰별 리뷰 없음 텍스트 반환

    Args:
        account_name (str): 쇼핑몰 계정명 (예: "Amazon", "BestBuy", "Walmart")

    Returns:
        str: 쇼핑몰별 리뷰 없음 텍스트

    Examples:
        - get_no_reviews_text("Amazon") → "No customer reviews"
        - get_no_reviews_text("BestBuy") → "Not yet reviewed"
        - get_no_reviews_text("Walmart") → "No ratings yet"
    """
    no_reviews_mapping = {
        'Amazon': 'No customer reviews',
        'BestBuy': 'Not yet reviewed',
        'Walmart': 'No ratings yet'
    }

    return no_reviews_mapping.get(account_name, 'No reviews')


def extract_star_ratings_count(tree, count_of_reviews, xpath, account_name):
    """
    별점 분포를 추출하여 "5star:개수,4star:개수,..." 형식으로 변환
    쇼핑몰별 분기 처리

    Args:
        tree: lxml HTML element
        count_of_reviews (str): 전체 리뷰 개수 (예: "3,572")
        xpath (str): DB에서 로드한 XPath
        account_name (str): 쇼핑몰 계정명 (예: "Amazon", "BestBuy")

    Returns:
        str: 별점 분포 문자열 또는 None

    Examples: "5star:2931,4star:286,3star:107,2star:36,1star:214"
    """
    if not xpath or not account_name:
        return None

    # 쇼핑몰별 분기 처리
    if account_name == 'Amazon':
        return _extract_star_ratings_count_amazon(tree, count_of_reviews, xpath, account_name)
    else:
        # 기타 쇼핑몰: 개수를 직접 추출
        return _extract_star_ratings_count_generic(tree, xpath, account_name)


def _extract_star_ratings_count_amazon(tree, count_of_reviews, xpath, account_name):
    """
    Amazon 전용 별점 분포 추출 로직

    Args:
        tree: lxml HTML element
        count_of_reviews (str): 전체 리뷰 개수 (예: "3,572")
        xpath (str): DB에서 로드한 XPath
        account_name (str): 쇼핑몰 계정명

    Returns:
        str: 별점 분포 문자열 또는 쇼핑몰별 리뷰 없음 텍스트
    """
    try:
        # count_of_reviews 검증
        if not count_of_reviews or not isinstance(count_of_reviews, str):
            return get_no_reviews_text(account_name)

        # 전체 리뷰 수를 숫자로 변환 (숫자가 아니면 리뷰 없음 텍스트 반환)
        try:
            total_count = int(count_of_reviews.replace(',', ''))
        except (ValueError, AttributeError):
            print(f"[WARNING] Invalid count_of_reviews format: {count_of_reviews}")
            return get_no_reviews_text(account_name)

        # XPath로 퍼센트 텍스트 추출
        percent_texts = tree.xpath(xpath)

        if not percent_texts or len(percent_texts) < 5:
            return get_no_reviews_text(account_name)

        # 마지막 5개 텍스트가 실제 퍼센트 (첫 5개는 aria-hidden)
        actual_percents = percent_texts[-5:]

        star_data = []
        star_names = ['5star', '4star', '3star', '2star', '1star']

        for i, percent_text in enumerate(actual_percents):
            # "82%" → 82
            percent_str = percent_text.strip().replace('%', '')
            try:
                percent = int(percent_str)
            except ValueError:
                print(f"[WARNING] Invalid percent format: {percent_text}")
                continue

            # 개수 계산 (전체 리뷰 수 * 퍼센트 / 100, 반올림)
            count = round(total_count * percent / 100)

            star_data.append(f"{star_names[i]}:{count}")

        return ','.join(star_data) if star_data else get_no_reviews_text(account_name)

    except Exception as e:
        print(f"[WARNING] Failed to extract Amazon star ratings distribution: {e}")
        return None


def _extract_star_ratings_count_generic(tree, xpath, account_name):
    """
    기타 쇼핑몰 전용 별점 분포 추출 로직
    개수를 직접 추출하여 포맷팅

    Args:
        tree: lxml HTML element
        xpath (str): DB에서 로드한 XPath
        account_name (str): 쇼핑몰 계정명

    Returns:
        str: 별점 분포 문자열 또는 쇼핑몰별 리뷰 없음 텍스트

    Examples:
        - "5star:2931,4star:286,3star:107,2star:36,1star:214"
    """
    try:
        # XPath로 개수 텍스트 추출
        count_texts = tree.xpath(xpath)

        if not count_texts or len(count_texts) < 5:
            return get_no_reviews_text(account_name)

        star_data = []
        star_names = ['5star', '4star', '3star', '2star', '1star']

        # 첫 5개 또는 마지막 5개 중 선택 (쇼핑몰에 따라 다를 수 있음)
        counts_to_process = count_texts[:5] if len(count_texts) >= 5 else count_texts

        for i, count_text in enumerate(counts_to_process):
            if i >= 5:  # 최대 5개만 처리
                break

            # 텍스트에서 숫자만 추출 (쉼표 포함)
            count_str = extract_review_count(count_text.strip())

            if count_str:
                # 쉼표 제거하여 순수 숫자로 변환
                count_clean = count_str.replace(',', '')
                star_data.append(f"{star_names[i]}:{count_clean}")

        return ','.join(star_data) if len(star_data) == 5 else get_no_reviews_text(account_name)

    except Exception as e:
        print(f"[WARNING] Failed to extract generic star ratings distribution: {e}")
        return None