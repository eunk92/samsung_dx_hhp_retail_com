"""
공통 설정 모듈
- 작업 디렉토리 설정
- Windows 콘솔 한글 출력 설정
- 경로 설정

모든 크롤러에서 import하여 사용:
    from common.setup import setup_environment
    setup_environment(__file__)
"""

import sys
import os


def setup_environment(script_file):
    """
    크롤러 실행 환경 설정

    Args:
        script_file (str): 실행 중인 스크립트의 __file__ 값

    기능:
        1. 작업 디렉토리를 프로젝트 루트로 설정
        2. Windows 콘솔 한글 출력 설정
        3. 프로젝트 루트를 sys.path에 추가

    사용 예시:
        from common.setup import setup_environment
        setup_environment(__file__)
    """
    # 1. 작업 디렉토리 설정
    script_dir = os.path.dirname(os.path.abspath(script_file))
    project_root = os.path.dirname(script_dir)  # 크롤러 폴더의 상위 (프로젝트 루트)
    os.chdir(project_root)  # 작업 디렉토리 변경

    # 2. Windows 콘솔 한글 출력 설정
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    # 3. 프로젝트 루트를 sys.path에 추가 (중복 방지)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    return project_root