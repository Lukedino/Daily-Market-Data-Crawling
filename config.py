"""
config.py — 환경변수 참조 설정
⚠️  모든 민감 정보는 환경변수 참조만 — 절대 하드코딩 금지
"""

import os
import sys

from dotenv import load_dotenv
load_dotenv()  # 로컬 실행 시 .env 파일 자동 로드

# ── API 키 (GitHub Secrets → 환경변수) ─────────────────────────────────────────
DART_API_KEY      = os.environ.get("DART_API_KEY", "")
GDRIVE_FOLDER_ID      = os.environ.get("GDRIVE_FOLDER_ID", "")
GDRIVE_OHLC_FOLDER_ID = os.environ.get("GDRIVE_OHLC_FOLDER_ID", "")  # [Database] Market Crawling Data 폴더 ID
GDRIVE_CREDS_PATH     = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "gdrive_creds.json")
GDRIVE_TOKEN_PATH     = os.environ.get("GDRIVE_TOKEN_PATH", "")  # OAuth2 사용자 토큰

# ── 환경 자동 감지 ─────────────────────────────────────────────────────────────
IN_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS") == "true"
IN_COLAB          = "google.colab" in str(sys.modules)

# ── 로컬 임시 저장 경로 (Actions 실행 중 → Drive 업로드 후 삭제 가능) ─────────
LOCAL_DATA_DIR = os.environ.get("LOCAL_DATA_DIR", "data/local")

# ── Google Drive 폴더 구조 (GDRIVE_FOLDER_ID 하위) ────────────────────────────
# ⚠️  Crawler와 폴더 충돌 방지: quant-korea-data/ 서브폴더에 격리
DRIVE_PATHS = {
    "market":     "quant-korea-data/market",      # 월별 시장 스냅샷 YYYYMM.parquet
    "financials": "quant-korea-data/financials",  # 연간 재무제표 YYYY.parquet
    "prices":     "quant-korea-data/prices",      # 월별 일별 주가 YYYYMM.parquet
    "progress":   "quant-korea-data/progress",    # 수집 진행 현황 collection_status.json
    # ── OHLC DB (루트: GDRIVE_OHLC_FOLDER_ID = [Database] Market Crawling Data) ──
    "ohlc_us":       "us",            # US 주식/ETF OHLC (연도별 parquet)
    "ohlc_crypto":   "crypto",        # 크립토 OHLC (연도별 parquet)
    "ohlc_kr":       "kr",            # KR 시장 OHLC+시총 (연도별 parquet, 구 marcap 폴더)
    "ohlc_meta":     "_meta",         # DB 상태 메타 (db_status.json)
    "us_financials": "us/financials", # US 분기 재무제표
    "kr_financials": "kr/financials", # KR 분기 재무제표 (섹터리더 2단계 KR)
    "us_ratios":     "us/ratios",     # US 재무비율 스냅샷
    "crypto_ratios": "crypto/ratios", # Crypto 시장 데이터 스냅샷
}

# ── 수집 대상 시장 ─────────────────────────────────────────────────────────────
MARKETS = ["KOSPI", "KOSDAQ"]

# ── 재시도 & 딜레이 설정 ────────────────────────────────────────────────────────
MAX_RETRY    = 3
BACKOFF_BASE = 2            # 지수 백오프: 2^n 초
DELAY_MARKET = (1.5, 3.0)  # 마켓 간 랜덤 딜레이 범위(초)
DELAY_API    = (0.5, 1.0)  # API 호출 간 랜덤 딜레이 범위(초)
DELAY_TICKER = (0.3, 0.7)  # 종목별 딜레이 (CompanyGuide)
DELAY_DAILY  = (1.0, 2.0)  # 일별 주가 수집 딜레이 범위(초)

# ── 수집 옵션 ──────────────────────────────────────────────────────────────────
SKIP_IF_DONE = True  # 이미 수집 완료된 월/연도 스킵
