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

# KR 공식값 정정(data/kr_official.py) — 2026-10-10.
# 하한: 2026년 전체를 공식값과 맞춘다(사용자 결정 2026-10-10). 하한은 "어디까지 대조하나" 이고
# 실제 쓰기는 공식값과 다른 행뿐이다(일괄 정정 뒤 매일 쓰는 행은 새로 게시된 날짜분).
KR_OFFICIAL_SETTLE_FROM = os.environ.get("KR_OFFICIAL_SETTLE_FROM", "2026-01-01")
# 자동 경로(kr-daily·kr-backfill)의 정정 모드. "plan" = 받기·검증·계획·게이트까지 하고 쓰지
# 않는다(로그에 계획 수치만). "apply" = 실제 정정. 2026 일괄 정정이 승인·실행되기 전에는
# plan 이어야 한다 — 하한 2026-01-01 그대로 apply 면 첫 kr-daily 가 30만 행을 바꾼다.
# 알 수 없는 값은 plan 으로 본다. 수동 --mode kr-settle 은 이 값과 무관하다(--plan 으로 계획만).
# 2026-10-13 10:01 KST 일괄 정정·사후 검증 완료(같은 파일 ID 갱신, 541,187행) → apply 로 전환.
KR_OFFICIAL_SETTLE_MODE = os.environ.get("KR_OFFICIAL_SETTLE_MODE", "apply")
# 이 연도 이전(마감 연도)은 어떤 경로로도 정정하지 않는다 — 2025년은 공식값과 696,524행
# 전부 일치했고, 하류 ML 의 연구 해시가 마감 연도 데이터를 쓴다.
KR_OFFICIAL_MIN_YEAR = 2026

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
