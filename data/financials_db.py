"""
data/financials_db.py — US/Crypto 재무제표 & 재무비율 로컬 Parquet DB 관리

저장 구조:
  data/local/ohlc_db/us/financials/us_financials_YYYY.parquet
  data/local/ohlc_db/us/ratios/us_ratios_YYYY.parquet
  data/local/ohlc_db/crypto/ratios/crypto_ratios_YYYY.parquet

financials 스키마 (분기별 재무제표):
  Ticker | PeriodDate | Year | Quarter |
  Revenue | GrossProfit | OperatingIncome | NetIncome | EBITDA |
  TotalAssets | TotalLiabilities | Equity |
  OperatingCashFlow | FreeCashFlow | CapEx |
  SnapDate
  PrimaryKey: (Ticker, PeriodDate)

ratios 스키마 (스냅샷, 실행할 때마다 누적):
  Ticker | SnapDate | Name | Sector | Industry |
  MarketCap | SharesOutstanding |
  PE | ForwardPE | PB | PS |
  ROE | ROA | DebtToEquity | Beta |
  DividendYield | EPS | ProfitMargin | OperatingMargin |
  RevenueGrowth | EarningsGrowth | CurrentRatio
  PrimaryKey: (Ticker, SnapDate)

압축: snappy (pyarrow.parquet)
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db"

# ── 스키마 컬럼 정의 ──────────────────────────────────────────────────────────
_FINANCIALS_COLS = [
    "Ticker", "PeriodDate", "Year", "Quarter",
    "Revenue", "GrossProfit", "OperatingIncome", "NetIncome", "EBITDA",
    "TotalAssets", "TotalLiabilities", "Equity",
    "OperatingCashFlow", "FreeCashFlow", "CapEx",
    "SnapDate",
]

_RATIOS_COLS = [
    "Ticker", "SnapDate", "Name", "Sector", "Industry",
    "MarketCap", "SharesOutstanding",
    "PE", "ForwardPE", "PB", "PS",
    "ROE", "ROA", "DebtToEquity", "Beta",
    "DividendYield", "EPS", "ProfitMargin", "OperatingMargin",
    "RevenueGrowth", "EarningsGrowth", "CurrentRatio",
]


# ══════════════════════════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def local_financials_path(market: str, year: int) -> Path:
    """financials Parquet 파일 경로."""
    return _LOCAL_ROOT / market / "financials" / f"{market}_financials_{year}.parquet"


def local_ratios_path(market: str, year: int) -> Path:
    """ratios Parquet 파일 경로."""
    return _LOCAL_ROOT / market / "ratios" / f"{market}_ratios_{year}.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# 읽기
# ══════════════════════════════════════════════════════════════════════════════

def load_financials_year(market: str, year: int) -> pd.DataFrame:
    """연도별 financials Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_financials_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_FINANCIALS_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        if "PeriodDate" in df.columns:
            df["PeriodDate"] = pd.to_datetime(df["PeriodDate"]).dt.date
        if "SnapDate" in df.columns:
            df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date
        return df
    except Exception as e:
        logger.error(f"[FinancialsDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=_FINANCIALS_COLS)


def load_ratios_year(market: str, year: int) -> pd.DataFrame:
    """연도별 ratios Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_ratios_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_RATIOS_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        if "SnapDate" in df.columns:
            df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date
        return df
    except Exception as e:
        logger.error(f"[FinancialsDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=_RATIOS_COLS)


# ══════════════════════════════════════════════════════════════════════════════
# 저장 (병합 → 중복제거 → snappy 압축)
# ══════════════════════════════════════════════════════════════════════════════

def save_financials(new_df: pd.DataFrame, market: str):
    """
    financials 저장.
    PeriodDate 기준 연도별 분할 저장.
    기존 파일 병합 → (Ticker, PeriodDate) 기준 중복제거(최신우선) → snappy 저장.
    """
    if new_df.empty:
        logger.warning(f"[FinancialsDB] save_financials: 빈 DataFrame (market={market})")
        return

    df = new_df.copy()
    if "PeriodDate" in df.columns:
        df["PeriodDate"] = pd.to_datetime(df["PeriodDate"]).dt.date
    if "SnapDate" in df.columns:
        df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date

    # 연도별 분할
    df["_year"] = df["PeriodDate"].apply(lambda d: d.year if d is not None else None)
    df = df.dropna(subset=["_year"])
    df["_year"] = df["_year"].astype(int)

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        _save_financials_year(year_df, market, int(year))


def _save_financials_year(df: pd.DataFrame, market: str, year: int):
    """단일 연도 financials 저장 (내부 함수)."""
    path = local_financials_path(market, year)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 기존 파일 병합
    if path.exists():
        try:
            existing = load_financials_year(market, year)
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"[FinancialsDB] 기존 파일 병합 실패 → 덮어씀: {e}")

    # 중복 제거 (최신 우선)
    if "Ticker" in df.columns and "PeriodDate" in df.columns:
        df = df.drop_duplicates(subset=["Ticker", "PeriodDate"], keep="last")

    # 컬럼 순서 정렬
    cols = [c for c in _FINANCIALS_COLS if c in df.columns]
    extra = [c for c in df.columns if c not in _FINANCIALS_COLS]
    df = df[cols + extra].sort_values(["Ticker", "PeriodDate"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[FinancialsDB] 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Ticker'].nunique() if 'Ticker' in df.columns else '?'}종목, "
        f"{size_kb:.1f}KB)"
    )


def save_ratios(new_df: pd.DataFrame, market: str):
    """
    ratios 저장.
    SnapDate 기준 연도별 분할 저장.
    기존 파일 병합 → (Ticker, SnapDate) 기준 중복제거(최신우선) → snappy 저장.
    """
    if new_df.empty:
        logger.warning(f"[FinancialsDB] save_ratios: 빈 DataFrame (market={market})")
        return

    df = new_df.copy()
    if "SnapDate" in df.columns:
        df["SnapDate"] = pd.to_datetime(df["SnapDate"]).dt.date

    # 연도별 분할
    df["_year"] = df["SnapDate"].apply(lambda d: d.year if d is not None else None)
    df = df.dropna(subset=["_year"])
    df["_year"] = df["_year"].astype(int)

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        _save_ratios_year(year_df, market, int(year))


def _save_ratios_year(df: pd.DataFrame, market: str, year: int):
    """단일 연도 ratios 저장 (내부 함수)."""
    path = local_ratios_path(market, year)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 기존 파일 병합
    if path.exists():
        try:
            existing = load_ratios_year(market, year)
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"[FinancialsDB] 기존 ratios 파일 병합 실패 → 덮어씀: {e}")

    # 중복 제거 (최신 우선)
    if "Ticker" in df.columns and "SnapDate" in df.columns:
        df = df.drop_duplicates(subset=["Ticker", "SnapDate"], keep="last")

    # 컬럼 순서 정렬
    cols = [c for c in _RATIOS_COLS if c in df.columns]
    extra = [c for c in df.columns if c not in _RATIOS_COLS]
    df = df[cols + extra].sort_values(["Ticker", "SnapDate"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[FinancialsDB] ratios 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Ticker'].nunique() if 'Ticker' in df.columns else '?'}종목, "
        f"{size_kb:.1f}KB)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def _get_uploader(uploader=None):
    """DriveUploader 인스턴스 반환 (인자로 받거나 새로 생성).
    루트 폴더를 GDRIVE_OHLC_FOLDER_ID([Database] Market Crawling Data)로 설정.
    """
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception as e:
        logger.error(f"[FinancialsDB] DriveUploader 초기화 실패: {e}")
        return None


def upload_financials(market: str, years: list[int], uploader=None):
    """지정 연도 financials Parquet을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[FinancialsDB] uploader 없음 → 업로드 건너뜀")
        return

    remote_path = config.DRIVE_PATHS.get("us_financials")
    if not remote_path:
        logger.error("[FinancialsDB] DRIVE_PATHS에 'us_financials' 없음")
        return

    for year in years:
        path = local_financials_path(market, year)
        if not path.exists():
            logger.warning(f"[FinancialsDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
            logger.info(f"[FinancialsDB] 업로드 완료: {path.name}")
        except Exception as e:
            logger.error(f"[FinancialsDB] {path.name} 업로드 실패: {e}")


def upload_ratios(market: str, years: list[int], uploader=None):
    """지정 연도 ratios Parquet을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[FinancialsDB] uploader 없음 → 업로드 건너뜀")
        return

    # market에 따라 Drive 경로 선택
    if market == "us":
        remote_path = config.DRIVE_PATHS.get("us_ratios")
        drive_key = "us_ratios"
    else:
        remote_path = config.DRIVE_PATHS.get("crypto_ratios")
        drive_key = "crypto_ratios"

    if not remote_path:
        logger.error(f"[FinancialsDB] DRIVE_PATHS에 '{drive_key}' 없음")
        return

    for year in years:
        path = local_ratios_path(market, year)
        if not path.exists():
            logger.warning(f"[FinancialsDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
            logger.info(f"[FinancialsDB] ratios 업로드 완료: {path.name}")
        except Exception as e:
            logger.error(f"[FinancialsDB] {path.name} 업로드 실패: {e}")


def download_financials_all(market: str, uploader=None):
    """Drive에서 financials 모든 연도 파일 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("us_financials")
    if not remote_path:
        logger.error("[FinancialsDB] DRIVE_PATHS에 'us_financials' 없음")
        return

    local_d = _LOCAL_ROOT / market / "financials"
    local_d.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(local_d), extensions=(".parquet",))
        logger.info(f"[FinancialsDB] {market} financials 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[FinancialsDB] {market} financials 다운로드 실패: {e}")


def download_ratios_all(market: str, uploader=None):
    """Drive에서 ratios 모든 연도 파일 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    if market == "us":
        remote_path = config.DRIVE_PATHS.get("us_ratios")
    else:
        remote_path = config.DRIVE_PATHS.get("crypto_ratios")

    if not remote_path:
        logger.error(f"[FinancialsDB] DRIVE_PATHS에 ratios 경로 없음 (market={market})")
        return

    local_d = _LOCAL_ROOT / market / "ratios"
    local_d.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(local_d), extensions=(".parquet",))
        logger.info(f"[FinancialsDB] {market} ratios 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[FinancialsDB] {market} ratios 다운로드 실패: {e}")


# ── Drive 베이스라인 (2026-09-01) ─────────────────────────────────────────
# save_*() 는 로컬 파일하고만 병합한다. GHA 러너는 매 실행 빈 파일시스템에서 시작하는데
# 수집 경로 어디에도 선다운로드가 없어, upload_*() 가 이번 실행분만으로 Drive 기존 파일을
# 덮어썼다(ratios 는 실행당 SnapDate 1개라 스냅샷 이력이 매번 전멸 — 실측 SnapDate 1개뿐).
# 위의 download_*_all() 은 존재했지만 아무도 호출하지 않았고, 오류를 삼켜 "원격에 없음"과
# "다운로드 실패"를 구분하지 못한다 — 실패인 채 진행하면 그대로 덮어쓴다.


def _baseline_remote_and_local(market: str, kind: str):
    """kind('financials'|'ratios')의 Drive 경로와 로컬 폴더. DRIVE_PATHS 미설정이면 remote=None."""
    if kind == "financials":
        remote = config.DRIVE_PATHS.get("us_financials")
    elif market == "us":
        remote = config.DRIVE_PATHS.get("us_ratios")
    else:
        remote = config.DRIVE_PATHS.get("crypto_ratios")
    return remote, _LOCAL_ROOT / market / kind


def ensure_drive_baseline(market: str, kinds=("financials", "ratios"), uploader=None):
    """수집 시작 전 Drive 기존 파일을 로컬로 내려받아 병합 기반을 만든다.

    ohlc_db.download_year_state 와 동일한 3상태 철학:
      absent → 최초 실행, 새로 쓰는 것이 정상. 진행
      failed → 기존 데이터가 있는지조차 모른다. 덮어쓰기 방지를 위해 RuntimeError 로 중단
    uploader 초기화 실패(None)·DRIVE_PATHS 미설정은 upload_*() 도 전부 건너뛰어
    Drive 가 안전하므로 경고만 남기고 진행한다.
    """
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[FinancialsDB] uploader 없음 → 베이스라인 다운로드 건너뜀 (업로드도 안 되므로 안전)")
        return
    for kind in kinds:
        remote, local_d = _baseline_remote_and_local(market, kind)
        if not remote:
            logger.warning(f"[FinancialsDB] DRIVE_PATHS 미설정 → {market}/{kind} 베이스라인 건너뜀 (업로드도 안 됨)")
            continue
        state = u.download_all_state(remote, str(local_d), extensions=(".parquet",))
        if state == "failed":
            raise RuntimeError(
                f"[FinancialsDB] Drive 기존 {market}/{kind} 다운로드 실패 — "
                f"이대로 진행하면 이번 실행분이 Drive 를 덮어쓴다. 중단"
            )
        logger.info(f"[FinancialsDB] 베이스라인 {market}/{kind}: {state}")
