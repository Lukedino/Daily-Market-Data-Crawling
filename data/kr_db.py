"""
data/kr_db.py — KR 시장 OHLC+시총 연도별 Parquet DB 관리

저장 구조:
  data/local/ohlc_db/kr/marcap-YYYY.parquet

스키마 (marcap 표준):
  Code | Name | Close | Dept | ChangeCode | Changes | ChangesRatio |
  Volume | Amount | Open | High | Low | Marcap | Stocks |
  Market | MarketId | Rank | Date

  - Code     : 종목코드 6자리 문자열
  - Market   : KOSPI / KOSDAQ / KONEX / KOSDAQ GLOBAL
  - MarketId : STK / KSQ / KNX
  - Marcap   : 시가총액 (원)
  - Rank     : 시총 순위 (시장 내)
  - Date     : datetime64[ns]

Primary Key: (Code, Date)
압축: snappy
"""

import json
import logging
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db" / "kr"
_STATUS_PATH = Path(config.LOCAL_DATA_DIR) / "ohlc_db" / "_meta" / "kr_status.json"

SCHEMA_COLS = [
    "Code", "Name", "Close", "Dept", "ChangeCode", "Changes", "ChangesRatio",
    "Volume", "Amount", "Open", "High", "Low", "Marcap", "Stocks",
    "Market", "MarketId", "Rank", "Date",
]


# ══════════════════════════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def local_path(year: int) -> Path:
    return _LOCAL_ROOT / f"marcap-{year}.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# 읽기 / 저장
# ══════════════════════════════════════════════════════════════════════════════

class KrStateError(RuntimeError):
    """A KR baseline or candidate cannot be safely merged or published."""


def _validated_frame(df: pd.DataFrame, year: int, *, allow_duplicates=False) -> pd.DataFrame:
    if df.columns.has_duplicates:
        raise KrStateError("duplicate schema columns")
    if df.empty:
        return df.reindex(columns=SCHEMA_COLS) if not len(df.columns) else df.copy()
    if not {"Code", "Date"}.issubset(df.columns):
        raise KrStateError("Code/Date columns required")
    result = df.copy()
    result["Date"] = pd.to_datetime(result["Date"], errors="raise")
    if result["Date"].isna().any() or not result["Date"].dt.year.eq(year).all():
        raise KrStateError("invalid date or year partition")
    if result["Code"].isna().any() or result["Code"].astype(str).str.strip().eq("").any():
        raise KrStateError("missing Code")
    if not allow_duplicates and result.duplicated(["Code", "Date"]).any():
        raise KrStateError("duplicate Code/Date keys")
    return result


def _read_year_path(path: Path, year: int) -> pd.DataFrame:
    try:
        return _validated_frame(pq.read_table(str(path)).to_pandas(), year)
    except Exception as error:
        raise KrStateError(f"marcap-{year} read failed: {type(error).__name__}") from None


def load_year(year: int, *, strict=False) -> pd.DataFrame:
    """연도별 Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_path(year)
    if not path.exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    try:
        return _read_year_path(path, year)
    except Exception as e:
        logger.error(f"[KrDB] {path.name} 로드 실패: {type(e).__name__}")
        if strict:
            raise
        return pd.DataFrame(columns=SCHEMA_COLS)


def ensure_year_baselines(years, *, download=False, uploader=None) -> dict[int, str]:
    """Resolve every target year's ok/absent/failed state before any collection/write."""
    states = {}
    for year in sorted(set(years)):
        outcome = download_year_state(year, uploader=uploader) if download else "absent"
        if outcome not in {"ok", "absent"}:
            states[year] = "failed"
            continue
        try:
            if local_path(year).exists():
                load_year(year, strict=True)
                states[year] = outcome if download else "ok"
            else:
                states[year] = "failed" if outcome == "ok" else "absent"
        except Exception:
            states[year] = "failed"
    return states


def _merge_year(df: pd.DataFrame, existing: pd.DataFrame, year: int, *, ohlc_only=False):
    incoming = _validated_frame(df, year, allow_duplicates=True)
    incoming = incoming.drop_duplicates(["Code", "Date"], keep="last")
    if existing.empty:
        merged = incoming
    else:
        keys = ["Code", "Date"]
        incoming = incoming.set_index(keys)
        previous = existing.set_index(keys)
        # These unavailable fields are not refreshed by yfinance OHLC backfills.
        for column in ("Name", "Dept", "ChangeCode", "Market", "MarketId",
                       "Marcap", "Stocks", "Rank", "Amount"):
            if column not in previous:
                continue
            if column not in incoming:
                incoming[column] = None
            prior = previous[column].reindex(incoming.index)
            if column == "Amount" and ohlc_only:
                # Backfill Amount = Close * Volume is an estimate, not the day's
                # exchange turnover; keep an existing measurement (including zero).
                incoming[column] = prior.combine_first(incoming[column])
            else:
                missing = incoming[column].isna()
                if column in {"Marcap", "Stocks", "Rank"}:
                    missing = missing | incoming[column].eq(0)
                incoming[column] = incoming[column].where(~missing, prior)
        merged = pd.concat([existing, incoming.reset_index()], ignore_index=True)
        merged = merged.drop_duplicates(keys, keep="last")
        old_keys = pd.MultiIndex.from_frame(existing[keys])
        new_keys = pd.MultiIndex.from_frame(merged[keys])
        if not old_keys.isin(new_keys).all():
            raise KrStateError("merge would remove existing Code/Date keys")
    cols = [c for c in SCHEMA_COLS if c in merged.columns]
    merged = merged[cols].sort_values(["Date", "Code"]).reset_index(drop=True)
    return _validated_frame(merged, year)


def _write_staged_frame(df: pd.DataFrame, year: int, destination: Path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(destination), compression="snappy")
    staged = _read_year_path(destination, year)
    pd.testing.assert_frame_equal(staged, df, check_dtype=False, check_exact=True)
    # Windows _commit requires a descriptor opened for writing.
    with destination.open("r+b") as handle:
        os.fsync(handle.fileno())


def save_year(df: pd.DataFrame, year: int, *, ohlc_only=False):
    """
    연도별 Parquet 저장.
    기존 파일은 strict 읽기 후 병합. 검증된 임시 Parquet만 원자 교체한다.
    ohlc_only=True이면 기존 실측 Amount도 yfinance 추정치보다 우선한다.
    """
    if df.empty:
        logger.warning(f"[KrDB] 빈 DataFrame → 저장 건너뜀: marcap-{year}")
        return

    path = local_path(year)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing = load_year(year, strict=True)
    df = _merge_year(df, existing, year, ohlc_only=ohlc_only)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".marcap-{year}-",
                                         suffix=".parquet", delete=False) as handle:
            temporary = Path(handle.name)
        _write_staged_frame(df, year, temporary)
        os.replace(temporary, path)
        temporary = None
    except Exception as error:
        raise KrStateError(f"marcap-{year} save failed: {type(error).__name__}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[KrDB] 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Date'].dt.date.nunique()}거래일, {size_kb:.1f}KB)"
    )


def append_rows(new_df: pd.DataFrame, *, ohlc_only=False) -> list[int]:
    """
    새 데이터를 연도별로 분할하여 기존 파일에 append.
    Returns: 업데이트된 연도 목록
    """
    if new_df.empty:
        return []

    df = new_df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="raise")
    if df["Date"].isna().any():
        raise KrStateError("missing Date in incoming rows")
    df["_year"] = df["Date"].dt.year
    updated_years = []

    # A corrupt later year must stop this multi-year append before the first save.
    states = ensure_year_baselines(int(year) for year in df["_year"].unique())
    if "failed" in states.values():
        raise KrStateError("one or more KR baselines are unreadable")

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        save_year(year_df, int(year), ohlc_only=ohlc_only)
        updated_years.append(int(year))

    return sorted(updated_years)


def get_last_date(year: Optional[int] = None) -> Optional[date]:
    """저장된 데이터 중 가장 최근 Date 반환."""
    if not _LOCAL_ROOT.exists():
        return None

    if year:
        files = [local_path(year)] if local_path(year).exists() else []
    else:
        files = sorted(_LOCAL_ROOT.glob("marcap-*.parquet"), reverse=True)

    for pfile in files:
        try:
            df = pq.read_table(str(pfile), columns=["Date"]).to_pandas()
            if df.empty:
                continue
            return pd.to_datetime(df["Date"]).max().date()
        except Exception as e:
            logger.warning(f"[KrDB] {pfile.name} Date 조회 실패: {e}")

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 상태 관리
# ══════════════════════════════════════════════════════════════════════════════

def load_status() -> dict:
    if not _STATUS_PATH.exists():
        return {}
    try:
        with open(_STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[KrDB] status 로드 실패: {e}")
        return {}


def save_status(last_date: date, trading_days: int):
    _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "last_updated": str(last_date),
        "trading_days_total": trading_days,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }
    temporary = None
    try:
        payload = json.dumps(status, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=_STATUS_PATH.parent,
                                         prefix=".kr-status-", suffix=".json", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if json.loads(temporary.read_text(encoding="utf-8")) != status:
            raise KrStateError("status roundtrip mismatch")
        os.replace(temporary, _STATUS_PATH)
        temporary = None
        logger.info(f"[KrDB] status 저장: last={last_date}, days={trading_days}")
    except Exception as e:
        raise KrStateError(f"KR status save failed: {type(e).__name__}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def _get_uploader(uploader=None):
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception as e:
        logger.error(f"[KrDB] DriveUploader 초기화 실패: {e}")
        return None


def upload_years(years: list[int], uploader=None) -> list[str]:
    """지정 연도 Parquet을 Drive kr/ 폴더에 업로드하고 **실패한 파일 이름 목록**을 돌려준다.

    예전에는 예외를 로그로 삼켜 호출자가 "Drive 업로드 완료" 를 무조건 찍었다(D-02).
    그날의 Marcap·Rank 는 FDR 당일 스냅샷이라 다시 받을 수 없으므로, 실패는 반드시 드러나야 한다.
    """
    u = _get_uploader(uploader)
    if u is None:
        return [f"marcap-{year}.parquet" for year in years]

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    failed: list[str] = []
    for year in years:
        path = local_path(year)
        if not path.exists():
            logger.warning(f"[KrDB] 업로드 대상 없음: {path.name}")
            failed.append(path.name)
            continue
        try:
            load_year(year, strict=True)
            if u.upload(str(path), remote_path) is False:
                raise KrStateError("uploader explicitly reported failure")
            logger.info(f"[KrDB] Drive 업로드 완료: {path.name}")
        except Exception as e:
            # 공개 저장소 로그 — 예외 문자열(Drive ID 포함 가능) 대신 종류만 남긴다.
            logger.error(f"[KrDB] {path.name} 업로드 실패: {type(e).__name__}")
            failed.append(path.name)
    return failed


def download_year_state(year: int, uploader=None) -> str:
    """Drive 다운로드 결과를 세 상태로 구분한다: "ok" | "absent" | "failed".

    download_year() 는 "Drive 에 없음" 과 "다운로드 실패" 를 모두 False 로 돌려준다. 실패를 없음으로
    취급하면 run_kr_daily 가 1/1 부터 백필한 파일로 당해 연도를 교체하고, yfinance 로는 복구되지 않는
    Marcap·Rank·Stocks·Amount 과거값이 사라진다(D-01, ohlc_db.download_year_state 와 같은 규칙).
    """
    u = _get_uploader(uploader)
    if u is None:
        logger.error("[KrDB] 업로더 없음 — Drive 상태를 확인할 수 없다")
        return "failed"

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    filename = f"marcap-{year}.parquet"
    dest = local_path(year)
    dest.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        # A prior failed upload may have left irreplaceable FDR metadata only
        # locally. Validate it before download, then retain its unmatched keys.
        existing = load_year(year, strict=True)
        with tempfile.NamedTemporaryFile(dir=dest.parent, prefix=f".baseline-{year}-",
                                         suffix=".parquet", delete=False) as handle:
            temporary = Path(handle.name)
        try:
            u.download(remote_path, filename, str(temporary))
        except FileNotFoundError:
            logger.info(f"[KrDB] Drive 에 아직 없음: {filename}")
            return "absent"
        remote = _read_year_path(temporary, year)
        if not existing.empty:
            remote = _merge_year(remote, existing, year)
            _write_staged_frame(remote, year, temporary)
        os.replace(temporary, dest)
        temporary = None
        logger.info(f"[KrDB] Drive 다운로드 완료: {filename}")
        return "ok"
    except Exception as e:
        logger.error(f"[KrDB] {filename} 다운로드 실패: {type(e).__name__}")
        return "failed"
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def download_year(year: int, uploader=None) -> bool:
    """Drive에서 연도별 Parquet 다운로드."""
    return download_year_state(year, uploader=uploader) == "ok"


def download_all(uploader=None):
    """Drive kr/ 폴더의 모든 parquet 다운로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    _LOCAL_ROOT.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(_LOCAL_ROOT), extensions=(".parquet",))
        logger.info("[KrDB] Drive 전체 다운로드 완료")
    except Exception as e:
        logger.error(f"[KrDB] 전체 다운로드 실패: {e}")
