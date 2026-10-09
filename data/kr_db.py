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


# ══════════════════════════════════════════════════════════════════════════════
# 날짜별 종목 범위 게이트 (2026-10-10)
# ══════════════════════════════════════════════════════════════════════════════
# 2026-10-01 에 1순위 원천이 실패하고 검증된 yfinance 폴백이 저장본 대조를 약 8%만
# 통과해, 234/2,873종목뿐인 하루가 저장·게시되고 GHA 는 success 로 끝났다. US·Crypto
# 저장 경로(ohlc_db)에는 날짜별 급감 게이트가 있지만 KR 에는 없었고, 하류 ML 은 그
# 하루 때문에 학습 게이트에 7일 막혔다.
#
# US 게이트와 다른 점 두 가지(의도):
#   - US 는 부분집합 병합 때 기존 파일에 없던 날짜를 검사에서 뺀다. 10-01 같은 부분
#     수집일은 바로 그 "새 날짜" 로 들어온다. 여기서는 새 날짜도 검사한다.
#   - US 는 파일 전체의 인접일 비교라, 이미 저장된 결손 하루가 이후 모든 저장을 막는다
#     (2026-09-05 U-UN.TO 사고). 여기서는 **이번 쓰기가 건드린 날짜만** 판정하고,
#     기준은 인접 기존 거래일들의 중앙값이라 기준 창의 결손 하루가 기준을 줄이지 못한다.
COVERAGE_DROP_PCT = 10.0          # US ohlc_db·ML 소비 측 게이트와 같은 임계
COVERAGE_BASELINE_SESSIONS = 5    # 기준 = 인접 기존 거래일 5개의 종목 수 중앙값
COVERAGE_MIN_UNIVERSE = 100       # 기준이 이보다 작으면 판정하지 않는다(합성·초기 데이터)


class KrCoverageError(KrStateError):
    """이번 쓰기가 건드린 날짜의 종목 수가 정상 유니버스보다 크게 모자란다."""

    code = "kr_coverage_gap"


def session_counts(frame: Optional[pd.DataFrame]) -> pd.Series:
    """날짜별 고유 종목 수(Date 오름차순). 비었거나 열이 없으면 빈 Series."""
    if frame is None or frame.empty or not {"Code", "Date"}.issubset(frame.columns):
        return pd.Series(dtype="int64", index=pd.DatetimeIndex([]))
    dates = pd.to_datetime(frame["Date"], errors="coerce")
    codes = frame["Code"].astype(str)
    return codes.groupby(dates).nunique().sort_index()


def coverage_baseline(counts: pd.Series, day, *, exclude=(),
                      sessions: int = COVERAGE_BASELINE_SESSIONS) -> Optional[float]:
    """day 직전 기존 거래일 sessions 개(없으면 직후)의 종목 수 중앙값. 기준이 없으면 None.

    exclude(이번 쓰기 대상 날짜)는 기준에서 뺀다 — 다시 쓰는 날짜가 자기 기준이 되지 않게.
    """
    day = pd.Timestamp(day)
    skip = {pd.Timestamp(d) for d in exclude}
    usable = counts[[d not in skip for d in counts.index]] if len(counts) else counts
    window = usable[usable.index < day].tail(sessions)
    if window.empty:
        window = usable[usable.index > day].head(sessions)
    return None if window.empty else float(window.median())


def is_coverage_short(count: int, baseline: float) -> bool:
    """정수 비교로 경계를 고정한다 — 기준의 90.0% 는 통과, 그 아래는 미달."""
    return count * 100 < baseline * (100 - COVERAGE_DROP_PCT)


def check_write_coverage(merged: pd.DataFrame, existing: pd.DataFrame, write_dates, *,
                         baseline_extra: Optional[pd.DataFrame] = None):
    """병합 후 저장될 merged 에서 이번 쓰기가 건드린 날짜별 종목 수를 기존 기준과 비교한다.

    반환: (미달 목록 [{"date","count","baseline"}], 기준이 없어 판정 못 한 날짜 목록)
    """
    after = session_counts(merged)
    base = session_counts(existing)
    if baseline_extra is not None and len(base) < COVERAGE_BASELINE_SESSIONS:
        # 연초에는 같은 해 기준이 모자라다 — 전년도 마지막 거래일들을 앞에 붙인다.
        base = pd.concat([session_counts(baseline_extra), base])
        base = base.groupby(level=0).max().sort_index()
    targets = sorted({pd.Timestamp(d) for d in write_dates})
    shortfalls, unverified = [], []
    for day in targets:
        count = int(after.get(day, 0))
        baseline = coverage_baseline(base, day, exclude=targets)
        if baseline is None:
            unverified.append((day, count))
        elif baseline >= COVERAGE_MIN_UNIVERSE and is_coverage_short(count, baseline):
            shortfalls.append({"date": day, "count": count, "baseline": baseline})
    return shortfalls, unverified


def coverage_metrics(day, count: int, baseline: Optional[float], source: str) -> dict:
    """공개 로그용 검토된 필드(execution_safety._PUBLIC_METRICS)."""
    metrics = {"kr_date": pd.Timestamp(day).date().isoformat(), "kr_count": int(count),
               "kr_source": source}
    if baseline:
        metrics |= {"kr_baseline": int(round(baseline)), "kr_ratio_pct": count / baseline * 100}
    return metrics


def _require_write_coverage(merged, existing, write_dates, year: int):
    extra = None
    if len(session_counts(existing)) < COVERAGE_BASELINE_SESSIONS and local_path(year - 1).exists():
        try:
            extra = load_year(year - 1, strict=True)
        except Exception:
            extra = None                       # 전년도 기준을 못 읽으면 같은 해 기준만 쓴다
    shortfalls, unverified = check_write_coverage(merged, existing, write_dates,
                                                  baseline_extra=extra)
    for day, count in unverified:
        logger.warning("kr_coverage_unverified",
                       extra={"public_metrics": coverage_metrics(day, count, None, "save_year")})
    if shortfalls:
        worst = min(shortfalls, key=lambda item: item["count"] / item["baseline"])
        logger.error("kr_coverage_gap_blocked", extra={"public_metrics": coverage_metrics(
            worst["date"], worst["count"], worst["baseline"], "save_year")})
        raise KrCoverageError("kr_coverage_gap")


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
        frame = pq.read_table(str(path)).to_pandas()
        if not {"Code", "Date"}.issubset(frame.columns):
            raise KrStateError("kr_baseline_schema_invalid")
        return _validated_frame(frame, year)
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


def file_metadata(year: int) -> dict:
    """저장본 parquet 의 사용자 메타데이터(pandas 키 제외). 없거나 못 읽으면 빈 dict.

    열을 바꾸지 않고 파일에 붙여 두는 정보다 — 공식 확정 경계 등(data/kr_official.py).
    """
    path = local_path(year)
    if not path.exists():
        return {}
    try:
        metadata = pq.read_schema(str(path)).metadata or {}
    except Exception:
        return {}
    return {key: value for key, value in metadata.items() if key != b"pandas"}


def _existing_metadata(year: int) -> dict:
    return file_metadata(year)


def _write_staged_frame(df: pd.DataFrame, year: int, destination: Path, metadata: Optional[dict] = None):
    table = pa.Table.from_pandas(df, preserve_index=False)
    if metadata:
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), **metadata})
    pq.write_table(table, str(destination), compression="snappy")
    staged = _read_year_path(destination, year)
    pd.testing.assert_frame_equal(staged, df, check_dtype=False, check_exact=True)
    # Windows _commit requires a descriptor opened for writing.
    with destination.open("r+b") as handle:
        os.fsync(handle.fileno())


def _prepare_year(df: pd.DataFrame, year: int, *, ohlc_only=False, allow_gap=False,
                  file_metadata: Optional[dict] = None, drop_dates=(), drop_keys=()):
    """저장될 연도 프레임과 메타데이터를 만든다(쓰지 않는다). save_year·preview_year 공용.

    drop_dates 는 병합 전에 기존 파일에서 그 날짜의 행을 뺀다 — 검증된 휴장일 복사 행
    삭제(kr_official.verify_closed_sessions)만 이 경로를 쓴다. drop_keys 는 (Code, Date) 쌍 —
    검증된 상장 전 행 삭제(kr_official.verify_prelisting_keys)만 쓴다.
    """
    existing = load_year(year, strict=True)
    drops = {pd.Timestamp(d).normalize() for d in drop_dates}
    if drops and not existing.empty:
        existing = existing[~existing["Date"].isin(drops)].reset_index(drop=True)
    keys = {(str(c), pd.Timestamp(d).normalize()) for c, d in drop_keys}
    if keys and not existing.empty:
        pairs = pd.Series(list(zip(existing["Code"].astype(str), existing["Date"])), index=existing.index)
        existing = existing[~pairs.isin(keys)].reset_index(drop=True)
    metadata = {**_existing_metadata(year), **(file_metadata or {})}
    write_dates = set(pd.to_datetime(df["Date"], errors="coerce").dropna()) if "Date" in df else set()
    merged = _merge_year(df, existing, year, ohlc_only=ohlc_only)
    if not allow_gap:
        # 쓰기 전에 판정한다 — 실패하면 기존 파일은 한 바이트도 바뀌지 않는다.
        _require_write_coverage(merged, existing, write_dates, year)
    return merged, metadata


def preview_year(df: pd.DataFrame, year: int, **kwargs) -> pd.DataFrame:
    """save_year 가 쓸 결과를 같은 병합·게이트 경로로 만들어 돌려준다(파일 무변경)."""
    merged, _ = _prepare_year(df, year, **kwargs)
    return merged


def save_year(df: pd.DataFrame, year: int, *, ohlc_only=False, allow_gap=False,
              file_metadata: Optional[dict] = None, drop_dates=(), drop_keys=()):
    """
    연도별 Parquet 저장.
    기존 파일은 strict 읽기 후 병합. 검증된 임시 Parquet만 원자 교체한다.
    ohlc_only=True이면 기존 실측 Amount도 yfinance 추정치보다 우선한다.
    allow_gap=False(기본)이면 병합 결과에서 이번에 쓰는 날짜별 종목 수를 기존 기준과
    비교해, 90% 미만인 날이 있으면 아무것도 쓰지 않고 KrCoverageError 로 멈춘다.
    file_metadata 는 기존 파일의 사용자 메타데이터 위에 덮어쓴다. 주지 않으면 기존 것을
    그대로 이어받는다 — 잠정 저장이 공식 확정 경계를 지우지 않게. 빈 df 에 file_metadata
    만 주면 데이터는 그대로 두고 메타데이터만 갱신한다. drop_dates 는 _prepare_year 참고.
    """
    if df.empty and not file_metadata and not drop_dates and not drop_keys:
        logger.warning(f"[KrDB] 빈 DataFrame → 저장 건너뜀: marcap-{year}")
        return

    path = local_path(year)
    path.parent.mkdir(parents=True, exist_ok=True)

    df, metadata = _prepare_year(df, year, ohlc_only=ohlc_only, allow_gap=allow_gap,
                                 file_metadata=file_metadata, drop_dates=drop_dates,
                                 drop_keys=drop_keys)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".marcap-{year}-",
                                         suffix=".parquet", delete=False) as handle:
            temporary = Path(handle.name)
        _write_staged_frame(df, year, temporary, metadata)
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


def append_rows(new_df: pd.DataFrame, *, ohlc_only=False, allow_gap=False) -> list[int]:
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
        save_year(year_df, int(year), ohlc_only=ohlc_only, allow_gap=allow_gap)
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
    if not remote_path:
        return [f"marcap-{year}.parquet" for year in years]
    failed: list[str] = []
    for year in years:
        path = local_path(year)
        if not path.exists():
            logger.warning(f"[KrDB] 업로드 대상 없음: {path.name}")
            failed.append(path.name)
            continue
        try:
            load_year(year, strict=True)
            result = u.upload(str(path), remote_path)
            if result is not True and not (isinstance(result, str) and result.strip()):
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
    """Stage and validate all partitions before preserving the local/remote union."""
    import re
    u = _get_uploader(uploader)
    remote_path = config.DRIVE_PATHS.get("ohlc_kr")
    if u is None or not remote_path:
        raise RuntimeError("kr_baseline_unavailable")
    _LOCAL_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".kr-baseline-", dir=_LOCAL_ROOT) as directory:
        stage = Path(directory)
        state = u.download_all_state(remote_path, str(stage), extensions=(".parquet",))
        if state not in {"ok", "absent"}:
            raise RuntimeError("kr_baseline_failed")
        if state == "absent" and any(stage.iterdir()):
            raise RuntimeError("kr_baseline_state_mismatch")
        candidates = []
        for path in sorted(stage.glob("*.parquet")):
            match = re.fullmatch(r"marcap-([0-9]{4})\.parquet", path.name)
            if not match:
                raise RuntimeError("kr_baseline_filename_invalid")
            year = int(match.group(1))
            incoming = _read_year_path(path, year)
            existing = load_year(year, strict=True)
            merged = _merge_year(incoming, existing, year) if not existing.empty else incoming
            _write_staged_frame(merged, year, path)
            candidates.append((path, local_path(year)))
        # Local-only partitions are also validated before the first promotion.
        for path in _LOCAL_ROOT.glob("marcap-*.parquet"):
            match = re.fullmatch(r"marcap-([0-9]{4})\.parquet", path.name)
            if not match:
                raise RuntimeError("kr_local_filename_invalid")
            load_year(int(match.group(1)), strict=True)
        if state == "ok" and not candidates:
            raise RuntimeError("kr_baseline_empty_response")
        for source, destination in candidates:
            os.replace(source, destination)
        return state
