"""재무 연도 기준본의 엄격한 읽기·병합·검증 후 교체 및 게시."""

import logging
import math
import numbers
import os
import re
import tempfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import config

logger = logging.getLogger(__name__)
_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db"
_FINANCIALS_COLS = [
    "Ticker", "PeriodDate", "Year", "Quarter", "Revenue", "GrossProfit",
    "OperatingIncome", "NetIncome", "EBITDA", "DilutedEPS", "TotalAssets",
    "TotalLiabilities", "Equity", "OperatingCashFlow", "FreeCashFlow", "CapEx", "SnapDate",
]
_RATIOS_COLS = [
    "Ticker", "SnapDate", "Name", "Sector", "Industry", "MarketCap", "SharesOutstanding",
    "PE", "ForwardPE", "PB", "PS", "ROE", "ROA", "DebtToEquity", "Beta",
    "HeldPctInstitutions", "DividendYield", "EPS", "ProfitMargin", "OperatingMargin",
    "RevenueGrowth", "EarningsGrowth", "CurrentRatio",
]


class FinancialsStateError(RuntimeError):
    """원문 경로·응답·비밀을 담지 않는 실패 코드."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class FinancialsPublishError(FinancialsStateError):
    def __init__(self, failed):
        self.failed = tuple(failed)
        super().__init__("financials_publish_failed")


def _partition(market, kind, year=None):
    if market not in {"us", "kr", "crypto"} or kind not in {"financials", "ratios"}:
        raise FinancialsStateError("financials_partition_invalid")
    if year is not None and (isinstance(year, bool) or not isinstance(year, numbers.Integral)
                             or not 1900 <= year <= 9999):
        raise FinancialsStateError("financials_partition_invalid")


def local_financials_path(market, year):
    _partition(market, "financials", year)
    return _LOCAL_ROOT / market / "financials" / f"{market}_financials_{year}.parquet"


def local_ratios_path(market, year):
    _partition(market, "ratios", year)
    return _LOCAL_ROOT / market / "ratios" / f"{market}_ratios_{year}.parquet"


def _columns(kind):
    return _FINANCIALS_COLS if kind == "financials" else _RATIOS_COLS


def _date_column(kind):
    return "PeriodDate" if kind == "financials" else "SnapDate"


def _dates(series):
    values = []
    for value in series:
        if not isinstance(value, (str, date, datetime, pd.Timestamp)) or pd.isna(value):
            raise FinancialsStateError("financials_date_invalid")
        try:
            stamp = pd.Timestamp(value)
            if pd.isna(stamp) or stamp.tzinfo is not None or stamp != stamp.normalize():
                raise ValueError()
            values.append(stamp.date())
        except Exception:
            raise FinancialsStateError("financials_date_invalid") from None
    return pd.Series(values, index=series.index, dtype=object)


def _validated_frame(frame, kind, year=None):
    if not isinstance(frame, pd.DataFrame) or frame.columns.has_duplicates:
        raise FinancialsStateError("financials_schema_invalid")
    primary = _date_column(kind)
    required = {"Ticker", primary}
    if kind == "financials":
        required |= {"Year", "Quarter", "SnapDate"}
    if not required.issubset(frame.columns):
        raise FinancialsStateError("financials_schema_invalid")
    result = frame.copy()
    if not result["Ticker"].map(lambda x: isinstance(x, str) and bool(x.strip())
                                and x == x.strip()).all():
        raise FinancialsStateError("financials_ticker_invalid")
    for column in (["PeriodDate", "SnapDate"] if kind == "financials" else ["SnapDate"]):
        result[column] = _dates(result[column])
    if year is not None and not result[primary].map(lambda x: x.year == year).all():
        raise FinancialsStateError("financials_year_invalid")
    if result.duplicated(["Ticker", primary]).any():
        raise FinancialsStateError("financials_duplicate_key")
    if kind == "financials":
        for column in ("Year", "Quarter"):
            if not result[column].map(lambda x: not isinstance(x, bool)
                                     and isinstance(x, numbers.Real) and math.isfinite(x)
                                     and int(x) == x).all():
                raise FinancialsStateError("financials_period_invalid")
        if not all(y == d.year and q == (d.month - 1) // 3 + 1
                   for y, q, d in zip(result["Year"], result["Quarter"], result["PeriodDate"])):
            raise FinancialsStateError("financials_period_invalid")
    text = {"Ticker", "PeriodDate", "SnapDate", "Name", "Sector", "Industry", "Year", "Quarter"}
    for column in (set(_columns(kind)) & set(result.columns)) - text:
        if not result[column].map(lambda x: x is None or x is pd.NA or
                                 (isinstance(x, numbers.Real) and not isinstance(x, bool)
                                  and (pd.isna(x) or math.isfinite(x)))).all():
            raise FinancialsStateError("financials_value_invalid")
    columns = [c for c in _columns(kind) if c in result] + [c for c in result if c not in _columns(kind)]
    return result[columns].sort_values(["Ticker", primary]).reset_index(drop=True)


def _read_path(path, kind, year):
    try:
        return _validated_frame(pq.read_table(str(path)).to_pandas(), kind, year)
    except FinancialsStateError:
        raise
    except Exception:
        raise FinancialsStateError("financials_read_failed") from None


def _load(market, kind, year):
    _partition(market, kind, year)
    path = _LOCAL_ROOT / market / kind / f"{market}_{kind}_{year}.parquet"
    if not path.exists():
        return pd.DataFrame(columns=_columns(kind))
    return _read_path(path, kind, year)


def load_financials_year(market, year):
    """파일 부재만 빈 기준본이다. 존재하는 손상 파일은 실패한다."""
    return _load(market, "financials", year)


def load_ratios_year(market, year):
    return _load(market, "ratios", year)


def _merge(previous, incoming, kind, year):
    incoming = _validated_frame(incoming, kind, year)
    if previous.empty:
        return incoming
    previous = _validated_frame(previous, kind, year)
    keys = ["Ticker", _date_column(kind)]
    # 유효한 새 값이 우선이며 새 결측은 기존 관측값을 지우지 않는다.
    merged = incoming.set_index(keys).combine_first(previous.set_index(keys)).reset_index()
    return _validated_frame(merged, kind, year)


def _stage_frame(path, frame, kind, year):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".financials-", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    staged = Path(name)
    try:
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), str(staged), compression="snappy")
        actual = _read_path(staged, kind, year)
        pd.testing.assert_frame_equal(actual, frame, check_dtype=False, check_exact=True)
        with staged.open("r+b") as handle:
            os.fsync(handle.fileno())
        return staged
    except Exception:
        staged.unlink(missing_ok=True)
        raise FinancialsStateError("financials_stage_failed") from None


def _promote(candidates):
    """모든 후보 재검증을 끝낸 뒤 파일별 원자 교체. 다중 파일 트랜잭션은 아니다."""
    staged = []
    try:
        for path, frame, kind, year in candidates:
            staged.append((_stage_frame(path, frame, kind, year), path))
        for temporary, path in staged:
            os.replace(temporary, path)
    except FinancialsStateError:
        raise
    except Exception:
        raise FinancialsStateError("financials_replace_failed") from None
    finally:
        for temporary, _ in staged:
            temporary.unlink(missing_ok=True)


def _save(frame, market, kind):
    _partition(market, kind)
    if frame.empty:
        return []
    frame = _validated_frame(frame, kind)
    primary = _date_column(kind)
    candidates = []
    loader = load_financials_year if kind == "financials" else load_ratios_year
    # 두 번째 연도 기준본이 손상되었어도 첫 번째 연도를 먼저 바꾸지 않는다.
    for year in sorted({d.year for d in frame[primary]}):
        incoming = frame[frame[primary].map(lambda d: d.year == year)]
        try:
            previous = loader(market, year)
        except Exception:
            raise FinancialsStateError("financials_baseline_invalid") from None
        merged = _merge(previous, incoming, kind, year)
        path = _LOCAL_ROOT / market / kind / f"{market}_{kind}_{year}.parquet"
        candidates.append((path, merged, kind, year))
    _promote(candidates)
    return [year for _, _, _, year in candidates]


def save_financials(new_df, market):
    return _save(new_df, market, "financials")


def save_ratios(new_df, market):
    return _save(new_df, market, "ratios")


def _save_financials_year(frame, market, year):
    return _save(_validated_frame(frame, "financials", year), market, "financials")


def _save_ratios_year(frame, market, year):
    return _save(_validated_frame(frame, "ratios", year), market, "ratios")


def _get_uploader(uploader=None):
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception:
        raise FinancialsStateError("financials_uploader_unavailable") from None


def _financials_drive_key(market):
    return f"{market}_financials"


def _baseline_remote_and_local(market, kind):
    _partition(market, kind)
    remote = config.DRIVE_PATHS.get(f"{market}_{kind}")
    return remote, _LOCAL_ROOT / market / kind


def _all_local(market, kind, directory):
    """선택한 디렉터리의 모든 연도 파일을 검증한다. 임의 이름은 채택하지 않는다."""
    result = {}
    if not directory.exists():
        return result
    pattern = re.compile(re.escape(f"{market}_{kind}_") + r"([0-9]{4})\.parquet")
    for path in sorted(directory.glob("*.parquet")):
        match = pattern.fullmatch(path.name)
        if match is None or not path.is_file() or path.is_symlink():
            raise FinancialsStateError("financials_filename_invalid")
        year = int(match.group(1))
        _partition(market, kind, year)
        result[year] = _read_path(path, kind, year)
    return result


def ensure_local_baseline(market, kinds=("financials", "ratios")):
    """upload=False도 기존 손상을 수집 전에 감지한다."""
    for kind in kinds:
        _, directory = _baseline_remote_and_local(market, kind)
        _all_local(market, kind, directory)


def ensure_drive_baseline(market, kinds=("financials", "ratios"), uploader=None):
    """staging에서 전체 원격 기준본을 검증·병합한 뒤 승격하고 동일 client를 반환한다."""
    u = _get_uploader(uploader)
    if u is None:
        raise FinancialsStateError("financials_uploader_unavailable")
    candidates = []
    with tempfile.TemporaryDirectory(prefix="financials-baseline-") as root:
        for kind in kinds:
            remote, local = _baseline_remote_and_local(market, kind)
            if not remote:
                raise FinancialsStateError("financials_drive_path_missing")
            previous = _all_local(market, kind, local)
            stage = Path(root) / market / kind
            stage.mkdir(parents=True)
            try:
                state = u.download_all_state(remote, str(stage), extensions=(".parquet",))
            except Exception:
                raise FinancialsStateError("financials_baseline_download_failed") from None
            if state not in {"ok", "absent"}:
                raise FinancialsStateError("financials_baseline_download_failed")
            incoming = _all_local(market, kind, stage)
            if (state == "absent" and incoming) or (state == "ok" and not incoming):
                raise FinancialsStateError("financials_baseline_state_invalid")
            for year, frame in incoming.items():
                merged = _merge(previous.get(year, pd.DataFrame(columns=_columns(kind))), frame, kind, year)
                candidates.append((local / f"{market}_{kind}_{year}.parquet", merged, kind, year))
        _promote(candidates)
    return u


def _upload(market, kind, years, uploader):
    u = _get_uploader(uploader)
    if u is None:
        raise FinancialsStateError("financials_uploader_unavailable")
    remote, local = _baseline_remote_and_local(market, kind)
    if not remote:
        raise FinancialsStateError("financials_drive_path_missing")
    paths = []
    for year in sorted(set(years)):
        _partition(market, kind, year)
        path = local / f"{market}_{kind}_{year}.parquet"
        if not path.is_file():
            raise FinancialsStateError("financials_publish_file_missing")
        _read_path(path, kind, year)
        paths.append(path)
    failed = []
    for path in paths:
        try:
            outcome = u.upload(str(path), remote)
            # 실제 DriveUploader는 확인한 파일 ID를 반환한다. True 대역도 호환한다.
            confirmed = outcome is True or (isinstance(outcome, str)
                         and re.fullmatch(r"[A-Za-z0-9_-]+", outcome) is not None)
            if not confirmed:
                failed.append(path.name)
        except Exception:
            failed.append(path.name)
    if failed:
        raise FinancialsPublishError(failed)
    return []


def upload_financials(market, years, uploader=None):
    return _upload(market, "financials", years, uploader)


def upload_ratios(market, years, uploader=None):
    return _upload(market, "ratios", years, uploader)


def publish_local_baseline(market, kinds=("financials", "ratios"), uploader=None, *, published=None):
    """이전 게시 실패 후 증분 스킵된 로컬 이력도 같은 실행에서 다시 게시한다."""
    batches = []
    for kind in kinds:
        _, directory = _baseline_remote_and_local(market, kind)
        completed = (published or {}).get(kind, set())
        batches.append((kind, sorted(set(_all_local(market, kind, directory)) - set(completed))))
    for kind, years in batches:
        if years:
            _upload(market, kind, years, uploader)


def download_financials_all(market, uploader=None):
    return ensure_drive_baseline(market, ("financials",), uploader)


def download_ratios_all(market, uploader=None):
    return ensure_drive_baseline(market, ("ratios",), uploader)
