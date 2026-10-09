"""Cooperative local writer exclusion and public logs without provider payloads."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import logging
import math
import numbers
import os
from pathlib import Path
import re
import sys


class WriterBusyError(RuntimeError):
    pass


@contextmanager
def writer_lock(root=None):
    if root is None:
        import config
        root = config.LOCAL_DATA_DIR
    directory = Path(root).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / ".daily-writer.lock"
    if path.is_symlink():
        raise WriterBusyError("writer_lock_invalid")
    stream = path.open("a+b")
    acquired = False
    try:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            raise WriterBusyError("writer_busy") from None
        yield
    finally:
        if acquired:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
        # Never unlink: another process may hold/open this same lock inode.


_SOURCES = {
    "main.py", "execution_safety.py", "collector.py", "historical.py", "storage.py",
    "progress.py", "drive_uploader.py", "ohlc_collector.py", "ohlc_db.py",
    "kr_collector.py", "kr_db.py", "financials_collector.py", "financials_db.py",
    "kr_financials_collector.py", "resave_ohlc.py", "verify_ohlc.py", "verify_kr.py",
}
_EVENTS = {"operation_failed", "writer_busy", "external_output_suppressed",
           "collection_started", "collection_completed", "dry_run", "status_requested",
           "collection_incomplete_tolerated", "session_unverified_tolerated",
           "market_failed", "cursor_held_for_sparse_session",
           "sparse_session_abandoned", "sparse_session_excluded",
           "price_basis_mismatch_tolerated",
           "kr_market_closed", "kr_calendar_unavailable", "kr_source_fallback_attempted",
           "rebase_started", "rebase_completed", "rebase_deferred",
           "stale_ticker_excluded",
           "kr_coverage_checked", "kr_coverage_insufficient", "kr_coverage_unverified",
           "kr_coverage_gap_blocked"}
# Every snake_case code raised anywhere in data/, scripts/ and main.py. A code that is
# missing here is silently dropped from the artifact, so a failure shows as a bare
# operation_failed with no cause — that is how the 2026-09-22 OHLC outage stayed opaque
# (collection_incomplete was raised but unlisted, while a renamed incremental_incomplete
# lingered here unraised). tests/test_execution_safety_codes.py keeps the two in step.
_ERROR_CODES = {
    "collection_failed", "collection_incomplete", "collection_invalid_dates",
    "collection_unexpected_ticker", "coverage_gap", "coverage_shrink",
    "drive_baseline_failed", "drive_credentials_absent", "drive_sync_failed",
    "drive_destination_invalid", "drive_file_absent", "drive_file_ambiguous",
    "drive_folder_ambiguous", "drive_id_invalid", "drive_listing_duplicate",
    "drive_listing_incomplete", "drive_listing_invalid", "drive_listing_limit",
    "drive_name_invalid", "drive_pagination_invalid", "drive_path_invalid",
    "drive_promotion_failed", "drive_setup_unavailable", "drive_upload_directory_absent",
    "drive_upload_files_absent", "drive_upload_identity_mismatch",
    "drive_upload_source_invalid", "drive_upload_unconfirmed", "empty_universe",
    "empty_unverified", "fdr_version_unverified", "financials_baseline_download_failed",
    "financials_baseline_invalid", "financials_baseline_state_invalid",
    "financials_collection_empty", "financials_dart_unavailable", "financials_date_invalid",
    "financials_drive_path_missing", "financials_duplicate_key", "financials_filename_invalid",
    "financials_partition_invalid", "financials_period_invalid",
    "financials_publish_failed", "financials_publish_file_missing", "financials_read_failed",
    "financials_replace_failed",
    "financials_schema_invalid", "financials_stage_failed", "financials_ticker_invalid",
    "financials_uploader_unavailable", "financials_value_invalid", "financials_year_invalid",
    "kr_baseline_empty_response", "kr_baseline_failed", "kr_baseline_filename_invalid",
    "kr_baseline_invalid", "kr_baseline_schema_invalid", "kr_baseline_state_mismatch",
    "kr_baseline_unavailable", "kr_collection_empty_unverified", "kr_coverage_gap",
    "kr_coverage_insufficient", "kr_local_filename_invalid",
    "kr_snapshot_invalid", "kr_source_failed", "kr_source_too_large", "ohlc_baseline_absent",
    "ohlc_baseline_failed", "ohlc_baseline_incomplete", "ohlc_baseline_inconsistent",
    "ohlc_baseline_name_invalid", "ohlc_market_invalid", "ohlc_remote_unconfigured",
    "ohlc_uploader_unavailable", "pending_baseline_failed", "pending_baseline_invalid",
    "pending_baseline_unavailable", "price_basis_mismatch", "price_basis_unverified",
    "price_values_unverified", "rebase_incomplete", "resave_files_absent", "resave_publication_failed",
    "resave_requested_file_absent", "sector_baseline_invalid", "sector_collection_empty",
    "sector_download_failed", "sector_download_unavailable", "sector_field_invalid",
    "sector_info_unverified", "sector_keys_invalid", "sector_observation_invalid",
    "sector_publication_failed", "sector_schema_invalid",
    "sector_ticker_coverage_shrink", "sector_timestamp_invalid", "sector_upload_unavailable",
    "sector_upload_unconfirmed", "session_unverified", "source_date_unverified",
    "universe_unverified", "writer_busy", "writer_lock_invalid",
}


_DATE_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}")
_METRIC_SOURCES = frozenset({"fdr_snapshot", "yfinance_fallback", "save_year", "official_marcap"})


def _metric_date(value):
    return value if isinstance(value, str) and _DATE_TEXT.fullmatch(value) else None


def _metric_count(value):
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        return None
    return str(int(value)) if 0 <= int(value) < 10 ** 9 else None


def _metric_pct(value):
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        return None
    value = float(value)
    return f"{value:.1f}" if math.isfinite(value) and 0 <= value <= 10_000 else None


def _metric_source(value):
    return value if isinstance(value, str) and value in _METRIC_SOURCES else None


# 공개 artifact 에 숫자로만 남기는 검토된 필드(2026-10-10, KR 종목 범위 게이트).
# 보존 계약(docs/2026-09-22-data-preservation.md)의 "원문 대신 별도 검토한 구조화 필드"
# 다. 값마다 형식을 검사하고 통과한 것만 고정 순서로 쓴다 — 경로·URL·임의 문자열은
# 형식이 맞지 않아 버려진다. 새 필드는 여기에 검사기와 함께 추가한다.
_PUBLIC_METRICS = (
    ("kr_date", _metric_date),
    ("kr_count", _metric_count),
    ("kr_baseline", _metric_count),
    ("kr_ratio_pct", _metric_pct),
    ("kr_source", _metric_source),
)


def _format_metrics(record):
    metrics = getattr(record, "public_metrics", None)
    if not isinstance(metrics, dict):
        return ""
    parts = []
    for name, check in _PUBLIC_METRICS:
        if name in metrics:
            text = check(metrics[name])
            if text is not None:
                parts.append(f"{name}={text}")
    return (" " + " ".join(parts)) if parts else ""


class PublicFormatter(logging.Formatter):
    """Never interpolate raw messages, args, exception or stack text into artifacts.

    Stable source/line identifies the operation. Explicit reviewed event codes
    remain readable; provider URLs, bodies, IDs and even unrecognizable secrets
    are omitted instead of relying on a secret-shaped regular expression.
    """
    def format(self, record):
        source = record.filename if record.filename in _SOURCES else "external"
        event = record.msg if isinstance(record.msg, str) and record.msg in _EVENTS else "application_event"
        level = record.levelname if record.levelname in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"} else "LOG"
        code = getattr(record, "failure_code", None)
        detail = f" code={code}" if isinstance(code, str) and code in _ERROR_CODES else ""
        return (f"{self.formatTime(record)} [{level}] {source}:{int(record.lineno)} {event}{detail}"
                f"{_format_metrics(record)}")


def configure_logging(path=None):
    previous_factory = logging.getLogRecordFactory()
    if not getattr(previous_factory, "_public_record_factory", False):
        def safe_factory(*args, **kwargs):
            record = previous_factory(*args, **kwargs)
            source = record.filename if record.filename in _SOURCES else "external"
            record.name = source
            record.pathname = source
            record.msg = record.msg if isinstance(record.msg, str) and record.msg in _EVENTS else "application_event"
            record.args = ()
            record.exc_info = record.exc_text = record.stack_info = None
            return record
        safe_factory._public_record_factory = True
        logging.setLogRecordFactory(safe_factory)
    stream = logging.StreamHandler(sys.stdout)
    handlers = [stream]
    if path is not None:
        handlers.append(logging.FileHandler(path, encoding="utf-8"))
    for handler in handlers:
        handler.setFormatter(PublicFormatter())
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
    # Existing provider-specific handlers could otherwise bypass the formatter.
    for name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger):
            logger.handlers.clear()
            logger.propagate = True
    return handlers


class _SuppressedOutput:
    encoding = "utf-8"
    def __init__(self):
        self.reported = False
        self.buffer = self
    def write(self, value):
        if value and str(value).strip() and not self.reported:
            self.reported = True
            logging.getLogger(__name__).warning("external_output_suppressed")
        return len(value)
    def flush(self):
        pass
    def isatty(self):
        return False


@contextmanager
def public_output():
    """Contain Python-level provider print/progress on public collection CLI."""
    with redirect_stdout(_SuppressedOutput()), redirect_stderr(_SuppressedOutput()):
        yield


def cli_entry(function):
    try:
        with public_output():
            return function() or 0
    except WriterBusyError:
        logging.getLogger(__name__).error("writer_busy")
        return 2
    except Exception as error:
        code = getattr(error, "code", None)
        if code is None and len(error.args) == 1:
            code = error.args[0]
        if not isinstance(code, str) or code not in _ERROR_CODES:
            code = None
        logging.getLogger(__name__).error("operation_failed", extra={"failure_code": code})
        return 1
