"""Cooperative local writer exclusion and public logs without provider payloads."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import logging
import os
from pathlib import Path
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
           "collection_started", "collection_completed", "dry_run", "status_requested"}
_ERROR_CODES = {
    "price_basis_unverified", "price_basis_mismatch", "source_date_unverified",
    "universe_unverified", "empty_unverified", "session_unverified",
    "incremental_incomplete", "fdr_version_unverified", "kr_source_failed",
    "kr_snapshot_invalid", "kr_baseline_failed", "kr_baseline_invalid",
    "sector_observation_unverified", "sector_baseline_invalid", "sector_download_failed",
    "sector_publication_failed", "sector_collection_empty", "sector_ticker_coverage_shrink",
    "pending_baseline_unavailable", "pending_baseline_failed", "pending_baseline_invalid",
    "resave_publication_failed", "drive_baseline_failed", "drive_listing_incomplete",
    "drive_file_ambiguous", "drive_folder_ambiguous", "drive_credentials_absent",
}


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
        return f"{self.formatTime(record)} [{level}] {source}:{int(record.lineno)} {event}{detail}"


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
