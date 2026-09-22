"""Synthetic public-entry boundaries; no provider, real config or operational data."""
import argparse
import ast
import builtins
from contextlib import contextmanager
from datetime import datetime
import io
import logging
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from data import execution_safety as safety
import main as entry


CANARY = "ordinary violet paper kite 7391"
MODES = ("daily", "bootstrap", "ohlc-backfill", "ohlc-update", "ohlc-new-backfill",
         "financials-update", "kr-daily", "kr-backfill", "sector-meta")
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def isolated_logging():
    """Do not close pytest's handlers or leave the process-wide factory changed."""
    root = logging.getLogger()
    factory = logging.getLogRecordFactory()
    original_handlers, original_level = root.handlers[:], root.level
    originals = {name: (logger, logger.handlers[:], logger.propagate, logger.level, logger.disabled)
                 for name, logger in logging.Logger.manager.loggerDict.items()
                 if isinstance(logger, logging.Logger)}
    root.handlers = []
    for logger, _, _, _, _ in originals.values():
        logger.handlers = []
    try:
        yield
    finally:
        original_ids = {id(handler) for handler in original_handlers}
        original_ids.update(id(handler) for _, handlers, _, _, _ in originals.values() for handler in handlers)
        current = [root, *(logger for logger in logging.Logger.manager.loggerDict.values()
                          if isinstance(logger, logging.Logger))]
        closed = set()
        for logger in current:
            for handler in logger.handlers:
                if id(handler) not in original_ids and id(handler) not in closed:
                    handler.close()
                    closed.add(id(handler))
            logger.handlers = []
        root.handlers, root.level = original_handlers, original_level
        for logger, handlers, propagate, level, disabled in originals.values():
            logger.handlers, logger.propagate = handlers, propagate
            logger.level, logger.disabled = level, disabled
        logging.setLogRecordFactory(factory)


def forbidden(*args, **kwargs):
    raise AssertionError("Unexpected provider/storage/Drive invocation")


@contextmanager
def no_product_imports(monkeypatch):
    original = builtins.__import__
    calls = []

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"data", "config", "yfinance", "FinanceDataReader", "pykrx",
                                  "OpenDartReader", "googleapiclient", "requests"}:
            calls.append(name)
            raise AssertionError("Dry-run imported an operational dependency")
        return original(name, *args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(builtins, "__import__", guarded)
        yield calls


@pytest.mark.parametrize("mode", MODES)
def test_each_run_dry_run_returns_before_other_args_or_dependencies(mode, monkeypatch):
    class DryOnly:
        dry_run = True

        def __getattr__(self, name):
            raise AssertionError("Dry-run inspected an operational option")

    with no_product_imports(monkeypatch) as imports:
        assert getattr(entry, "run_" + mode.replace("-", "_"))(DryOnly()) is None
    assert imports == []


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("extra", [[], ["--upload-drive", "--status", "--force", "--market", "kr",
                                       "--start-date", CANARY, "--year", "2020", "--years-range", "4"]])
def test_main_dry_run_has_no_dispatch_lock_provider_or_storage(mode, extra, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--mode", mode, "--dry-run", *extra])
    monkeypatch.setattr(entry, "writer_lock", forbidden)
    for name in MODES:
        monkeypatch.setattr(entry, "run_" + name.replace("-", "_"), forbidden)
    with no_product_imports(monkeypatch) as imports:
        assert entry.main() == 0
    assert imports == []


def test_real_lock_reentry_rejected_and_persistent_inode_reusable(tmp_path):
    with safety.writer_lock(tmp_path):
        with pytest.raises(safety.WriterBusyError, match="^writer_busy$"):
            with safety.writer_lock(tmp_path):
                pytest.fail("Second writer entered")
    inode = (tmp_path / ".daily-writer.lock").stat().st_ino
    with safety.writer_lock(tmp_path):
        assert (tmp_path / ".daily-writer.lock").stat().st_ino == inode


def test_real_lock_exception_releases_and_other_root_is_independent(tmp_path):
    with pytest.raises(ValueError):
        with safety.writer_lock(tmp_path / "first"):
            with safety.writer_lock(tmp_path / "second"):
                raise ValueError(CANARY)
    with safety.writer_lock(tmp_path / "first"), safety.writer_lock(tmp_path / "second"):
        pass


def test_default_lock_uses_only_synthetic_config_local_data_dir(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(LOCAL_DATA_DIR=tmp_path))
    with safety.writer_lock():
        with pytest.raises(safety.WriterBusyError, match="writer_busy"):
            with safety.writer_lock(tmp_path):
                pytest.fail("Default root did not share the writer lock")


def test_symbolic_lock_path_is_rejected_before_open(tmp_path, monkeypatch):
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda path: True if path.name == ".daily-writer.lock" else original(path))
    with pytest.raises(safety.WriterBusyError, match="^writer_lock_invalid$"):
        with safety.writer_lock(tmp_path):
            pytest.fail("Unsafe lock entered")
    assert not (tmp_path / ".daily-writer.lock").exists()


@pytest.mark.parametrize("mode", MODES)
def test_main_dispatch_holds_the_same_real_lock(mode, tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(LOCAL_DATA_DIR=tmp_path))
    monkeypatch.setattr(sys, "argv", ["main.py", "--mode", mode])
    called = []

    def dispatch(args):
        called.append(args.mode)
        with pytest.raises(safety.WriterBusyError, match="writer_busy"):
            with safety.writer_lock(tmp_path):
                pytest.fail("Main did not hold shared lock")

    for name in MODES:
        monkeypatch.setattr(entry, "run_" + name.replace("-", "_"), dispatch if name == mode else forbidden)
    entry.main()
    assert called == [mode]
    with safety.writer_lock(tmp_path):
        pass


@pytest.mark.parametrize("script,argv,delegate", [
    ("resave_ohlc", ["--market", "us", "--upload"], "resave"),
    ("verify_ohlc", ["--drive", "--fix"], "verify"),
    ("verify_kr", ["--drive", "--fix"], "verify"),
])
def test_manual_cli_main_holds_same_lock_before_any_work(script, argv, delegate, tmp_path, monkeypatch):
    """Execute the actual main AST only, replacing all data work with a spy."""
    path = ROOT / "scripts" / (script + ".py")
    parsed = ast.parse(path.read_text(encoding="utf-8-sig"))
    function = next(node for node in parsed.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(LOCAL_DATA_DIR=tmp_path))
    monkeypatch.setattr(sys, "argv", [script, *argv])
    calls = []

    def spy(*args):
        calls.append(args)
        with pytest.raises(safety.WriterBusyError, match="writer_busy"):
            with safety.writer_lock(tmp_path):
                pytest.fail("Manual writer did not share main lock")
        return 19

    namespace = {"argparse": argparse, "datetime": datetime, "writer_lock": safety.writer_lock,
                 "__doc__": "synthetic entry", delegate: spy}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    assert namespace["main"]() == 19
    assert len(calls) == 1
    with safety.writer_lock(tmp_path):
        with pytest.raises(safety.WriterBusyError, match="writer_busy"):
            namespace["main"]()
    assert len(calls) == 1


@pytest.mark.parametrize("relative", ["main.py", "scripts/resave_ohlc.py", "scripts/verify_ohlc.py", "scripts/verify_kr.py"])
def test_actual_public_entry_block_configures_then_wraps_main(relative, monkeypatch):
    path = ROOT / relative
    parsed = ast.parse(path.read_text(encoding="utf-8-sig"))
    block = next(node for node in parsed.body if isinstance(node, ast.If)
                 and isinstance(node.test, ast.Compare) and isinstance(node.test.left, ast.Name)
                 and node.test.left.id == "__name__")
    calls = []
    main_spy = object()

    def configure(*args):
        calls.append("configure")

    def enter(function):
        assert function is main_spy
        calls.append("entry")
        return 17

    namespace = {"__name__": "__main__", "configure_logging": configure, "cli_entry": enter,
                 "main": main_spy, "sys": sys}
    with pytest.raises(SystemExit) as caught:
        exec(compile(ast.Module(body=[block], type_ignores=[]), str(path), "exec"), namespace)
    assert caught.value.code == 17
    assert calls == ["configure", "entry"]


def test_public_formatter_never_stringifies_raw_objects_or_exception():
    class Raw:
        def __str__(self):
            pytest.fail("Raw provider payload was interpolated")

    record = logging.LogRecord(CANARY, logging.ERROR, CANARY + ".py", 42, Raw(), (Raw(),),
                               (ValueError, ValueError(CANARY), None), sinfo=CANARY)
    record.exc_text = CANARY
    rendered = safety.PublicFormatter().format(record)
    assert CANARY not in rendered
    assert "[ERROR] external:42 application_event" in rendered


def test_print_tqdm_raw_logs_and_tracebacks_absent_from_all_sinks(tmp_path, monkeypatch, isolated_logging):
    from tqdm import tqdm

    stdout, stderr, late = io.StringIO(), io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    public_file = tmp_path / "public.log"
    safety.configure_logging(public_file)
    logger = logging.getLogger("synthetic.late.provider")
    handler = logging.StreamHandler(late)
    handler.setFormatter(logging.Formatter("%(message)s %(exc_text)s %(stack_info)s"))
    logger.addHandler(handler)
    late_file = logging.FileHandler(tmp_path / "late.log", encoding="utf-8")
    late_file.setFormatter(logging.Formatter("%(message)s %(exc_text)s %(stack_info)s"))
    logger.addHandler(late_file)

    def operation():
        print(CANARY)
        print(CANARY, file=sys.stderr)
        for _ in tqdm(range(2), desc=CANARY, mininterval=0):
            pass
        logger.warning("argument=%s", CANARY)
        try:
            raise ValueError(CANARY)
        except ValueError:
            logger.exception(CANARY, stack_info=True)
        raise RuntimeError(CANARY)

    assert safety.cli_entry(operation) == 1
    for active in [*logging.getLogger().handlers, handler, late_file]:
        active.flush()
    outputs = [stdout.getvalue(), stderr.getvalue(), late.getvalue(),
               public_file.read_text(encoding="utf-8"), (tmp_path / "late.log").read_text(encoding="utf-8")]
    assert all(CANARY not in value and "Traceback" not in value for value in outputs)
    assert "external_output_suppressed" in stdout.getvalue()
    assert "operation_failed" in stdout.getvalue()
    assert "application_event" in late.getvalue()
    assert stderr.getvalue() == ""


def test_configure_clears_existing_provider_handler_and_is_idempotent(monkeypatch, isolated_logging):
    stdout, provider_output = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    logger = logging.getLogger("synthetic.existing.provider")
    logger.handlers = [logging.StreamHandler(provider_output)]
    logger.propagate = False
    safety.configure_logging()
    first_factory = logging.getLogRecordFactory()
    safety.configure_logging()
    assert logging.getLogRecordFactory() is first_factory
    logger.error(CANARY)
    assert provider_output.getvalue() == ""
    assert CANARY not in stdout.getvalue()
    assert stdout.getvalue().count("application_event") == 1


@pytest.mark.parametrize("error,code,event", [(ValueError, 1, "operation_failed"),
                                             (safety.WriterBusyError, 2, "writer_busy")])
def test_cli_fixed_failure_code_and_lock_release(error, code, event, tmp_path, monkeypatch, isolated_logging):
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    safety.configure_logging()

    def operation():
        with safety.writer_lock(tmp_path):
            raise error(CANARY)

    assert safety.cli_entry(operation) == code
    assert event in stdout.getvalue()
    assert CANARY not in stdout.getvalue()
    with safety.writer_lock(tmp_path):
        pass


def test_argparse_invalid_raw_input_suppressed_by_actual_entry_wrapper(monkeypatch, isolated_logging):
    stdout, stderr = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(sys, "argv", ["main.py", "--mode", CANARY])
    safety.configure_logging()
    with pytest.raises(SystemExit) as caught:
        safety.cli_entry(entry.main)
    assert caught.value.code == 2
    assert CANARY not in stdout.getvalue() + stderr.getvalue()


@pytest.mark.parametrize("has_code", [False, True])
def test_unknown_failure_code_not_retained_in_late_custom_handler(has_code, monkeypatch, isolated_logging):
    stdout, late = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    safety.configure_logging()
    handler = logging.StreamHandler(late)
    handler.setFormatter(logging.Formatter("%(message)s %(failure_code)s"))
    logging.getLogger().addHandler(handler)

    def operation():
        error = ValueError(CANARY)
        if has_code:
            error.code = CANARY
        raise error

    assert safety.cli_entry(operation) == 1
    assert CANARY not in stdout.getvalue() + late.getvalue()
    assert "operation_failed" in late.getvalue()


def test_raw_binary_stdout_is_suppressed(monkeypatch, isolated_logging):
    stdout, stderr = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    safety.configure_logging()

    def operation():
        sys.stdout.buffer.write(CANARY.encode("utf-8"))
        sys.stderr.buffer.write(CANARY.encode("utf-8"))

    assert safety.cli_entry(operation) == 0
    assert CANARY not in stdout.getvalue() + stderr.getvalue()
    assert "external_output_suppressed" in stdout.getvalue()


def test_main_status_is_guarded_and_uses_only_synthetic_modules(tmp_path, monkeypatch):
    import data

    calls = []

    def summary():
        with pytest.raises(safety.WriterBusyError, match="writer_busy"):
            with safety.writer_lock(tmp_path):
                pytest.fail("Status read escaped the common lock")
        calls.append("summary")

    monkeypatch.setitem(sys.modules, "config", SimpleNamespace(LOCAL_DATA_DIR=tmp_path))
    monkeypatch.setattr(data, "progress", SimpleNamespace(print_summary=summary), raising=False)
    monkeypatch.setattr(data, "storage", SimpleNamespace(print_local_summary=summary), raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--mode", "daily", "--status"])
    for mode in MODES:
        monkeypatch.setattr(entry, "run_" + mode.replace("-", "_"), forbidden)
    entry.main()
    assert calls == ["summary", "summary"]


@pytest.mark.parametrize("event", sorted(safety._EVENTS))
def test_reviewed_events_remain_readable_and_raw_format_args_removed(event, monkeypatch, isolated_logging):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    safety.configure_logging()
    logging.getLogger("synthetic.event").warning(event, CANARY)
    assert event in output.getvalue()
    assert CANARY not in output.getvalue()
