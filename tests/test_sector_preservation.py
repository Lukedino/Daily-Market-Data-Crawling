"""sector 기준본·실패 관측 보존·게시 및 실제 main 함수 배선의 합성 회귀."""
import ast
import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from data import ohlc_db as db
from data import ohlc_collector as collector


def meta(tickers=("AAPL",), *, stamp="2026-09-01T00:00:00", sector="Technology", industry="Software", market="US"):
    return pd.DataFrame([{"Ticker": t, "Market": market, "Sector": sector,
                          "Industry": industry, "updated_at": stamp} for t in tickers])


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path / "ohlc")
    monkeypatch.setattr(db.config, "DRIVE_PATHS", {"ohlc_us": "us", "ohlc_crypto": "crypto"})
    monkeypatch.setattr(collector.time, "sleep", lambda *a: None)


def prior(frame=None, market="us"):
    path = db.sector_meta_path(market)
    path.parent.mkdir(parents=True, exist_ok=True)
    (meta() if frame is None else frame).to_parquet(path, index=False)
    return path, path.read_bytes()


class Drive:
    def __init__(self, outcome="ok", frame=None, result=True):
        self.outcome, self.frame, self.result = outcome, frame, result
        self.calls = []
    def download(self, remote, filename, destination):
        self.calls.append(("download", remote))
        if self.outcome == "absent": raise FileNotFoundError()
        if self.outcome == "failed": return False
        if self.outcome == "raise": raise OSError("SYNTHETIC_SECRET")
        if self.outcome == "vanished":
            Path(destination).unlink()
            return True
        if self.outcome == "corrupt": Path(destination).write_bytes(b"broken")
        else: (meta() if self.frame is None else self.frame).to_parquet(destination, index=False)
        return True
    def upload(self, local, remote):
        self.calls.append(("upload", remote))
        if isinstance(self.result, Exception): raise self.result
        return self.result


@pytest.mark.parametrize("mutation", ["corrupt", "missing", "duplicate", "timestamp", "field"])
def test_strict_local_baseline_blocks_replacement(mutation):
    frame = meta()
    if mutation == "missing": frame = frame.drop(columns="Market")
    if mutation == "duplicate": frame = pd.concat([frame, frame])
    if mutation == "timestamp": frame["updated_at"] = "not-time"
    if mutation == "field": frame["Sector"] = 123
    path, before = prior(frame)
    if mutation == "corrupt":
        path.write_bytes(b"corrupt")
        before = b"corrupt"
    with pytest.raises(db.DriveSyncError): db.save_sector_meta(meta(), "us")
    assert path.read_bytes() == before


def test_first_valid_candidate_saves_on_windows():
    db.save_sector_meta(meta(("MSFT", "AAPL")), "us")
    assert list(db.load_sector_meta("us")["Ticker"]) == ["MSFT", "AAPL"]


@pytest.mark.parametrize("fault", ["partial", "row_loss", "fsync", "replace"])
def test_candidate_failure_preserves_bytes(monkeypatch, fault):
    path, before = prior()
    if fault == "partial":
        def fail(table, destination, **kwargs):
            Path(destination).write_bytes(b"partial")
            raise OSError("SYNTHETIC_SECRET")
        monkeypatch.setattr(db.pq, "write_table", fail)
    elif fault == "row_loss":
        original = db.pq.write_table
        monkeypatch.setattr(db.pq, "write_table", lambda table, path, **kw: original(table.slice(0, 0), path, **kw))
    else:
        def fail(*args): raise OSError("SYNTHETIC_SECRET")
        monkeypatch.setattr(db.os, fault, fail)
    with pytest.raises(Exception): db.save_sector_meta(meta(("AAPL", "MSFT")), "us")
    assert path.read_bytes() == before
    assert not list(path.parent.glob("*.tmp"))


@pytest.mark.parametrize("failed_fields", [["Sector", "Industry"], ["Sector"]])
def test_failed_fields_preserve_values_and_timestamp(failed_fields):
    prior()
    fresh = meta(stamp="2026-09-22T00:00:00", sector="", industry="New Industry")
    fresh.attrs["sector_failed_fields"] = {"AAPL": failed_fields}
    db.save_sector_meta(fresh, "us")
    observed = db.load_sector_meta("us").iloc[0]
    assert observed["Sector"] == "Technology"
    assert observed["Industry"] == ("Software" if "Industry" in failed_fields else "New Industry")
    assert observed["updated_at"] == "2026-09-01T00:00:00"


def test_failed_new_ticker_leaves_its_own_fields_empty_and_publishes_the_rest():
    """되돌릴 이전 값이 없는 신규 종목이 실패해도 그 주 산출물 전체(US 1,061행)를
    버리지 않는다. 나쁜 심볼 하나가 들어오면 매주 반복 실패하던 구조였다."""
    prior()
    fresh = meta(("AAPL", "NEW"))
    fresh.attrs["sector_failed_fields"] = {"NEW": ["Sector", "Industry"]}
    assert db.save_sector_meta(fresh, "us") is True
    observed = db.load_sector_meta("us").set_index("Ticker")
    assert observed.at["NEW", "Sector"] == "" and observed.at["NEW", "Industry"] == ""
    assert observed.at["AAPL", "Sector"] == "Technology"   # 나머지는 정상 게시


def setup_info(monkeypatch, info, *, tickers=("AAPL",)):
    import yfinance
    monkeypatch.setattr(collector, "load_tickers", lambda market: list(tickers))
    for function in ("_fetch_sp500", "_fetch_nasdaq100", "_fetch_dow30"):
        monkeypatch.setattr(collector, function, lambda: [])
    def ticker(t):
        value = info[t]
        if isinstance(value, Exception): raise value
        return SimpleNamespace(info=value)
    monkeypatch.setattr(yfinance, "Ticker", ticker)


# {"trailingPegRatio": None} 이 야후 404 의 실제 응답이다 — 예외도 빈 dict 도
# 아니라서 `not info` 로는 못 잡았고, 빈 섹터가 정상 관측으로 저장돼 직전 값을
# 덮어썼다(09-20 BLD·CPRX, 09-13·09-06 BLD, 08-31 U-UN-TO 로 실측).
@pytest.mark.parametrize("value", [OSError("SYNTHETIC_SECRET"), {}, None, ["bad"],
                                   {"trailingPegRatio": None}])
def test_actual_info_failure_preserves_existing(monkeypatch, value):
    prior()
    setup_info(monkeypatch, {"AAPL": value})
    fresh = collector.collect_sector_meta("us")
    assert fresh.attrs["sector_failed_fields"] == {"AAPL": ["Sector", "Industry"]}
    db.save_sector_meta(fresh, "us")
    observed = db.load_sector_meta("us").iloc[0]
    assert observed["Sector"] == "Technology" and observed["Industry"] == "Software"
    assert observed["updated_at"] == "2026-09-01T00:00:00"


def test_valid_etf_without_sector_and_crypto_empty_fields_are_allowed(monkeypatch):
    setup_info(monkeypatch, {"SPY": {"quoteType": "ETF"}}, tickers=("SPY",))
    fresh = collector.collect_sector_meta("us")
    assert fresh.attrs["sector_failed_fields"] == {}
    db.save_sector_meta(fresh, "us")
    assert db.load_sector_meta("us").iloc[0]["Market"] == "ETF"
    assert db.load_sector_meta("us").iloc[0]["Sector"] == ""
    monkeypatch.setattr(collector, "load_tickers", lambda market: ["BTC-USD"])
    fresh = collector.collect_sector_meta("crypto")
    db.save_sector_meta(fresh, "crypto")
    assert db.load_sector_meta("crypto").iloc[0]["Sector"] == ""


def test_market_priority_and_input_order_unchanged(monkeypatch):
    tickers = ["SPY", "DOW", "SP", "NDQ", "OTHER"]
    setup_info(monkeypatch, {t: {"quoteType": "EQUITY"} for t in tickers}, tickers=tickers)
    monkeypatch.setattr(collector, "_fetch_sp500", lambda: ["DOW", "SP"])
    monkeypatch.setattr(collector, "_fetch_dow30", lambda: ["DOW"])
    monkeypatch.setattr(collector, "_fetch_nasdaq100", lambda: ["DOW", "SP", "NDQ"])
    frame = collector.collect_sector_meta("us")
    assert list(frame["Ticker"]) == tickers
    assert list(frame["Market"]) == ["ETF", "DOW30", "S&P500", "NASDAQ100", "US"]


@pytest.mark.parametrize("outcome", ["failed", "raise", "corrupt", "vanished"])
def test_remote_failure_is_not_absence(outcome):
    path, before = prior()
    with pytest.raises(db.DriveSyncError): db.download_sector_meta("us", Drive(outcome))
    assert path.read_bytes() == before


def test_only_confirmed_remote_absence_is_false():
    path, before = prior()
    assert db.download_sector_meta("us", Drive("absent")) is False
    assert path.read_bytes() == before


def test_local_candidate_missing_during_download_is_failure(monkeypatch):
    path, before = prior()
    def disappeared(*args): raise FileNotFoundError("SYNTHETIC_SECRET")
    monkeypatch.setattr(db, "_write_sector_frame", disappeared)
    with pytest.raises(db.DriveSyncError): db.download_sector_meta("us", Drive())
    assert path.read_bytes() == before


def test_remote_merge_keeps_local_only_and_more_recent_rows():
    prior(meta(("AAPL", "LOCAL"), stamp="2026-09-22T00:00:00"))
    remote = pd.concat([meta(("AAPL",), stamp="2026-09-01T00:00:00", sector="Old"),
                        meta(("REMOTE",), stamp="2026-09-22T00:00:00")])
    assert db.download_sector_meta("us", Drive(frame=remote)) is True
    rows = db.load_sector_meta("us").set_index("Ticker")
    assert set(rows.index) == {"AAPL", "LOCAL", "REMOTE"}
    assert rows.loc["AAPL", "Sector"] == "Technology"


@pytest.mark.parametrize("result", [False, None, OSError("SYNTHETIC_SECRET")])
def test_upload_not_confirmed_raises_and_keeps_local(result):
    path, before = prior()
    with pytest.raises(db.DriveSyncError) as error: db.upload_sector_meta("us", Drive(result=result))
    assert "SYNTHETIC_SECRET" not in str(error.value)
    assert path.read_bytes() == before


def run_sector(args):
    # main 최상위 로깅·인증 초기화 없이 실제 함수 AST만 실행한다.
    tree = ast.parse((Path(__file__).parents[1] / "main.py").read_text(encoding="utf-8-sig"))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_sector_meta")
    scope = {"logger": logging.getLogger("synthetic-main")}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "<sector-main>", "exec"), scope)
    return scope["run_sector_meta"](args)


@pytest.mark.parametrize("boundary", ["local", "remote"])
def test_main_baseline_failure_prevents_collection(monkeypatch, boundary):
    calls = []
    def fail(*a, **kw): raise db.DriveSyncError("synthetic_baseline_failed")
    monkeypatch.setattr(db, "load_sector_meta", fail if boundary == "local" else lambda *a: meta())
    monkeypatch.setattr(db, "download_sector_meta", fail)
    monkeypatch.setattr(collector, "collect_sector_meta", lambda *a: calls.append(a))
    with pytest.raises(db.DriveSyncError): run_sector(SimpleNamespace(market="us", dry_run=False, upload_drive=True))
    assert calls == []


def test_main_dry_run_has_no_baseline_collection_or_write(monkeypatch):
    def forbidden(*a, **kw): pytest.fail("dry-run must not touch a writer")
    for name in ("load_sector_meta", "download_sector_meta", "save_sector_meta", "upload_sector_meta"):
        monkeypatch.setattr(db, name, forbidden)
    monkeypatch.setattr(collector, "collect_sector_meta", forbidden)
    run_sector(SimpleNamespace(market="all", dry_run=True, upload_drive=True))


def test_main_upload_failure_propagates_after_actual_local_save(monkeypatch):
    u = Drive("absent", result=False)
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: uploader or u)
    monkeypatch.setattr(collector, "collect_sector_meta", lambda *a: meta())
    with pytest.raises(db.DriveSyncError): run_sector(SimpleNamespace(market="us", dry_run=False, upload_drive=True))
    assert len(db.load_sector_meta("us")) == 1
    assert u.calls == [("download", "us"), ("upload", "us")]


def test_all_markets_baselines_before_first_collection(monkeypatch):
    calls = []
    monkeypatch.setattr(db, "load_sector_meta", lambda market: meta())
    def download(market):
        calls.append(market)
        if market == "crypto": raise db.DriveSyncError("sector_download_failed")
        return True
    monkeypatch.setattr(db, "download_sector_meta", download)
    monkeypatch.setattr(collector, "collect_sector_meta", lambda market: pytest.fail("all baselines first"))
    monkeypatch.setattr(db, "save_sector_meta", lambda *a, **kw: pytest.fail("no candidate saved"))
    with pytest.raises(db.DriveSyncError): run_sector(SimpleNamespace(market="all", dry_run=False, upload_drive=True))
    assert calls == ["us", "crypto"]
