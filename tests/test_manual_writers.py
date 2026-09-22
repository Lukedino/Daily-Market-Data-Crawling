"""Manual writers use the same baseline/publication boundaries as collection."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import main
from data import kr_collector, kr_db, ohlc_db
from scripts import resave_ohlc, verify_ohlc, verify_kr


@pytest.fixture
def pending(tmp_path, monkeypatch):
    monkeypatch.setattr(ohlc_db, "_PENDING_PATH", tmp_path / "pending.json")
    monkeypatch.setitem(ohlc_db.config.DRIVE_PATHS, "ohlc_meta", "synthetic-meta")
    ohlc_db.save_pending({"us": ["LOCAL"], "crypto": ["BTC-USD"]})
    return tmp_path


class PendingDrive:
    def __init__(self, remote=None, failure=None, publish=True):
        self.remote = remote or {"us": ["REMOTE"], "other": ["KEPT"]}
        self.failure, self.publish = failure, publish
        self.uploaded = []
    def download(self, remote, name, target):
        if self.failure:
            raise self.failure
        Path(target).write_text(json.dumps(self.remote), encoding="utf-8")
        return True
    def upload(self, source, remote):
        self.uploaded.append(json.loads(Path(source).read_text(encoding="utf-8")))
        return self.publish


def test_pending_merges_all_markets_once_after_remote_confirmation(pending, monkeypatch):
    drive = PendingDrive()
    monkeypatch.setattr(ohlc_db, "_get_uploader", lambda uploader=None: drive)
    verify_ohlc.apply_fix([{"market": "us", "new": ["NEW"]}, {"market": "crypto", "new": ["ETH-USD"]}])
    assert len(drive.uploaded) == 1
    assert drive.uploaded[0] == {"us": ["LOCAL", "NEW", "REMOTE"], "crypto": ["BTC-USD", "ETH-USD"], "other": ["KEPT"]}


@pytest.mark.parametrize("failure", [OSError("synthetic"), RuntimeError("synthetic")])
def test_failed_pending_read_never_overwrites_local(pending, monkeypatch, failure):
    before = ohlc_db._PENDING_PATH.read_bytes()
    drive = PendingDrive(failure=failure)
    monkeypatch.setattr(ohlc_db, "_get_uploader", lambda uploader=None: drive)
    with pytest.raises(Exception):
        verify_ohlc.apply_fix([{"market": "us", "new": ["NEW"]}])
    assert ohlc_db._PENDING_PATH.read_bytes() == before
    assert drive.uploaded == []


@pytest.mark.parametrize("outcome", [False, None, ""])
def test_failed_pending_publish_restores_original_local_intent(pending, monkeypatch, outcome):
    before = ohlc_db._PENDING_PATH.read_bytes()
    drive = PendingDrive(publish=outcome)
    monkeypatch.setattr(ohlc_db, "_get_uploader", lambda uploader=None: drive)
    with pytest.raises(ohlc_db.DriveSyncError):
        verify_ohlc.apply_fix([{"market": "us", "new": ["NEW"]}])
    assert ohlc_db._PENDING_PATH.read_bytes() == before


def test_absent_pending_retains_local(pending, monkeypatch):
    drive = PendingDrive(failure=FileNotFoundError("synthetic"))
    monkeypatch.setattr(ohlc_db, "_get_uploader", lambda uploader=None: drive)
    assert verify_ohlc.pending_baseline() == ohlc_db.load_pending()


def test_resave_checks_all_requested_local_baselines_before_first_save(tmp_path, monkeypatch):
    monkeypatch.setattr(ohlc_db, "download_all_years", lambda market: "ok")
    monkeypatch.setattr(ohlc_db, "local_path", lambda market, year: tmp_path / f"{year}.parquet")
    (tmp_path / "2025.parquet").touch()
    (tmp_path / "2026.parquet").touch()
    def load(market, year, *, strict):
        assert strict
        if year == 2026:
            raise ohlc_db.DriveSyncError("synthetic_corrupt")
        return pd.DataFrame({"Ticker": ["OLD"]})
    monkeypatch.setattr(ohlc_db, "load_year", load)
    monkeypatch.setattr(ohlc_db, "save_year", lambda *args: pytest.fail("saved before complete preflight"))
    with pytest.raises(ohlc_db.DriveSyncError):
        resave_ohlc.resave(SimpleNamespace(market="us", years=[2025, 2026], upload=True))


def test_resave_rejects_failed_publication(tmp_path, monkeypatch):
    monkeypatch.setattr(ohlc_db, "download_all_years", lambda market: "ok")
    path = tmp_path / "year.parquet"
    path.touch()
    monkeypatch.setattr(ohlc_db, "local_path", lambda *args: path)
    frame = pd.DataFrame({"Ticker": ["A"], "Date": [pd.Timestamp("2026-01-05")]})
    monkeypatch.setattr(ohlc_db, "load_year", lambda *args, **kwargs: frame)
    monkeypatch.setattr(ohlc_db, "save_year", lambda *args: None)
    monkeypatch.setattr(ohlc_db, "check_coverage_continuity", lambda frame: {"n_bad": 0})
    monkeypatch.setattr(ohlc_db, "upload_years", lambda *args: ["us_2026.parquet"])
    with pytest.raises(ohlc_db.DriveSyncError, match="resave_publication_failed"):
        resave_ohlc.resave(SimpleNamespace(market="us", years=[2026], upload=True))


def test_actual_kr_yahoo_price_guard_precedes_backfill_save(monkeypatch):
    frame = pd.DataFrame({"Code": ["000001"], "Date": [pd.Timestamp("2026-01-05")]})
    frame.attrs["kr_price_basis"] = {"provider": "yfinance", "auto_adjust": True}
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda *args, **kwargs: {2026: "absent"})
    monkeypatch.setattr(kr_collector, "collect_backfill", lambda *args: frame)
    monkeypatch.setattr(kr_db, "append_rows", lambda *args, **kwargs: pytest.fail("unverified price stored"))
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        main.run_kr_backfill(SimpleNamespace(dry_run=False, upload_drive=False,
            start_date="2026-01-05", end_date="2026-01-06"))


def test_verify_kr_report_has_no_write_side_effect(monkeypatch):
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda *args, **kwargs: pytest.fail("report wrote data"))
    monkeypatch.setattr(verify_kr, "find_gaps", lambda analysis: [(pd.Timestamp("2026-01-06").date(), pd.Timestamp("2026-01-06").date())])
    result = verify_kr.print_report([dict(year=2026, exists=True, first_date="2026-01-05",
        last_date="2026-01-05", rows=1, trading_days=1, ticker_count=1)])
    assert len(result) == 1


@pytest.mark.parametrize("valid", [False, True])
def test_ohlc_empty_baseline_requires_real_key_schema(tmp_path, valid):
    path = tmp_path / "us_2026.parquet"
    frame = pd.DataFrame(columns=["Ticker", "Date"] if valid else ["garbage"])
    frame.to_parquet(path)
    if valid:
        assert ohlc_db._read_year_path(path, 2026).empty
    else:
        with pytest.raises(ValueError, match="key schema"):
            ohlc_db._read_year_path(path, 2026)
