"""Synthetic OHLC/Drive preservation and retry regressions."""
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from data import ohlc_collector as oc
from data import ohlc_db as db


class FrozenDate(date):
    @classmethod
    def today(cls):
        return cls(2027, 1, 2)


def rows(year=2026, ticker="NEW", day=2):
    return pd.DataFrame({"Ticker": [ticker], "Date": [date(year, 1, day)],
                         "Open": [100.0], "High": [101.0], "Low": [99.0],
                         "Close": [100.0], "Volume": [1000.0]})


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    monkeypatch.setattr(db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setattr(oc, "date", FrozenDate)
    monkeypatch.setattr(db, "download_all_years", lambda *a, **k: None)
    monkeypatch.setattr(db, "download_status", lambda *a, **k: None)
    monkeypatch.setattr(db, "download_pending", lambda *a, **k: None)
    monkeypatch.setattr(db, "download_year_state", lambda *a, **k: "absent")
    monkeypatch.setattr(db, "upload_years", lambda *a, **k: [])
    monkeypatch.setattr(db, "upload_status", lambda *a, **k: True)
    monkeypatch.setattr(db, "upload_pending", lambda *a, **k: True)
    monkeypatch.setattr(oc, "load_tickers", lambda market: ["NEW"])
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda tickers: {})
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda df: df)
    monkeypatch.setattr(oc, "_enrich_crypto_marketcap", lambda df: df)
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda tickers, start, end, **k: (rows(int(start[:4])), []))
    db.save_status({"us": {"last_updated": "2026-12-29"}, "crypto": {"last_updated": "2026-12-29"}})
    db.save_pending({"us": ["NEW"], "crypto": []})
    return tmp_path


@pytest.mark.parametrize("kind", ["full", "new"])
def test_backfill_upload_failure_preserves_cursor_and_pending(isolated, monkeypatch, kind):
    before = db.load_status()
    monkeypatch.setattr(db, "upload_years", lambda *a, **k: ["us_2026.parquet"])
    with pytest.raises(db.DriveSyncError):
        if kind == "full":
            oc.backfill_market("us", 2026, 2027, upload=True)
        else:
            oc.backfill_new_tickers("us", start_year=2026, upload=True)
    assert db.load_status() == before
    assert "NEW" in db.load_pending()["us"]


@pytest.mark.parametrize("kind", ["full", "new", "incremental"])
def test_every_year_baseline_checked_before_any_save(isolated, monkeypatch, kind):
    checked, saved = [], []
    def baseline(market, year):
        checked.append(year)
        return "failed" if year == 2026 else "absent"
    monkeypatch.setattr(db, "download_year_state", baseline)
    monkeypatch.setattr(db, "save_year", lambda *a, **k: saved.append(a))
    with pytest.raises(db.DriveSyncError):
        if kind == "full":
            oc.backfill_market("us", 2026, 2027)
        elif kind == "new":
            oc.backfill_new_tickers("us", start_year=2026)
        else:
            oc.update_market("crypto")
    assert 2026 in checked
    assert saved == []
    assert db.load_pending()["us"] == ["NEW"]


def test_corrupt_existing_date_column_cannot_be_overwritten(isolated):
    path = db.local_path("us", 2026)
    path.parent.mkdir()
    original = rows(ticker="OLD")
    original["Date"] = "not-a-date"
    original.to_parquet(path)
    before = path.read_bytes()
    with pytest.raises(Exception):
        db.save_year(rows(ticker="NEW"), "us", 2026)
    assert path.read_bytes() == before


def test_partial_save_preserves_old_bytes(isolated, monkeypatch):
    db.save_year(rows(ticker="OLD"), "us", 2026)
    path = db.local_path("us", 2026)
    before = path.read_bytes()
    def broken(table, destination, **kwargs):
        Path(destination).write_bytes(b"partial")
        raise OSError("synthetic disk failure")
    monkeypatch.setattr(db.pq, "write_table", broken)
    with pytest.raises(OSError):
        db.save_year(rows(ticker="NEW"), "us", 2026)
    assert path.read_bytes() == before


def test_invalid_drive_parquet_preserves_valid_local(tmp_path, monkeypatch):
    from data.drive_uploader import DriveUploader
    import googleapiclient.http

    path = tmp_path / "us_2026.parquet"
    rows(ticker="OLD").to_parquet(path)
    before = path.read_bytes()
    uploader = DriveUploader(root_folder_id="synthetic-root")
    service = SimpleNamespace(files=lambda: SimpleNamespace(get_media=lambda **k: object()))
    monkeypatch.setattr(uploader, "_get_service", lambda: service)
    monkeypatch.setattr(uploader, "_lookup_folder", lambda *a: "synthetic-folder")
    monkeypatch.setattr(uploader, "_find_file", lambda *a: "synthetic-file")
    class Downloader:
        calls = 0
        def __init__(self, target, request, **kwargs):
            self.target = target
        def next_chunk(self, **kwargs):
            Downloader.calls += 1
            self.target.write(b"corrupt parquet")
            return None, True
    monkeypatch.setattr(googleapiclient.http, "MediaIoBaseDownload", Downloader)
    with pytest.raises(Exception):
        uploader.download("us", path.name, str(path))
    assert path.read_bytes() == before
    assert Downloader.calls == 1


@pytest.mark.parametrize("kind", ["full", "new", "incremental"])
def test_status_upload_failure_restores_local_cursor(isolated, monkeypatch, kind):
    before = db.load_status()
    def fail(*a, **k):
        raise db.DriveSyncError("synthetic metadata failure")
    monkeypatch.setattr(db, "upload_status", fail)
    with pytest.raises(db.DriveSyncError):
        if kind == "full":
            oc.backfill_market("us", 2026, 2027)
        elif kind == "new":
            oc.backfill_new_tickers("us", start_year=2026)
        else:
            oc.update_market("crypto")
    assert db.load_status() == before
    assert db.load_pending()["us"] == ["NEW"]


@pytest.mark.parametrize("kind", ["full", "new", "incremental"])
def test_existing_corrupt_baseline_blocks_all_writes(isolated, monkeypatch, kind):
    market = "crypto" if kind == "incremental" else "us"
    path = db.local_path(market, 2026)
    path.parent.mkdir()
    path.write_bytes(b"corrupt existing baseline")
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: pytest.fail("must not collect"))
    with pytest.raises(db.DriveSyncError):
        if kind == "full":
            oc.backfill_market(market, 2026, 2027)
        elif kind == "new":
            oc.backfill_new_tickers(market, start_year=2026)
        else:
            oc.update_market(market)
    assert path.read_bytes() == b"corrupt existing baseline"


def test_valid_but_wrong_staged_save_preserves_previous_file(isolated, monkeypatch):
    db.save_year(rows(ticker="OLD"), "us", 2026)
    path = db.local_path("us", 2026)
    before = path.read_bytes()
    writer = db.pq.write_table
    def wrong(table, destination, **kwargs):
        writer(db.pa.Table.from_pandas(rows(ticker="WRONG"), preserve_index=False), destination, **kwargs)
    monkeypatch.setattr(db.pq, "write_table", wrong)
    with pytest.raises(AssertionError):
        db.save_year(rows(ticker="NEW"), "us", 2026)
    assert path.read_bytes() == before


def test_wrong_year_cannot_replace_existing_file(isolated):
    db.save_year(rows(ticker="OLD"), "us", 2026)
    path = db.local_path("us", 2026)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="partition"):
        db.save_year(rows(year=2027), "us", 2026)
    assert path.read_bytes() == before


def test_full_backfill_empty_absent_year_continues(isolated, monkeypatch):
    uploads = []
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda tickers, start, end, **k:
                        (pd.DataFrame() if start.startswith("2026") else rows(2027), []))
    monkeypatch.setattr(db, "upload_years", lambda market, years: (uploads.extend(years), [])[1])
    oc.backfill_market("us", 2026, 2027)
    assert uploads == [2027]
    assert not db.local_path("us", 2026).exists()


@pytest.mark.parametrize("kind", ["full", "new", "incremental"])
def test_local_mode_never_calls_drive(isolated, monkeypatch, kind):
    def forbidden(*args, **kwargs):
        pytest.fail("local mode must not require Drive")
    for name in ("download_status", "download_pending", "download_year_state",
                 "upload_status", "upload_pending", "upload_years"):
        monkeypatch.setattr(db, name, forbidden)
    if kind == "full":
        oc.backfill_market("us", 2026, 2027, upload=False)
    elif kind == "new":
        oc.backfill_new_tickers("us", start_year=2026, upload=False)
    else:
        oc.update_market("crypto", upload=False)


def test_remote_refresh_preserves_unpublished_local_history(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    local = pd.concat([rows(ticker="BOTH"), rows(ticker="LOCAL")], ignore_index=True)
    local["MarketCap"] = 123.0
    db.save_year(local, "us", 2026)
    remote = pd.concat([rows(ticker="BOTH"), rows(ticker="REMOTE")], ignore_index=True)
    remote["Close"] = 200.0
    remote["MarketCap"] = float("nan")
    class Uploader:
        def download(self, folder, filename, destination):
            remote.to_parquet(destination, index=False)
    assert db.download_year_state("us", 2026, uploader=Uploader()) == "ok"
    output = db.load_year("us", 2026, strict=True).set_index("Ticker")
    assert set(output.index) == {"BOTH", "LOCAL", "REMOTE"}
    assert output.loc["BOTH", "Close"] == 200
    assert output.loc["BOTH", "MarketCap"] == 123


@pytest.mark.parametrize("payload", [b"partial", b'[]', b'{"us": "invalid"}'])
def test_bad_pending_download_preserves_previous_bytes(tmp_path, monkeypatch, payload):
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    db.save_pending({"us": ["OLD"]})
    before = db._PENDING_PATH.read_bytes()
    class Uploader:
        def download(self, folder, filename, destination):
            Path(destination).write_bytes(payload)
    with pytest.raises(db.DriveSyncError):
        db.download_pending(uploader=Uploader())
    assert db._PENDING_PATH.read_bytes() == before


@pytest.mark.parametrize("metadata", ["status", "pending"])
def test_real_metadata_helpers_propagate_upload_failure(tmp_path, monkeypatch, metadata):
    monkeypatch.setattr(db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    db.save_status({"us": {"last_updated": "2026-01-02"}})
    db.save_pending({"us": ["OLD"]})
    class Uploader:
        def upload(self, local, folder):
            raise OSError("synthetic private identifier")
    with pytest.raises(db.DriveSyncError, match="OSError") as error:
        getattr(db, "upload_" + metadata)(uploader=Uploader())
    assert "synthetic private" not in str(error.value)


def test_pending_completion_upload_failure_keeps_retry_intent(isolated, monkeypatch):
    db.save_pending({"us": [], "crypto": ["UNRELATED"]})
    published = []
    def publish():
        published.append(db.load_pending())
        if len(published) == 2:
            raise db.DriveSyncError("synthetic pending publication failure")
        return True
    monkeypatch.setattr(db, "upload_pending", publish)
    with pytest.raises(db.DriveSyncError):
        oc.backfill_new_tickers("us", start_year=2026)
    assert published[0]["us"] == ["NEW"]
    assert db.load_pending() == {"us": ["NEW"], "crypto": ["UNRELATED"]}
    assert db.load_status()["us"]["last_updated"] == "2026-12-29"


def test_pending_intent_upload_failure_blocks_first_data_write(isolated, monkeypatch):
    def fail():
        raise db.DriveSyncError("synthetic pending publication failure")
    monkeypatch.setattr(db, "upload_pending", fail)
    monkeypatch.setattr(db, "save_year", lambda *a, **k: pytest.fail("intent not published"))
    with pytest.raises(db.DriveSyncError):
        oc.backfill_new_tickers("us", start_year=2026)
    assert db.load_pending()["us"] == ["NEW"]
