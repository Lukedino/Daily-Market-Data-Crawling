"""Retry a partially published backfill against an in-memory Drive byte store."""
from datetime import date
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from data import ohlc_collector as oc
from data import ohlc_db as db


class FrozenDate(date):
    @classmethod
    def today(cls):
        return cls(2027, 1, 4)


def rows(year, ticker):
    return pd.DataFrame({
        "Ticker": [ticker], "Date": [date(year, 1, 4)],
        "Open": [10.0], "High": [12.0], "Low": [9.0],
        "Close": [11.0], "Volume": [100.0],
    })


def test_partial_remote_publication_is_retried_without_losing_history(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    monkeypatch.setattr(db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setitem(db.config.DRIVE_PATHS, "ohlc_us", "us")
    monkeypatch.setitem(db.config.DRIVE_PATHS, "ohlc_meta", "_meta")
    monkeypatch.setattr(oc, "date", FrozenDate)
    monkeypatch.setattr(oc, "load_tickers", lambda market: ["OLD", "NEW"])
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda tickers: {})
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda frame: frame)
    fetched = []

    def fetch(tickers, start, end, **kwargs):
        fetched.append((int(start[:4]), tuple(tickers)))
        return pd.concat([rows(int(start[:4]), ticker) for ticker in tickers]), []

    monkeypatch.setattr(oc, "fetch_ohlc_range", fetch)
    initial_status = {
        "us": {"last_updated": "2026-12-30", "oldest_date": "2026-01-04"},
        "crypto": {"last_updated": "2026-12-31", "ticker_count": 5},
    }
    remote = {
        ("us", f"us_{year}.parquet"): rows(year, "OLD").to_parquet(index=False)
        for year in (2026, 2027)
    }
    remote[("_meta", "db_status.json")] = json.dumps(initial_status).encode()
    remote[("_meta", "backfill_pending.json")] = json.dumps({"crypto": ["RETRY-USD"]}).encode()

    class Drive:
        fail_year = True

        def download(self, folder, filename, destination):
            try:
                value = remote[(folder, filename)]
            except KeyError:
                raise FileNotFoundError(filename) from None
            Path(destination).write_bytes(value)

        def upload(self, source, folder):
            name = Path(source).name
            if name == "us_2027.parquet" and self.fail_year:
                self.fail_year = False
                raise OSError("synthetic second-year publication failure")
            remote[(folder, name)] = Path(source).read_bytes()
            return "synthetic-file"

    drive = Drive()
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: uploader or drive)

    with pytest.raises(db.DriveSyncError):
        oc.backfill_new_tickers("us", start_year=2026, upload=True)

    assert json.loads(remote[("_meta", "db_status.json")]) == initial_status
    pending = json.loads(remote[("_meta", "backfill_pending.json")])
    assert pending["us"] == ["NEW"]
    assert pending["crypto"] == ["RETRY-USD"]
    assert set(pd.read_parquet(io.BytesIO(remote[("us", "us_2026.parquet")]))["Ticker"]) == {"OLD", "NEW"}
    assert set(pd.read_parquet(io.BytesIO(remote[("us", "us_2027.parquet")]))["Ticker"]) == {"OLD"}

    assert oc.backfill_new_tickers("us", start_year=2026, upload=True) == ["NEW"]
    assert fetched == [(2026, ("NEW",)), (2027, ("NEW",))] * 2
    for year in (2026, 2027):
        saved = pd.read_parquet(io.BytesIO(remote[("us", f"us_{year}.parquet")]))
        assert set(saved["Ticker"]) == {"OLD", "NEW"}
        assert len(saved) == 2
    pending = json.loads(remote[("_meta", "backfill_pending.json")])
    assert pending == {"crypto": ["RETRY-USD"], "us": []}
    status = json.loads(remote[("_meta", "db_status.json")])
    assert status["us"]["last_updated"] == "2026-12-30"  # subset cannot move the global cursor
    assert status["crypto"] == initial_status["crypto"]
