"""수동 writer의 전체 OHLC 다운로드도 합성 staging과 strict 병합을 통과한다."""
from pathlib import Path
from datetime import date

import pandas as pd
import pytest
from data import ohlc_db as db


def rows(year=2026, ticker="OLD", cap=100.):
    return pd.DataFrame({"Ticker": [ticker], "Date": [date(year, 1, 2)], "Close": [10.], "MarketCap": [cap]})


class Uploader:
    def __init__(self, files, outcome="ok"):
        self.files, self.outcome = files, outcome
    def download_all_state(self, remote, directory, extensions):
        for name, value in self.files.items():
            path = Path(directory) / name
            if isinstance(value, bytes): path.write_bytes(value)
            else: value.to_parquet(path, index=False)
        return self.outcome


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    monkeypatch.setitem(db.config.DRIVE_PATHS, "ohlc_us", "synthetic/us")
    path = db.local_path("us", 2026)
    path.parent.mkdir(parents=True)
    rows().to_parquet(path, index=False)
    return path


def test_download_merges_local_only_rows_and_preserves_nonnull_metadata(local):
    remote = pd.concat([rows(cap=float("nan")), rows(ticker="NEW", cap=200)], ignore_index=True)
    assert db.download_all_years("us", Uploader({"us_2026.parquet": remote})) == "ok"
    result = db.load_year("us", 2026, strict=True).set_index("Ticker")
    assert set(result.index) == {"OLD", "NEW"} and result.loc["OLD", "MarketCap"] == 100.


@pytest.mark.parametrize("fault", ["failed", "corrupt", "wrong_year", "unknown_name", "empty_ok"])
def test_all_candidates_validate_before_any_local_replace(local, fault):
    before = local.read_bytes()
    files = {"us_2026.parquet": rows(ticker="NEW")}
    outcome = "ok"
    if fault == "failed": outcome = "failed"
    elif fault == "corrupt": files["us_2027.parquet"] = b"SYNTHETIC_SECRET"
    elif fault == "wrong_year": files["us_2027.parquet"] = rows(2026)
    elif fault == "unknown_name": files["other.parquet"] = rows()
    else: files = {}
    with pytest.raises(db.DriveSyncError) as caught:
        db.download_all_years("us", Uploader(files, outcome))
    assert "SYNTHETIC_SECRET" not in str(caught.value) and local.read_bytes() == before
    assert not db.local_path("us", 2027).exists()


def test_absent_preserves_valid_local_and_corrupt_local_is_not_absent(local):
    before = local.read_bytes()
    assert db.download_all_years("us", Uploader({}, "absent")) == "absent"
    assert local.read_bytes() == before
    local.write_bytes(b"broken")
    with pytest.raises(db.DriveSyncError):
        db.download_all_years("us", Uploader({}, "absent"))
    assert local.read_bytes() == b"broken"
