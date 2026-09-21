"""매일 도는 증분 경로의 Drive 동기화 불변식 (2026-09-19 심층 검토 D-01·D-02).

D-01  Drive 다운로드 **실패**를 "파일 없음" 으로 취급하면 당해 연도 파일이 증분 며칠 치로 교체돼 올라간다.
      3상태 가드는 백필 경로에만 있었다.
D-02  업로드가 실패해도 커서(db_status.json)가 전진하면 그날은 다시 수집되지 않는 영구 구멍이 된다.
"""
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data import ohlc_collector as oc
from data import ohlc_db


def _new_rows():
    day = date.today() - timedelta(days=1)
    return pd.DataFrame({"Date": [day], "Ticker": ["AAA"], "Open": [1.0], "High": [1.0], "Low": [1.0],
                         "Close": [1.0], "Volume": [1.0]})


@pytest.fixture
def harness(tmp_path, monkeypatch):
    calls = {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    monkeypatch.setattr(ohlc_db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(ohlc_db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setattr(ohlc_db, "download_status", lambda *a, **k: None)
    monkeypatch.setattr(ohlc_db, "load_status",
                        lambda: {"us": {"last_updated": str(date.today() - timedelta(days=3))}})
    monkeypatch.setattr(oc, "load_tickers", lambda m: ["AAA"])
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda t: {})
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (_new_rows(), []))
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda df: df)

    def count(name, result=None):
        def inner(*a, **k):
            calls[name] += 1
            return result
        return inner
    monkeypatch.setattr(ohlc_db, "append_rows", count("append", [date.today().year]))
    monkeypatch.setattr(ohlc_db, "upload_years", count("upload_years", []))
    monkeypatch.setattr(ohlc_db, "update_status", count("update_status"))
    monkeypatch.setattr(ohlc_db, "upload_status", count("upload_status"))
    return calls, monkeypatch


def test_failed_baseline_download_stops_before_anything_is_written(harness):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "failed")
    with pytest.raises(ohlc_db.DriveSyncError):
        oc.update_market("us", upload=True)
    assert calls == {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}


@pytest.mark.parametrize("state", ["ok", "absent"])
def test_healthy_baseline_proceeds(harness, state):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: state)
    oc.update_market("us", upload=True)
    assert calls == {"append": 1, "upload_years": 1, "update_status": 1, "upload_status": 1}


def test_failed_upload_does_not_advance_the_cursor(harness):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")

    def failing_upload(market, years, uploader=None):
        calls["upload_years"] += 1
        return [f"{market}_{years[0]}.parquet"]              # 실패한 파일 목록
    monkeypatch.setattr(ohlc_db, "upload_years", failing_upload)
    with pytest.raises(ohlc_db.DriveSyncError):
        oc.update_market("us", upload=True)
    assert calls["update_status"] == 0 and calls["upload_status"] == 0   # 다음 실행이 같은 날을 다시 수집


def test_upload_years_reports_which_files_failed(tmp_path, monkeypatch):
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    for year in (2025, 2026):
        path = ohlc_db.local_path("us", year)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = _new_rows()
        data["Date"] = date(year, 1, 2)
        data.to_parquet(path, index=False)

    class Uploader:
        def upload(self, local, remote):
            if "2026" in Path(local).name:
                raise OSError("503")
    monkeypatch.setitem(ohlc_db.config.DRIVE_PATHS, "ohlc_us", "data/ohlc/us")
    assert ohlc_db.upload_years("us", [2025, 2026], uploader=Uploader()) == ["us_2026.parquet"]


# ── KR 일별: 같은 불변식. KR 은 Marcap·Rank 과거값을 다시 받을 길이 없어 더 나쁘다 ──────────
from types import SimpleNamespace

import main as kr_main
from data import kr_db


def _kr_rows(year=2026):
    return pd.DataFrame({"Code": ["000001"], "Date": [date(year, 1, 2)],
                         "Open": [100], "High": [101], "Low": [99], "Close": [100],
                         "Volume": [1000], "Marcap": [100000], "Rank": [1], "Stocks": [1000]})


class _Uploader:
    def __init__(self, download=None, upload=None):
        self._download, self._upload = download, upload

    def download(self, remote, name, dest):
        if isinstance(self._download, Exception):
            raise self._download
        year = int(name.removeprefix("marcap-").removesuffix(".parquet"))
        _kr_rows(year).to_parquet(dest, index=False)

    def upload(self, local, remote):
        if isinstance(self._upload, Exception):
            raise self._upload


@pytest.fixture
def kr_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "local_path", lambda year: tmp_path / f"marcap-{year}.parquet")
    monkeypatch.setitem(kr_db.config.DRIVE_PATHS, "ohlc_kr", "data/ohlc/kr")
    return tmp_path


@pytest.mark.parametrize("outcome, expected", [(None, "ok"), (FileNotFoundError("none"), "absent"),
                                               (OSError("503"), "failed")])
def test_kr_download_distinguishes_absent_from_failed(kr_paths, outcome, expected):
    assert kr_db.download_year_state(2026, uploader=_Uploader(download=outcome)) == expected


def test_kr_upload_reports_failed_files(kr_paths):
    _kr_rows().to_parquet(kr_paths / "marcap-2026.parquet", index=False)
    assert kr_db.upload_years([2026], uploader=_Uploader(upload=OSError("503"))) == ["marcap-2026.parquet"]
    assert kr_db.upload_years([2026], uploader=_Uploader()) == []


def test_kr_daily_stops_when_the_baseline_download_failed(kr_paths, monkeypatch):
    monkeypatch.setattr(kr_db, "download_year_state", lambda year, uploader=None: "failed")
    monkeypatch.setattr(kr_db, "append_rows", lambda df: pytest.fail("기준 파일 없이 저장하면 연도 파일이 교체된다"))
    monkeypatch.setattr(kr_db, "upload_years", lambda years, uploader=None: pytest.fail("업로드 금지"))
    with pytest.raises(SystemExit) as stopped:
        kr_main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True))
    assert stopped.value.code == 1
