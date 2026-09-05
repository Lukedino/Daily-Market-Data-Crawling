"""financials 연도 파일 병합 실패 시 덮어쓰지 않는다 (2026-09-05).

`_save_financials_year()` 는 기존 파일을 읽어 병합한 뒤 저장하는데, 읽기가 실패하면
경고만 남기고 이번 배치(≤50종목)만으로 파일을 다시 써서 그 연도가 통째로 줄어들었다 —
ohlc_db.save_year 의 "구멍 B" 와 같은 함정. 병합 실패는 예외로 중단한다.
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data import financials_db as fdb


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(fdb, "_LOCAL_ROOT", tmp_path)
    return fdb


def _rows(tickers, period=date(2026, 6, 30)):
    n = len(tickers)
    return pd.DataFrame({
        "Ticker": tickers, "PeriodDate": [period] * n,
        "Year": [period.year] * n, "Quarter": [2] * n,
        "Revenue": [1.0] * n, "OperatingIncome": [1.0] * n, "NetIncome": [1.0] * n,
        "DilutedEPS": [0.1] * n, "SnapDate": [date(2026, 9, 5)] * n,
    })


def test_normal_merge_adds_rows(db):
    db.save_financials(_rows(["005930", "000660"]), "kr")
    db.save_financials(_rows(["035420"]), "kr")
    out = pd.read_parquet(db.local_financials_path("kr", 2026))
    assert sorted(out["Ticker"]) == ["000660", "005930", "035420"]


def test_merge_failure_aborts_without_overwrite(db, monkeypatch):
    db.save_financials(_rows(["005930", "000660"]), "kr")
    path = db.local_financials_path("kr", 2026)
    before = path.read_bytes()

    def _corrupt(market, year):
        raise OSError("parquet read failed")
    monkeypatch.setattr(db, "load_financials_year", _corrupt)

    with pytest.raises(RuntimeError):
        db.save_financials(_rows(["035420"]), "kr")
    assert path.read_bytes() == before          # 파일은 손대지 않았다
