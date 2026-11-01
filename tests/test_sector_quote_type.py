"""QuoteType 열(2026-11-01) — 야후 quoteType 을 sector_meta 에 저장해 하류(Druckenmiller 러너)가
이름 휴리스틱 없이 ETF·펀드를 판정하게 한다. 옛 파일(열 없음)은 빈 값으로 읽는다."""
import pandas as pd
import pytest
from data import ohlc_db as db
from data import ohlc_collector as collector
from tests.test_sector_preservation import isolated, meta, prior, setup_info  # noqa: F401


def test_quote_type_is_stored_upper_case(monkeypatch):
    setup_info(monkeypatch, {"SPY": {"quoteType": "etf"},
                             "AAPL": {"symbol": "AAPL", "quoteType": "EQUITY",
                                      "sector": "Technology", "industry": "Software"}},
               tickers=("SPY", "AAPL"))
    db.save_sector_meta(collector.collect_sector_meta("us"), "us")
    rows = db.load_sector_meta("us").set_index("Ticker")
    assert rows.loc["SPY", "QuoteType"] == "ETF"
    assert rows.loc["AAPL", "QuoteType"] == "EQUITY"


def test_old_file_without_quote_type_loads_with_empty_value():
    prior()   # 옛 스키마(5열) 파일
    loaded = db.load_sector_meta("us")
    assert loaded.iloc[0]["QuoteType"] == ""
    assert loaded.iloc[0]["Sector"] == "Technology"


@pytest.mark.parametrize("value", [OSError("SYNTHETIC_SECRET"), {"trailingPegRatio": None}])
def test_failed_observation_keeps_previous_quote_type(monkeypatch, value):
    frame = meta()
    frame["QuoteType"] = "EQUITY"
    prior(frame)
    setup_info(monkeypatch, {"AAPL": value})
    db.save_sector_meta(collector.collect_sector_meta("us"), "us")
    assert db.load_sector_meta("us").iloc[0]["QuoteType"] == "EQUITY"


def test_non_string_quote_type_is_empty_and_crypto_is_empty(monkeypatch):
    setup_info(monkeypatch, {"X": {"symbol": "X", "quoteType": 7, "sector": "S", "industry": "I"}},
               tickers=("X",))
    assert collector.collect_sector_meta("us").iloc[0]["QuoteType"] == ""
    monkeypatch.setattr(collector, "load_tickers", lambda market: ["BTC-USD"])
    assert collector.collect_sector_meta("crypto").iloc[0]["QuoteType"] == ""


def test_quote_type_must_be_text():
    frame = meta()
    frame["QuoteType"] = [3]
    prior(frame)
    with pytest.raises(db.DriveSyncError):
        db.load_sector_meta("us")
