"""실제 parquet의 시총 보존과 관측 가능한 가격 기준 불일치만 검증한다."""
from datetime import date
import math
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from data import ohlc_collector as oc
from data import ohlc_db as db


def rows(day=date(2026, 1, 5), cap=1000., close=10.):
    return pd.DataFrame({"Ticker": ["AAA"], "Date": [day], "Open": [close], "High": [close],
                         "Low": [close], "Close": [close], "Volume": [10.], "MarketCap": [cap]})


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    return tmp_path


@pytest.mark.parametrize("new_cap,expected", [(float("nan"), 1000.), (0., 0.), (2000., 2000.)])
def test_overlap_preserves_only_missing_marketcap_and_updates_prices(local, new_cap, expected):
    db.save_year(rows(), "crypto", 2026)
    db.save_year(rows(cap=new_cap, close=20.), "crypto", 2026)
    result = db.load_year("crypto", 2026, strict=True).iloc[0]
    assert result.MarketCap == expected and result.Close == 20.


def test_purge_does_not_resurrect_old_token_marketcap(local):
    db.save_year(rows(), "crypto", 2026)
    db.save_year(rows(cap=float("nan")), "crypto", 2026, replace_tickers=["AAA"])
    assert math.isnan(db.load_year("crypto", 2026, strict=True).iloc[0].MarketCap)


def test_two_missing_caps_stay_missing_and_other_day_is_not_used(local):
    db.save_year(rows(), "crypto", 2026)
    db.save_year(rows(day=date(2026, 1, 6), cap=float("nan")), "crypto", 2026)
    assert math.isnan(db.load_year("crypto", 2026, strict=True).iloc[-1].MarketCap)


@pytest.mark.parametrize("change", [0.5, 2., 1.01])
def test_confirmed_overlap_change_holds_candidate_without_rewriting(local, change):
    db.save_year(rows(), "us", 2026)
    before = db.local_path("us", 2026).read_bytes()
    with pytest.raises(db.PriceBasisError, match="price_basis_mismatch"):
        db.validate_price_basis(rows(close=10 * change), "us")
    assert db.local_path("us", 2026).read_bytes() == before


def test_new_day_jump_and_float_representation_are_not_overlap_mismatch(local):
    db.save_year(rows(), "us", 2026)
    db.validate_price_basis(rows(close=10 + 1e-9), "us")
    db.validate_price_basis(rows(day=date(2026, 1, 6), close=100), "us")


def test_crypto_unfinished_previous_utc_day_can_complete(local):
    db.save_year(rows(day=date(2026, 9, 21)), "crypto", 2026)
    db.validate_price_basis(rows(day=date(2026, 9, 21), close=11), "crypto", as_of=date(2026, 9, 22))
    with pytest.raises(db.PriceBasisError):
        db.validate_price_basis(rows(day=date(2026, 9, 21), close=11), "crypto", as_of=date(2026, 9, 23))


@pytest.mark.parametrize("dividend,split", [(1., 0.), (0., 2.), (0., 0.)])
def test_provider_actions_are_requested_and_observed_without_changing_stored_convention(local, monkeypatch, dividend, split):
    calls = []
    raw = rows().drop(columns=["Ticker", "MarketCap"]).set_index("Date")
    raw["Dividends"], raw["Stock Splits"] = dividend, split
    def download(*a, **kwargs):
        calls.append(kwargs)
        return raw
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=download))
    monkeypatch.setattr(oc.time, "sleep", lambda *a: None)
    result, failed = oc.fetch_ohlc_range(["AAA"], "2026-01-05", "2026-01-06")
    assert not failed and calls[0]["auto_adjust"] is True and calls[0]["actions"] is True
    assert result.iloc[0].Dividends == 0 and result.iloc[0].Splits == 1
    if dividend or split:
        with pytest.raises(db.PriceBasisError, match="price_basis_unverified"):
            db.validate_price_basis(result, "us")
    else:
        db.validate_price_basis(result, "us")


def test_actions_missing_is_not_certified_as_zero(local):
    frame = rows()
    frame.attrs["ohlc_request"] = {"actions_complete": False, "action_tickers": []}
    with pytest.raises(db.PriceBasisError, match="price_basis_unverified"):
        db.validate_price_basis(frame, "us")


def test_capital_gains_action_also_holds_adjusted_candidate(local, monkeypatch):
    raw = rows().drop(columns=["Ticker", "MarketCap"]).set_index("Date")
    raw["Dividends"], raw["Stock Splits"], raw["Capital Gains"] = 0., 0., 1.
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=lambda *a, **k: raw))
    monkeypatch.setattr(oc.time, "sleep", lambda *a: None)
    result, failed = oc.fetch_ohlc_range(["AAA"], "2026-01-05", "2026-01-06")
    assert not failed
    with pytest.raises(db.PriceBasisError, match="price_basis_unverified"):
        db.validate_price_basis(result, "us")


def test_current_marketcap_is_never_backdated_to_the_latest_historical_row(monkeypatch):
    frame = rows(day=date(2000, 1, 3), cap=float("nan"))
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(
        Ticker=lambda *a: pytest.fail("historical cap must not be fetched")))
    monkeypatch.setattr(oc.requests, "Session", lambda: pytest.fail("historical cap must not be fetched"))
    assert oc._enrich_us_marketcap(frame).MarketCap.isna().all()
    assert oc._enrich_crypto_marketcap(frame).MarketCap.isna().all()


def test_yfinance_calendar_alignment_nan_rows_are_not_partial_prices(local, monkeypatch):
    first = rows().drop(columns=["Ticker", "MarketCap"]).set_index("Date")
    first["Dividends"], first["Stock Splits"] = 0., 0.
    second = first.copy()
    second.index = pd.DatetimeIndex(["2026-01-06"], name="Date")
    # 실제 yfinance multi.reindex_dfs와 같은 날짜 합집합 후 concat 결과이다.
    union = first.index.union(second.index)
    raw = pd.concat({"AAA": first.reindex(union), "U-UN.TO": second.reindex(union)}, axis=1)
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=lambda *a, **k: raw))
    monkeypatch.setattr(oc.time, "sleep", lambda *a: None)
    result, failed = oc.fetch_ohlc_range(["AAA", "U-UN.TO"], "2026-01-05", "2026-01-07")
    assert not failed and len(result) == 2
    assert result.attrs["ohlc_request"]["actions_complete"] is True
    assert oc._incremental_cursor(result, ["AAA", "U-UN.TO"], failed, date(2026, 1, 5), "us") == date(2026, 1, 5)
    db.validate_price_basis(result, "us")


def test_partially_missing_price_row_still_blocks_the_ticker():
    raw = rows().drop(columns=["Ticker", "MarketCap"]).set_index("Date")
    raw["Open"] = float("nan")
    raw["Dividends"], raw["Stock Splits"] = 0., 0.
    with pytest.raises(oc.CollectionIncompleteError, match="price_values_unverified"):
        oc._finalize_ticker_frame(raw, "AAA")
