"""고정 FDR 응답과 원천일자의 결합, 후보 가격/nullable 경계를 합성 검사한다."""
from datetime import date
import io
import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from data import kr_collector as kc
from data import kr_db


def snapshot(day="2026-09-18"):
    frame = pd.DataFrame({"Code": ["005930", "247540", "000001"],
        "Name": ["SYNTHETIC1", "SYNTHETIC2", "SYNTHETIC3"], "MarketId": ["STK", "KSQ", "KNX"],
        "Open": [10., 20., 30.], "High": [11., 21., 31.], "Low": [9., 19., 29.],
        "Close": [10., 20., 30.], "Volume": [100, 200, 300], "Marcap": [1000, 2000, 3000],
        "Stocks": [100, 100, 100], "Date": pd.to_datetime([day] * 3)})
    frame.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache", "source_date": day}
    return frame


class Response:
    def __init__(self, body, status=200):
        self.body, self.status_code, self.closed = body, status, False
    def __enter__(self): return self
    def __exit__(self, *a): self.closed = True
    def iter_content(self, size):
        for i in range(0, len(self.body), size): yield self.body[i:i+size]


def source(frame=None, day="20260918"):
    frame = snapshot() if frame is None else frame
    responses = [Response(json.dumps({"result": {"output": [{"max_work_dt": day}]}}).encode()),
                 Response(frame.to_csv().encode())]
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return responses[len(calls)-1]
    return get, calls, responses


def test_one_source_date_response_selects_the_same_csv_without_today_query():
    get, calls, responses = source()
    frame = kc.read_krx_snapshot(request_get=get)
    assert kc.snapshot_source_date(frame) == date(2026, 9, 18)
    assert len(calls) == 2 and calls[1][0].endswith("/2026-09-18.csv")
    assert all(call[1]["allow_redirects"] is False for call in calls)
    assert all(response.closed for response in responses)


@pytest.mark.parametrize("day", ["20260999", "2026-09-18", None, 20260918, ""])
def test_unknown_source_date_never_fetches_csv_or_becomes_today(day):
    get, calls, responses = source(day=day)
    with pytest.raises(kc.KrCollectionError):
        kc.read_krx_snapshot(request_get=get)
    assert len(calls) == 1 and responses[0].closed


def test_csv_date_different_from_selected_snapshot_is_rejected():
    get, calls, _ = source(frame=snapshot("2026-09-17"))
    with pytest.raises(kc.KrCollectionError, match="source_date_unverified"):
        kc.read_krx_snapshot(request_get=get)
    assert len(calls) == 2


def test_source_version_change_is_not_silently_accepted(monkeypatch):
    monkeypatch.setattr(kc, "version", lambda *a: "0.0.0")
    with pytest.raises(kc.KrCollectionError, match="fdr_version_unverified"):
        kc.read_krx_snapshot(request_get=lambda *a, **k: pytest.fail("no network"))


@pytest.mark.parametrize("day", ["2026-09-18", "2026-01-02", "2025-12-30"])
def test_daily_uses_source_session_even_on_holiday_or_timezone_boundary(monkeypatch, day):
    calls = []
    monkeypatch.setattr(kc, "read_krx_snapshot", lambda: calls.append(1) or snapshot(day))
    frame = kc.collect_daily()
    assert calls == [1] and set(frame["Market"]) == {"KOSPI", "KOSDAQ", "KONEX"}
    assert set(frame["Date"].dt.date) == {date.fromisoformat(day)}
    assert kc.snapshot_source_date(frame) == date.fromisoformat(day)
    kc.validate_price_basis(frame)


def test_unknown_or_adjusted_kr_candidates_cannot_be_saved_as_verified_fdr():
    frame = snapshot()
    frame.attrs = {}
    with pytest.raises(kc.KrCollectionError, match="source_date_unverified"):
        kc.validate_price_basis(frame)
    frame.attrs["kr_price_basis"] = {"provider": "yfinance", "auto_adjust": True}
    with pytest.raises(kc.KrCollectionError, match="price_basis_unverified"):
        kc.validate_price_basis(frame)


def test_yahoo_single_multiindex_candidate_retains_null_meta_and_is_held(monkeypatch):
    raw = pd.DataFrame({("Open", "005930.KS"): [10.], ("High", "005930.KS"): [11.],
        ("Low", "005930.KS"): [9.], ("Close", "005930.KS"): [10.],
        ("Volume", "005930.KS"): [100.]}, index=pd.DatetimeIndex(["2026-09-18"], name="Date"))
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=lambda *a, **k: raw))
    monkeypatch.setattr(kc.time, "sleep", lambda *a: None)
    result = kc.collect_daily_fallback({"005930": {"Name": "SYNTHETIC", "Market": "KOSPI"}}, date(2026, 9, 18))
    assert len(result) == 1 and result.Stocks.isna().all() and result.Rank.isna().all()
    assert str(result.Stocks.dtype) == str(result.Rank.dtype) == "Int64"
    with pytest.raises(kc.KrCollectionError, match="price_basis_unverified"):
        kc.validate_price_basis(result)
    # 순수 소비/저장 형식만 검사한다. 후보를 운영 DB에 게시하지 않는다.
    buffer = io.BytesIO()
    result.to_parquet(buffer, index=False)
    restored = pd.read_parquet(io.BytesIO(buffer.getvalue()))
    assert restored.Stocks.isna().all() and restored.Rank.isna().all()
    assert restored.dropna(subset=["Marcap", "Stocks"]).empty
    assert pd.to_numeric(restored.Stocks).dropna().empty


def test_nullable_new_row_does_not_rewrite_prior_zero_or_valid_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "local_path", lambda year: tmp_path / f"marcap-{year}.parquet")
    prior = kc._normalize_schema(snapshot().assign(Stocks=[100, 0, 100], Rank=[1, 0, 1]))
    kr_db.append_rows(prior)
    incoming = prior.iloc[:1].copy()
    incoming["Date"] = pd.Timestamp("2026-09-21")
    incoming["Stocks"] = pd.Series([pd.NA], dtype="Int64")
    incoming["Rank"] = pd.Series([pd.NA], dtype="Int64")
    incoming["Marcap"] = float("nan")
    kr_db.append_rows(incoming, ohlc_only=True)
    restored = kr_db.load_year(2026, strict=True)
    assert restored.loc[restored.Code == "247540", "Stocks"].iloc[0] == 0
    assert pd.isna(restored.loc[restored.Date == pd.Timestamp("2026-09-21"), "Stocks"].iloc[0])
