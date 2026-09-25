"""yfinance 후보는 FDR 저장본과 종목별로 대조된 것만 저장한다 (B안, 2026-09-25).

2026-09-22 계약은 provider=yfinance 를 무조건 보류해 09-08 의 자가 치유(FDR 장애일
전량 폴백·다음날 갭 백필)가 거래일에도 항상 exit 1 이었다. 이제 원시가(auto_adjust=False)로
받아 직전 공통 세션 종가를 저장본과 대조하고, 1호가 안에서 일치한 종목만 채택한다.
"""
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main as kr_main
from data import kr_collector, kr_db, krx_calendar


def _frame(rows, provider="yfinance"):
    frame = pd.DataFrame(rows)
    frame["Date"] = pd.to_datetime(frame["Date"])
    frame.attrs["kr_price_basis"] = {"provider": provider, "auto_adjust": False}
    return frame


REFERENCE = pd.DataFrame({
    "Code": ["005930", "247540", "000660"],
    "Date": [pd.Timestamp("2026-09-23")] * 3,
    "Close": [85_000.0, 120_000.0, 700_000.0],
})


# ── 호가 단위 ─────────────────────────────────────────────────────

@pytest.mark.parametrize("price,tick", [(1_500, 1), (3_000, 5), (15_000, 10), (30_000, 50),
                                        (85_000, 100), (300_000, 500), (700_000, 1_000)])
def test_krx_tick_size(price, tick):
    assert kr_collector.krx_tick_size(price) == tick


# ── 대조 ─────────────────────────────────────────────────────────

def test_matching_codes_are_kept_and_only_new_dates_remain():
    cand = _frame([
        {"Code": "005930", "Date": "2026-09-23", "Close": 85_000.0},   # 겹치는 세션 — 일치
        {"Code": "005930", "Date": "2026-09-28", "Close": 86_000.0},   # 새 행
        {"Code": "000660", "Date": "2026-09-23", "Close": 700_900.0},  # 1호가(1,000) 안
        {"Code": "000660", "Date": "2026-09-28", "Close": 710_000.0},
    ])
    out = kr_collector.verify_against_reference(cand, REFERENCE, keep_from=date(2026, 9, 28))
    assert sorted(out["Code"]) == ["000660", "005930"]
    assert (out["Date"].dt.date == date(2026, 9, 28)).all(), "대조용 과거 세션은 결과에서 뺀다"
    assert out.attrs["kr_price_basis"]["verified"] is True
    assert out.attrs["kr_price_basis"]["codes_verified"] == 2


def test_mismatched_and_unmatched_codes_are_held():
    cand = _frame([
        {"Code": "005930", "Date": "2026-09-23", "Close": 85_000.0},
        {"Code": "005930", "Date": "2026-09-28", "Close": 86_000.0},
        {"Code": "247540", "Date": "2026-09-23", "Close": 118_000.0},  # 2,000원 어긋남(호가 100) — 수정주가 흔적
        {"Code": "247540", "Date": "2026-09-28", "Close": 121_000.0},
        {"Code": "999999", "Date": "2026-09-28", "Close": 10_000.0},   # 저장본에 없음 — 대조 불가
    ])
    out = kr_collector.verify_against_reference(cand, REFERENCE, keep_from=date(2026, 9, 28))
    assert list(out["Code"]) == ["005930"]
    basis = out.attrs["kr_price_basis"]
    assert basis["codes_rejected"] == 1 and basis["codes_unmatched"] == 1


def test_without_reference_the_candidate_stays_unverified():
    cand = _frame([{"Code": "005930", "Date": "2026-09-28", "Close": 86_000.0}])
    out = kr_collector.verify_against_reference(cand, None, keep_from=date(2026, 9, 28))
    assert out.attrs["kr_price_basis"]["verified"] is False
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_collector.validate_price_basis(out)


def test_validate_accepts_only_verified_yahoo_frames():
    ok = _frame([{"Code": "005930", "Date": "2026-09-28", "Close": 86_000.0}])
    ok.attrs["kr_price_basis"]["verified"] = True
    kr_collector.validate_price_basis(ok)                      # 통과
    raw = _frame([{"Code": "005930", "Date": "2026-09-28", "Close": 86_000.0}])
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_collector.validate_price_basis(raw)


# ── 컬렉터가 원시가로 받아 대조한다 ─────────────────────────────────

def _yahoo_raw(tickers, closes_by_date):
    """group_by="ticker" MultiIndex 응답 흉내: (ticker, field)."""
    idx = pd.to_datetime(sorted(closes_by_date))
    frames = {}
    for t in tickers:
        code = t.split(".")[0]
        c = pd.Series([closes_by_date[str(d.date())][code] for d in idx], index=idx)
        frames[(t, "Open")] = c
        frames[(t, "High")] = c
        frames[(t, "Low")] = c
        frames[(t, "Close")] = c
        frames[(t, "Adj Close")] = c * 0.97   # auto_adjust=False 응답에만 있는 열 — 쓰면 안 된다
        frames[(t, "Volume")] = pd.Series([1000] * len(idx), index=idx)
    raw = pd.DataFrame(frames)
    raw.index.name = "Date"
    return raw


META = {"005930": {"Name": "삼성전자", "Market": "KOSPI"},
        "247540": {"Name": "에코프로비엠", "Market": "KOSDAQ"}}


def test_daily_fallback_downloads_raw_prices_and_verifies(monkeypatch):
    calls = []
    closes = {"2026-09-23": {"005930": 85_000.0, "247540": 118_000.0},   # 247540 는 저장본과 어긋난다
              "2026-09-28": {"005930": 86_000.0, "247540": 121_000.0}}

    def fake_download(tickers, **kwargs):
        calls.append(kwargs)
        return _yahoo_raw(list(tickers), closes)
    monkeypatch.setattr(kr_collector, "time", SimpleNamespace(sleep=lambda s: None))
    import yfinance
    monkeypatch.setattr(yfinance, "download", fake_download)

    out = kr_collector.collect_daily_fallback(META, date(2026, 9, 28), reference=REFERENCE)
    assert calls and calls[0]["auto_adjust"] is False, "원시가로 받아야 FDR 저장본과 대조된다"
    assert calls[0]["start"] < "2026-09-23", "직전 공통 세션이 창에 들어와야 한다"
    assert list(out["Code"]) == ["005930"]
    assert out["Date"].dt.date.tolist() == [date(2026, 9, 28)]
    assert float(out["Close"].iloc[0]) == 86_000.0, "Adj Close 가 아니라 원시 Close"
    kr_collector.validate_price_basis(out)                     # 저장 가능


def test_backfill_downloads_raw_prices_and_verifies(monkeypatch):
    calls = []
    closes = {"2026-09-23": {"005930": 85_000.0, "247540": 120_000.0},
              "2026-09-28": {"005930": 86_000.0, "247540": 121_000.0},
              "2026-09-29": {"005930": 87_000.0, "247540": 122_000.0}}

    def fake_download(tickers, **kwargs):
        calls.append(kwargs)
        return _yahoo_raw(list(tickers), closes)
    monkeypatch.setattr(kr_collector, "time", SimpleNamespace(sleep=lambda s: None))
    monkeypatch.setattr(kr_collector, "_build_universe",
                        lambda fdr, fallback_meta=None: pd.DataFrame({
                            "Code": ["005930", "247540"], "Name": ["삼성전자", "에코프로비엠"],
                            "Market": ["KOSPI", "KOSDAQ"], "yf_ticker": ["005930.KS", "247540.KQ"]}))
    import yfinance
    monkeypatch.setattr(yfinance, "download", fake_download)

    out = kr_collector.collect_backfill("2026-09-28", "2026-09-29", fallback_meta=META, reference=REFERENCE)
    assert calls[0]["auto_adjust"] is False and calls[0]["start"] < "2026-09-28"
    assert sorted(set(out["Code"])) == ["005930", "247540"]
    assert sorted(set(out["Date"].dt.date)) == [date(2026, 9, 28), date(2026, 9, 29)], "대조용 09-23 은 뺀다"
    kr_collector.validate_price_basis(out)


# ── run_kr_daily 배선: 저장본이 reference 로 넘어가고, 검증된 후보만 저장된다 ─────

@pytest.fixture
def kr_stubs(monkeypatch, tmp_path):
    calls = {}
    prior = pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"], "Market": ["KOSPI"],
                          "Close": [85_000.0], "Date": [pd.Timestamp("2026-09-23")]})
    parquet = tmp_path / "marcap.parquet"
    prior.to_parquet(parquet, index=False)
    monkeypatch.setattr(kr_db, "local_path", lambda year: parquet)
    monkeypatch.setattr(kr_db, "get_last_date", lambda year=None: date(2026, 9, 23))
    monkeypatch.setattr(kr_db, "load_year", lambda year, **kwargs: prior)
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **kwargs: {y: "ok" for y in years})
    monkeypatch.setattr(kr_db, "load_status", lambda: {"trading_days_total": 10})
    monkeypatch.setattr(kr_db, "save_status", lambda last_date, days: None)
    monkeypatch.setattr(kr_db, "upload_years", lambda years: None)
    monkeypatch.setattr(kr_db, "append_rows", lambda df, **kwargs: calls.setdefault("appended", []).append(df) or [2026])
    monkeypatch.setattr(kr_collector, "collect_missing_today", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(krx_calendar, "session_status", lambda day: ("open", ""))
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: pd.DataFrame())   # FDR 죽음
    return calls, prior


def test_daily_fallback_receives_the_stored_reference_and_verified_rows_are_saved(kr_stubs, monkeypatch):
    calls, prior = kr_stubs
    seen = {}

    def fallback(code_meta, target_date=None, reference=None):
        seen["reference"] = reference
        frame = _frame([{"Code": "005930", "Name": "삼성전자", "Market": "KOSPI",
                         "Date": "2026-09-24", "Close": 86_000.0}])
        frame.attrs["kr_price_basis"]["verified"] = True
        return frame
    monkeypatch.setattr(kr_collector, "collect_daily_fallback", fallback)
    kr_main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True), today=date(2026, 9, 24))
    assert seen["reference"] is prior, "저장본이 대조 기준으로 넘어간다"
    assert calls["appended"], "검증된 후보는 저장된다"


def test_unverified_fallback_still_holds(kr_stubs, monkeypatch):
    calls, _ = kr_stubs
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda code_meta, target_date=None, reference=None:
                            _frame([{"Code": "005930", "Name": "삼성전자", "Market": "KOSPI",
                                     "Date": "2026-09-24", "Close": 86_000.0}]))
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True), today=date(2026, 9, 24))
    assert "appended" not in calls
