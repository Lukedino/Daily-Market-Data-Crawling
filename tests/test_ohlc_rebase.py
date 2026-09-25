"""이벤트(배당·분할) 종목 전체 재수집 — DM-04 (2026-09-26 사용자 결정 "이벤트 종목 전체 재수집").

증분은 auto_adjust=True 라 배당·분할 종목의 과거 봉이 소급 재조정되는데, 파일에는 옛 기준
행이 남아 이력 안에서 기준이 갈라졌다. 그날 액션이 관측된 종목만 전체 이력을 다시 받아
연도 파일에서 그 종목 행만 교체한다. Drive·네트워크 없이 로컬 parquet 으로 검증한다."""
from datetime import date, timedelta
from types import SimpleNamespace
import json
import logging
import math

import pandas as pd
import pytest

from data import ohlc_collector as oc
from data import ohlc_db as db
import main as entry

YEARS = [date.today().year - 1, date.today().year]
COLS = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume", "MarketCap"]


def _frame(ticker, year, close, cap=1000., days=5):
    stamps = [date(year, 1, 5) + timedelta(days=i) for i in range(days)]
    return pd.DataFrame({"Ticker": [ticker] * days, "Date": stamps, "Open": close, "High": close,
                         "Low": close, "Close": close, "Volume": 10., "MarketCap": cap})


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path)
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda tickers: {})
    # 저장본: AAA·BBB, 두 연도, 옛 기준 종가 10, 시총 1000
    for year in YEARS:
        db.save_year(pd.concat([_frame("AAA", year, 10.), _frame("BBB", year, 10.)], ignore_index=True),
                     "crypto", year)
    return tmp_path


def _fake_fetch(monkeypatch, frames_for, failed=(), complete=True):
    """연도별 요청을 받아 frames_for(year, tickers) 가 돌려주는 프레임을 합쳐 준다."""
    calls = []

    def fetch(tickers, start, end, symbol_overrides=None):
        year = int(start[:4])
        calls.append((tuple(tickers), start, end))
        parts = list(frames_for(year, tickers))
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLS)
        df.attrs["ohlc_request"] = {"actions_complete": complete, "action_tickers": []}
        return df, list(failed)

    monkeypatch.setattr(oc, "fetch_ohlc_range", fetch)
    return calls


def _snapshot(tmp_path):
    return {p.name: p.read_bytes() for p in tmp_path.rglob("*.parquet")}


def _pending(tmp_path):
    path = tmp_path / "backfill_pending.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _new_basis(year, tickers, close=9.9, days=5):
    return [_frame(t, year, close, cap=float("nan"), days=days) for t in tickers if t == "AAA"]


def test_rebase_replaces_the_event_ticker_across_all_years_and_keeps_the_rest(local, monkeypatch):
    calls = _fake_fetch(monkeypatch, _new_basis)
    result = oc.rebase_action_tickers("crypto", ["aaa"], upload=False)
    assert result == {"targets": ["AAA"], "replaced": ["AAA"], "deferred": []}
    assert [c[0] for c in calls] == [("AAA",)] * len(YEARS), "존재하는 연도 파일마다 대상 종목만 받는다(없는 해는 건너뛴다)"
    for year in YEARS:
        stored = db.load_year("crypto", year, strict=True)
        aaa, bbb = stored[stored.Ticker == "AAA"], stored[stored.Ticker == "BBB"]
        assert (aaa.Close == 9.9).all() and len(aaa) == 5, "옛 기준 행이 남지 않는다"
        assert (bbb.Close == 10.).all() and len(bbb) == 5, "이벤트가 없는 종목은 그대로"
        assert (aaa.MarketCap == 1000.).all(), "교체해도 시총 이력은 잇는다"
    assert "crypto:rebase" not in (_pending(local) or {}), "끝난 목록은 지운다"


def test_rebase_publishes_the_target_list_before_fetching(local, monkeypatch):
    before = _snapshot(local)

    def boom(*a, **k):
        raise RuntimeError("rate limited")
    monkeypatch.setattr(oc, "fetch_ohlc_range", boom)
    with pytest.raises(db.DriveSyncError):
        oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert _pending(local)["crypto:rebase"] == ["AAA"], "죽어도 다음 실행이 같은 종목을 안다"
    assert _snapshot(local) == before


def test_rebase_merges_previously_deferred_targets_with_todays(local, monkeypatch):
    db.save_pending({"crypto": ["ZZZ"], "crypto:rebase": ["BBB"]})
    seen = _fake_fetch(monkeypatch, lambda year, tickers: [_frame(t, year, 9.9, cap=float("nan")) for t in tickers])
    result = oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert result["targets"] == ["AAA", "BBB"] and result["replaced"] == ["AAA", "BBB"]
    assert seen[0][0] == ("AAA", "BBB")
    assert _pending(local) == {"crypto": ["ZZZ"]}, "신규 종목 백필 목록은 건드리지 않는다"


def test_rebase_defers_a_ticker_whose_earlier_year_came_back_empty(local, monkeypatch, caplog):
    before = _snapshot(local)
    _fake_fetch(monkeypatch, lambda year, tickers: _new_basis(year, tickers) if year == YEARS[-1] else [])
    with caplog.at_level(logging.WARNING):
        result = oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert result["replaced"] == [] and result["deferred"] == ["AAA"]
    assert _snapshot(local) == before, "한 연도라도 비면 어느 연도도 건드리지 않는다"
    assert _pending(local)["crypto:rebase"] == ["AAA"]
    assert any(r.msg == "rebase_deferred" for r in caplog.records)


def test_rebase_defers_a_ticker_whose_history_shrank(local, monkeypatch):
    before = _snapshot(local)
    _fake_fetch(monkeypatch, lambda year, tickers: _new_basis(year, tickers, days=3))   # 5 → 3 (< 90%)
    result = oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert result["deferred"] == ["AAA"] and _snapshot(local) == before


def test_rebase_hard_failure_keeps_the_list_and_raises_a_code(local, monkeypatch):
    before = _snapshot(local)
    _fake_fetch(monkeypatch, _new_basis, failed=["AAA"])
    with pytest.raises(oc.CollectionIncompleteError, match="rebase_incomplete"):
        oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert _pending(local)["crypto:rebase"] == ["AAA"] and _snapshot(local) == before


def test_rebase_holds_when_actions_are_not_confirmed(local, monkeypatch):
    before = _snapshot(local)
    _fake_fetch(monkeypatch, _new_basis, complete=False)
    with pytest.raises(db.PriceBasisError, match="price_basis_unverified"):
        oc.rebase_action_tickers("crypto", ["AAA"], upload=False)
    assert _pending(local)["crypto:rebase"] == ["AAA"] and _snapshot(local) == before


def test_rebase_is_a_noop_without_targets(local, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("대상이 없으면 야후를 부르지 않는다")
    monkeypatch.setattr(oc, "fetch_ohlc_range", boom)
    assert oc.rebase_action_tickers("crypto", [], upload=False) == {"targets": [], "replaced": [], "deferred": []}
    assert _pending(local) is None


def test_safe_targets_rule():
    existing = {2025: _frame("AAA", 2025, 10.), 2026: _frame("AAA", 2026, 10.)}
    full = {2025: _frame("AAA", 2025, 9.), 2026: _frame("AAA", 2026, 9.)}
    assert oc.rebase_safe_targets(["AAA"], full, existing, []) == (["AAA"], [])
    assert oc.rebase_safe_targets(["AAA"], full, existing, ["aaa"]) == ([], ["AAA"])
    partial = {2025: pd.DataFrame(columns=COLS), 2026: _frame("AAA", 2026, 9.)}
    assert oc.rebase_safe_targets(["AAA"], partial, existing, []) == ([], ["AAA"])
    unlisted = {2025: pd.DataFrame(columns=COLS), 2026: _frame("NEW", 2026, 9.)}
    assert oc.rebase_safe_targets(["NEW"], unlisted, {2025: existing[2025], 2026: existing[2026]}, []) == (["NEW"], []), \
        "기존 행이 없는 연도가 비는 것은 정상(상장 전)"


def test_daily_runs_the_rebase_only_after_a_published_increment(monkeypatch):
    seen = []
    monkeypatch.setattr(oc, "backfill_new_tickers", lambda **kwargs: [])
    monkeypatch.setattr(oc, "update_market", lambda market, upload: ["AAA"] if market == "us" else None)
    monkeypatch.setattr(oc, "rebase_action_tickers", lambda market, tickers, upload: seen.append((market, tickers, upload)))
    entry.run_ohlc_update(SimpleNamespace(dry_run=False, market="all", upload_drive=True))
    assert seen == [("us", ["AAA"], True)], "크립토는 '이미 최신'(None) 이라 재수집을 돌리지 않는다"


def test_update_market_returns_the_action_tickers_it_observed(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    monkeypatch.setattr(db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setattr(db, "download_status", lambda *a, **k: None)
    monkeypatch.setattr(db, "load_status", lambda: {"us": {"last_updated": str(date.today() - timedelta(days=3))}})
    monkeypatch.setattr(db, "download_year_state", lambda *a, **k: "absent")
    monkeypatch.setattr(oc, "load_tickers", lambda m: ["AAA", "BBB"])
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda t: {})
    rows = pd.concat([_frame("AAA", date.today().year, 10.), _frame("BBB", date.today().year, 10.)], ignore_index=True)
    rows["Date"] = [date.today() - timedelta(days=3 - i % 3) for i in range(len(rows))]
    rows.attrs["ohlc_request"] = {"actions_complete": True, "action_tickers": ["bbb"]}
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (rows, []))
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda df: df)
    monkeypatch.setattr(db, "append_rows", lambda df, market: [date.today().year])
    monkeypatch.setattr(db, "upload_years", lambda *a, **k: [])
    monkeypatch.setattr(db, "publish_status", lambda *a, **k: None)
    monkeypatch.setattr(oc, "_incremental_cursor", lambda *a, **k: date.today())
    monkeypatch.setattr(oc, "sparse_session_dates", lambda df: [])
    assert oc.update_market("us", upload=True) == ["BBB"]
