"""KRX 휴장일은 갭도 수집 대상도 아니다.

2026-09-25(추석) kr-daily 실패: 저장본 마지막 날짜 09-23, 어제 09-24 가 평일이라
pd.bdate_range 가 갭으로 판정 → yfinance 백필 → 야후가 휴장 구간에 직전 거래일(09-23)
봉을 돌려줘 결과가 비어 있지 않음 → validate_price_basis 가 yfinance 출처 거부 → exit 1.
09-24 는 갭이 없어 FDR 경로로 성공했다. 다음 함정은 10-06(개천절 대체휴일 10-05 다음 날).
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

_ARGS = SimpleNamespace(dry_run=False, upload_drive=True)


# ── 달력 ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("day,expected", [
    (date(2026, 9, 24), "closed"),   # 추석 전날
    (date(2026, 9, 25), "closed"),   # 추석
    (date(2026, 9, 26), "closed"),   # 토요일
    (date(2026, 9, 28), "open"),     # 다음 거래일
    (date(2026, 10, 5), "closed"),   # 개천절 대체 휴일
    (date(2026, 10, 6), "open"),
])
def test_session_status_knows_krx_holidays(day, expected):
    assert krx_calendar.session_status(day)[0] == expected


def test_trading_days_skip_holidays_and_weekends():
    assert krx_calendar.trading_days(date(2026, 9, 24), date(2026, 9, 27)) == []
    assert krx_calendar.trading_days(date(2026, 9, 23), date(2026, 9, 28)) == [date(2026, 9, 23), date(2026, 9, 28)]


def test_unknown_calendar_falls_back_to_weekdays(monkeypatch):
    def broken(year):
        raise RuntimeError("no calendar")
    monkeypatch.setattr(krx_calendar, "_xkrx", broken)
    assert krx_calendar.session_status(date(2026, 9, 25)) == ("unknown", "")
    assert krx_calendar.trading_days(date(2026, 9, 24), date(2026, 9, 27)) == [date(2026, 9, 24), date(2026, 9, 25)]


# ── run_kr_daily ─────────────────────────────────────────────────

@pytest.fixture
def kr_stubs(monkeypatch, tmp_path):
    """저장본 마지막 날짜 09-23 인 환경. 무엇이 불렸는지만 기록한다."""
    calls = []
    prior = pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"], "Market": ["KOSPI"],
                          "Date": [pd.Timestamp("2026-09-23")]})
    parquet = tmp_path / "marcap.parquet"
    prior.to_parquet(parquet, index=False)
    monkeypatch.setattr(kr_db, "local_path", lambda year: parquet)
    monkeypatch.setattr(kr_db, "get_last_date", lambda year=None: date(2026, 9, 23))
    monkeypatch.setattr(kr_db, "load_year", lambda year, **kwargs: prior)
    monkeypatch.setattr(kr_db, "ensure_year_baselines",
                        lambda years, **kwargs: calls.append("baseline") or {y: "ok" for y in years})
    monkeypatch.setattr(kr_db, "load_status", lambda: {"trading_days_total": 10})
    monkeypatch.setattr(kr_db, "save_status", lambda last_date, days: calls.append("status"))
    monkeypatch.setattr(kr_db, "upload_years", lambda years: calls.append("upload"))
    monkeypatch.setattr(kr_db, "append_rows", lambda df, **kwargs: calls.append("append") or [2026])
    monkeypatch.setattr(kr_collector, "collect_backfill",
                        lambda *a, **k: calls.append("backfill") or pd.DataFrame())
    monkeypatch.setattr(kr_collector, "collect_missing_today", lambda *a, **k: pd.DataFrame())

    def no_cached_session(day):
        raise kr_collector.KrCollectionError("kr_source_failed")
    monkeypatch.setattr(kr_collector, "collect_session", no_cached_session)

    def fdr_snapshot():
        calls.append("collect_daily")
        frame = pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"], "Market": ["KOSPI"],
                              "Close": [270000.0], "Date": [pd.Timestamp("2026-09-28")]})
        frame.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache",
                                       "source_date": "2026-09-28"}
        return frame
    monkeypatch.setattr(kr_collector, "collect_daily", fdr_snapshot)
    return calls


def test_holiday_run_does_nothing_and_exits_zero(kr_stubs):
    """2026-09-25 재현: 백필도 수집도 없이 조용히 끝난다 — 실패 알림이 나가지 않는다."""
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 25))
    assert kr_stubs == []


def test_next_trading_day_after_holidays_sees_no_gap(kr_stubs):
    """09-28(월): 09-24·25 는 휴장, 26·27 은 주말 — 갭 백필 없이 FDR 스냅샷으로 간다."""
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 28))
    assert "backfill" not in kr_stubs
    assert "collect_daily" in kr_stubs and "append" in kr_stubs


def test_real_trading_day_gap_still_backfills(kr_stubs, monkeypatch):
    """09-29(화)에 저장본이 09-23 이면 09-28 이 진짜 갭이다 — 백필 경로는 그대로."""
    monkeypatch.setattr(kr_collector, "validate_price_basis", lambda frame: None)
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 29))
    assert "backfill" in kr_stubs


def _cached_session(calls, unavailable=()):
    def collect(day):
        calls.append(("session", day))
        if day in unavailable:
            raise kr_collector.KrCollectionError("kr_source_failed")
        frame = pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"], "Market": ["KOSPI"],
                              "Close": [270000.0], "Date": [pd.Timestamp(day)]})
        frame.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache",
                                       "source_date": day.isoformat()}
        return frame
    return collect


def test_missed_session_is_filled_from_source_cache_not_yfinance(kr_stubs, monkeypatch):
    """원천 장애가 끝난 다음 실행이 사람 없이 놓친 날을 복구한다(10-29·10-30·11-02 수동 복구 재발 방지)."""
    monkeypatch.setattr(kr_collector, "collect_session", _cached_session(kr_stubs))
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 29))
    assert ("session", date(2026, 9, 28)) in kr_stubs
    assert "backfill" not in kr_stubs
    assert kr_stubs.index("upload") < kr_stubs.index("collect_daily"), "오늘 수집이 실패해도 복구분은 남는다"


def test_unfilled_sessions_fall_through_to_backfill_from_first_failure(kr_stubs, monkeypatch):
    monkeypatch.setattr(kr_collector, "collect_session",
                        _cached_session(kr_stubs, unavailable={date(2026, 9, 29)}))
    monkeypatch.setattr(kr_collector, "validate_price_basis", lambda frame: None)
    seen = {}
    def backfill(start, end, **kwargs):
        seen.update(start=start, end=end)
        return pd.DataFrame()
    monkeypatch.setattr(kr_collector, "collect_backfill", backfill)
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 30))
    assert seen == {"start": "2026-09-29", "end": "2026-09-29"}


def test_weekday_logic_is_kept_when_the_calendar_is_unavailable(kr_stubs, monkeypatch):
    def broken(year):
        raise RuntimeError("no calendar")
    monkeypatch.setattr(krx_calendar, "_xkrx", broken)
    kr_main.run_kr_daily(_ARGS, today=date(2026, 9, 25))
    assert "backfill" in kr_stubs, "달력을 모르면 예전처럼 평일 기준으로 판정한다"
