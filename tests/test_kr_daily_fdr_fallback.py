"""
FDR StockListing이 죽었을 때의 KR 일별 수집 — yfinance 전량 폴백 + 실패 노출.

2026-09-08 사고: fdr.StockListing("KOSPI"/"KOSDAQ"/"KONEX")는 KRX가 아니라
제3자 GitHub 캐시 저장소의 날짜별 CSV(.../data/listing/krx/{날짜}.csv)를 읽는다.
그 저장소의 자동 업데이트가 그날 04:33 KST 이후 멈춰 오늘자 파일이 없었고,
세 시장 전부 HTTP 404 → 수집 0건 → marcap-2026.parquet이 09-07에서 멈췄다.
그날은 실제 거래일이었다(삼성전자 269,500원 마감).

두 가지가 함께 문제였다:
  1. 수집 실패인데 run_kr_daily가 sys.exit(1)이 아니라 return이라 GHA가
     success로 끝났다 — 실패 알림이 뜰 수가 없었다.
  2. 폴백 경로인 yfinance 백필도 종목 목록을 같은 fdr.StockListing으로 얻어서
     (_build_universe) 같은 404에 걸린다 — "다음날 자동으로 메워진다"는
     자가 치유 자체가 성립하지 않았다.

종목 목록을 만드는 이전 폴백 계약은 유지한다. Q4에서는 원천 날짜나 가격 기준을
확인하지 못한 Yahoo 후보를 저장하지 않는다. 실제 typed FDR 오류는 폴백 호출 전
실패하며, 빈 결과 대역이 만든 폴백 후보도 validate_price_basis에서 보류한다.
"""
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data import kr_collector


# ── 유니버스 폴백 — FDR이 죽어도 parquet에서 종목 목록을 만든다 ────────────────

class _DeadFdr:
    """모든 StockListing 호출이 404로 죽는 FDR 스텁 (2026-09-08 상태)."""

    @staticmethod
    def StockListing(market):
        raise OSError("HTTP Error 404: Not Found")


class _LiveFdr:
    @staticmethod
    def StockListing(market):
        return pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"]})


_DB_META = {
    "005930": {"Name": "삼성전자", "Market": "KOSPI"},
    "247540": {"Name": "에코프로비엠", "Market": "KOSDAQ"},
}


def test_universe_falls_back_to_db_codes_when_fdr_is_dead():
    universe = kr_collector._build_universe(_DeadFdr, fallback_meta=_DB_META)

    assert set(universe["Code"]) == {"005930", "247540"}
    assert set(universe["yf_ticker"]) == {"005930.KS", "247540.KQ"}


def test_universe_fallback_maps_market_suffix_from_db_meta():
    """KOSDAQ 종목에 .KS를 붙이면 yfinance가 전부 빈 결과를 준다."""
    universe = kr_collector._build_universe(_DeadFdr, fallback_meta=_DB_META)

    row = universe[universe["Code"] == "247540"].iloc[0]
    assert row["yf_ticker"] == "247540.KQ"
    assert row["Market"] == "KOSDAQ"


def test_universe_prefers_fdr_when_it_works():
    """FDR이 살아 있으면 기존 경로 그대로 — 폴백은 비상용이다."""
    universe = kr_collector._build_universe(_LiveFdr, fallback_meta=_DB_META)

    assert set(universe["Code"]) == {"005930"}


def test_universe_without_fallback_returns_empty_when_fdr_is_dead():
    """폴백을 안 넘긴 호출부의 기존 동작(빈 결과)은 바뀌면 안 된다."""
    assert kr_collector._build_universe(_DeadFdr).empty


# ── 당일 전종목 폴백 수집 ──────────────────────────────────────────────────────

def test_daily_fallback_requests_every_code_not_just_missing_ones(monkeypatch):
    seen = {}

    def _spy(codes, code_meta, target_date, label, reference=None):
        seen["codes"] = list(codes)
        seen["label"] = label
        return pd.DataFrame({"Code": codes})

    monkeypatch.setattr(kr_collector, "_collect_yfinance_day", _spy)

    kr_collector.collect_daily_fallback(_DB_META, date(2026, 9, 8))

    assert sorted(seen["codes"]) == ["005930", "247540"]


def test_daily_fallback_passes_target_date_through(monkeypatch):
    seen = {}
    monkeypatch.setattr(kr_collector, "_collect_yfinance_day",
                        lambda codes, code_meta, target_date, label, reference=None:
                            seen.update(d=target_date) or pd.DataFrame())

    kr_collector.collect_daily_fallback(_DB_META, date(2026, 9, 8))

    assert seen["d"] == date(2026, 9, 8)


def test_daily_fallback_on_empty_meta_returns_empty():
    """종목이 하나도 없으면 네트워크를 건드리지 않고 빈 결과 — 실제 호출로 확인한다."""
    assert kr_collector.collect_daily_fallback({}, date(2026, 9, 8)).empty


def test_missing_today_still_requests_only_the_missing_codes(monkeypatch):
    """기존 '누락 종목 보완' 경로는 전량으로 바뀌면 안 된다 — 회귀 방지."""
    seen = {}
    monkeypatch.setattr(kr_collector, "_collect_yfinance_day",
                        lambda codes, code_meta, target_date, label, reference=None:
                            seen.update(codes=list(codes)) or pd.DataFrame())

    kr_collector.collect_missing_today(["247540"], _DB_META, date(2026, 9, 8))

    assert seen["codes"] == ["247540"]


# ── run_kr_daily — 수집 실패를 종료 코드로 드러내고, 폴백을 실제로 탄다 ────────
# 2026-09-08 실행은 수집 0건인데도 GHA가 success로 끝났다(run_kr_daily가 return).
# 워크플로우에 실패 알림을 붙여도 실패로 끝나지 않으면 알림이 뜰 수가 없다.

import logging

import main as kr_main
from data import kr_db, krx_calendar


class _Recorder:
    def __init__(self):
        self.appended = None
        self.uploaded = None
        self.status = None
        self.path = None
        self.before = None
        self.append_kwargs = None


@pytest.fixture
def kr_env(monkeypatch, tmp_path):
    """run_kr_daily가 Drive·네트워크 없이 돌도록 주변을 전부 대역으로 바꾼다."""
    rec = _Recorder()
    yesterday = date.today() - timedelta(days=1)

    prior = pd.DataFrame({
        "Code": ["005930", "247540"],
        "Name": ["삼성전자", "에코프로비엠"],
        "Market": ["KOSPI", "KOSDAQ"],
        "Date": [pd.Timestamp(yesterday)] * 2,
    })
    parquet = tmp_path / "marcap.parquet"
    prior.to_parquet(parquet, index=False)
    rec.path, rec.before = parquet, parquet.read_bytes()

    monkeypatch.setattr(kr_db, "local_path", lambda year: parquet)
    monkeypatch.setattr(kr_db, "get_last_date", lambda year=None: yesterday)
    monkeypatch.setattr(krx_calendar, "session_status", lambda day: ("open", ""))   # 실행일이 휴장일이어도 날짜에 매이지 않게
    monkeypatch.setattr(kr_db, "load_year", lambda year, **kwargs: prior)
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **kwargs: {year: "ok" for year in years})
    monkeypatch.setattr(kr_db, "load_status", lambda: {"trading_days_total": 10})
    monkeypatch.setattr(kr_db, "save_status",
                        lambda last_date, days: setattr(rec, "status", (last_date, days)))
    monkeypatch.setattr(kr_db, "upload_years",
                        lambda years: setattr(rec, "uploaded", years))

    def _append(df, **kwargs):
        rec.appended = df
        rec.append_kwargs = kwargs
        return [2026]

    monkeypatch.setattr(kr_db, "append_rows", _append)
    monkeypatch.setattr(kr_collector, "collect_missing_today",
                        lambda *a, **k: pd.DataFrame())
    return rec


def _today_row(source="yfinance"):
    frame = pd.DataFrame({
        "Code": ["005930"],
        "Name": ["삼성전자"],
        "Market": ["KOSPI"],
        "Close": [269500.0],
        "Date": [pd.Timestamp(date.today())],
    })
    if source == "fdr":
        frame.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache",
                                       "source_date": date.today().isoformat()}
    else:
        frame.attrs["kr_price_basis"] = {"provider": "yfinance", "auto_adjust": True}
    return frame


def _assert_hold(rec):
    assert rec.appended is None
    assert rec.uploaded is None
    assert rec.status is None
    assert rec.path.read_bytes() == rec.before


_ARGS = SimpleNamespace(dry_run=False, upload_drive=True)


def test_exits_nonzero_when_fdr_and_yfinance_both_yield_nothing(kr_env, monkeypatch):
    """조용히 return하면 GHA가 success로 끝나 실패 알림이 뜰 수가 없다."""
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: pd.DataFrame())
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda code_meta, target_date=None, reference=None: pd.DataFrame())

    with pytest.raises(SystemExit) as exc:
        kr_main.run_kr_daily(_ARGS)

    assert exc.value.code == 1
    _assert_hold(kr_env)


def test_empty_fdr_yahoo_candidate_holds_without_price_basis(kr_env, monkeypatch):
    """빈 FDR 대역의 Yahoo 후보는 받아도 가격 기준 증명 전에는 저장하지 않는다."""
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: pd.DataFrame())
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda code_meta, target_date=None, reference=None: _today_row())

    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_main.run_kr_daily(_ARGS)
    _assert_hold(kr_env)


def test_fallback_receives_universe_built_from_existing_parquet(kr_env, monkeypatch):
    """폴백의 종목 목록은 FDR이 아니라 이미 받아둔 parquet에서 와야 한다."""
    seen = {}
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: pd.DataFrame())

    def _fallback(code_meta, target_date=None, reference=None):
        seen["meta"] = code_meta
        return _today_row()

    monkeypatch.setattr(kr_collector, "collect_daily_fallback", _fallback)

    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_main.run_kr_daily(_ARGS)

    assert sorted(seen["meta"]) == ["005930", "247540"]
    assert seen["meta"]["247540"]["Market"] == "KOSDAQ"
    _assert_hold(kr_env)


def test_normal_fdr_path_does_not_call_fallback(kr_env, monkeypatch):
    """FDR이 살아 있으면 폴백은 타지 않는다 — 기존 동작 회귀 방지."""
    def _boom(*a, **k):
        raise AssertionError("FDR이 정상인데 폴백을 타면 안 된다")

    monkeypatch.setattr(kr_collector, "collect_daily", lambda: _today_row("fdr"))
    monkeypatch.setattr(kr_collector, "collect_daily_fallback", _boom)

    kr_main.run_kr_daily(_ARGS)

    assert kr_env.appended is not None


# 실제 collect_daily 의 원천 실패(404 등)는 빈 DF 가 아니라 typed raise 다. 2026-09-25 의
# 결정 B(검증된 폴백)는 바로 이 실패를 메우려는 것인데, 폴백이 빈 DF 분기에만 걸려 있어
# 실제 장애에서는 실행된 적이 없었다(2026-09-26 확인). typed 실패 → 검증된 폴백 → 폴백도
# 비면 같은 typed 오류로 끝나 실패 알림이 원인 코드를 싣는다.
def _typed_source_failure(monkeypatch, code="kr_source_failed"):
    def source_failed():
        raise kr_collector.KrCollectionError(code)
    monkeypatch.setattr(kr_collector, "read_krx_snapshot", source_failed)


def test_typed_fdr_failure_reaches_the_verified_yahoo_fallback(kr_env, monkeypatch, caplog):
    _typed_source_failure(monkeypatch)
    seen = {}

    def fallback(code_meta, target_date=None, reference=None):
        seen["meta"] = code_meta
        frame = _today_row()
        frame.attrs["kr_price_basis"]["verified"] = True
        return frame
    monkeypatch.setattr(kr_collector, "collect_daily_fallback", fallback)
    with caplog.at_level(logging.WARNING):
        kr_main.run_kr_daily(_ARGS)
    assert sorted(seen["meta"]) == ["005930", "247540"], "폴백 유니버스는 저장본에서 온다"
    assert kr_env.appended is not None and kr_env.append_kwargs == {"ohlc_only": True}
    fallback_events = [r for r in caplog.records if r.msg == "kr_source_fallback_attempted"]
    assert fallback_events and fallback_events[0].failure_code == "kr_source_failed"


def test_typed_fdr_failure_with_empty_fallback_raises_the_source_code(kr_env, monkeypatch):
    _typed_source_failure(monkeypatch, "source_date_unverified")
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda code_meta, target_date=None, reference=None: pd.DataFrame())
    with pytest.raises(kr_collector.KrCollectionError, match="source_date_unverified"):
        kr_main.run_kr_daily(_ARGS)
    _assert_hold(kr_env)


def test_typed_fdr_failure_with_unverified_fallback_still_holds(kr_env, monkeypatch):
    _typed_source_failure(monkeypatch)
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda code_meta, target_date=None, reference=None: _today_row())
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_main.run_kr_daily(_ARGS)
    _assert_hold(kr_env)


def test_yahoo_supplement_holds_before_append(kr_env, monkeypatch):
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: _today_row("fdr"))
    monkeypatch.setattr(kr_collector, "collect_missing_today", lambda *a, **kw: _today_row())
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        kr_main.run_kr_daily(_ARGS)
    _assert_hold(kr_env)


# ── 갭 backfill도 FDR 없이 돌아야 한다 ────────────────────────────────────────
# "오늘 못 받아도 내일 갭 backfill이 메운다"는 자가 치유가 성립하려면, backfill의
# 종목 목록(_build_universe)이 FDR에 의존하지 않아야 한다. 2026-09-08에는
# 그것마저 같은 404라서 다음날도 못 메울 상태였다.



def test_backfill_survives_dead_fdr_when_given_db_universe(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "FinanceDataReader", _DeadFdr)
    monkeypatch.setitem(sys.modules, "yfinance",
                        SimpleNamespace(download=lambda *a, **k: pd.DataFrame()))

    with caplog.at_level(logging.INFO):
        kr_collector.collect_backfill("2026-09-08", "2026-09-08",
                                      fallback_meta=_DB_META)

    assert "종목 목록 없음" not in caplog.text, "폴백을 줬는데도 중단하면 안 된다"
    assert "backfill 시작" in caplog.text


def test_backfill_without_fallback_still_stops_when_fdr_is_dead(monkeypatch, caplog):
    """폴백을 안 넘긴 기존 호출부(kr-backfill 모드)의 동작은 그대로여야 한다."""
    monkeypatch.setitem(sys.modules, "FinanceDataReader", _DeadFdr)
    monkeypatch.setitem(sys.modules, "yfinance",
                        SimpleNamespace(download=lambda *a, **k: pd.DataFrame()))

    with caplog.at_level(logging.INFO):
        result = kr_collector.collect_backfill("2026-09-08", "2026-09-08")

    assert result.empty
    assert "종목 목록 없음" in caplog.text
