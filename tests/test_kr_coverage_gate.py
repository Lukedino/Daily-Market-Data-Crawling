"""
KR 일별 데이터의 종목 범위 게이트 — 수집 단계(2A)와 저장 계층(2B).

2026-10-01 사고: 그날 1순위 원천(FDR 캐시)이 `kr_source_failed` 로 실패했고, 검증된
yfinance 폴백은 저장본과의 종목별 가격 대조를 약 8%만 통과했다. 그 결과
234/2,873종목만 든 하루가 그대로 저장·게시되고 GHA 는 success 로 끝났다. US·Crypto
저장 경로에는 날짜별 급감 게이트(`CoverageGapError`)가 있지만 KR `kr_db` 에는 없었다.
하류 ML 은 그 하루 때문에 학습 게이트에 7일 연속 막혔다(종목별 10행 뒤 라벨이라
결손일 앞 마지막 라벨일이 ~800 → 73종목으로 얇아짐).

여기서 잠그는 것
  2A  수집 단계: 폴백 후에도 오늘 종목 수가 정상 최근 유니버스의 90% 미만이면 저장·게시
      없이 `kr_coverage_insufficient` 로 실패한다(→ kr-daily.yml 텔레그램 알림).
  2B  저장 계층: 일일·백필·폴백·정정 어느 경로든 `save_year` 가 **병합 후 실제로 저장될**
      데이터에서 이번 쓰기가 건드린 날짜의 종목 수를 기존 기준과 비교한다.
      - US 게이트는 부분집합 병합 시 기존 파일에 없던 날짜를 검사에서 뺀다 — 바로 그
        구멍으로 부분 수집일이 새 날짜로 들어온다. KR 은 새 날짜도 검사한다.
      - 기준은 인접 기존 거래일 5개의 종목 수 **중앙값**이라 기준 창 안의 부분 수집일
        하나가 기준을 끌어내리지 못한다. 이번 쓰기 대상 날짜는 기준에서 뺀다.
      - 이미 저장된 다른 날의 결손은 이번 쓰기를 막지 않는다(오염 하나가 이후 모든 저장을
        영구히 막던 US 2026-09-05 사고의 반복 방지).
      - 실패 시 기존 파일 바이트는 그대로다.
"""
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main as kr_main
from data import execution_safety, kr_collector, kr_db, krx_calendar

UNIVERSE = [f"{i:06d}" for i in range(100000, 100300)]          # 300종목 (하한 100 이상)


def _sessions(start="2026-09-01", n=8):
    return list(pd.bdate_range(start, periods=n))


def _day(day, codes=UNIVERSE, close=1000.0):
    return pd.DataFrame({
        "Code": list(codes), "Name": ["n"] * len(codes), "Close": [close] * len(codes),
        "Market": ["KOSPI"] * len(codes), "Date": [pd.Timestamp(day)] * len(codes),
    })


def _history(days, codes=UNIVERSE):
    return pd.concat([_day(d, codes) for d in days], ignore_index=True)


# ── 2B: 저장 계층 ────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "_LOCAL_ROOT", tmp_path / "kr")
    monkeypatch.setattr(kr_db, "_STATUS_PATH", tmp_path / "meta" / "kr_status.json")
    return kr_db


def _seed(db, frame, year=2026):
    db.save_year(frame, year)
    return db.local_path(year).read_bytes()


def test_full_new_day_is_saved(db):
    days = _sessions()
    _seed(db, _history(days[:-1]))
    db.save_year(_day(days[-1]), 2026)
    saved = db.load_year(2026, strict=True)
    assert saved[saved.Date == days[-1]].Code.nunique() == len(UNIVERSE)


def test_partial_new_day_like_2026_10_01_is_refused_and_file_kept(db):
    """10-01 재현: 기존 날짜엔 없던 새 날짜에 8% 만 — US 의 부분집합 구멍으로는 통과했을 것."""
    days = _sessions()
    before = _seed(db, _history(days[:-1]))
    partial = _day(days[-1], UNIVERSE[:24])                       # 24/300 = 8%
    with pytest.raises(kr_db.KrCoverageError) as caught:
        db.save_year(partial, 2026)
    assert caught.value.code == "kr_coverage_gap"
    assert db.local_path(2026).read_bytes() == before


@pytest.mark.parametrize("kept, ok", [(270, True), (269, False)])   # 270/300 = 90.0% · 269 = 89.7%
def test_ninety_percent_boundary(db, kept, ok):
    days = _sessions()
    before = _seed(db, _history(days[:-1]))
    write = _day(days[-1], UNIVERSE[:kept])
    if ok:
        db.save_year(write, 2026)
    else:
        with pytest.raises(kr_db.KrCoverageError):
            db.save_year(write, 2026)
        assert db.local_path(2026).read_bytes() == before


def test_subset_supplement_into_an_existing_full_day_is_allowed(db):
    """누락 종목 보완처럼 이미 꽉 찬 날짜에 일부만 쓰는 것은 정상이다 — 병합 후 범위로 본다."""
    days = _sessions()
    _seed(db, _history(days))
    db.save_year(_day(days[-1], UNIVERSE[:5], close=2000.0), 2026)
    saved = db.load_year(2026, strict=True)
    assert saved[saved.Date == days[-1]].Code.nunique() == len(UNIVERSE)


def test_subset_write_cannot_slip_a_partial_new_date_in(db):
    """부분집합 쓰기에 새 날짜가 섞여 있어도 그 날짜는 검사된다(US subset_merge 구멍 금지)."""
    days = _sessions()
    before = _seed(db, _history(days[:-1]))
    mixed = pd.concat([_day(days[-2], UNIVERSE[:5]), _day(days[-1], UNIVERSE[:5])])
    with pytest.raises(kr_db.KrCoverageError):
        db.save_year(mixed, 2026)
    assert db.local_path(2026).read_bytes() == before


def test_one_partial_day_in_the_baseline_window_does_not_shrink_the_baseline(db):
    """기준 창 안의 10-01 류 결손일 하나가 기준을 끌어내리면 다음 결손도 통과한다."""
    days = _sessions()
    history = pd.concat([_history(days[:4]), _day(days[4], UNIVERSE[:24]), _history(days[5:7])])
    db.save_year(history, 2026, allow_gap=True)                  # 과거 오염을 그대로 심는다
    with pytest.raises(kr_db.KrCoverageError):
        db.save_year(_day(days[7], UNIVERSE[:200]), 2026)          # 200/300 — 중앙값 기준이면 거부


def test_existing_partial_day_does_not_block_an_unrelated_write(db):
    """이미 저장된 결손일은 이번 쓰기의 대상이 아니면 판정하지 않는다(영구 정체 방지)."""
    days = _sessions()
    history = pd.concat([_history(days[:3]), _day(days[3], UNIVERSE[:24]), _history(days[4:7])])
    db.save_year(history, 2026, allow_gap=True)
    db.save_year(_day(days[7]), 2026)


def test_repairing_a_partial_day_is_allowed(db):
    """결손일을 전 종목으로 채우는 쓰기(정정·복구)는 범위를 늘리므로 통과한다."""
    days = _sessions()
    history = pd.concat([_history(days[:3]), _day(days[3], UNIVERSE[:24]), _history(days[4:])])
    db.save_year(history, 2026, allow_gap=True)
    db.save_year(_day(days[3]), 2026)
    saved = db.load_year(2026, strict=True)
    assert saved[saved.Date == days[3]].Code.nunique() == len(UNIVERSE)


def test_small_synthetic_universe_is_not_judged(db):
    """기준 100종목 미만은 판정하지 않는다 — US 게이트와 같은 하한(합성·초기 데이터)."""
    days = _sessions()
    small = [c for c in UNIVERSE[:10]]
    _seed(db, _history(days[:-1], small))
    db.save_year(_day(days[-1], small[:1]), 2026)


def test_year_start_uses_previous_year_tail_as_baseline(db):
    """1월 첫 거래일엔 같은 해 기준이 없다 — 전년도 마지막 거래일들이 기준이 된다."""
    _seed(db, _history(list(pd.bdate_range("2025-12-22", "2025-12-30")) ), year=2025)
    with pytest.raises(kr_db.KrCoverageError):
        db.save_year(_day("2026-01-02", UNIVERSE[:24]), 2026)
    db.save_year(_day("2026-01-02"), 2026)


def test_no_baseline_at_all_is_unverified_not_blocked(db, caplog):
    caplog.set_level(logging.WARNING)
    db.save_year(_day("2026-01-02", UNIVERSE[:24]), 2026)
    assert any(r.msg == "kr_coverage_unverified" for r in caplog.records)


def test_append_rows_passes_the_gate_through(db):
    days = _sessions()
    before = _seed(db, _history(days[:-1]))
    with pytest.raises(kr_db.KrCoverageError):
        db.append_rows(_day(days[-1], UNIVERSE[:24]))
    assert db.local_path(2026).read_bytes() == before


def test_refusal_logs_the_worst_date_with_metrics(db, caplog):
    caplog.set_level(logging.ERROR)
    days = _sessions()
    _seed(db, _history(days[:-1]))
    with pytest.raises(kr_db.KrCoverageError):
        db.save_year(_day(days[-1], UNIVERSE[:24]), 2026)
    record = next(r for r in caplog.records if r.msg == "kr_coverage_gap_blocked")
    m = record.public_metrics
    assert (m["kr_date"], m["kr_count"], m["kr_baseline"]) == (days[-1].date().isoformat(), 24, 300)
    assert m["kr_ratio_pct"] == pytest.approx(8.0)


# ── 2A: 수집 단계 (run_kr_daily) ──────────────────────────────────────────────

class _Recorder:
    appended = uploaded = status = None


@pytest.fixture
def kr_env(monkeypatch):
    rec = _Recorder()
    today = date.today()
    prior = _history([pd.Timestamp(today) - pd.Timedelta(days=k) for k in range(7, 0, -1)])
    monkeypatch.setattr(krx_calendar, "session_status", lambda day: ("open", ""))
    monkeypatch.setattr(krx_calendar, "trading_days", lambda a, b: [])
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **k: {y: "ok" for y in years})
    monkeypatch.setattr(kr_db, "load_year", lambda year, **k: prior)
    monkeypatch.setattr(kr_db, "get_last_date", lambda year=None: today - timedelta(days=1))
    monkeypatch.setattr(kr_db, "load_status", lambda: {})
    monkeypatch.setattr(kr_db, "save_status", lambda d, n: setattr(rec, "status", d))
    monkeypatch.setattr(kr_db, "upload_years", lambda years: setattr(rec, "uploaded", years) or [])
    monkeypatch.setattr(kr_db, "append_rows",
                        lambda df, **k: setattr(rec, "appended", df) or [today.year])
    monkeypatch.setattr(kr_collector, "collect_missing_today", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(kr_collector, "validate_price_basis", lambda frame: None)
    rec.today = today
    return rec


ARGS = SimpleNamespace(dry_run=False, upload_drive=True)


def _fdr_today(today, codes):
    frame = _day(today, codes)
    frame.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache",
                                   "source_date": today.isoformat()}
    return frame


def test_daily_full_snapshot_is_saved_and_published(kr_env, monkeypatch):
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: _fdr_today(kr_env.today, UNIVERSE))
    kr_main.run_kr_daily(ARGS)
    assert kr_env.appended is not None and kr_env.uploaded == [kr_env.today.year]


def test_daily_fallback_with_eight_percent_fails_without_saving(kr_env, monkeypatch, caplog):
    """10-01 재현: 1순위 실패 → 폴백 234/2,873 → 예전엔 저장·success."""
    caplog.set_level(logging.ERROR)

    def source_failed():
        raise kr_collector.KrCollectionError("kr_source_failed")

    monkeypatch.setattr(kr_collector, "collect_daily", source_failed)
    monkeypatch.setattr(kr_collector, "collect_daily_fallback",
                        lambda meta, day, **k: _day(kr_env.today, UNIVERSE[:24]))
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_main.run_kr_daily(ARGS)
    assert caught.value.args[0] == "kr_coverage_insufficient"
    assert kr_env.appended is None and kr_env.uploaded is None and kr_env.status is None
    record = next(r for r in caplog.records if r.msg == "kr_coverage_insufficient")
    assert record.public_metrics["kr_source"] == "yfinance_fallback"
    assert (record.public_metrics["kr_count"], record.public_metrics["kr_baseline"]) == (24, 300)


@pytest.mark.parametrize("kept, ok", [(270, True), (269, False)])
def test_daily_ninety_percent_boundary(kr_env, monkeypatch, kept, ok):
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: _fdr_today(kr_env.today, UNIVERSE[:kept]))
    if ok:
        kr_main.run_kr_daily(ARGS)
        assert kr_env.appended is not None
    else:
        with pytest.raises(kr_collector.KrCollectionError):
            kr_main.run_kr_daily(ARGS)
        assert kr_env.appended is None


def test_coverage_failure_reaches_the_public_artifact_and_exit_code(kr_env, monkeypatch):
    """cli_entry 가 코드를 살린 채 1로 끝나야 kr-daily.yml 의 실패 알림이 뜬다."""
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: _fdr_today(kr_env.today, UNIVERSE[:24]))
    assert "kr_coverage_insufficient" in execution_safety._ERROR_CODES
    assert execution_safety.cli_entry(lambda: kr_main.run_kr_daily(ARGS)) == 1


def test_daily_workflow_notifies_on_failure():
    text = (Path(__file__).resolve().parents[1] / ".github" / "workflows" / "kr-daily.yml").read_text(encoding="utf-8")
    assert "if: failure()" in text and "TELEGRAM_TOKEN" in text


# ── 공개 로그의 검토된 구조화 필드 ───────────────────────────────────────────

def _format(**extra):
    record = logging.LogRecord("x", logging.ERROR, "kr_db.py", 1, "kr_coverage_gap_blocked", (), None)
    for key, value in extra.items():
        setattr(record, key, value)
    return execution_safety.PublicFormatter().format(record)


def test_public_metrics_are_printed_in_a_fixed_order():
    line = _format(public_metrics={"kr_source": "fdr_snapshot", "kr_count": 234, "kr_baseline": 2873,
                                   "kr_ratio_pct": 8.14, "kr_date": "2026-10-01"})
    assert line.endswith("kr_date=2026-10-01 kr_count=234 kr_baseline=2873 kr_ratio_pct=8.1 kr_source=fdr_snapshot")


@pytest.mark.parametrize("bad", [
    {"kr_date": "C:/secret/path"}, {"kr_count": "234"}, {"kr_count": True}, {"kr_count": -1},
    {"kr_ratio_pct": float("nan")}, {"kr_source": "https://example.invalid/token"},
    {"unknown_field": 1},
])
def test_unreviewed_or_malformed_metrics_are_dropped(bad):
    line = _format(public_metrics=bad)
    assert line.rstrip().endswith("kr_coverage_gap_blocked")
