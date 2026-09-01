"""섹터리더 2단계용 수집 필드 2종 (2026-09-01).

- financials: 분기 Diluted EPS (`DilutedEPS`) — "최근 2분기 연속 EPS 성장" 필수조건 재료
- ratios: 기관 보유비율 (`HeldPctInstitutions`, %) — "기관 보유 증가 추세" 가산조건 재료.
  스냅샷 누적(ensure_drive_baseline 수정으로 이제 가능)이 이 필드부터 시작돼야
  2~3개월 뒤 추세 판정이 성립하므로, 2단계 본체보다 먼저 넣는다.
"""
import sys
import types
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_income_map_has_diluted_eps():
    from data import financials_collector as fc
    import inspect
    src = inspect.getsource(fc._fetch_quarterly_financials)
    assert '"Diluted EPS"' in src and '"DilutedEPS"' in src


def test_financials_cols_include_diluted_eps():
    from data import financials_db
    assert "DilutedEPS" in financials_db._FINANCIALS_COLS


def test_ratios_cols_include_held_pct():
    from data import financials_db
    assert "HeldPctInstitutions" in financials_db._RATIOS_COLS


def test_ratios_snapshot_maps_held_pct(monkeypatch):
    """heldPercentInstitutions 0.853(소수) → 85.3(%)로 저장돼야 한다."""
    from data import financials_collector as fc

    class _StubTicker:
        def __init__(self, _):
            self.info = {"longName": "Apple Inc.", "heldPercentInstitutions": 0.853}

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=_StubTicker))
    row = fc._fetch_ratios_snapshot("AAPL", date(2026, 9, 1))
    assert abs(row["HeldPctInstitutions"] - 85.3) < 1e-9


def test_ratios_snapshot_held_pct_missing_is_none(monkeypatch):
    from data import financials_collector as fc

    class _StubTicker:
        def __init__(self, _):
            self.info = {"longName": "Apple Inc."}

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=_StubTicker))
    row = fc._fetch_ratios_snapshot("AAPL", date(2026, 9, 1))
    assert row["HeldPctInstitutions"] is None
