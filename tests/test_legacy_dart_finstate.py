"""레거시 data/collector.get_dart_financials — OpenDartReader 0.2.2 시그니처 준수 (2026-09-05).

`finstate(corp, bsns_year, reprt_code='11011')` 는 fs_div 인자를 받지 않는다. 옛 코드는
`fs_div=` 를 넘겨 TypeError → 바깥 except 가 삼켜 전 종목이 조용히 빈 dict 로 빠졌다
(kr_financials_collector 최종 리뷰에서 발견된 잠복 버그, 미사용 경로라 후속 처리).
응답에는 CFS/OFS 행이 함께 오고 fs_div 컬럼으로 구분한다 — CFS 우선, 없으면 OFS.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data import collector


class _FakeDart:
    """실제 0.2.2 시그니처 — fs_div 키워드를 받으면 TypeError 를 낸다."""
    def __init__(self, frame, calls):
        self.frame, self.calls = frame, calls

    def finstate(self, corp, bsns_year, reprt_code="11011"):
        self.calls.append((corp, bsns_year, reprt_code))
        return self.frame

    def corp_name(self, corp):
        return "테스트"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(collector.time, "sleep", lambda *_: None)


def _frame(rows):
    return pd.DataFrame(rows, columns=["fs_div", "account_nm", "thstrm_amount"])


def test_single_call_prefers_cfs(monkeypatch):
    calls = []
    frame = _frame([("OFS", "매출액", "1,000"), ("CFS", "매출액", "2,000"), ("CFS", "당기순이익", "300")])
    monkeypatch.setattr(collector, "_get_dart", lambda: _FakeDart(frame, calls))
    out = collector.get_dart_financials("005930", 2025)
    assert calls == [("005930", 2025, "11011")]       # 한 번만, fs_div 없이
    assert out["fs_type"] == "CFS"
    assert out["매출액"] == 2000.0 and out["당기순이익"] == 300.0


def test_falls_back_to_ofs(monkeypatch):
    frame = _frame([("OFS", "매출액", "1,000"), ("OFS", "영업이익", "50")])
    monkeypatch.setattr(collector, "_get_dart", lambda: _FakeDart(frame, []))
    out = collector.get_dart_financials("000660", 2025)
    assert out["fs_type"] == "OFS" and out["매출액"] == 1000.0


def test_empty_or_missing_fs_div_returns_empty(monkeypatch):
    monkeypatch.setattr(collector, "_get_dart", lambda: _FakeDart(pd.DataFrame(), []))
    assert collector.get_dart_financials("035420", 2025) == {}
    monkeypatch.setattr(collector, "_get_dart",
                        lambda: _FakeDart(pd.DataFrame({"account_nm": ["매출액"]}), []))
    assert collector.get_dart_financials("035420", 2025) == {}
