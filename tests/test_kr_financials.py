# -*- coding: utf-8 -*-
"""KR 재무 수집 순수함수 테스트 — 네트워크/파일시스템 의존 없음 (스펙 §A)"""
from datetime import date

import pandas as pd
import pytest

from data.kr_financials_collector import (
    extract_cumulative_accounts, derive_quarters, build_universe,
    compute_eps, required_reports, quarter_period_date, REPORT_CODES,
)


def _finstate_df(rows):
    """OpenDartReader finstate 응답 형태의 합성 df"""
    return pd.DataFrame(rows)


class TestExtractCumulativeAccounts:
    def test_basic_accounts(self):
        df = _finstate_df([
            {"sj_div": "IS", "account_nm": "매출액", "thstrm_amount": "1,000", "thstrm_add_amount": ""},
            {"sj_div": "IS", "account_nm": "영업이익", "thstrm_amount": "200", "thstrm_add_amount": ""},
            {"sj_div": "IS", "account_nm": "당기순이익", "thstrm_amount": "150", "thstrm_add_amount": ""},
        ])
        out = extract_cumulative_accounts(df)
        assert out == {"Revenue": 1000.0, "OperatingIncome": 200.0, "NetIncome": 150.0}

    def test_add_amount_preferred(self):
        # 분기보고서(11013/11014): thstrm_amount=3개월, thstrm_add_amount=누적 → 누적 우선
        df = _finstate_df([
            {"sj_div": "IS", "account_nm": "매출액", "thstrm_amount": "300", "thstrm_add_amount": "900"},
        ])
        assert extract_cumulative_accounts(df)["Revenue"] == 900.0

    def test_account_name_variants(self):
        # 금융사 영업수익 / 손실 표기 / 분기순이익 변형
        df = _finstate_df([
            {"sj_div": "IS", "account_nm": "영업수익", "thstrm_amount": "500", "thstrm_add_amount": ""},
            {"sj_div": "IS", "account_nm": "영업이익(손실)", "thstrm_amount": "-30", "thstrm_add_amount": ""},
            {"sj_div": "IS", "account_nm": "분기순이익", "thstrm_amount": "-50", "thstrm_add_amount": ""},
        ])
        out = extract_cumulative_accounts(df)
        assert out["Revenue"] == 500.0
        assert out["OperatingIncome"] == -30.0
        assert out["NetIncome"] == -50.0

    def test_missing_account_is_none_not_zero(self):
        # 결측 계정은 None — 절대 0 아님 (판정불가 원칙)
        df = _finstate_df([
            {"sj_div": "IS", "account_nm": "당기순이익", "thstrm_amount": "10", "thstrm_add_amount": ""},
        ])
        out = extract_cumulative_accounts(df)
        assert out["Revenue"] is None
        assert out["NetIncome"] == 10.0

    def test_bs_rows_ignored(self):
        # 손익계산서(IS/CIS) 외 시트(재무상태표 BS)는 무시
        df = _finstate_df([
            {"sj_div": "BS", "account_nm": "자산총계", "thstrm_amount": "9,999", "thstrm_add_amount": ""},
            {"sj_div": "IS", "account_nm": "매출액", "thstrm_amount": "100", "thstrm_add_amount": ""},
        ])
        assert extract_cumulative_accounts(df)["Revenue"] == 100.0

    def test_empty_or_none_df(self):
        assert extract_cumulative_accounts(None) is None
        assert extract_cumulative_accounts(pd.DataFrame()) is None

    def test_missing_sj_div_column_returns_none(self):
        # sj_div 컬럼 자체가 없는 응답 → 예외가 아니라 None (판정불가 강등)
        df = _finstate_df([{"account_nm": "매출액", "thstrm_amount": "100", "thstrm_add_amount": ""}])
        assert extract_cumulative_accounts(df) is None

    def test_nan_add_amount_falls_back_to_thstrm(self):
        # add_amount가 NaN이면 thstrm_amount 폴백이 살아야 한다
        df = _finstate_df([{"sj_div": "IS", "account_nm": "매출액",
                            "thstrm_amount": "900", "thstrm_add_amount": float("nan")}])
        assert extract_cumulative_accounts(df)["Revenue"] == 900.0

    def test_nan_amount_is_none(self):
        df = _finstate_df([{"sj_div": "IS", "account_nm": "매출액",
                            "thstrm_amount": float("nan"), "thstrm_add_amount": ""}])
        assert extract_cumulative_accounts(df)["Revenue"] is None


class TestDeriveQuarters:
    def _cums(self, q1, h1, q3, fy):
        def d(v):
            return None if v is None else {"Revenue": v, "OperatingIncome": v / 10, "NetIncome": v / 5}
        return {1: d(q1), 2: d(h1), 3: d(q3), 4: d(fy)}

    def test_full_year(self):
        q = derive_quarters(self._cums(100, 250, 420, 600))
        assert q[1]["Revenue"] == 100
        assert q[2]["Revenue"] == 150   # 250-100
        assert q[3]["Revenue"] == 170   # 420-250
        assert q[4]["Revenue"] == 180   # 600-420

    def test_missing_h1_blocks_q2_q3_only(self):
        # 반기 결측 → Q2·Q3 산출 불가(결측), Q1·Q4는 정상
        q = derive_quarters(self._cums(100, None, 420, 600))
        assert q[1]["Revenue"] == 100
        assert q[2]["Revenue"] is None
        assert q[3]["Revenue"] is None
        assert q[4]["Revenue"] == 180

    def test_per_account_independence(self):
        # 반기 보고서에 매출액만 결측 → Q2 매출액만 결측, 순이익은 산출
        cums = self._cums(100, 250, None, None)
        cums[2]["Revenue"] = None
        q = derive_quarters(cums)
        assert q[2]["Revenue"] is None
        assert q[2]["NetIncome"] == pytest.approx(50 - 20)   # 250/5 - 100/5

    def test_all_missing(self):
        q = derive_quarters({1: None, 2: None, 3: None, 4: None})
        assert all(q[i]["NetIncome"] is None for i in (1, 2, 3, 4))


class TestBuildUniverse:
    def test_top_n_by_marcap_latest_date(self):
        df = pd.DataFrame({
            "Code":   ["005930", "000660", "035420", "005930"],
            "Marcap": [500, 300, 100, 480],
            "Stocks": [100, 50, 30, 100],
            "Date":   [date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 3), date(2026, 9, 3)],
        })
        uni = build_universe(df, top_n=2)
        # 최신일(9/3)만 사용 → 005930(480) > 000660(300) > 035420(100)
        assert list(uni["Code"]) == ["005930", "000660"]
        assert list(uni["Stocks"]) == [100, 50]

    def test_code_zero_padding_preserved(self):
        df = pd.DataFrame({
            "Code": ["660"], "Marcap": [1], "Stocks": [1], "Date": [date(2026, 9, 3)],
        })
        assert list(build_universe(df, 1)["Code"]) == ["000660"]

    def test_empty(self):
        assert build_universe(pd.DataFrame(), 10).empty


class TestComputeEps:
    def test_basic(self):
        assert compute_eps(1_000_000, 1_000) == 1000.0

    def test_missing_or_zero_stocks_none(self):
        assert compute_eps(1_000_000, None) is None
        assert compute_eps(1_000_000, 0) is None
        assert compute_eps(None, 1_000) is None

    def test_nan_inputs_none(self):
        assert compute_eps(float("nan"), 1_000) is None
        assert compute_eps(1_000_000, float("nan")) is None


class TestRequiredReports:
    def test_first_run_needs_all(self):
        assert required_reports(set(), {1, 2, 3, 4}) == {1, 2, 3, 4}

    def test_only_new_quarter(self):
        # Q1·Q2 보유, Q3 필요 → 반기(차분 기준)+3분기 보고서
        assert required_reports({1, 2}, {1, 2, 3}) == {2, 3}

    def test_q4_needs_annual_and_q3(self):
        assert required_reports({1, 2, 3}, {1, 2, 3, 4}) == {3, 4}

    def test_nothing_missing(self):
        assert required_reports({1, 2, 3, 4}, {1, 2, 3, 4}) == set()


def test_quarter_period_date():
    assert quarter_period_date(2026, 1) == date(2026, 3, 31)
    assert quarter_period_date(2026, 2) == date(2026, 6, 30)
    assert quarter_period_date(2026, 3) == date(2026, 9, 30)
    assert quarter_period_date(2026, 4) == date(2026, 12, 31)


def test_report_codes():
    assert REPORT_CODES == {1: "11013", 2: "11012", 3: "11014", 4: "11011"}


class _FakeDart:
    """finstate 스텁 — (code, year, reprt_code) → 합성 df. 호출 기록."""
    def __init__(self, tables):
        self.tables = tables      # {(code, year, reprt_code): df|None}
        self.calls = []

    def finstate(self, code, year, reprt_code="11011", fs_div="CFS"):
        self.calls.append((code, year, reprt_code, fs_div))
        df = self.tables.get((code, year, reprt_code))
        return df if fs_div == "CFS" else None   # OFS 폴백 경로는 별도 케이스로


def _is_row(nm, amount, add=""):
    return {"sj_div": "IS", "account_nm": nm, "thstrm_amount": amount, "thstrm_add_amount": add}


class TestCollectKrFinancials:
    def _setup(self, monkeypatch, tmp_path, tables, marcap):
        from data import kr_financials_collector as kfc, financials_db, kr_db
        monkeypatch.setattr(financials_db, "_LOCAL_ROOT", tmp_path)
        monkeypatch.setattr(kr_db, "download_year", lambda year, uploader=None: True)
        monkeypatch.setattr(kr_db, "load_year", lambda year: marcap)
        monkeypatch.setattr(kfc, "_SLEEP_SEC", 0)   # 테스트 무지연
        return kfc

    def _marcap(self):
        from datetime import date
        return pd.DataFrame({
            "Code": ["005930"], "Marcap": [500], "Stocks": [100],
            "Date": [date(2026, 9, 3)],
        })

    def test_collects_and_saves_quarters(self, monkeypatch, tmp_path):
        # 2026년 1Q·반기 보고서만 존재 → Q1·Q2 저장, EPS = NetIncome/Stocks
        tables = {
            ("005930", 2026, "11013"): _finstate_df([_is_row("매출액", "100"), _is_row("당기순이익", "50")]),
            ("005930", 2026, "11012"): _finstate_df([_is_row("매출액", "250"), _is_row("당기순이익", "120")]),
        }
        kfc = self._setup(monkeypatch, tmp_path, tables, self._marcap())
        fake = _FakeDart(tables)
        kfc.collect_kr_financials(top_n=10, upload=False, dart=fake, years=[2026])

        from data import financials_db
        saved = financials_db.load_financials_year("kr", 2026)
        assert len(saved) == 2
        by_q = saved.set_index("Quarter")
        assert by_q.loc[1, "NetIncome"] == 50
        assert by_q.loc[2, "NetIncome"] == 70          # 120-50
        assert by_q.loc[2, "DilutedEPS"] == pytest.approx(0.7)  # 70/100
        assert str(by_q.loc[1, "Ticker"]) == "005930"

    def test_incremental_skips_existing_quarters(self, monkeypatch, tmp_path):
        # Q1이 이미 저장돼 있으면 1Q 보고서는 재조회하지 않는다 (반기 차분에 필요한 1은 예외적으로 조회)
        tables = {
            ("005930", 2026, "11013"): _finstate_df([_is_row("당기순이익", "50")]),
            ("005930", 2026, "11012"): _finstate_df([_is_row("당기순이익", "120")]),
        }
        kfc = self._setup(monkeypatch, tmp_path, tables, self._marcap())
        from data import financials_db
        financials_db.save_financials(pd.DataFrame([{
            "Ticker": "005930", "PeriodDate": date(2026, 3, 31), "Year": 2026,
            "Quarter": 1, "NetIncome": 50.0, "SnapDate": date(2026, 9, 1),
        }]), "kr")

        fake = _FakeDart(tables)
        kfc.collect_kr_financials(top_n=10, upload=False, dart=fake, years=[2026])
        # Q2 산출에 1Q 누적이 필요하므로 11013·11012 조회는 허용되나, Q1 행은 dedup으로 1개 유지
        saved = financials_db.load_financials_year("kr", 2026)
        assert len(saved) == 2
        assert set(saved["Quarter"]) == {1, 2}

    def test_ticker_failure_skips_and_continues(self, monkeypatch, tmp_path):
        from datetime import date as _d
        marcap = pd.DataFrame({
            "Code": ["005930", "000660"], "Marcap": [500, 300], "Stocks": [100, 50],
            "Date": [_d(2026, 9, 3)] * 2,
        })
        class _BoomDart:
            calls = []
            def finstate(self, code, year, reprt_code="11011", fs_div="CFS"):
                if code == "005930":
                    raise RuntimeError("DART boom")
                return _finstate_df([_is_row("당기순이익", "10")])
        kfc = self._setup(monkeypatch, tmp_path, {}, marcap)
        kfc.collect_kr_financials(top_n=10, upload=False, dart=_BoomDart(), years=[2026])
        from data import financials_db
        saved = financials_db.load_financials_year("kr", 2026)
        assert set(saved["Ticker"]) == {"000660"}   # 실패 종목 스킵, 나머지 계속
