# -*- coding: utf-8 -*-
"""
data/kr_financials_collector.py — KR 분기 재무 수집 (섹터리더 2단계 KR, 스펙 §A)

DART 주요계정 API(OpenDartReader.finstate)에서 보고서별 '누적' 손익을 뽑아
차분으로 분기화하고, EPS는 당기순이익 ÷ 상장주식수(marcap Stocks)로 근사한다.
결측은 절대 0으로 채우지 않는다(섹터리뷰 판정불가 원칙).
"""
import logging
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# 분기 → DART 보고서 코드 (1Q/반기/3Q/사업보고서)
REPORT_CODES = {1: "11013", 2: "11012", 3: "11014", 4: "11011"}

# 손익계산서 계정명 변형 (기존 data/collector.py 매핑에서 발췌·보강)
ACCOUNT_MAP = {
    "Revenue":         ["매출액", "영업수익", "수익(매출액)"],
    "OperatingIncome": ["영업이익", "영업이익(손실)"],
    "NetIncome":       ["당기순이익", "당기순이익(손실)", "분기순이익", "반기순이익"],
}

# 분기 차분에 필요한 보고서 키: Q1=1Q누적 / Q2=반기−1Q / Q3=3Q−반기 / Q4=연간−3Q
_QUARTER_DEPS = {1: {1}, 2: {1, 2}, 3: {2, 3}, 4: {3, 4}}


def _parse_amount(v) -> Optional[float]:
    """DART 금액 문자열('1,234' / '-56') → float. 빈값·비수치 → None."""
    s = str(v if v is not None else "").strip().replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_cumulative_accounts(df) -> Optional[dict]:
    """finstate 응답 df → 누적 손익 dict. 손익계산서(sj_div IS/CIS) 행만 사용.
    thstrm_add_amount(분기보고서의 누적)가 있으면 우선, 없으면 thstrm_amount
    (반기·사업보고서는 thstrm_amount가 누적/연간) — 이 규칙 하나로 전 보고서 커버."""
    if df is None or len(df) == 0:
        return None
    rows = df[df.get("sj_div", pd.Series(dtype=str)).isin(["IS", "CIS"])]
    if rows.empty:
        return None
    names = rows["account_nm"].astype(str).str.strip()
    out = {}
    for field, variants in ACCOUNT_MAP.items():
        val = None
        for kw in variants:
            hit = rows[names == kw]
            if not hit.empty:
                r = hit.iloc[0]
                val = _parse_amount(r.get("thstrm_add_amount"))
                if val is None:
                    val = _parse_amount(r.get("thstrm_amount"))
                break
        out[field] = val
    return out


def derive_quarters(cums: dict) -> dict:
    """보고서별 누적 dict({1:1Q,2:반기,3:3Q,4:연간}, 각각 dict|None) → 분기값.
    계정별 독립 차분 — 필요한 두 값 중 하나라도 결측이면 그 분기·그 계정은 None."""
    def get(report_key: int, field: str):
        d = cums.get(report_key)
        return d.get(field) if d else None

    out = {}
    for q in (1, 2, 3, 4):
        row = {}
        for field in ACCOUNT_MAP:
            if q == 1:
                row[field] = get(1, field)
            else:
                cur, prev = get(q, field), get(q - 1, field)
                row[field] = (cur - prev) if (cur is not None and prev is not None) else None
        out[q] = row
    return out


def build_universe(marcap_df: pd.DataFrame, top_n: int = 1000) -> pd.DataFrame:
    """marcap 스냅샷 → 최신 Date의 시총 상위 N. 반환 [Code(6자리 str), Stocks]."""
    if marcap_df is None or marcap_df.empty or "Date" not in marcap_df.columns:
        return pd.DataFrame(columns=["Code", "Stocks"])
    latest = marcap_df[marcap_df["Date"] == marcap_df["Date"].max()].copy()
    latest["Code"] = latest["Code"].astype(str).str.strip().str.zfill(6)
    latest["Marcap"] = pd.to_numeric(latest["Marcap"], errors="coerce")
    latest = latest.dropna(subset=["Marcap"]).sort_values("Marcap", ascending=False)
    return latest.head(top_n)[["Code", "Stocks"]].reset_index(drop=True)


def compute_eps(net_income, stocks) -> Optional[float]:
    """근사 EPS = 분기 당기순이익 ÷ 상장주식수 (스펙 A-1 명시 근사)."""
    try:
        ni, st = float(net_income), float(stocks)
    except (TypeError, ValueError):
        return None
    if st <= 0:
        return None
    return ni / st


def required_reports(existing_quarters: set, target_quarters: set) -> set:
    """이미 저장된 분기를 빼고, 부족한 분기의 차분에 필요한 보고서 키 집합."""
    needed = set()
    for q in target_quarters - set(existing_quarters):
        needed |= _QUARTER_DEPS[q]
    return needed


def quarter_period_date(year: int, quarter: int) -> date:
    """12월 결산 가정 — 분기말 달력일."""
    return {1: date(year, 3, 31), 2: date(year, 6, 30),
            3: date(year, 9, 30), 4: date(year, 12, 31)}[quarter]
