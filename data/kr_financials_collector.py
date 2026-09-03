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
        val = float(s)
    except ValueError:
        return None
    return None if val != val else val   # NaN check (NaN != NaN is True)


def extract_cumulative_accounts(df) -> Optional[dict]:
    """finstate 응답 df → 누적 손익 dict. 손익계산서(sj_div IS/CIS) 행만 사용.
    thstrm_add_amount(분기보고서의 누적)가 있으면 우선, 없으면 thstrm_amount
    (반기·사업보고서는 thstrm_amount가 누적/연간) — 이 규칙 하나로 전 보고서 커버."""
    if df is None or len(df) == 0 or "sj_div" not in df.columns:
        return None
    rows = df[df["sj_div"].isin(["IS", "CIS"])]
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
    if ni != ni or st != st:   # NaN check
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


# ══════════════════════════════════════════════════════════════════════════
# 수집 오케스트레이션 (I/O)
# ══════════════════════════════════════════════════════════════════════════
import time

_SLEEP_SEC = 0.15   # DART 한도(분당 1,000) 대비 보수적. 크롤링용 DELAY_API와 별개


def _get_dart():
    """OpenDartReader 인스턴스 (data/collector.py:320 동일 3줄 — 레거시 모듈 import 회피용 복제).
    인스턴스 생성 시 corp_code 매핑을 1회 다운로드한다."""
    import OpenDartReader
    import config
    if not config.DART_API_KEY:
        raise ValueError("DART_API_KEY가 설정되지 않았습니다.")
    import inspect
    if inspect.isclass(OpenDartReader):
        return OpenDartReader(config.DART_API_KEY)
    return OpenDartReader.OpenDartReader(config.DART_API_KEY)


def _fetch_report(dart, code: str, year: int, reprt_code: str):
    """finstate 1회 호출 — 응답은 CFS·OFS 행이 함께 오며 fs_div '컬럼'으로 구분된다
    (OpenDartReader.finstate는 fs_div '인자'를 받지 않는다 — 최종 리뷰에서 실증).
    CFS(연결) 행 우선, 없으면 OFS(개별) 행. 둘 다 없으면 None."""
    df = dart.finstate(code, year, reprt_code=reprt_code)
    time.sleep(_SLEEP_SEC)
    if df is None or len(df) == 0:
        return None
    if "fs_div" in df.columns:
        for fs in ("CFS", "OFS"):
            sub = df[df["fs_div"] == fs]
            if len(sub) > 0:
                return sub
        return None
    return df


def collect_kr_financials(top_n: int = 1000, upload: bool = True,
                          dart=None, years: list | None = None):
    """KR 시총 상위 top_n 종목의 분기 재무 수집 (스펙 §A).

    1. marcap 최신 스냅샷 → 유니버스 (kr_db.download_year + load_year)
    2. ensure_drive_baseline('kr', financials만) — failed면 중단
    3. 종목별: 기존 분기 확인 → 부족 분기의 필요 보고서만 DART 조회 → 누적 차분 → EPS
    4. 50종목마다 중간 저장·업로드 (US 수집기와 동일 패턴)
    """
    from datetime import date as _date
    from data import financials_db, kr_db

    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        _tqdm = None

    this_year = _date.today().year
    years = years or [this_year - 1, this_year]

    # ── 유니버스 ──
    uni_year = this_year
    kr_db.download_year(uni_year)
    marcap = kr_db.load_year(uni_year)
    universe = build_universe(marcap, top_n)
    if universe.empty:
        logger.error("[KrFinancials] marcap 유니버스 비어있음 — 중단")
        return
    stocks_map = dict(zip(universe["Code"], universe["Stocks"]))

    # ── Drive 베이스라인 (kr financials만) ──
    if upload:
        financials_db.ensure_drive_baseline("kr", kinds=("financials",))

    # ── 기존 저장 분기 (증분 스킵 기준) ──
    existing: dict[str, set] = {}
    for y in years:
        df_y = financials_db.load_financials_year("kr", y)
        if df_y.empty or "Ticker" not in df_y.columns:
            continue
        for _, r in df_y.iterrows():
            existing.setdefault(str(r["Ticker"]).zfill(6), set()).add((int(r["Year"]), int(r["Quarter"])))

    if dart is None:
        dart = _get_dart()

    snap = _date.today()
    rows: list[dict] = []
    failed: list[str] = []
    codes = list(universe["Code"])
    it = _tqdm(codes, desc="KR Financials", unit="종목") if _tqdm else codes

    def _flush():
        nonlocal rows
        if not rows:
            return
        df = pd.DataFrame(rows)
        financials_db.save_financials(df, "kr")
        if upload:
            ys = sorted({r["Year"] for r in rows})
            financials_db.upload_financials("kr", ys)
        rows = []

    for idx, code in enumerate(it, start=1):
        try:
            for y in years:
                # 목표 분기: 과거 연도는 4개 전부, 당해 연도도 4개 시도(미공시 보고서는 응답 없음 → 자연 결측)
                have = {q for (yy, q) in existing.get(code, set()) if yy == y}
                need_reports = required_reports(have, {1, 2, 3, 4})
                if not need_reports:
                    continue
                cums = {}
                for rk in sorted(need_reports):
                    df_r = _fetch_report(dart, code, y, REPORT_CODES[rk])
                    cums[rk] = extract_cumulative_accounts(df_r)
                quarters = derive_quarters(cums)
                for q, vals in quarters.items():
                    if q in have:
                        continue
                    if all(v is None for v in vals.values()):
                        continue   # 아무 값도 없는 분기는 저장하지 않음 (미공시)
                    rows.append({
                        "Ticker": code,
                        "PeriodDate": quarter_period_date(y, q),
                        "Year": y, "Quarter": q,
                        "Revenue": vals["Revenue"],
                        "OperatingIncome": vals["OperatingIncome"],
                        "NetIncome": vals["NetIncome"],
                        "DilutedEPS": compute_eps(vals["NetIncome"], stocks_map.get(code)),
                        "SnapDate": snap,
                    })
        except Exception as e:
            logger.warning(f"[KrFinancials] {code} 실패 — 스킵: {type(e).__name__}: {e}")
            failed.append(code)

        if idx % 50 == 0:
            _flush()
            logger.info(f"[KrFinancials] 진행 {idx}/{len(codes)}")

    _flush()
    if failed:
        logger.warning(f"[KrFinancials] 실패 {len(failed)}종목: {failed[:20]}")
    logger.info(f"[KrFinancials] 완료: {len(codes) - len(failed)}/{len(codes)}종목")

    # 실패율 90% 이상 → 파이프라인 자체 결함으로 간주하고 워크플로우를 실패로 종료한다.
    # int(len(codes)*0.9)로 내림하면 소규모 유니버스(예: 2종목)에서 문턱이 1로 무너져
    # 50% 실패에도 오탐 — 정수 내림 없이 비율 그대로 비교한다(최종 리뷰 시정).
    if codes and len(failed) >= len(codes) * 0.9:
        raise RuntimeError(
            f"[KrFinancials] 실패율 과다({len(failed)}/{len(codes)}) — "
            f"수집 파이프라인 자체 결함 가능성. 워크플로우를 실패로 종료한다"
        )
