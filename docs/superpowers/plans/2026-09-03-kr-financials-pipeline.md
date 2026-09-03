# KR 재무 파이프라인 (섹터리더 2단계 KR — A) 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** DART 주요계정 API로 KR 시총 상위 1,000종목의 분기 재무(매출액·영업이익·당기순이익·근사 EPS)를 수집해 `kr/financials/kr_financials_YYYY.parquet`으로 Drive에 축적한다 — US financials-update와 대칭.

**Architecture:** 순수함수(계정 추출·누적 차분 분기화·유니버스 선정·EPS 근사)는 신규 `data/kr_financials_collector.py`에 두고, DART 호출은 저장소에 이미 핀 고정된 **OpenDartReader**(`dart.finstate` = fnlttSinglAcnt 래퍼, corp_code 매핑 내장)를 사용한다. 저장·업로드·베이스라인은 기존 `financials_db.py`를 **market 키 일반화**(`{market}_financials`)로 재사용한다.

**Tech Stack:** OpenDartReader==0.2.2(기존 핀), pandas/pyarrow(기존), pytest. 신규 의존성 없음.

**Spec:** `docs/superpowers/specs/2026-09-03-sector-review-stage2-kr-design.md` §A (사용자 승인본 사본 — 원본은 Pactolus 저장소)

## Global Constraints

- **결측은 절대 0으로 채우지 않는다** — 계정·분기·EPS 결측은 None/NaN 그대로 (섹터리뷰 2단계의 판정불가 원칙).
- **분기화 = 누적 차분**: Q1=1Q누적 / Q2=반기누적−1Q누적 / Q3=3Q누적−반기누적 / Q4=연간−3Q누적. 차분에 필요한 값이 하나라도 결측이면 그 분기 값은 결측. **계정별 독립 차분**(매출액만 결측이어도 순이익은 산출).
- **누적값 선택 규칙**: DART 응답 행에서 `thstrm_add_amount`가 있으면(비어있지 않으면) 그것, 없으면 `thstrm_amount` — 분기보고서(11013·11014)는 add가 누적이고, 반기(11012)·사업보고서(11011)는 thstrm_amount가 누적/연간이라 이 규칙 하나로 전부 커버된다.
- **EPS 근사(스펙 A-1 명시)**: `DilutedEPS = 분기 당기순이익 ÷ 상장주식수(marcap 최신 Stocks)`. Stocks 결측·0이면 EPS 결측. 12월 결산 가정, PeriodDate = 분기말 달력일(03-31/06-30/09-30/12-31).
- **fs_div**: 보고서별 CFS(연결) 우선 → 비었으면 OFS(개별) 폴백 (기존 `collector.py` 관례).
- **계정명 변형 허용**(기존 collector.py 매핑 재사용): 매출액=[매출액, 영업수익, 수익(매출액)] / 영업이익=[영업이익, 영업이익(손실)] / 당기순이익=[당기순이익, 당기순이익(손실), 분기순이익, 반기순이익].
- **대상**: marcap 최신 스냅샷 Marcap 내림차순 상위 1,000 (KOSPI+KOSDAQ 통합, Code 6자리 문자열 유지).
- **증분**: 이미 저장된 (Ticker, PeriodDate) 분기는 재조회하지 않는다 — 부족한 분기에 필요한 보고서만 조회(`required_reports`). 첫 실행 = 종목당 최대 8건(당해+전년 × 4보고서).
- **DART 레이트**: 요청 간 sleep **0.15초**(한도 분당 1,000 대비 보수적 — 기존 `DELAY_API`(0.5~1.0)는 크롤링용이라 따르지 않음, 첫 실행 8,000요청을 러너 시간 안에 끝내기 위함). 종목별 실패는 스킵·계속.
- **Drive 안전장치 그대로**: `ensure_drive_baseline("kr", kinds=("financials",))` 선행(failed면 중단), (Ticker, PeriodDate) dedup keep-last.
- 기존 60개 테스트 회귀 없음. 커밋 메시지 한국어 + 마지막 줄 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- ⚠️ 이 저장소는 **public** — 커밋에 키·폴더 ID 등 비밀값 금지.

## 참조 — 기존 코드 (구현이 기대는 지점)

- `data/collector.py:320 _get_dart()` — OpenDartReader 인스턴스(레거시 모듈이라 import하지 않고 **동일 3줄을 신규 모듈에 복제**: pykrx 등 무거운 레거시 import 회피). `dart.finstate(stock_code, year, reprt_code=, fs_div=)`는 6자리 종목코드를 직접 받음(corp_code 매핑 내장, 인스턴스 생성 시 1회 다운로드 — 스펙의 "매 실행 1회").
- `data/financials_db.py` — `save_financials(df, market)`(연도 분할·병합·dedup), `upload_financials(market, years)`, `ensure_drive_baseline(market, kinds)`. 단 financials의 Drive 경로가 `us_financials`로 하드코딩(253~397행) → Task 2에서 일반화.
- `data/kr_db.py` — `download_year(year)`, `load_year(year)` (marcap: `Code/Name/Close/.../Marcap/Stocks/.../Date`).
- `main.py:496 run_financials_update` — market 분기(us/crypto), `--market` argparse, `financials-update.yml`(options [all, us, crypto], env에 DART_API_KEY 없음 → 추가 필요).
- `config.py:13 DART_API_KEY` — 이미 존재. `DRIVE_PATHS`(28~41행)에 `kr_financials` 키 추가 필요.
- 테스트 관례: `tests/test_*.py` pytest, 네트워크 없음(합성 DataFrame·모킹).

---

### Task 1: 순수함수 — 계정 추출·분기화·유니버스·EPS·필요보고서

**Files:**
- Create: `data/kr_financials_collector.py` (순수함수 부분)
- Create: `tests/test_kr_financials.py`

**Interfaces:**
- Consumes: pandas만
- Produces (Task 3이 사용):
  - `REPORT_CODES = {1: "11013", 2: "11012", 3: "11014", 4: "11011"}` (분기→보고서)
  - `extract_cumulative_accounts(df: pd.DataFrame | None) -> dict | None` — finstate 응답 df → `{"Revenue": float|None, "OperatingIncome": float|None, "NetIncome": float|None}` (누적값). df가 None/빈 것 → None
  - `derive_quarters(cums: dict[int, dict | None]) -> dict[int, dict]` — `{1: {...}|None, 2:…, 3:…, 4:…}` (키=보고서 시점: 1=1Q누적, 2=반기누적, 3=3Q누적, 4=연간) → `{분기: {"Revenue":…, "OperatingIncome":…, "NetIncome":…}}` 차분 결과(계정별 독립)
  - `build_universe(marcap_df: pd.DataFrame, top_n: int = 1000) -> pd.DataFrame` — 최신 Date 스냅샷에서 Marcap 상위 N, 컬럼 `[Code, Stocks]` (Code 6자리 str)
  - `compute_eps(net_income, stocks) -> float | None`
  - `required_reports(existing_quarters: set[int], target_quarters: set[int]) -> set[int]` — 부족 분기가 필요로 하는 보고서 키 집합 (Q1→{1}, Q2→{1,2}, Q3→{2,3}, Q4→{3,4})
  - `quarter_period_date(year: int, quarter: int) -> date`
  - `ACCOUNT_MAP` — Global Constraints의 계정명 변형 매핑

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_kr_financials.py` 신규:

```python
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
```

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_kr_financials.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'data.kr_financials_collector'`

- [ ] **Step 3: 구현**

`data/kr_financials_collector.py` 신규 (순수함수 부분 — I/O는 Task 3에서 추가):

```python
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
```

- [ ] **Step 4: 통과 확인 + 회귀**

Run: `python -m pytest tests/test_kr_financials.py -q` — 전부 PASS
Run: `python -m pytest tests/ -q` — 기존 60건 포함 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add data/kr_financials_collector.py tests/test_kr_financials.py docs/superpowers/specs/2026-09-03-sector-review-stage2-kr-design.md docs/superpowers/plans/2026-09-03-kr-financials-pipeline.md
git commit -m "feat(kr-financials): 순수함수 — DART 계정 추출·누적 차분 분기화·유니버스·EPS 근사 (+스펙·계획 문서)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: financials_db market 키 일반화 + config

**Files:**
- Modify: `config.py` (DRIVE_PATHS 1줄)
- Modify: `data/financials_db.py` (financials Drive 경로 하드코딩 3곳)
- Modify: `tests/test_financials_baseline.py` (kr 케이스 추가)

**Interfaces:**
- Consumes: 기존 `financials_db` 함수들
- Produces (Task 3이 사용): `upload_financials("kr", years)` / `ensure_drive_baseline("kr", kinds=("financials",))` / `download_financials_all("kr")`가 `DRIVE_PATHS["kr_financials"]="kr/financials"`로 동작. US 동작 무변경.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_financials_baseline.py`에 추가 (기존 테스트의 모킹 관례를 따라 — 파일을 먼저 읽고 기존 픽스처/모킹 방식 그대로 kr 케이스를 덧붙인다):

```python
def test_baseline_kr_financials_uses_kr_drive_path(monkeypatch):
    """market='kr' + kinds=('financials',) → DRIVE_PATHS['kr_financials'] 경로로 다운로드."""
    from data import financials_db

    calls = []

    class _FakeUploader:
        def download_all_state(self, remote, local, extensions):
            calls.append(remote)
            return "absent"

    monkeypatch.setattr(financials_db, "_get_uploader", lambda uploader=None: _FakeUploader())
    financials_db.ensure_drive_baseline("kr", kinds=("financials",))
    assert calls == ["kr/financials"]


def test_upload_financials_kr_drive_path(monkeypatch, tmp_path):
    """upload_financials('kr', ...)가 kr/financials 원격 경로를 쓴다."""
    from data import financials_db

    uploaded = []

    class _FakeUploader:
        def upload(self, local, remote):
            uploaded.append(remote)

    # 로컬 파일 존재 조건 충족
    p = financials_db.local_financials_path("kr", 2026)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x")
    try:
        monkeypatch.setattr(financials_db, "_get_uploader", lambda uploader=None: _FakeUploader())
        financials_db.upload_financials("kr", [2026])
        assert uploaded == ["kr/financials"]
    finally:
        p.unlink()
```

(주의: 기존 파일의 import·헬퍼와 충돌하지 않게 파일 끝에 추가. `local_financials_path("kr", 2026)`는 `LOCAL_DATA_DIR` 하위라 실제 레포 경로를 오염시키지 않는지 확인 — 기존 테스트가 같은 패턴을 쓰면 그대로, 아니면 `monkeypatch.setattr(financials_db, "_LOCAL_ROOT", tmp_path)`로 격리)

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_financials_baseline.py -q`
Expected: 신규 2건 FAIL — 현재 코드는 `us_financials` 경로("us/financials")를 반환

- [ ] **Step 3: 구현**

**(a)** `config.py` DRIVE_PATHS에 추가 (`"us_financials"` 줄 근처):

```python
    "kr_financials": "kr/financials", # KR 분기 재무제표 (섹터리더 2단계 KR)
```

**(b)** `data/financials_db.py` — financials 경로 하드코딩 3곳을 market 키로 교체:

모듈에 헬퍼 추가(≈250행, upload_financials 위):

```python
def _financials_drive_key(market: str) -> str:
    return f"{market}_financials"
```

- `upload_financials` (260행): `config.DRIVE_PATHS.get("us_financials")` → `config.DRIVE_PATHS.get(_financials_drive_key(market))`, 에러 메시지도 키 변수 사용
- `download_financials_all` (314행): 동일 교체
- `_baseline_remote_and_local` (364행): `if kind == "financials": remote = config.DRIVE_PATHS.get("us_financials")` → `config.DRIVE_PATHS.get(_financials_drive_key(market))`

(ratios 분기는 손대지 않는다 — kr ratios는 이 스펙 범위 밖)

- [ ] **Step 4: 통과 확인 + 회귀**

Run: `python -m pytest tests/test_financials_baseline.py -q` — 신규 포함 전부 PASS
Run: `python -m pytest tests/ -q` — 전부 PASS (US 경로 무변경 확인)

- [ ] **Step 5: 커밋**

```bash
git add config.py data/financials_db.py tests/test_financials_baseline.py
git commit -m "feat(kr-financials): financials Drive 경로 market 키 일반화 — kr/financials 지원 (us 동작 무변경)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: 수집 오케스트레이션 + CLI/워크플로우 배선

**Files:**
- Modify: `data/kr_financials_collector.py` (I/O 부분 추가)
- Modify: `main.py` (`run_financials_update` kr 분기 + argparse)
- Modify: `.github/workflows/financials-update.yml` (options·env)
- Modify: `tests/test_kr_financials.py` (오케스트레이션 단위 테스트 — DART 모킹)

**Interfaces:**
- Consumes: Task 1 순수함수, Task 2의 kr 경로, `kr_db.download_year/load_year`, `financials_db.save_financials/upload_financials/ensure_drive_baseline/load_financials_year`
- Produces: `collect_kr_financials(top_n=1000, upload=True, dart=None)` — `dart` 파라미터는 테스트 주입용(None이면 실제 OpenDartReader 생성)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_kr_financials.py`에 추가:

```python
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
```

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_kr_financials.py -q`
Expected: 신규 3건 FAIL — `collect_kr_financials` 미구현 (`AttributeError`)

- [ ] **Step 3: 구현 — kr_financials_collector.py에 I/O 부분 추가**

```python
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
    """finstate CFS 우선 → OFS 폴백. 실패는 호출부에서 처리."""
    for fs_div in ("CFS", "OFS"):
        df = dart.finstate(code, year, reprt_code=reprt_code, fs_div=fs_div)
        time.sleep(_SLEEP_SEC)
        if df is not None and len(df) > 0:
            return df
    return None


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
```

구현 시 주의: `derive_quarters`는 cums에 없는 보고서 키를 None으로 취급하므로(`cums.get`), 필요 보고서만 담긴 cums로도 안전. `test_incremental_skips_existing_quarters`처럼 이미 있는 분기는 `q in have`로 이중 방어(재저장 안 함 — dedup은 db가 또 한 번 보장).

**main.py** — `run_financials_update`:

```python
    markets = ["us", "crypto", "kr"] if args.market == "all" else [args.market]
```

kr 분기 추가:

```python
        elif market == "kr":
            logger.info("[FinancialsUpdate] KR 재무 데이터 수집 시작")
            if not args.dry_run:
                from data import kr_financials_collector
                kr_financials_collector.collect_kr_financials(upload=args.upload_drive)
            else:
                logger.info("[DryRun] KR financials update 시뮬레이션")
```

argparse `--market` choices에 `kr` 추가 (기존 choices 위치를 grep으로 찾아 `["all", "us", "crypto", "kr"]` 형태로).

**financials-update.yml**:
- `options: [all, us, crypto]` → `[all, us, crypto, kr]`
- env 블록에 `DART_API_KEY: ${{ secrets.DART_API_KEY }}` 추가

- [ ] **Step 4: 통과 확인 + 회귀**

Run: `python -m pytest tests/test_kr_financials.py -q` — 전부 PASS
Run: `python -m pytest tests/ -q` — 전부 PASS
Run: `python -c "import ast; ast.parse(open('main.py', encoding='utf-8').read()); print('main OK')"`

- [ ] **Step 5: 커밋**

```bash
git add data/kr_financials_collector.py main.py .github/workflows/financials-update.yml tests/test_kr_financials.py
git commit -m "feat(kr-financials): 수집 오케스트레이션 — 유니버스·증분 스킵·중간 저장 + financials-update market=kr 배선

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 문서 + placeholder 준비 + 최종 검증

**Files:**
- Modify: `CLAUDE.md`
- Create: `kr_financials_placeholders/kr_financials_2025.parquet` 외 (사용자 업로드용 빈 parquet)

- [ ] **Step 1: placeholder 생성**

빈 스키마 parquet 3개(2025·2026·2027)를 `kr_financials_placeholders/`에 생성 (ohlc_placeholders 관례):

```python
# 1회성 실행 (커밋되는 것은 산출물 parquet)
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq, os
cols = ["Ticker", "PeriodDate", "Year", "Quarter", "Revenue", "OperatingIncome",
        "NetIncome", "DilutedEPS", "SnapDate"]
os.makedirs("kr_financials_placeholders", exist_ok=True)
for y in (2025, 2026, 2027):
    df = pd.DataFrame(columns=cols)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   f"kr_financials_placeholders/kr_financials_{y}.parquet")
print("OK")
```

- [ ] **Step 2: CLAUDE.md 갱신** — 세 곳:

1. Drive 서브폴더 구조 그림의 `kr/` 아래에 `└─ financials/kr_financials_YYYY.parquet` 추가
2. 워크플로우 표 `financials-update.yml` 행: "US 재무데이터 수집" → "US/KR 재무데이터 수집 (KR은 DART, 시총 상위 1,000)"
3. "주요 이력" 표에 행 추가:

```markdown
| 2026-09-03 | **[FEAT-KR-FINANCIALS]** KR 분기 재무 파이프라인 (섹터리더 2단계 KR §A) — DART 주요계정(OpenDartReader)·누적 차분 분기화·EPS=순이익÷상장주식수 근사·시총 상위 1,000·증분 스킵. `kr/financials/kr_financials_YYYY.parquet` (placeholder 수동 업로드 필요 — SA 제약). financials-update에 market=kr 추가 |
```

- [ ] **Step 3: 전체 테스트 + 커밋**

Run: `python -m pytest tests/ -q` — 전부 PASS

```bash
git add CLAUDE.md kr_financials_placeholders/
git commit -m "docs+chore(kr-financials): CLAUDE.md 반영 + Drive placeholder parquet 3개 (사용자 업로드용)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## 계획 밖 검증 (병합·push 후)

1. **사용자**: Drive `[Database] Market Crawling Data/kr/` 아래 `financials/` 폴더 생성 + `kr_financials_placeholders/`의 parquet 3개 업로드
2. `gh workflow run financials-update.yml -f market=kr` 수동 1회 (첫 수집 ≈ 60분 예상 — 8,000요청 × 0.15s+HTTP)
3. 로그 확인: 유니버스 1,000·베이스라인 상태·실패 종목 수. Drive `kr_financials_2025/2026.parquet` 행 수·(Ticker, PeriodDate) 중복 0·Q2=반기−1Q 표본 대조
4. 이후 B(Mr.Market step7 KR 활성화) 착수 — 이 parquet이 선행 조건
