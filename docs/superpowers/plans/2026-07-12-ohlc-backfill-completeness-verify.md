# US/Crypto 백필 완결성 검증 스크립트 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 티커별 최초 수집일을 스캔해 `--after` 기준일 이후에 데이터가 시작되는
("의심스럽게 최근에 추가된") 티커를 자동으로 찾아 리포트하고, `--fix`로
`backfill_pending.json`에 반영해 기존 백필 파이프라인이 재시도하도록 한다.

**Architecture:** `data/ohlc_db.py`에 `list_known_tickers()`와 동일한 패턴의
`first_seen_dates()`를 추가하고, `scripts/verify_ohlc.py`(신규, `verify_kr.py`와
동일 구조)가 이를 호출해 리포트를 출력한다. `--fix`는 기존
`load_pending()`/`save_pending()`/`upload_pending()`을 재사용해 병합만 하며,
실제 yfinance fetch는 하지 않는다(기존 `backfill_new_tickers()`에 위임).

**Tech Stack:** Python 3.12, pandas, pyarrow, unittest.mock (스크래치 검증)

## Global Constraints

- 이 프로젝트는 pytest 등 테스트 프레임워크가 없다. 검증은 `unittest.mock`(표준
  라이브러리)을 사용한 스크래치 스크립트로 하고, 커밋하지 않는다.
- `--fix`는 `backfill_pending.json`을 갱신만 한다 — 실제 데이터 수집(yfinance
  fetch)은 수행하지 않는다. 재시도/에러 처리 로직은 `backfill_new_tickers()`
  한 곳에만 둔다(중복 금지).
- `--fix`는 기존 pending 항목을 지우지 않고 합집합으로 병합한다.
- 티커의 실제 상장일을 자동 판별하는 기능은 추가하지 않는다 — `--after` 이후
  시작하는 티커는 전부 "의심 후보"로만 리포트하고, pending 반영 여부는 사람이
  `--fix` 실행 여부로 결정한다.
- GHA 워크플로우를 신규로 추가하지 않는다 — `verify_kr.py`와 동일하게 수동
  실행 진단 도구로 유지한다.

---

### Task 1: `data/ohlc_db.py` — `first_seen_dates()` 함수 추가

**Files:**
- Modify: `data/ohlc_db.py` (94번째 줄 `list_known_tickers()` 끝과 97번째 줄
  `def save_year(` 사이에 함수 추가)

**Interfaces:**
- Produces: `first_seen_dates(market: str) -> dict[str, date]` — 티커별 최초
  수집일(Date 최솟값). `list_known_tickers(market) -> set[str]`(기존 함수,
  변경 없음)와 같은 파일 순회 패턴을 재사용.
- Consumes: 없음 (기존 `local_dir()`, `re`, `pd`, `pq`, `logger`를 그대로 사용 —
  전부 이미 `data/ohlc_db.py` 상단에 임포트되어 있음).

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_first_seen_dates.py` 파일을 아래 내용으로 작성한다.

```python
import sys
import shutil
from pathlib import Path
from datetime import date

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data import ohlc_db

TMP_ROOT = Path(r"%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\first_seen_test")
shutil.rmtree(TMP_ROOT, ignore_errors=True)
TMP_ROOT.mkdir(parents=True)

ohlc_db._LOCAL_ROOT = TMP_ROOT
mdir = ohlc_db.local_dir("us")
mdir.mkdir(parents=True)

# us_2020.parquet: AAPL만, 2020-01-02부터 시작
df_2020 = pd.DataFrame({"Ticker": ["AAPL"], "Date": [date(2020, 1, 2)]})
pq.write_table(pa.Table.from_pandas(df_2020), str(mdir / "us_2020.parquet"))

# us_2024.parquet: AAPL(더 늦은 날짜 — 2020년 파일의 더 이른 날짜가 이겨야 함) + NEWCO(여기만 있음)
df_2024 = pd.DataFrame({
    "Ticker": ["AAPL", "NEWCO"],
    "Date": [date(2024, 6, 1), date(2024, 3, 15)],
})
pq.write_table(pa.Table.from_pandas(df_2024), str(mdir / "us_2024.parquet"))

result = ohlc_db.first_seen_dates("us")
assert result.get("AAPL") == date(2020, 1, 2), f"FAIL AAPL: {result.get('AAPL')}"
assert result.get("NEWCO") == date(2024, 3, 15), f"FAIL NEWCO: {result.get('NEWCO')}"
print("OK:", result)

shutil.rmtree(TMP_ROOT, ignore_errors=True)
print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_first_seen_dates.py"`
Expected: `AttributeError: module 'data.ohlc_db' has no attribute 'first_seen_dates'`

- [x] **Step 3: `data/ohlc_db.py`에 함수 추가**

94번째 줄(`list_known_tickers()`의 `return known` 다음, 빈 줄 두 개 뒤)과
97번째 줄(`def save_year(`) 사이에 아래 함수를 추가한다:

```python
def first_seen_dates(market: str) -> dict[str, date]:
    """
    로컬 연도별 parquet 파일들({market}_YYYY.parquet)에서
    티커별 최초 수집일(Date의 최솟값)을 반환.
    """
    mdir = local_dir(market)
    if not mdir.exists():
        return {}

    year_pattern = re.compile(rf"^{re.escape(market)}_\d{{4}}\.parquet$")
    first_dates: dict[str, date] = {}

    for pfile in sorted(mdir.glob(f"{market}_*.parquet")):
        if not year_pattern.match(pfile.name):
            continue
        try:
            table = pq.read_table(str(pfile), columns=["Ticker", "Date"])
            df = table.to_pandas()
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            for ticker, d in df.groupby("Ticker")["Date"].min().items():
                if ticker not in first_dates or d < first_dates[ticker]:
                    first_dates[ticker] = d
        except Exception as e:
            logger.warning(f"[OhlcDB] {pfile.name} Ticker/Date 읽기 실패: {e}")

    return first_dates
```

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_first_seen_dates.py"`
Expected: `OK: {...}` 출력 후 `ALL OK`

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/fc92b968-9d81-4ad8-b785-85992094b4aa/scratchpad/test_first_seen_dates.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_db.py
git commit -m "$(cat <<'EOF'
Add first_seen_dates() to find each ticker's earliest collection date

Same file-scanning pattern as list_known_tickers(), but reads Date
alongside Ticker and keeps the minimum across all year files. Used by
the upcoming verify_ohlc.py to flag tickers whose data suspiciously
starts recently instead of at the project's start_year — the same
"known but incomplete" gap backfill_pending.json already tracks for
hard failures, but for tickers that were added to the universe before
that tracking existed and never triggered one.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `scripts/verify_ohlc.py` — CLI + 리포트 (신규 파일, `--fix` 제외)

**Files:**
- Create: `scripts/verify_ohlc.py`

**Interfaces:**
- Consumes: `ohlc_db.first_seen_dates(market) -> dict[str, date]` (Task 1),
  `ohlc_db.load_pending() -> dict`, `ohlc_db.local_dir(market) -> Path`,
  `ohlc_db.download_all_years(market)`, `ohlc_db.download_pending()`
  (전부 기존 `data/ohlc_db.py` 함수).
- Produces: `analyze_market(market: str, after: date) -> dict` — 반환 형태
  `{"market": str, "new": list[str], "already_pending": list[str],
  "first_dates": dict[str, date]}`. Task 3의 `--fix` 로직이 이 반환값의
  `"market"`/`"new"` 키를 그대로 사용한다.

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_report.py`:

```python
import sys
from unittest.mock import patch
from datetime import date

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")
sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\scripts")

import verify_ohlc

fake_dates = {
    "AAA": date(2020, 1, 2),   # --after 이전 → 후보에서 제외
    "BBB": date(2025, 6, 1),   # --after 이후 + 이미 pending
    "CCC": date(2025, 1, 1),   # --after 이후 + pending에 없음 (신규 후보)
}

with patch("data.ohlc_db.first_seen_dates", return_value=fake_dates), \
     patch("data.ohlc_db.load_pending", return_value={"us": ["BBB"]}):
    result = verify_ohlc.analyze_market("us", date(2024, 1, 1))

assert result["new"] == ["CCC"], f"FAIL new: {result['new']}"
assert result["already_pending"] == ["BBB"], f"FAIL already_pending: {result['already_pending']}"
assert "AAA" not in result["first_dates"], "FAIL: AAA는 --after 이전이라 제외돼야 함"
print("OK:", result)
print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_report.py"`
Expected: `ModuleNotFoundError: No module named 'verify_ohlc'`

- [x] **Step 3: `scripts/verify_ohlc.py` 작성**

```python
"""
scripts/verify_ohlc.py — US/Crypto 백필 완결성 검증

사용법:
  python scripts/verify_ohlc.py --market us              # 로컬 파일만 검증
  python scripts/verify_ohlc.py --market all --drive     # Drive에서 다운로드 후 검증
  python scripts/verify_ohlc.py --market us --after 2024-06-01

출력 예시:
  [US]
    ⚠️  DNLI         최초 수집일 2026-01-02  (조치 필요)
    ⏳ TSM          최초 수집일 2026-04-21  (이미 pending)
"""

import argparse
import io
import sys
from datetime import date, datetime
from pathlib import Path

# Windows 콘솔 UTF-8 출력
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import ohlc_db


# ══════════════════════════════════════════════════════════════════════════════
# 시장별 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_market(market: str, after: date) -> dict:
    """
    market의 티커별 최초 수집일을 조사해 after 이후 시작하는 티커를
    "신규 후보"(pending에 없음)와 "이미 pending"으로 분류한다.
    """
    first_dates = ohlc_db.first_seen_dates(market)
    pending = set(ohlc_db.load_pending().get(market, []))

    suspects = {t: d for t, d in first_dates.items() if d > after}

    return {
        "market": market,
        "new": sorted(set(suspects) - pending),
        "already_pending": sorted(set(suspects) & pending),
        "first_dates": suspects,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 출력
# ══════════════════════════════════════════════════════════════════════════════

def print_report(results: list[dict], after: date):
    print()
    print("=" * 60)
    print("  US/Crypto 백필 완결성 검증")
    print(f"  기준일(--after): {after} 이후 시작하는 티커를 의심 후보로 분류")
    print("=" * 60)

    total_new = 0

    for r in results:
        market = r["market"]
        new = r["new"]
        already = r["already_pending"]
        first_dates = r["first_dates"]
        total_new += len(new)

        print(f"\n  [{market.upper()}]")
        if not new and not already:
            print("    ✅ 의심 후보 없음")
            continue

        for t in new:
            print(f"    ⚠️  {t:<12} 최초 수집일 {first_dates[t]}  (조치 필요)")
        for t in already:
            print(f"    ⏳ {t:<12} 최초 수집일 {first_dates[t]}  (이미 pending)")

    print()
    print("=" * 60)

    if total_new:
        print(f"\n  [보완 안내] 신규 후보 {total_new}개 발견")
        print("  python scripts/verify_ohlc.py --fix  # pending에 반영")
    else:
        print("\n  ✅ 신규 후보 없음")

    print()


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="US/Crypto 백필 완결성 검증")
    parser.add_argument(
        "--market", choices=["us", "crypto", "all"], default="all",
        help="검증할 시장 (기본: all)"
    )
    parser.add_argument(
        "--after", type=str, default="2024-01-01", metavar="YYYY-MM-DD",
        help="이 날짜 이후에 최초 데이터가 시작되는 티커를 의심 후보로 분류 (기본: 2024-01-01)"
    )
    parser.add_argument(
        "--drive", action="store_true",
        help="Drive에서 최신 parquet + backfill_pending.json 다운로드 후 검증"
    )
    args = parser.parse_args()

    try:
        after = datetime.strptime(args.after, "%Y-%m-%d").date()
    except ValueError:
        print(f"--after 형식 오류: {args.after} (YYYY-MM-DD 형식 필요)")
        return

    markets = ["us", "crypto"] if args.market == "all" else [args.market]

    if args.drive:
        print("Drive에서 최신 데이터 다운로드 중...")
        for market in markets:
            ohlc_db.download_all_years(market)
        ohlc_db.download_pending()

    results = []
    for market in markets:
        mdir = ohlc_db.local_dir(market)
        if not mdir.exists():
            print(f"로컬 {market} 폴더 없음: {mdir}")
            print("--drive 옵션으로 Drive에서 다운로드하세요.")
            continue
        results.append(analyze_market(market, after))

    if not results:
        return

    print_report(results, after)


if __name__ == "__main__":
    main()
```

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_report.py"`
Expected: `OK: {...}` 출력 후 `ALL OK`

추가로 CLI 스모크 테스트 (프로젝트 루트에서, 실제 로컬 데이터로 리포트가
에러 없이 출력되는지 확인 — Drive 접근 없음):

Run: `python scripts/verify_ohlc.py --market us`
Expected: 에러 없이 종료, `[US]` 섹션과 요약 출력 (로컬에 `data/local/ohlc_db/us/` 폴더가
없으면 "로컬 us 폴더 없음" 안내만 출력되고 그것도 정상 종료임)

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/fc92b968-9d81-4ad8-b785-85992094b4aa/scratchpad/test_verify_ohlc_report.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add scripts/verify_ohlc.py
git commit -m "$(cat <<'EOF'
Add scripts/verify_ohlc.py to report tickers with suspiciously recent backfill

Same structure as scripts/verify_kr.py (analyze -> report -> main).
Flags tickers whose earliest collection date (first_seen_dates()) is
after a configurable --after cutoff (default 2024-01-01), separating
ones already in backfill_pending.json from genuinely new discoveries.
Report-only for now; --fix (next commit) will merge new discoveries
into pending.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `scripts/verify_ohlc.py` — `--fix` 옵션 추가

**Files:**
- Modify: `scripts/verify_ohlc.py` (Task 2에서 만든 파일에 `apply_fix()` 함수
  추가 + `main()`에 `--fix` 인자와 호출 추가)

**Interfaces:**
- Consumes: `analyze_market()`의 반환값 리스트 (Task 2, `"market"`/`"new"` 키
  사용), `ohlc_db.load_pending()`/`save_pending()`/`upload_pending()` (기존
  `data/ohlc_db.py` 함수, Task 1의 backfill_pending 기능에서 이미 구현됨).
- Produces: `apply_fix(results: list[dict])` — 반환값 없음, `backfill_pending.json`을
  갱신하고 Drive에 업로드하는 부수효과만 있음.

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_fix.py`:

```python
import sys
from unittest.mock import patch

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")
sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\scripts")

import verify_ohlc

saved = {}

def fake_save_pending(pending):
    saved.clear()
    saved.update(pending)

results = [
    {"market": "us", "new": ["CCC"], "already_pending": ["BBB"], "first_dates": {}},
    {"market": "crypto", "new": [], "already_pending": [], "first_dates": {}},
]

# 기존 pending에 다른 티커(TSM)가 있는 상태 — 지워지지 않고 합쳐져야 함
with patch("data.ohlc_db.load_pending", return_value={"us": ["TSM", "BBB"], "crypto": []}), \
     patch("data.ohlc_db.save_pending", side_effect=fake_save_pending), \
     patch("data.ohlc_db.upload_pending"):
    verify_ohlc.apply_fix(results)

assert saved.get("us") == ["BBB", "CCC", "TSM"], f"FAIL us: {saved.get('us')}"
print("OK:", saved)
print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_fix.py"`
Expected: `AttributeError: module 'verify_ohlc' has no attribute 'apply_fix'`

- [x] **Step 3: `scripts/verify_ohlc.py`에 `apply_fix()` 추가 + `main()` 배선**

`print_report` 함수 정의와 `# ══...` "메인" 구분선 사이에 아래 함수를 추가한다:

```python
# ══════════════════════════════════════════════════════════════════════════════
# pending 반영 (--fix)
# ══════════════════════════════════════════════════════════════════════════════

def apply_fix(results: list[dict]):
    """
    각 시장의 신규 후보("new")를 backfill_pending.json에 병합하고 Drive에 업로드한다.
    기존 pending 항목은 지우지 않고 합집합으로 합친다. 실제 fetch는 하지 않는다.
    """
    any_added = False

    for r in results:
        market = r["market"]
        new = r["new"]
        if not new:
            continue

        all_pending = ohlc_db.load_pending()
        merged = sorted(set(all_pending.get(market, [])) | set(new))
        all_pending[market] = merged
        ohlc_db.save_pending(all_pending)
        ohlc_db.upload_pending()

        print(f"  [{market.upper()}] pending에 {len(new)}개 추가 → 총 {len(merged)}개")
        any_added = True

    if any_added:
        print(
            "\npending 반영 완료 — ohlc-new-ticker-backfill 워크플로우를 "
            "실행하면 다음 회차에 자동 재시도됩니다."
        )
    else:
        print("\n반영할 신규 후보 없음")
```

`main()`의 `parser.add_argument("--drive", ...)` 블록 다음에 인자를 추가:

```python
    parser.add_argument(
        "--fix", action="store_true",
        help="신규 후보를 backfill_pending.json에 반영 (Drive 업로드 포함, 실제 fetch는 안 함)"
    )
```

`main()`의 `print_report(results, after)` 호출 다음에 추가:

```python
    print_report(results, after)

    if args.fix:
        apply_fix(results)
```

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\fc92b968-9d81-4ad8-b785-85992094b4aa\scratchpad\test_verify_ohlc_fix.py"`
Expected: `OK: {...}` 출력 후 `ALL OK`

추가로 CLI 인자 파싱만 스모크 확인 (실제 Drive 업로드는 하지 않음 — `--fix`
없이 실행해 `--fix` 인자 자체가 파싱 오류를 일으키지 않는지만 확인):

Run: `python scripts/verify_ohlc.py --market us --help`
Expected: `--fix` 옵션이 도움말에 나열되고 에러 없이 종료

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/fc92b968-9d81-4ad8-b785-85992094b4aa/scratchpad/test_verify_ohlc_fix.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add scripts/verify_ohlc.py
git commit -m "$(cat <<'EOF'
Add --fix to verify_ohlc.py to merge new candidates into backfill_pending.json

Reuses load_pending()/save_pending()/upload_pending() as-is — no new
Drive or fetch logic. Merges each market's newly-discovered candidates
into the existing pending list as a union (never drops entries already
pending for other reasons), then uploads. Actual data collection stays
owned by backfill_new_tickers() via the existing
ohlc-new-ticker-backfill workflow or the next daily run.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
