# 신규 종목 자동 백필 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** US/Crypto 유니버스에 새로 추가된 종목의 과거 이력을 자동으로 백필하는 공통 로직을 만들고, daily 워크플로우와 별도 수동 GHA 워크플로우 양쪽에서 재사용한다.

**Architecture:** `ohlc_db.py`에 "지금까지 한 번이라도 수집된 티커 집합"을 로컬 parquet에서 스캔하는 헬퍼를 추가하고, `ohlc_collector.py`에 이 헬퍼와 현재 유니버스를 비교해 신규 종목만 골라 과거 이력을 백필하는 공통 함수를 추가한다. 이 공통 함수를 `main.py`의 daily 증분 경로(`run_ohlc_update`)에서 자동 호출하고, 동일 함수를 호출하는 신규 CLI 모드(`ohlc-new-backfill`) + 신규 GHA 워크플로우로도 노출한다.

**Tech Stack:** Python 3.12, pandas, pyarrow, yfinance, GitHub Actions

## Global Constraints

- US/Crypto만 대상 (KR은 변경 없음 — 이미 전종목 방식이라 문제 없음).
- 신규 종목 판별은 상태 파일이 아니라 실제 로컬 parquet 파일 스캔으로 한다.
- 백필 시작 연도는 고정값(기본 2020, 기존 `ohlc-backfill` 기본값과 동일)이며 티커별 실제 상장일 자동 탐지는 하지 않는다.
- MarketCap은 "오늘" 시점 행에만 채운다 (과거 이력 MarketCap 보강은 하지 않음 — 기존 정책과 동일).
- 이 프로젝트에는 pytest 등 테스트 프레임워크가 없다. 검증은 `unittest.mock`(표준 라이브러리)을 사용한 스크래치 스크립트 + 실제 CLI 실행으로 한다. 새 테스트 파일이나 테스트 프레임워크를 프로젝트에 추가하지 않는다.
- 스크래치 검증 스크립트는 저장소에 커밋하지 않는다 (스크래치패드 디렉터리에서만 실행).

---

### Task 1: `ohlc_db.list_known_tickers()` 헬퍼

**Files:**
- Modify: `data/ohlc_db.py` (상단 import 및 `load_year()`와 `save_year()` 사이에 함수 추가, `save_year()` 시작 줄은 현재 70번째 줄)

**Interfaces:**
- Produces: `list_known_tickers(market: str) -> set[str]` — 로컬 `{market}_YYYY.parquet` 파일들(연도별 파일만, `{market}_sector_meta.parquet` 제외)에서 한 번이라도 등장한 `Ticker` 값의 집합을 반환. 파일이 없으면 빈 set.

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_list_known_tickers.py` 파일을 아래 내용으로 작성한다.

```python
import sys
import shutil
from pathlib import Path

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data import ohlc_db

TMP_ROOT = Path(r"%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\ohlc_db_test")
shutil.rmtree(TMP_ROOT, ignore_errors=True)
TMP_ROOT.mkdir(parents=True)

ohlc_db._LOCAL_ROOT = TMP_ROOT
market_dir = ohlc_db.local_dir("us")
market_dir.mkdir(parents=True)

# 연도별 파일 2개 (AAPL, MSFT)
df1 = pd.DataFrame({"Ticker": ["AAPL"], "Date": ["2020-01-02"]})
pq.write_table(pa.Table.from_pandas(df1, preserve_index=False), str(market_dir / "us_2020.parquet"))

df2 = pd.DataFrame({"Ticker": ["MSFT"], "Date": ["2021-01-04"]})
pq.write_table(pa.Table.from_pandas(df2, preserve_index=False), str(market_dir / "us_2021.parquet"))

# sector_meta 파일 — 여기 있는 티커(GOOGL)는 known_tickers에 포함되면 안 됨
meta_df = pd.DataFrame({
    "Ticker": ["AAPL", "MSFT", "GOOGL"],
    "Market": ["US", "US", "US"],
    "Sector": ["", "", ""],
    "Industry": ["", "", ""],
    "updated_at": ["2026-01-01"] * 3,
})
pq.write_table(pa.Table.from_pandas(meta_df, preserve_index=False), str(market_dir / "us_sector_meta.parquet"))

result = ohlc_db.list_known_tickers("us")
assert result == {"AAPL", "MSFT"}, f"FAIL: {result}"
print("OK:", result)

shutil.rmtree(TMP_ROOT, ignore_errors=True)
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_list_known_tickers.py"`
Expected: `AttributeError: module 'data.ohlc_db' has no attribute 'list_known_tickers'`

- [ ] **Step 3: `data/ohlc_db.py`에 함수 구현**

파일 상단 import 블록(현재 12~20번째 줄)에 `re` 추가:

```python
import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional
```

`load_year()` 함수(70번째 줄에서 시작하는 `save_year()`) 바로 앞에 아래 함수를 추가:

```python
def list_known_tickers(market: str) -> set[str]:
    """
    로컬 연도별 parquet 파일들({market}_YYYY.parquet)에서
    한 번이라도 수집된 Ticker 집합을 반환.
    {market}_sector_meta.parquet 등 연도 파일이 아닌 것은 제외한다.
    """
    mdir = local_dir(market)
    if not mdir.exists():
        return set()

    year_pattern = re.compile(rf"^{re.escape(market)}_\d{{4}}\.parquet$")
    known: set[str] = set()

    for pfile in sorted(mdir.glob(f"{market}_*.parquet")):
        if not year_pattern.match(pfile.name):
            continue
        try:
            table = pq.read_table(str(pfile), columns=["Ticker"])
            known.update(table.column("Ticker").to_pylist())
        except Exception as e:
            logger.warning(f"[OhlcDB] {pfile.name} Ticker 컬럼 읽기 실패: {e}")

    return known
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_list_known_tickers.py"`
Expected: `OK: {'AAPL', 'MSFT'}`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_list_known_tickers.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_db.py
git commit -m "$(cat <<'EOF'
Add list_known_tickers() to detect tickers never collected before

Reads only the Ticker column of each year-pattern parquet file
(excluding *_sector_meta.parquet) so new-ticker detection doesn't need
to load full OHLC history into memory.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `ohlc_collector.backfill_new_tickers()` 공통 로직

**Files:**
- Modify: `data/ohlc_collector.py` (`backfill_market()` 함수 끝, 현재 506번째 줄 직후 · "증분 업데이트" 섹션 헤더 앞에 추가)

**Interfaces:**
- Consumes: `ohlc_db.download_all_years(market)`, `ohlc_db.download_status()`, `ohlc_db.list_known_tickers(market)` (Task 1), `ohlc_db.load_status()`, `ohlc_db.update_status(market, last_date, ticker_count, oldest_date)`, `ohlc_db.upload_status()`, `ohlc_db.save_year(df, market, year)`, `ohlc_db.upload_years(market, years)`, 이 파일에 이미 있는 `load_tickers(market)`, `fetch_ohlc_range(tickers, start, end)`, `_enrich_us_marketcap(df)`, `_enrich_crypto_marketcap(df)`.
- Produces: `backfill_new_tickers(market: str, start_year: int = 2020, upload: bool = True) -> list[str]` — 새로 발견되어 백필된 티커 목록 반환 (없으면 빈 리스트).

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_new_tickers.py` 파일을 아래 내용으로 작성한다.

```python
import sys
from datetime import date
from unittest.mock import patch

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
from data import ohlc_collector

FAKE_DF = pd.DataFrame({
    "Ticker": ["NEWCO"],
    "Date": [date(2026, 1, 2)],
    "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [100],
    "Amount": [100.0], "ChangesRatio": [0.0], "MarketCap": [float("nan")],
    "Dividends": [0.0], "Splits": [1.0],
})

saved_years = []

with patch("data.ohlc_collector.load_tickers", return_value=["AAPL", "NEWCO"]), \
     patch("data.ohlc_db.download_all_years"), \
     patch("data.ohlc_db.download_status"), \
     patch("data.ohlc_db.list_known_tickers", return_value={"AAPL"}), \
     patch("data.ohlc_db.load_status", return_value={}), \
     patch("data.ohlc_db.update_status") as mock_update_status, \
     patch("data.ohlc_db.upload_status"), \
     patch("data.ohlc_db.upload_years"), \
     patch("data.ohlc_db.save_year") as mock_save_year, \
     patch("data.ohlc_collector.fetch_ohlc_range", return_value=FAKE_DF) as mock_fetch, \
     patch("data.ohlc_collector._enrich_us_marketcap", side_effect=lambda df: df) as mock_enrich:

    result = ohlc_collector.backfill_new_tickers("us", start_year=2026, upload=True)

assert result == ["NEWCO"], f"FAIL new_tickers: {result}"
# fetch_ohlc_range가 신규 종목(NEWCO)만 받았는지 확인 (AAPL 제외)
called_tickers = mock_fetch.call_args_list[0][0][0]
assert called_tickers == ["NEWCO"], f"FAIL tickers passed: {called_tickers}"
assert mock_enrich.called, "FAIL: 올해 데이터에 MarketCap enrichment가 호출되지 않음"
assert mock_save_year.called, "FAIL: save_year가 호출되지 않음"
assert mock_update_status.called, "FAIL: update_status가 호출되지 않음"

print("OK:", result)
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_new_tickers.py"`
Expected: `AttributeError: module 'data.ohlc_collector' has no attribute 'backfill_new_tickers'`

- [ ] **Step 3: `data/ohlc_collector.py`에 함수 구현**

`backfill_market()` 함수(443~506번째 줄) 바로 뒤, `# 증분 업데이트` 섹션 헤더(509번째 줄) 앞에 추가:

```python
def backfill_new_tickers(
    market: str,
    start_year: int = 2020,
    upload: bool = True,
) -> list[str]:
    """
    현재 유니버스에서 지금까지 한 번도 수집되지 않은 신규 종목을 찾아
    start_year ~ 올해까지 과거 이력을 백필한다.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 백필 시작 연도 (기본 2020, ohlc-backfill 기본값과 동일)
        upload:     True면 연도별 저장 후 Drive 업로드

    Returns:
        새로 발견되어 백필된 티커 목록 (없으면 빈 리스트)
    """
    from data import ohlc_db

    ohlc_db.download_all_years(market)
    ohlc_db.download_status()

    known = ohlc_db.list_known_tickers(market)
    universe = load_tickers(market)
    new_tickers = [t for t in universe if t not in known]

    if not new_tickers:
        logger.info(f"[NewTickerBackfill] {market.upper()} 신규 종목 없음")
        return []

    logger.info(
        f"[NewTickerBackfill] {market.upper()} 신규 종목 {len(new_tickers)}개 발견: {new_tickers}"
    )

    current_year = date.today().year
    all_dates: list[date] = []

    for year in range(start_year, current_year + 1):
        start_str = f"{year}-01-01"
        if year == current_year:
            end_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            end_str = f"{year + 1}-01-01"

        logger.info(
            f"[NewTickerBackfill] {market.upper()} {year}년 수집 중... "
            f"({len(new_tickers)}종목)"
        )
        try:
            df = fetch_ohlc_range(new_tickers, start_str, end_str)
        except Exception as e:
            logger.error(f"[NewTickerBackfill] {market} {year}년 수집 실패: {e}")
            continue

        if df.empty:
            logger.warning(f"[NewTickerBackfill] {market} {year}년 데이터 없음")
            continue

        if year == current_year:
            if market == "crypto":
                df = _enrich_crypto_marketcap(df)
            else:
                df = _enrich_us_marketcap(df)

        ohlc_db.save_year(df, market, year)

        if "Date" in df.columns:
            all_dates.extend(df["Date"].tolist())

        if upload:
            ohlc_db.upload_years(market, [year])

    if all_dates:
        status = ohlc_db.load_status()
        market_status = status.get(market, {})

        new_last = max(all_dates)
        new_oldest = min(all_dates)

        last_dt = new_last
        existing_last_str = market_status.get("last_updated")
        if existing_last_str:
            try:
                existing_last = datetime.strptime(existing_last_str, "%Y-%m-%d").date()
                last_dt = max(existing_last, new_last)
            except ValueError:
                pass

        oldest_dt = new_oldest
        existing_oldest_str = market_status.get("oldest_date")
        if existing_oldest_str:
            try:
                existing_oldest = datetime.strptime(existing_oldest_str, "%Y-%m-%d").date()
                oldest_dt = min(existing_oldest, new_oldest)
            except ValueError:
                pass

        ohlc_db.update_status(market, last_dt, len(universe), oldest_dt)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[NewTickerBackfill] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
    return new_tickers
```

`status`의 `last_updated`/`oldest_date`를 새로 백필한 데이터의 날짜로 무조건 덮어쓰지 않고, 기존 값과 비교해 `max`/`min`으로 병합하는 이유: 신규 종목 1~2개만 백필한 결과로 시장 전체의 `last_updated`/`oldest_date`가 실제보다 좁혀지거나 과거로 밀리면 안 되기 때문.

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_new_tickers.py"`
Expected: `OK: ['NEWCO']`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_backfill_new_tickers.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Add backfill_new_tickers() to catch up history for newly added tickers

Diffs the current universe against ohlc_db.list_known_tickers() and
backfills only the tickers that have never been collected, from
start_year through today, enriching only the current year's MarketCap
to match the daily update's existing behavior.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Daily 워크플로우 통합 (`run_ohlc_update`)

**Files:**
- Modify: `main.py:248-263` (`run_ohlc_update` 함수)

**Interfaces:**
- Consumes: `ohlc_collector.backfill_new_tickers(market, upload)` (Task 2), `ohlc_collector.update_market(market, upload)` (기존)

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_run_ohlc_update_order.py`:

```python
import sys
import argparse
from unittest.mock import patch

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import main

call_order = []

def fake_backfill(**kwargs):
    call_order.append("backfill_new_tickers")
    return []

def fake_update(**kwargs):
    call_order.append("update_market")

args = argparse.Namespace(market="us", dry_run=False, upload_drive=False)

with patch("data.ohlc_collector.backfill_new_tickers", side_effect=fake_backfill), \
     patch("data.ohlc_collector.update_market", side_effect=fake_update):
    main.run_ohlc_update(args)

assert call_order == ["backfill_new_tickers", "update_market"], f"FAIL: {call_order}"
print("OK:", call_order)
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_run_ohlc_update_order.py"`
Expected: `AssertionError: FAIL: ['update_market']` (현재는 `backfill_new_tickers` 호출이 없으므로)

- [ ] **Step 3: `main.py`의 `run_ohlc_update` 수정**

`main.py:248-263`을 아래로 교체:

```python
def run_ohlc_update(args):
    """
    US/Crypto OHLC 증분 업데이트.
    마지막 업데이트 이후 누락된 데이터를 수집.
    신규 종목이 유니버스에 추가된 경우, 증분 업데이트 전에 과거 이력을 먼저 백필한다.
    """
    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        logger.info(f"[OhlcUpdate] {market.upper()} 증분 업데이트 시작")
        if not args.dry_run:
            new_tickers = ohlc_collector.backfill_new_tickers(
                market=market,
                upload=args.upload_drive,
            )
            if new_tickers:
                logger.info(f"[OhlcUpdate] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
            ohlc_collector.update_market(
                market=market,
                upload=args.upload_drive,
            )
        else:
            logger.info(f"[DryRun] {market.upper()} ohlc update 시뮬레이션")
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_run_ohlc_update_order.py"`
Expected: `OK: ['backfill_new_tickers', 'update_market']`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_run_ohlc_update_order.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add main.py
git commit -m "$(cat <<'EOF'
Auto-backfill new tickers before daily incremental OHLC update

run_ohlc_update() now calls backfill_new_tickers() ahead of
update_market() so tickers newly added to the universe get their full
history instead of only data from the day they were added onward.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: 신규 CLI 모드 `ohlc-new-backfill`

**Files:**
- Modify: `main.py` (새 함수 추가 + `--mode` choices/help 갱신 + dispatch 분기 추가)

**Interfaces:**
- Consumes: `ohlc_collector.backfill_new_tickers(market, start_year, upload)` (Task 2)
- Produces: `run_ohlc_new_backfill(args)` — CLI에서 `--mode ohlc-new-backfill`로 호출됨

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_ohlc_new_backfill_mode.py`:

```python
import subprocess
import sys

result = subprocess.run(
    [sys.executable, "main.py", "--mode", "ohlc-new-backfill", "--market", "us", "--dry-run"],
    cwd=r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling",
    capture_output=True, text=True,
)
assert result.returncode == 0, f"FAIL rc={result.returncode}\nstderr={result.stderr}"
assert "DryRun" in result.stdout or "DryRun" in result.stderr or "시뮬레이션" in result.stdout, (
    f"FAIL output missing dry-run marker:\n{result.stdout}\n{result.stderr}"
)
print("OK: exit 0, dry-run 로그 확인됨")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_ohlc_new_backfill_mode.py"`
Expected: `AssertionError` — `argparse`가 `ohlc-new-backfill`을 모르는 `--mode` 값으로 거부 (`invalid choice`), returncode 2

- [ ] **Step 3: `main.py`에 신규 모드 추가**

`main.py:274` (`run_kr_daily` 정의) 바로 앞에 함수 추가 (즉, `run_ohlc_update` 다음 위치):

```python
def run_ohlc_new_backfill(args):
    """
    US/Crypto 유니버스에 새로 추가된 종목만 골라 과거 이력을 백필한다.
    daily(ohlc-update)에서도 자동으로 실행되지만, 즉시 반영하고 싶을 때 수동으로 실행 가능.
    """
    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        if args.dry_run:
            logger.info(f"[DryRun] {market.upper()} 신규 종목 백필 시뮬레이션")
            continue
        new_tickers = ohlc_collector.backfill_new_tickers(
            market=market,
            start_year=args.start_year,
            upload=args.upload_drive,
        )
        if new_tickers:
            logger.info(f"[OhlcNewBackfill] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
        else:
            logger.info(f"[OhlcNewBackfill] {market.upper()} 신규 종목 없음")
```

`main.py:524-525`의 `--mode` choices를 수정:

```python
        choices=["daily", "bootstrap", "ohlc-backfill", "ohlc-update", "ohlc-new-backfill",
                 "financials-update", "kr-daily", "kr-backfill", "sector-meta"],
```

`main.py:527-535`의 help 문자열에 항목 추가:

```python
        help=(
            "daily: 오늘 수집 / bootstrap: 과거 연도 일괄 수집 / "
            "ohlc-backfill: US/Crypto OHLC 초기 적재 / "
            "ohlc-update: US/Crypto OHLC 증분 업데이트 / "
            "ohlc-new-backfill: US/Crypto 신규 종목만 골라 과거 이력 백필 / "
            "financials-update: US 재무제표 + Crypto 시장 데이터 수집 / "
            "kr-daily: KR 오늘 스냅샷 수집 (FDR) / "
            "kr-backfill: KR 과거 누락 구간 수집 (yfinance) / "
            "sector-meta: US/Crypto 종목 메타데이터 수집 (Sector/Industry, 주 1회)"
        ),
```

`main.py:632-633` (`elif args.mode == "ohlc-update":` 다음)에 분기 추가:

```python
    elif args.mode == "ohlc-update":
        run_ohlc_update(args)
    elif args.mode == "ohlc-new-backfill":
        run_ohlc_new_backfill(args)
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_ohlc_new_backfill_mode.py"`
Expected: `OK: exit 0, dry-run 로그 확인됨`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_ohlc_new_backfill_mode.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add main.py
git commit -m "$(cat <<'EOF'
Add ohlc-new-backfill CLI mode for on-demand new-ticker catch-up

Exposes backfill_new_tickers() as a standalone --mode so a new ticker's
history can be backfilled immediately instead of waiting for the next
daily run.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: 신규 GHA 워크플로우 + 문서 갱신

**Files:**
- Create: `.github/workflows/ohlc-new-ticker-backfill.yml`
- Modify: `[Claude Code] Daily-Market-Data-Crawling/CLAUDE.md`

**Interfaces:**
- Consumes: `main.py --mode ohlc-new-backfill` (Task 4)

- [ ] **Step 1: 워크플로우 파일 작성**

`.github/workflows/ohlc-new-ticker-backfill.yml` 신규 생성 (기존 `ohlc-backfill.yml` 구조를 따름):

```yaml
name: "OHLC 신규 종목 백필 — US/Crypto"

on:
  workflow_dispatch:
    inputs:
      market:
        description: '대상 시장'
        type: choice
        options: [all, us, crypto]
        default: all
      start_year:
        description: '수집 시작 연도'
        required: false
        default: '2020'
      dry_run:
        description: '테스트 실행 (저장 없음)'
        type: boolean
        default: false

jobs:
  new-ticker-backfill:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: 'pip'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: OHLC New Ticker Backfill
        env:
          GDRIVE_FOLDER_ID:            ${{ secrets.GDRIVE_FOLDER_ID }}
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
        run: |
          ARGS="--mode ohlc-new-backfill --upload-drive --market ${{ inputs.market }} --start-year ${{ inputs.start_year }}"
          ${{ inputs.dry_run }} && ARGS="$ARGS --dry-run"
          python main.py $ARGS
      - name: Upload log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: ohlc-new-ticker-backfill-log-${{ github.run_id }}
          path: collection.log
      - name: Cleanup credentials
        if: always()
        run: rm -f gdrive_creds.json
```

- [ ] **Step 2: YAML 문법 검증**

Run: `python -c "import yaml; yaml.safe_load(open(r'.github/workflows/ohlc-new-ticker-backfill.yml', encoding='utf-8'))" ` (프로젝트 루트에서 실행)
Expected: 에러 없이 종료 (출력 없음)

- [ ] **Step 3: `CLAUDE.md`의 워크플로우 표에 항목 추가**

`[Claude Code] Daily-Market-Data-Crawling/CLAUDE.md`의 "GitHub Actions 워크플로우" 표에서
`| \`ohlc-backfill.yml\` | workflow_dispatch | US/Crypto OHLC 과거 수집 |` 행 다음에 추가:

```markdown
| `ohlc-new-ticker-backfill.yml` | workflow_dispatch | 유니버스에 새로 추가된 종목만 골라 과거 이력 백필 (daily에도 자동 통합됨) |
```

"실행 방법 (main.py)" 섹션에 예시 추가:

```markdown
# 신규 종목만 과거 이력 백필
python main.py --mode ohlc-new-backfill --market us --upload-drive
```

"주요 이력" 표 맨 위(가장 최근)에 행 추가:

```markdown
| 2026-07-10 | **[FEAT-NEW-TICKER-BACKFILL]** US/Crypto 신규 종목 자동 백필 추가: `ohlc_db.list_known_tickers()` + `ohlc_collector.backfill_new_tickers()` 신규 함수, `run_ohlc_update()`에 자동 통합 + `ohlc-new-backfill` CLI 모드 + `ohlc-new-ticker-backfill.yml` 신규 워크플로우 |
```

- [ ] **Step 4: 최종 통합 스모크 테스트 (실 Drive 접근 없이 dry-run)**

Run (프로젝트 루트에서): `python main.py --mode ohlc-new-backfill --market us --start-year 2020 --dry-run`
Expected: 로그에 `[DryRun] US 신규 종목 백필 시뮬레이션` 출력, 에러 없이 exit code 0

- [ ] **Step 5: 커밋**

```bash
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add .github/workflows/ohlc-new-ticker-backfill.yml CLAUDE.md
git commit -m "$(cat <<'EOF'
Add ohlc-new-ticker-backfill GHA workflow for on-demand catch-up

Manual workflow_dispatch entry point that calls the new
ohlc-new-backfill CLI mode directly, for when a new ticker needs its
history backfilled immediately instead of waiting for the next daily
run. Also documents the new workflow and CLI mode in CLAUDE.md.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
