# 백필 미완료 종목 추적(backfill_pending) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `backfill_new_tickers()`가 한 번 실패한 연도를 가진 종목을 영구히 재시도 대상에서 빠뜨리는 문제를, 하드 실패 티커를 별도로 추적하는 `backfill_pending.json` 레지스트리로 해결한다.

**Architecture:** `fetch_ohlc_range()`가 하드 실패 티커 목록을 반환하도록 시그니처를 확장하고, `data/ohlc_db.py`에 `db_status.json`과 동일한 패턴의 pending 저장/로드/Drive 연동 함수를 추가한 뒤, `backfill_new_tickers()`가 "한 번도 없던 종목" + "pending에 남은 종목"을 대상으로 삼고 실행 결과에 따라 pending 목록을 갱신하도록 재작성한다.

**Tech Stack:** Python 3.12, pandas, unittest.mock (스크래치 검증)

## Global Constraints

- 정상적인 빈 응답(`raw.empty`, 예외 없음 — 상장 전 등)은 실패로 치지 않는다. 배치 재시도(`_BATCH_MAX_RETRIES`)까지 소진하며 예외가 발생한 경우만 "하드 실패"로 pending에 남긴다.
- yfinance 버전 이슈, `PSTG`/`BITF`/`U-UN-TO`의 근본 원인은 이번 작업 범위 밖 — 다루지 않는다.
- 티커의 실제 상장일을 자동 판별하는 기능은 추가하지 않는다.
- 이 프로젝트는 pytest 등 테스트 프레임워크가 없다. 검증은 `unittest.mock`(표준 라이브러리)을 사용한 스크래치 스크립트로 하고, 커밋하지 않는다.
- `backfill_market()`/`update_market()`은 이번 변경으로 동작이 달라지면 안 된다 — `fetch_ohlc_range()`의 반환값 언패킹만 바뀐다.

---

### Task 1: `data/ohlc_db.py` — pending 목록 저장/로드/Drive 연동

**Files:**
- Modify: `data/ohlc_db.py` (28번째 줄 근처 상수 추가, 362~364번째 줄 사이에 4개 함수 추가)

**Interfaces:**
- Produces: `load_pending() -> dict`, `save_pending(pending: dict)`,
  `download_pending(uploader=None) -> bool`, `upload_pending(uploader=None)` —
  모두 `db_status.json` 계열 함수와 동일한 시그니처/동작 패턴.

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_pending_storage.py` 파일을 아래 내용으로 작성한다.

```python
import sys
import shutil
from pathlib import Path

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

from data import ohlc_db

TMP_ROOT = Path(r"%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\pending_test")
shutil.rmtree(TMP_ROOT, ignore_errors=True)
TMP_ROOT.mkdir(parents=True)

ohlc_db._LOCAL_ROOT = TMP_ROOT
ohlc_db._PENDING_PATH = TMP_ROOT / "backfill_pending.json"

# 1) 파일 없을 때 load_pending()은 빈 dict
result = ohlc_db.load_pending()
assert result == {}, f"FAIL: {result}"
print("OK: load_pending() with no file -> {}")

# 2) save_pending() 후 load_pending()으로 그대로 읽히는지
ohlc_db.save_pending({"us": ["TSM", "ALAB"], "crypto": []})
result2 = ohlc_db.load_pending()
assert result2 == {"us": ["TSM", "ALAB"], "crypto": []}, f"FAIL: {result2}"
print("OK: save_pending() -> load_pending() round-trip:", result2)

shutil.rmtree(TMP_ROOT, ignore_errors=True)
print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_pending_storage.py"`
Expected: `AttributeError: module 'data.ohlc_db' has no attribute 'load_pending'`

- [x] **Step 3: `data/ohlc_db.py` 수정**

28번째 줄(`_STATUS_PATH = _LOCAL_ROOT / "db_status.json"`) 바로 다음 줄에 상수 추가:

```python
_STATUS_PATH = _LOCAL_ROOT / "db_status.json"
_PENDING_PATH = _LOCAL_ROOT / "backfill_pending.json"
```

362번째 줄(`upload_status()` 함수 끝, `except Exception as e: logger.error(...)` 다음)과
364번째 줄(`# ══...` 구분선, "종목 메타데이터" 섹션 시작) 사이에 아래 4개 함수를 추가:

```python
def load_pending() -> dict:
    """data/local/ohlc_db/backfill_pending.json 로드. 없으면 빈 dict 반환."""
    if not _PENDING_PATH.exists():
        return {}
    try:
        with open(_PENDING_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[OhlcDB] pending 로드 실패: {e}")
        return {}


def save_pending(pending: dict):
    """backfill_pending.json 저장."""
    _PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_PENDING_PATH, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2, ensure_ascii=False)
        logger.debug(f"[OhlcDB] pending 저장 완료: {_PENDING_PATH}")
    except Exception as e:
        logger.error(f"[OhlcDB] pending 저장 실패: {e}")


def download_pending(uploader=None) -> bool:
    """Drive에서 backfill_pending.json 다운로드. 성공 True, 실패 False."""
    u = _get_uploader(uploader)
    if u is None:
        return False

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return False

    _PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        u.download(remote_path, "backfill_pending.json", str(_PENDING_PATH))
        return True
    except FileNotFoundError:
        logger.debug("[OhlcDB] Drive에 backfill_pending.json 없음 (최초 실행)")
        return False
    except Exception as e:
        logger.error(f"[OhlcDB] backfill_pending.json 다운로드 실패: {e}")
        return False


def upload_pending(uploader=None):
    """로컬 backfill_pending.json을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return

    if not _PENDING_PATH.exists():
        logger.warning("[OhlcDB] backfill_pending.json 없음 → 업로드 건너뜀")
        return

    try:
        u.upload(str(_PENDING_PATH), remote_path)
    except Exception as e:
        logger.error(f"[OhlcDB] backfill_pending.json 업로드 실패: {e}")
```

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_pending_storage.py"`
Expected: 2개 케이스 모두 `OK`, 마지막에 `ALL OK`

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_pending_storage.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_db.py
git commit -m "$(cat <<'EOF'
Add backfill_pending.json storage/Drive sync helpers

Same pattern as db_status.json (load/save/download/upload). Will hold
a per-market list of tickers whose backfill hit a hard failure on some
year, so backfill_new_tickers() can retry them on the next run instead
of treating a single successful row anywhere as "fully backfilled."

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `fetch_ohlc_range()` 반환값 확장 + 호출부 2곳 업데이트

**Files:**
- Modify: `data/ohlc_collector.py` (320번째 줄 함수 시그니처, 469/484번째 줄 return문, 567/778번째 줄 호출부)

**Interfaces:**
- Produces: `fetch_ohlc_range(tickers, start, end) -> tuple[pd.DataFrame, list[str]]`
  (기존엔 `-> pd.DataFrame`만 반환) — 두 번째 값은 하드 실패 티커 목록.
- Consumes: 없음 (기존 내부 `failed` 변수를 그대로 반환하는 것뿐).

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_failed_return.py`:

```python
import sys
from unittest.mock import patch

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

from data import ohlc_collector

# --- Case (a): 배치가 재시도까지 모두 실패 → 두 번째 반환값(failed)에 포함 ---
def fake_download_always_fail(*args, **kwargs):
    raise Exception("network error")

with patch("yfinance.download", side_effect=fake_download_always_fail), \
     patch("data.ohlc_collector.time.sleep"):
    df, failed = ohlc_collector.fetch_ohlc_range(["FAKETICKER"], "2024-01-01", "2024-01-03")

assert isinstance(failed, list), f"FAIL: failed is not a list: {type(failed)}"
assert "FAKETICKER" in failed, f"FAIL: {failed}"
assert df.empty, f"FAIL: expected empty df, got {df}"
print("OK case (a): hard failure -> ticker in failed list:", failed)

# --- Case (b): 정상적인 빈 응답(예외 없음) → failed에 포함되지 않음 ---
import pandas as pd
EMPTY_DF = pd.DataFrame()

with patch("yfinance.download", return_value=EMPTY_DF), \
     patch("data.ohlc_collector.time.sleep"):
    df2, failed2 = ohlc_collector.fetch_ohlc_range(["FAKETICKER2"], "2024-01-01", "2024-01-03")

assert failed2 == [], f"FAIL: expected no hard failures, got {failed2}"
print("OK case (b): clean empty response -> failed list empty:", failed2)

print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_failed_return.py"`
Expected: `TypeError: cannot unpack non-iterable DataFrame object` (현재 `fetch_ohlc_range`는 DataFrame 하나만 반환하므로 `df, failed = ...` 언패킹이 실패)

- [x] **Step 3: `data/ohlc_collector.py` 수정**

320번째 줄 함수 시그니처를:

```python
def fetch_ohlc_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
```

아래로 변경:

```python
def fetch_ohlc_range(tickers: list[str], start: str, end: str) -> tuple[pd.DataFrame, list[str]]:
```

469번째 줄:

```python
        return pd.DataFrame(columns=_EXTENDED_COLS)
```
→
```python
        return pd.DataFrame(columns=_EXTENDED_COLS), failed
```

484번째 줄:

```python
    return result
```
→
```python
    return result, failed
```

`backfill_market()` 567번째 줄:

```python
            df = fetch_ohlc_range(tickers, start_str, end_str)
```
→
```python
            df, _ = fetch_ohlc_range(tickers, start_str, end_str)
```

`update_market()` 778번째 줄:

```python
        new_df = fetch_ohlc_range(tickers, start_str, end_str)
```
→
```python
        new_df, _ = fetch_ohlc_range(tickers, start_str, end_str)
```

(`backfill_new_tickers()` 645번째 줄의 호출부는 Task 3에서 함수 전체를 교체하며 함께 처리하므로 이 Task에서는 건드리지 않는다.)

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_failed_return.py"`
Expected: 2개 케이스 모두 `OK`, 마지막에 `ALL OK`

추가로 `backfill_market()`/`update_market()`가 여전히 정상 임포트/실행되는지 스모크 확인:

Run (프로젝트 루트에서): `python main.py --mode ohlc-backfill --market us --start-year 2020 --dry-run`
Expected: 에러 없이 종료 (dry-run이므로 실제 fetch는 호출되지 않지만, 문법/임포트 오류가 없는지 확인하는 목적)

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_fetch_ohlc_range_failed_return.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Return hard-failed tickers from fetch_ohlc_range()

Exposes the already-computed internal `failed` list (batch retries
exhausted, or per-ticker processing exception) as a second return
value, distinct from a clean empty response (no exception — e.g.
pre-IPO period). backfill_market()/update_market() just discard it via
`_` for now; backfill_new_tickers() will use it in the next commit to
track which tickers still need a retry.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `backfill_new_tickers()` — pending 목록 기반 재시도 로직

**Files:**
- Modify: `data/ohlc_collector.py` (596~700번째 줄 함수 전체 교체)

**Interfaces:**
- Consumes: `ohlc_db.load_pending()`/`save_pending()`/`download_pending()`/`upload_pending()` (Task 1),
  `fetch_ohlc_range(tickers, start, end) -> tuple[pd.DataFrame, list[str]]` (Task 2).
- Produces (기존 시그니처 유지): `backfill_new_tickers(market, start_year=2020, upload=True) -> list[str]`
  — 반환값 의미가 "신규 종목"에서 "이번에 시도한 티커(신규+pending 재시도)"로 확장됨.

- [x] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_pending_retry.py`:

```python
import sys
from datetime import date
from unittest.mock import patch

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
from data import ohlc_collector

TODAY = date.today()

FAKE_DF = pd.DataFrame({
    "Ticker": ["TSM"],
    "Date": [TODAY],
    "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [100],
    "Amount": [100.0], "ChangesRatio": [0.0], "MarketCap": [float("nan")],
    "Dividends": [0.0], "Splits": [1.0],
})
EMPTY_DF = pd.DataFrame(columns=FAKE_DF.columns)

# --- Case (a): TSM은 known(과거에 한 행 있음)이지만 pending에도 있음 -> candidates에 포함되는지 ---
saved_pending = {}

def fake_save_pending(pending):
    saved_pending.clear()
    saved_pending.update(pending)

def fake_fetch_ohlc_range(tickers, start, end):
    # 매 연도 하드 실패로 처리 (재시도까지 소진된 상황 재현)
    return EMPTY_DF, list(tickers)

with patch("data.ohlc_collector.load_tickers", return_value=["TSM"]), \
     patch("data.ohlc_db.download_all_years"), \
     patch("data.ohlc_db.download_status"), \
     patch("data.ohlc_db.download_pending"), \
     patch("data.ohlc_db.list_known_tickers", return_value={"TSM"}), \
     patch("data.ohlc_db.load_pending", return_value={"us": ["TSM"]}), \
     patch("data.ohlc_db.save_pending", side_effect=fake_save_pending), \
     patch("data.ohlc_db.upload_pending"), \
     patch("data.ohlc_db.load_status", return_value={}), \
     patch("data.ohlc_db.update_status"), \
     patch("data.ohlc_db.upload_status"), \
     patch("data.ohlc_db.save_year"), \
     patch("data.ohlc_db.upload_years"), \
     patch("data.ohlc_collector.fetch_ohlc_range", side_effect=fake_fetch_ohlc_range):
    result = ohlc_collector.backfill_new_tickers("us", start_year=TODAY.year, upload=True)

assert result == ["TSM"], f"FAIL case(a): known-but-pending ticker not retried: {result}"
assert saved_pending.get("us") == ["TSM"], f"FAIL case(a): TSM should remain pending after another hard failure: {saved_pending}"
print("OK case (a): known-but-pending ticker still retried, stays pending after failure:", result, saved_pending)

# --- Case (b): 이번엔 성공 -> pending에서 제거되는지 ---
saved_pending.clear()

def fake_fetch_ohlc_range_success(tickers, start, end):
    return FAKE_DF, []  # 하드 실패 없음

with patch("data.ohlc_collector.load_tickers", return_value=["TSM"]), \
     patch("data.ohlc_db.download_all_years"), \
     patch("data.ohlc_db.download_status"), \
     patch("data.ohlc_db.download_pending"), \
     patch("data.ohlc_db.list_known_tickers", return_value={"TSM"}), \
     patch("data.ohlc_db.load_pending", return_value={"us": ["TSM"]}), \
     patch("data.ohlc_db.save_pending", side_effect=fake_save_pending), \
     patch("data.ohlc_db.upload_pending"), \
     patch("data.ohlc_db.load_status", return_value={}), \
     patch("data.ohlc_db.update_status"), \
     patch("data.ohlc_db.upload_status"), \
     patch("data.ohlc_db.save_year"), \
     patch("data.ohlc_db.upload_years"), \
     patch("data.ohlc_collector.fetch_ohlc_range", side_effect=fake_fetch_ohlc_range_success), \
     patch("data.ohlc_collector._enrich_us_marketcap", side_effect=lambda df: df):
    result2 = ohlc_collector.backfill_new_tickers("us", start_year=TODAY.year, upload=True)

assert result2 == ["TSM"], f"FAIL case(b): {result2}"
assert saved_pending.get("us") == [], f"FAIL case(b): TSM should be removed from pending after success: {saved_pending}"
print("OK case (b): fully successful ticker removed from pending:", result2, saved_pending)

print("ALL OK")
```

- [x] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_pending_retry.py"`
Expected: `AssertionError: FAIL case(a): known-but-pending ticker not retried: []` (현재 코드는 `known`이면 무조건 제외하므로 TSM이 candidates에서 빠짐)

- [x] **Step 3: `data/ohlc_collector.py`의 `backfill_new_tickers()` 전체 교체**

596번째 줄부터 700번째 줄까지의 함수 전체를 아래로 교체한다:

```python
def backfill_new_tickers(
    market: str,
    start_year: int = 2020,
    upload: bool = True,
) -> list[str]:
    """
    현재 유니버스에서 아직 완전히 백필되지 않은 종목(한 번도 수집된 적 없는
    신규 종목 + 이전 실행에서 일부 연도가 실패해 pending으로 남은 종목)을 찾아
    start_year ~ 올해까지 과거 이력을 백필한다.

    "완전히 백필됨"의 기준은 하드 실패(레이트리밋 등으로 재시도까지 소진)가
    한 번도 없었는지이다. 정상적인 빈 응답(상장 전 등, 예외 없이 빈 결과)은
    실패로 치지 않으므로 pending에 남지 않는다.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 백필 시작 연도 (기본 2020, ohlc-backfill 기본값과 동일)
        upload:     True면 연도별 저장 후 Drive 업로드

    Returns:
        이번에 시도한 티커 목록 (신규 + pending 재시도, 없으면 빈 리스트)
    """
    from data import ohlc_db

    ohlc_db.download_all_years(market)
    ohlc_db.download_status()
    ohlc_db.download_pending()

    known = ohlc_db.list_known_tickers(market)
    pending = set(ohlc_db.load_pending().get(market, []))
    universe = load_tickers(market)
    candidates = [
        t for t in universe if t not in known or t in pending
    ]

    if not candidates:
        logger.info(f"[NewTickerBackfill] {market.upper()} 신규/미완료 종목 없음")
        return []

    new_count = len(candidates) - len(pending & set(candidates))
    retry_count = len(pending & set(candidates))
    logger.info(
        f"[NewTickerBackfill] {market.upper()} 백필 대상 {len(candidates)}개 "
        f"(신규 {new_count}개, 미완료 재시도 {retry_count}개): {candidates}"
    )

    current_year = date.today().year
    all_dates: list[date] = []
    ticker_failed: set[str] = set()

    for year in range(start_year, current_year + 1):
        start_str = f"{year}-01-01"
        if year == current_year:
            end_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            end_str = f"{year + 1}-01-01"

        logger.info(
            f"[NewTickerBackfill] {market.upper()} {year}년 수집 중... "
            f"({len(candidates)}종목)"
        )
        try:
            df, failed_tickers = fetch_ohlc_range(candidates, start_str, end_str)
        except Exception as e:
            logger.error(f"[NewTickerBackfill] {market} {year}년 수집 실패: {e}")
            ticker_failed.update(candidates)
            continue

        ticker_failed.update(failed_tickers)

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

    all_pending = ohlc_db.load_pending()
    all_pending[market] = sorted(ticker_failed)
    ohlc_db.save_pending(all_pending)
    if upload:
        ohlc_db.upload_pending()
    if ticker_failed:
        logger.info(
            f"[NewTickerBackfill] {market.upper()} 다음 실행에 재시도할 "
            f"미완료 종목 {len(ticker_failed)}개: {sorted(ticker_failed)}"
        )

    if all_dates:
        status = ohlc_db.load_status()
        market_status = status.get(market, {})

        new_last = max(all_dates)
        new_oldest = min(all_dates)

        # last_updated는 update_market()의 증분 커서이며 "기존 종목 전체가 이 날짜까지
        # 수집 완료됨"을 의미한다. backfill_new_tickers()는 신규 종목이라는 부분집합만
        # 수집하므로 이 커서를 앞당길 권한이 없다. 기존 값이 있으면 절대 건드리지 않고
        # 그대로 보존하며, status.json 자체가 없는 최초 부트스트랩(신규 마켓)의 경우에만
        # 이번에 수집한 날짜로 초기값을 채운다.
        existing_last_str = market_status.get("last_updated")
        if existing_last_str:
            try:
                last_dt = datetime.strptime(existing_last_str, "%Y-%m-%d").date()
            except ValueError:
                last_dt = new_last
        else:
            last_dt = new_last

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

    logger.info(f"[NewTickerBackfill] {market.upper()} 백필 완료: {candidates}")
    return candidates
```

- [x] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_backfill_pending_retry.py"`
Expected: 2개 케이스 모두 `OK`, 마지막에 `ALL OK`

추가로 CLI 스모크 테스트:

Run (프로젝트 루트에서): `python main.py --mode ohlc-new-backfill --market us --dry-run`
Expected: `[DryRun] US 신규 종목 백필 시뮬레이션` 출력, 에러 없이 종료

Run (프로젝트 루트에서): `python main.py --mode ohlc-update --market all --dry-run`
Expected: 에러 없이 종료 (Task 2의 튜플 언패킹 변경이 `update_market()` 경로에서도 깨지지 않는지 확인)

- [x] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_backfill_pending_retry.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Retry hard-failed backfill tickers via backfill_pending.json

list_known_tickers() only checks "does any row exist anywhere," so a
ticker that got a single day of data via daily incremental after its
historical backfill hard-failed (rate limiting, etc.) became invisible
to backfill_new_tickers() forever — confirmed in production for
TSM/ALAB/COHR/NBIS (only a few days of 2026 data, 2020-2025 entirely
missing) and STRL/AGX (specific missing years).

backfill_new_tickers() now also includes tickers still listed in
backfill_pending.json regardless of list_known_tickers(), and after
each run recomputes that market's pending list from scratch based on
which tickers had a hard failure (batch retries exhausted) this run —
tickers with zero hard failures drop out of pending automatically.
Legitimate empty responses (pre-IPO periods, no exception) are not
treated as failures, so genuinely-recent listings don't get retried
forever.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
