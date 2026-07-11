# Luke Picks 시장 필터링 + yfinance 재시도 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drive Luke Picks 파일에 섞인 한국 종목이 US 유니버스로 새어 들어가는 것을 막고, yfinance 배치 다운로드 실패 시 레이트리밋을 구분해 백오프 재시도하도록 해서, 정상 종목이 우연히 실패한 배치에 끼어 있다는 이유만으로 누락되는 문제를 해결한다.

**Architecture:** `data/ohlc_collector.py`의 `_load_luke_picks_from_drive()`에 `구분` 컬럼 기반 필터링을 추가하고, `fetch_ohlc_range()`의 배치 다운로드 실패 처리를 단발성 실패에서 백오프 재시도 루프로 교체한다.

**Tech Stack:** Python 3.12, pandas, yfinance 1.2.0, unittest.mock (스크래치 검증)

## Global Constraints

- 이 프로젝트는 pytest 등 테스트 프레임워크가 없다. 검증은 `unittest.mock`(표준 라이브러리)을 사용한 스크래치 스크립트로 하고, 커밋하지 않는다.
- `구분` 컬럼이 없는 파일(순수 티커 리스트)에서는 필터링 없이 기존 동작(전체 사용)을 그대로 유지해야 한다 — 하위호환.
- 배치 크기(`_BATCH_SIZE=50`)나 배치 간 기본 sleep(1.0초)은 변경하지 않는다.
- `yfinance.exceptions.YFRateLimitError` import가 실패하는 환경에서도 안전하게 동작해야 한다 (`None` 폴백).
- KR 시장 수집 로직(`kr_collector.py`)은 변경하지 않는다.

---

### Task 1: `_load_luke_picks_from_drive()` 시장(구분) 필터링

**Files:**
- Modify: `data/ohlc_collector.py` (137~192번째 줄 함수 내부 + 상수 1개 추가)

**Interfaces:**
- Produces (기존 시그니처 유지): `_load_luke_picks_from_drive() -> list[str]` — `구분` 컬럼이 있으면 `미국`/`ETF` 행만, 없으면 전체를 반환.

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_market_filter.py` 파일을 아래 내용으로 작성한다.

```python
import io
import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, r"C:\Users\Luke Skywalker\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
from data import ohlc_collector

# --- Case 1: 구분 컬럼 있음(한국/미국/ETF 섞임) → 미국/ETF만 반환 ---
csv_bytes = (
    "구분,Ticker\n"
    "한국,005930\n"
    "미국,TSM\n"
    "미국,ALAB\n"
    "코스닥,247540\n"
    "ETF,SPY\n"
    "크립토,BTC\n"
).encode("utf-8")

mock_service = MagicMock()
mock_service.files.return_value.get.return_value.execute.return_value = {
    "mimeType": "application/vnd.google-apps.spreadsheet"
}
mock_service.files.return_value.export.return_value.execute.return_value = csv_bytes

with patch.dict(os.environ, {
    "GOOGLE_SERVICE_ACCOUNT_JSON": '{"type": "service_account"}',
    "GDRIVE_LUKE_PICKS_FILE_ID": "fake_file_id",
}), \
     patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
     patch("googleapiclient.discovery.build", return_value=mock_service):
    result = ohlc_collector._load_luke_picks_from_drive()

assert set(result) == {"TSM", "ALAB", "SPY"}, f"FAIL case1: {result}"
assert "005930" not in result and "247540" not in result and "BTC" not in result, f"FAIL case1 leak: {result}"
print("OK case 1 (구분 컬럼 필터링 — 한국/코스닥/크립토 제외):", result)

# --- Case 2: 구분 컬럼 없음(순수 티커 리스트) → 필터링 없이 전체 반환 (하위호환) ---
csv_bytes2 = "Ticker\nCRDO\nSTRL\n".encode("utf-8")

mock_service2 = MagicMock()
mock_service2.files.return_value.get.return_value.execute.return_value = {
    "mimeType": "application/vnd.google-apps.spreadsheet"
}
mock_service2.files.return_value.export.return_value.execute.return_value = csv_bytes2

with patch.dict(os.environ, {
    "GOOGLE_SERVICE_ACCOUNT_JSON": '{"type": "service_account"}',
    "GDRIVE_LUKE_PICKS_FILE_ID": "fake_file_id",
}), \
     patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
     patch("googleapiclient.discovery.build", return_value=mock_service2):
    result2 = ohlc_collector._load_luke_picks_from_drive()

assert result2 == ["CRDO", "STRL"], f"FAIL case2: {result2}"
print("OK case 2 (구분 컬럼 없음 — 하위호환, 전체 반환):", result2)

print("ALL OK")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_market_filter.py"`
Expected: `AssertionError: FAIL case1: [...]` (현재는 `구분` 컬럼을 무시하고 6개 전부 반환하므로 `005930`/`247540`/`BTC`가 섞여 있어 실패)

- [ ] **Step 3: `data/ohlc_collector.py` 수정**

`_load_luke_picks_from_drive()` 함수(137번째 줄) 바로 앞에 상수 추가:

```python
_US_CATEGORY_VALUES = {"미국", "ETF"}


def _load_luke_picks_from_drive() -> list[str]:
```

함수 docstring(138~142번째 줄)을 아래로 교체:

```python
    """
    Google Drive에서 개인 관심종목 리스트를 다운로드하여 파싱.
    GOOGLE_SERVICE_ACCOUNT_JSON + GDRIVE_LUKE_PICKS_FILE_ID 둘 다 있어야 동작.
    둘 중 하나라도 없거나 실패하면 조용히 빈 리스트 반환 (필수 기능 아님).

    "구분"(시장분류) 컬럼이 있으면 미국/ETF로 분류된 행만 추출한다
    (Portfolio_ATR_Monitor의 Portfolio.xlsx와 동일 스키마 재사용 지원).
    구분 컬럼이 없으면 단순 티커 리스트로 간주해 전체를 사용한다.
    """
```

mimeType 분기로 `df`를 확보하는 코드(165~178번째 줄) 바로 다음, Ticker 컬럼
탐색 loop(180번째 줄 `for col in ("Ticker", "ticker", "Symbol"):`) 이전에 아래
필터링을 삽입:

```python
        if "구분" in df.columns:
            df = df[df["구분"].astype(str).str.strip().isin(_US_CATEGORY_VALUES)]

        for col in ("Ticker", "ticker", "Symbol"):
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_market_filter.py"`
Expected: 두 케이스 모두 `OK`, 마지막에 `ALL OK`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "/c/Users/LUKESK~1/AppData/Local/Temp/claude/C--Users-Luke-Skywalker/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_luke_picks_market_filter.py"
cd "/c/Users/Luke Skywalker/Desktop/Luke 작업공간/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Filter Luke Picks Drive file to US/ETF rows when a 구분 column exists

The Drive-hosted Luke Picks file turned out to be a mixed KR/US
portfolio (same schema as Portfolio_ATR_Monitor's Portfolio.xlsx), not
the US-only simple list originally assumed — Korean tickers were being
merged into the US universe unfiltered, inflating request volume and
contributing to yfinance rate limiting. Files with a 구분 column now
keep only 미국/ETF rows; files without one (plain ticker lists) are
unaffected.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `fetch_ohlc_range()` 배치 다운로드 재시도(백오프)

**Files:**
- Modify: `data/ohlc_collector.py` (상단 import/상수 추가, 296번째 줄 시작 함수 내부 338~356번째 줄 교체)

**Interfaces:**
- Produces (기존 시그니처 유지): `fetch_ohlc_range(tickers, start, end) -> pd.DataFrame` — 배치 실패 시 레이트리밋 여부에 따라 백오프 후 최대 3회 재시도.

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_retry.py`:

`fetch_ohlc_range()`는 `yfinance`를 함수 내부에서 `import yfinance as yf`로
지역 import한다(모듈 상단에 `yf`라는 이름이 없음). 따라서
`patch("data.ohlc_collector.yf.download", ...)`는 `AttributeError`로 즉시
실패한다 — 반드시 실제 패키지 경로인 `patch("yfinance.download", ...)`로
패치해야 한다(파이썬은 `sys.modules`에 캐시된 동일 모듈 객체를 참조하므로
이렇게 해도 함수 내부의 지역 import가 패치된 버전을 정상적으로 사용한다).

```python
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, r"C:\Users\Luke Skywalker\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

import pandas as pd
from yfinance.exceptions import YFRateLimitError
from data import ohlc_collector

FAKE_OK_DF = pd.DataFrame({
    "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [100],
}, index=pd.to_datetime(["2024-01-02"]))

sleep_calls = []

def fake_sleep(seconds):
    sleep_calls.append(seconds)

# --- Case (a): 첫 시도 YFRateLimitError, 두 번째 시도 성공 ---
call_count = {"n": 0}

def fake_download_rate_limit_then_ok(*args, **kwargs):
    call_count["n"] += 1
    if call_count["n"] == 1:
        raise YFRateLimitError("Too Many Requests. Rate limited.")
    return FAKE_OK_DF

with patch("yfinance.download", side_effect=fake_download_rate_limit_then_ok), \
     patch("data.ohlc_collector.time.sleep", side_effect=fake_sleep):
    df = ohlc_collector.fetch_ohlc_range(["AAPL"], "2024-01-01", "2024-01-03")

assert call_count["n"] == 2, f"FAIL case(a) call count: {call_count['n']}"
assert not df.empty, "FAIL case(a): expected non-empty df after successful retry"
assert sleep_calls and sleep_calls[0] in (30, 60, 120), f"FAIL case(a) backoff: {sleep_calls}"
print("OK case (a): rate-limit then success, backoff =", sleep_calls[0])

# --- Case (b): 3회 재시도 모두 실패 → 빈 결과, 다음 배치로 진행(예외 전파 없음) ---
sleep_calls.clear()

def fake_download_always_fail(*args, **kwargs):
    raise YFRateLimitError("Too Many Requests. Rate limited.")

with patch("yfinance.download", side_effect=fake_download_always_fail), \
     patch("data.ohlc_collector.time.sleep", side_effect=fake_sleep):
    df2 = ohlc_collector.fetch_ohlc_range(["MSFT"], "2024-01-01", "2024-01-03")

assert df2.empty, f"FAIL case(b): expected empty df, got {df2}"
assert len(sleep_calls) == 3, f"FAIL case(b) retry count: {sleep_calls}"
print("OK case (b): all retries exhausted, sleeps =", sleep_calls)

# --- Case (c): 일반 예외(YFRateLimitError 아님) → 짧은 백오프 사용 ---
sleep_calls.clear()
call_count["n"] = 0

def fake_download_generic_error_then_ok(*args, **kwargs):
    call_count["n"] += 1
    if call_count["n"] == 1:
        raise ConnectionError("temporary network error")
    return FAKE_OK_DF

with patch("yfinance.download", side_effect=fake_download_generic_error_then_ok), \
     patch("data.ohlc_collector.time.sleep", side_effect=fake_sleep):
    df3 = ohlc_collector.fetch_ohlc_range(["NVDA"], "2024-01-01", "2024-01-03")

assert not df3.empty, "FAIL case(c): expected non-empty df after retry"
assert sleep_calls and sleep_calls[0] in (5, 10, 20), f"FAIL case(c) backoff: {sleep_calls}"
print("OK case (c): generic error then success, backoff =", sleep_calls[0])

print("ALL OK")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_retry.py"`
Expected: `AssertionError: FAIL case(a) call count: 1` (현재는 재시도 없이 한 번만 시도하고 실패 처리하므로)

- [ ] **Step 3: `data/ohlc_collector.py` 수정**

`import requests`(26번째 줄) 바로 다음, `logger = logging.getLogger(__name__)`(28번째 줄) 이전에 조건부 import 추가:

```python
import requests

try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:
    YFRateLimitError = None

logger = logging.getLogger(__name__)
```

`_BATCH_SIZE = 50   # yfinance rate limit 방지`(56번째 줄) 바로 다음에 재시도
관련 상수 추가:

```python
_BATCH_SIZE = 50   # yfinance rate limit 방지
_BATCH_MAX_RETRIES = 3
_RATE_LIMIT_BACKOFF_SECONDS = [30, 60, 120]
_GENERIC_RETRY_BACKOFF_SECONDS = [5, 10, 20]
_CMC_TOP_N  = 200  # CoinMarketCap 상위 N개
```

`fetch_ohlc_range()` 안의 338~356번째 줄(현재 아래 내용)을:

```python
        try:
            raw = yf.download(
                batch,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 다운로드 실패: {e}")
            failed.extend(batch)
            time.sleep(2.0)
            continue

        if raw is None or raw.empty:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 빈 응답")
            continue
```

아래로 교체:

```python
        raw = None
        for attempt in range(_BATCH_MAX_RETRIES + 1):
            try:
                raw = yf.download(
                    batch,
                    start=start,
                    end=end,
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
                break
            except Exception as e:
                is_rate_limit = (
                    YFRateLimitError is not None and isinstance(e, YFRateLimitError)
                )
                if attempt >= _BATCH_MAX_RETRIES:
                    logger.warning(
                        f"[OhlcCollector] 배치 {batch_no} 다운로드 실패 "
                        f"(재시도 {_BATCH_MAX_RETRIES}회 모두 소진): {e}"
                    )
                    failed.extend(batch)
                    raw = None
                    break
                backoff = (
                    _RATE_LIMIT_BACKOFF_SECONDS if is_rate_limit
                    else _GENERIC_RETRY_BACKOFF_SECONDS
                )[attempt]
                kind = "rate limit" if is_rate_limit else "일시적 오류"
                logger.warning(
                    f"[OhlcCollector] 배치 {batch_no} 다운로드 실패({kind}): {e} "
                    f"— {backoff}초 후 재시도 ({attempt + 1}/{_BATCH_MAX_RETRIES})"
                )
                time.sleep(backoff)

        if raw is None:
            continue

        if raw.empty:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 빈 응답")
            continue
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "C:\Users\LUKESK~1\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_fetch_ohlc_range_retry.py"`
Expected: 3개 케이스 모두 `OK`, 마지막에 `ALL OK`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "/c/Users/LUKESK~1/AppData/Local/Temp/claude/C--Users-Luke-Skywalker/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_fetch_ohlc_range_retry.py"
cd "/c/Users/Luke Skywalker/Desktop/Luke 작업공간/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Retry yfinance batch downloads with backoff instead of dropping them

fetch_ohlc_range() previously gave up on a batch after a single
failed yf.download() call, silently losing every ticker in that batch
for the year (up to 50 tickers) — including perfectly valid ones that
just happened to share a rate-limited batch (e.g. TSM/ALAB/COHR/NBIS).
Now retries up to 3 times, using a longer backoff specifically for
YFRateLimitError and a shorter one for other transient errors, before
falling back to the previous behavior of recording the batch as
failed.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
