# Luke Picks 시장 필터링 + yfinance 재시도 설계

## 배경

`_load_luke_picks_from_drive()`(2026-07-11 추가)는 Drive 파일의 `Ticker` 컬럼 값을
시장 구분 없이 전부 US 유니버스에 병합한다. 실제 운영 중 확인해보니, 사용자가
Drive에 올린 Luke Picks 파일은 애초 설계에서 가정한 "US 전용 단순 티커 리스트"가
아니라, `Portfolio_ATR_Monitor`의 `Portfolio.xlsx`와 동일하게 한국/미국 주식이
섞인 전체 포트폴리오 파일이었다. 그 결과:

1. 한국 종목 코드(예: `225590-KS`)까지 US 유니버스에 섞여 들어가 전체 티커 수가
   크게 늘어남 (기존 539종목 → 배치 14개, 약 700종목으로 추정).
2. 늘어난 요청량 때문에 `data/ohlc_collector.py`의 `fetch_ohlc_range()`가 야후
   파이낸스 레이트리밋(`yfinance.exceptions.YFRateLimitError`)에 걸리는 빈도가
   높아짐.
3. `fetch_ohlc_range()`는 배치 다운로드가 예외를 던지면 **재시도 없이** 그 배치
   전체(최대 50종목)를 `failed`에 넣고 다음 배치로 넘어간다 — 그래서 하필 그
   배치에 포함된 정상 종목(TSM/ALAB/COHR/NBIS 등)까지 통째로 누락된다.

`Portfolio_ATR_Monitor`의 `_parse_portfolio_df()`(`config.py:56-142`)는 이미 같은
파일 스키마에서 `구분` 컬럼(값: `한국`/`코스닥`/`미국`/`크립토`/`ETF`)으로 시장을
구분하는 로직을 갖고 있다. 이번 작업은 그 필터링 개념을 재사용하고, 별도로
배치 다운로드 재시도 로직을 추가한다.

## 목표

1. `_load_luke_picks_from_drive()`가 `구분` 컬럼이 있는 파일(다중 시장 포트폴리오)에서는
   `미국`/`ETF`로 분류된 행만 추출하도록 한다.
2. `구분` 컬럼이 없는 파일(순수 티커 리스트)에서는 기존처럼 전체를 그대로 사용한다
   (하위호환 유지 — 이미 순수 리스트를 올려둔 사용자에게 영향 없어야 함).
3. `fetch_ohlc_range()`의 배치 다운로드가 실패했을 때, 레이트리밋 여부를 구분해서
   백오프 후 재시도하도록 한다.

## 비목표

- `Ticker`/`Symbol` 값 자체에 대한 형식 검증(정규식으로 한국 종목 코드 패턴을
  걸러내는 등)은 하지 않는다 — `구분` 컬럼 필터링만으로 충분하다고 판단.
- 배치 크기(`_BATCH_SIZE=50`)나 배치 간 기본 sleep(1.0초)은 변경하지 않는다 —
  재시도 로직 추가만으로 우선 대응하고, 여전히 부족하면 별도로 튜닝한다.
- KR 시장 수집 로직(`kr_collector.py`)은 이미 자체 파이프라인을 갖고 있으므로
  변경하지 않는다.

## 설계

### 1. `data/ohlc_collector.py` — `_load_luke_picks_from_drive()` 시장 필터링

모듈 상단에 상수 추가:

```python
_US_CATEGORY_VALUES = {"미국", "ETF"}
```

`_load_luke_picks_from_drive()`(현재 137~192번째 줄) 안에서, mimeType 분기로
`df`를 확보한 직후 · Ticker 컬럼 탐색 loop 이전에 아래 필터링을 삽입:

```python
if "구분" in df.columns:
    df = df[df["구분"].astype(str).str.strip().isin(_US_CATEGORY_VALUES)]
```

`구분` 컬럼이 없으면 아무 필터링도 하지 않고 기존 동작(전체 티커 사용) 그대로
유지된다. `구분` 컬럼이 있지만 필터링 결과가 빈 DataFrame이 되는 경우, 이어지는
Ticker 컬럼 탐색 loop은 빈 리스트를 반환하게 되며 — 이는 "이 파일에 미국/ETF로
분류된 종목이 없다"는 정상적인 결과이지 오류가 아니다.

### 2. `data/ohlc_collector.py` — `fetch_ohlc_range()` 배치 재시도

모듈 상단에 조건부 import 추가 (설치된 yfinance 버전에 따라 클래스가 없을 수도
있으므로 방어적으로 처리):

```python
try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:
    YFRateLimitError = None
```

상수 추가:

```python
_BATCH_MAX_RETRIES = 3
_RATE_LIMIT_BACKOFF_SECONDS = [30, 60, 120]
_GENERIC_RETRY_BACKOFF_SECONDS = [5, 10, 20]
```

`fetch_ohlc_range()`(현재 296번째 줄 시작) 안의 배치 루프에서, 현재 338~356번째
줄에 아래처럼 되어 있는 부분을:

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

아래처럼 재시도 루프로 교체한다:

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

배치 간 기존 `time.sleep(1.0)`(루프 마지막)은 그대로 둔다 — 재시도는 실패했을
때만 추가로 대기하는 것이고, 정상 배치 흐름의 기본 간격은 이번 변경 범위가
아니다.

## 에러 처리

- `YFRateLimitError` import가 실패하는 환경(예: 구버전 yfinance)에서도
  `YFRateLimitError = None`으로 안전하게 폴백하며, 이 경우 모든 배치 실패가
  "일반 오류"로 취급되어 짧은 백오프로 재시도된다 — 기능이 깨지지 않는다.
- 3회 재시도 후에도 실패하면 기존과 동일하게 `failed`에 추가하고 다음 배치로
  진행한다 — 전체 파이프라인을 막지 않는다.
- `_load_luke_picks_from_drive()`의 나머지 에러 처리(환경변수 없음, Drive
  인증/파싱 실패 시 빈 리스트 반환)는 변경하지 않는다.

## 테스트 계획

이 프로젝트는 pytest를 쓰지 않으므로, 기존 관례대로 스크래치 스크립트 +
`unittest.mock`으로 검증한다.

- `_load_luke_picks_from_drive()`: `구분` 컬럼이 있는 DataFrame(한국/미국/ETF
  섞임)을 모킹해서 미국/ETF 행만 반환되는지 확인. `구분` 컬럼이 없는
  DataFrame(순수 티커 리스트)에서는 필터링 없이 전체가 반환되는지 확인
  (하위호환 회귀 테스트).
- `fetch_ohlc_range()`: `yf.download`를 모킹해서 (a) 첫 시도에 `YFRateLimitError`를
  던지고 두 번째 시도에 성공하면 결과가 정상 반환되는지, (b) 3회 모두
  실패하면 해당 배치 티커들이 `failed`에 들어가고 다음 배치로 넘어가는지,
  (c) `YFRateLimitError`가 아닌 일반 예외일 때 짧은 백오프가 적용되는지 확인.
  실제 `time.sleep`은 모킹해서 테스트가 실제로 대기하지 않도록 한다.
