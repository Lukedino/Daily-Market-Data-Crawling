# 백필 미완료 종목 추적(backfill_pending) 설계

## 배경

`backfill_new_tickers()`는 `ohlc_db.list_known_tickers(market)`(parquet에 해당
티커가 한 행이라도 있는지)로 "신규 종목"을 판별한다. 그런데 이 판별 기준은
"완전히 백필됨"이 아니라 "어떤 이유로든 단 한 행이라도 존재함"이기 때문에,
아래와 같은 순서로 문제가 발생한다:

1. 신규 종목(예: `TSM`, `ALAB`, `COHR`, `NBIS`)이 유니버스에 추가됨
2. `backfill_new_tickers()`가 `start_year`(2020)~올해까지 과거 이력을 시도
3. 과거 여러 연도가 (레이트리밋 등으로) 실패하지만, **오늘 날짜(가장 최근 연도)만
   성공**하거나, 그날부터 daily 증분(`update_market()`)이 자동으로 계속 수집하며
   최근 데이터가 조금씩 쌓임
4. `list_known_tickers()`가 이 티커를 "이미 있음"으로 판단 → **`backfill_new_tickers()`가
   두 번 다시 이 티커를 대상으로 삼지 않음** — 과거 이력 구멍이 영구히 복구되지 않음

실제로 확인된 사례: `TSM`은 2026-04-21 이후 데이터만 있고 2020~2025는 전부
없음. `STRL`은 2024년만, `AGX`는 2024~2025년만 구멍이 뚫려 있다.

## 목표

한 번의 실행에서 특정 연도가 실패한 종목을, 다음 실행에서 **자동으로 다시
시도 대상에 포함**시킨다. 완전히(모든 연도) 성공할 때까지 계속 추적한다.

## 비목표

- yfinance 버전(현재 GHA에서 설치되는 1.5.1 등) 관련 이슈는 다루지 않는다 —
  사용자 지시에 따라 이번 작업 범위에서 명시적으로 제외.
- `PSTG`/`BITF`/`U-UN-TO`처럼 매 시도마다 즉시 빈 응답(`raw.empty`, 예외 없이)이
  오는 케이스의 근본 원인(원인 불명 또는 티커 정규화 버그)은 다루지 않는다.
  다만 이번 추적 메커니즘의 판별 기준(아래 참고)상 이런 티커들은 애초에
  "실패"로 집계되지 않으므로(예외가 발생하지 않는 정상적인 빈 응답으로 처리됨)
  이 변경으로 인해 새로 pending에 걸리거나 하지 않는다 — 현재 동작과 동일하게
  유지된다.
- 특정 티커의 실제 상장일을 자동 판별하는 기능은 추가하지 않는다. 따라서
  진짜로 최근 상장한 종목(예: 2025년 상장)은 `start_year`(2020)~상장 전 구간이
  매번 "정상적으로 비어있음"으로 확인되고 pending에 남지 않는다 — 기존
  `raw.empty` 처리와 동일한 방식으로 구분한다(아래 판별 기준 참고).

## 판별 기준: "실패"란 무엇인가

`fetch_ohlc_range()` 내부에는 이미 두 가지 실패 유형이 구분되어 있다:

- **하드 실패**: 배치 다운로드가 재시도(`_BATCH_MAX_RETRIES`)까지 모두 소진하며
  예외를 던진 경우(`failed.extend(batch)`), 또는 개별 티커 처리 중 예외가 발생한
  경우(`failed.append(ticker)`). 이 목록은 현재도 내부적으로 집계되지만 함수
  밖으로 반환되지 않고 로그로만 남는다.
- **정상적인 빈 응답**: `yf.download()`이 예외 없이 성공했지만 결과가 비어있는
  경우(`raw.empty`) — 대부분 "그 기간에 실제로 거래 데이터가 없음"(상장 전
  등)을 의미하며, 현재 `failed`에 포함되지 않는다.

이번 변경은 **하드 실패만**을 "다음에 재시도 필요"로 취급한다. 정상적인 빈
응답은 지금처럼 "그 해엔 데이터 없음"으로 조용히 넘어간다. 이 구분 덕분에
레이트리밋처럼 진짜 일시적 실패는 자동으로 재추적되고, 상장 전이라 데이터가
없는 정상 케이스는 매번 재시도되며 낭비되지 않는다.

## 설계

### 1. `data/ohlc_collector.py` — `fetch_ohlc_range()` 반환값 확장

현재 `-> pd.DataFrame`(320번째 줄 함수 시그니처)을
`-> tuple[pd.DataFrame, list[str]]`로 변경한다. 두 번째 반환값은 함수 내부에
이미 존재하는 `failed`(하드 실패 티커 목록, 350번째 줄에서 `failed: list[str] = []`로
초기화되어 이후 채워짐)를 그대로 반환한다. 두 return 지점을 각각 수정한다:

- 469번째 줄: `return pd.DataFrame(columns=_EXTENDED_COLS)` →
  `return pd.DataFrame(columns=_EXTENDED_COLS), failed`
- 484번째 줄: `return result` → `return result, failed`

두 지점 모두 이미 있는 `failed` 지역 변수를 그대로 반환하는 것이며, 새로운
계산 로직은 추가하지 않는다.

### 2. 호출부 3곳 업데이트

`fetch_ohlc_range(...)`를 호출하는 3곳 중 `backfill_new_tickers()`(645번째 줄)는
아래 4번 항목에서 함수 전체를 교체하며 함께 처리된다. 나머지 2곳은 반환값
언패킹만 바꾸고 나머지 로직은 그대로 둔다:

- `backfill_market()` 567번째 줄: `df = fetch_ohlc_range(tickers, start_str, end_str)`
  → `df, _ = fetch_ohlc_range(tickers, start_str, end_str)`
- `update_market()` 778번째 줄: `new_df = fetch_ohlc_range(tickers, start_str, end_str)`
  → `new_df, _ = fetch_ohlc_range(tickers, start_str, end_str)`

이 두 함수는 `failed` 목록을 쓰지 않으므로 `_`로 버린다. 이후 코드
(`df.empty`/`new_df.empty` 체크 등)는 변경하지 않는다.

### 3. `data/ohlc_db.py` — pending 목록 저장/로드/Drive 연동

`db_status.json`과 동일한 패턴으로 `backfill_pending.json`을 추가한다.

```python
_PENDING_PATH = _LOCAL_ROOT / "backfill_pending.json"


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

`load_status`/`save_status`/`download_status`/`upload_status`(198~350번째 줄
부근)와 완전히 동일한 패턴이며, 파일 경로 상수와 파일명만 다르다.

파일 구조(시장별로 아직 완전히 백필되지 않은 티커 목록):

```json
{
  "us": ["TSM", "ALAB", "COHR", "NBIS"],
  "crypto": []
}
```

Drive 저장 위치는 `db_status.json`과 동일하게 `config.DRIVE_PATHS["ohlc_meta"]`
(`_meta/` 폴더) 아래에 `backfill_pending.json`이라는 별도 파일로 둔다.

### 4. `data/ohlc_collector.py` — `backfill_new_tickers()` 전체 교체

현재 596~700번째 줄의 함수를 아래로 교체한다 (변경 지점에 주석으로 표시):

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
    ohlc_db.download_pending()  # 변경: pending 목록도 동기화

    known = ohlc_db.list_known_tickers(market)
    pending = set(ohlc_db.load_pending().get(market, []))  # 변경
    universe = load_tickers(market)
    candidates = [
        t for t in universe if t not in known or t in pending
    ]  # 변경: known 이더라도 pending 이면 다시 대상에 포함

    if not candidates:
        logger.info(f"[NewTickerBackfill] {market.upper()} 신규/미완료 종목 없음")
        return []

    new_count = len(candidates) - len(pending & set(candidates))
    retry_count = len(pending & set(candidates))
    logger.info(
        f"[NewTickerBackfill] {market.upper()} 백필 대상 {len(candidates)}개 "
        f"(신규 {new_count}개, 미완료 재시도 {retry_count}개): {candidates}"
    )  # 변경: 신규/재시도 구분 로그

    current_year = date.today().year
    all_dates: list[date] = []
    ticker_failed: set[str] = set()  # 변경: 하드 실패 티커 누적

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
            df, failed_tickers = fetch_ohlc_range(
                candidates, start_str, end_str
            )  # 변경: 튜플 언패킹
        except Exception as e:
            logger.error(f"[NewTickerBackfill] {market} {year}년 수집 실패: {e}")
            ticker_failed.update(candidates)  # 변경: 연도 전체 실패로 간주
            continue

        ticker_failed.update(failed_tickers)  # 변경: 하드 실패 누적

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

    # 변경: pending 목록 갱신 — all_dates 유무와 무관하게 항상 수행
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

`known`/`universe`/`current_year`/`all_dates` 등 기존 변수명과 상태-갱신
로직(마지막 `if all_dates:` 블록)은 그대로 유지한다 — 이번 변경은 "대상
티커 선정"과 "실패 추적/pending 갱신" 두 부분에만 관여한다.

## 에러 처리

- `download_pending()`이 실패(Drive에 파일이 아직 없는 최초 실행 등)하면
  `load_pending()`은 빈 dict를 반환하고, `pending = set()`이 되어 기존
  "신규 종목만" 로직과 동일하게 동작한다 — 하위호환.
- pending 저장/업로드 실패는 `save_status`/`upload_status`와 동일하게
  예외를 잡아 로그만 남기고 파이프라인을 막지 않는다 (기존 관례 재사용).

## 테스트 계획

이 프로젝트는 pytest를 쓰지 않으므로, 기존 관례대로 스크래치 스크립트 +
`unittest.mock`으로 검증한다.

- `fetch_ohlc_range()`: 배치가 재시도까지 모두 실패하는 경우 반환된 튜플의
  두 번째 값(`failed`)에 해당 티커들이 포함되는지 확인. 정상적으로 빈 응답
  (`raw.empty`, 예외 없음)인 경우 `failed`에 포함되지 않는지 확인.
- `backfill_new_tickers()`: (a) 특정 티커가 특정 연도에서 하드 실패하도록
  모킹 → 함수 종료 후 `ohlc_db.save_pending()`에 전달된 데이터에 그 티커가
  포함되는지 확인. (b) 모든 연도가 성공하도록 모킹 → 저장된 pending 목록에
  해당 티커가 없는지(제거됨) 확인. (c) 기존에 pending으로 등록된 티커가
  `list_known_tickers()`상 "이미 있음"으로 나오더라도 `candidates`에
  포함되는지 확인 — 이번 수정의 핵심 회귀 테스트.
- `backfill_market()`/`update_market()`: 튜플 언패킹만 바뀌었으므로, 기존
  스모크 테스트(`--dry-run` CLI 실행)로 임포트/문법 오류가 없는지 확인.
