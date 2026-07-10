# 신규 종목 자동 백필 설계

## 배경

US/Crypto OHLC 유니버스(`data/ohlc_collector.py`)는 S&P500/NASDAQ100/DOW30/CoinMarketCap
Top200/`Luke Picks.xlsx`/수동 리스트를 합쳐서 매번 동적으로 구성된다. 이 유니버스에 신규
종목이 추가되면, 기존 `update_market()`의 증분 수집 로직은 **시장 전체 단위의 전역
`last_date`**(`db_status.json`)만 기준으로 `[last_date+1, 오늘]` 구간만 수집하기 때문에
신규 종목은 추가된 시점 이후 데이터만 쌓이고 과거 이력은 영구적으로 비어 있게 된다.

KR 시장은 `kr_collector.collect_backfill()`이 FDR 전종목 목록을 매번 새로 조회하는 방식이라
이 문제가 없다 (신규 상장/편입 종목도 자동으로 전종목 로직에 포함됨). 따라서 이번 설계는
**US/Crypto에 한정**한다.

## 목표

1. 신규 종목이 유니버스에 추가되면, 그 종목의 과거 이력을 자동으로 채우는 공통 로직을 만든다.
2. 이 로직을 daily 증분 워크플로우에서 자동으로 실행되게 한다.
3. 동일 로직을 수동으로도 즉시 실행할 수 있는 별도 GHA 워크플로우를 만든다.

## 비목표

- KR 시장 변경 없음 (이미 문제 없음).
- 신규 종목의 실제 상장일 자동 탐지는 하지 않음 (고정 시작연도 사용).
- MarketCap 과거 이력 보강은 하지 않음 (기존 정책과 동일하게 "오늘" 시점만 채움).

## 신규 종목 판별 기준

DB(로컬에 동기화된 연도별 parquet 전체)에 **한 번이라도 등장한 적 있는 티커 집합**과
현재 `load_tickers(market)` 결과를 비교(diff)한다. 상태 파일(`db_status.json`)에 별도
"known tickers" 목록을 유지하지 않고, 매번 실제 parquet 파일을 스캔해서 판단한다
(단순하고 명확하지만, 매 실행마다 Drive에서 해당 시장의 연도별 parquet 전체를 내려받아야
하므로 기존보다 Drive 다운로드가 늘어난다 — "성능/트레이드오프" 절 참고).

## 설계

### 1. 공통 로직 — `ohlc_collector.backfill_new_tickers(market, start_year=2020, upload=True) -> list[str]`

`data/ohlc_collector.py`에 신규 함수로 추가.

절차:
1. `ohlc_db.download_all_years(market)` 호출 — Drive의 해당 시장 연도별 parquet 전체를
   로컬에 동기화 (매번 최신 상태 기준으로 스캔하기 위함).
2. `ohlc_db.list_known_tickers(market)` (신규 헬퍼, 아래 참고)로 지금까지 한 번이라도
   수집된 티커 집합을 구함.
3. `load_tickers(market)`로 현재 유니버스를 가져와
   `new_tickers = [t for t in universe if t not in known]` 계산.
4. `new_tickers`가 비어 있으면 `"[NewTickerBackfill] {market} 신규 종목 없음"` 로그 후
   빈 리스트 반환.
5. `new_tickers`가 있으면 `start_year` ~ 올해(`date.today().year`)까지 연도별로 순회하며
   `fetch_ohlc_range(new_tickers, start_str, end_str)` 호출 (기존 `backfill_market()`과
   동일 패턴, 대상 티커만 축소).
   - 과거 연도(올해 미만): 그대로 `ohlc_db.save_year()`.
   - 올해 데이터: `fetch_ohlc_range`로 오늘까지 가져온 뒤, 시장에 맞는 기존 enrichment
     함수(`_enrich_us_marketcap` 또는 `_enrich_crypto_marketcap`)를 적용해 오늘 날짜 행의
     MarketCap을 채운 후 저장. (다른 기존 종목들과 동일한 상태로 맞추기 위함.)
   - `upload=True`이면 연도별로 `ohlc_db.upload_years(market, [year])` 호출.
6. 완료 후 `ohlc_db.update_status(market, ...)`로 `ticker_count`를
   `len(universe)`로 갱신하고, `upload=True`이면 `ohlc_db.upload_status()` 호출.
7. 발견된 `new_tickers` 리스트를 반환 (호출자가 로그/요약에 사용).

에러 처리: 개별 연도 수집 실패는 기존 `backfill_market()`과 동일하게 `try/except`로
로그만 남기고 다음 연도로 계속 진행 (전체 프로세스를 중단시키지 않음).

### 2. 신규 헬퍼 — `ohlc_db.list_known_tickers(market) -> set[str]`

`data/ohlc_db.py`에 추가.

- `local_dir(market)`의 `{market}_*.parquet` 파일들을 순회.
- 각 파일에서 `pq.read_table(path, columns=["Ticker"])`로 **Ticker 컬럼만** 읽어
  (전체 로드 대비 가벼움) unique 값을 모음.
- 파일이 하나도 없으면 빈 set 반환 (이 경우 유니버스 전체가 "신규"로 판별되어
  `backfill_new_tickers`가 사실상 전체 백필처럼 동작함 — 최초 실행 시 의도된 동작).

### 3. Daily 통합 지점

`main.py`의 `run_ohlc_update()`에서, 시장(`us`/`crypto`)별로 기존
`ohlc_collector.update_market()` 호출 **전에** `ohlc_collector.backfill_new_tickers(market, upload=args.upload_drive)`를 먼저 호출한다.

```python
def run_ohlc_update(args):
    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        if not args.dry_run:
            new_tickers = ohlc_collector.backfill_new_tickers(market, upload=args.upload_drive)
            if new_tickers:
                logger.info(f"[OhlcUpdate] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
            ohlc_collector.update_market(market=market, upload=args.upload_drive)
        else:
            logger.info(f"[DryRun] {market.upper()} ohlc update 시뮬레이션")
```

`dry_run`일 때는 기존과 동일하게 아무 것도 실행하지 않는다 (신규 종목 백필도 스킵).

### 4. 별도 GHA 워크플로우 — `.github/workflows/ohlc-new-ticker-backfill.yml`

- 트리거: `workflow_dispatch`만 (스케줄 없음 — daily에서 이미 자동 처리되므로, 수동
  실행은 "지금 바로 반영하고 싶을 때"용).
- 입력: `market` (choice: all/us/crypto, 기본 all), `start_year` (기본 '2020'),
  `dry_run` (boolean, 기본 false).
- `main.py`에 신규 모드 `ohlc-new-backfill` 추가 (`run_ohlc_new_backfill(args)` 함수),
  내부적으로 시장별 `ohlc_collector.backfill_new_tickers(market, start_year=int(args.start_year), upload=args.upload_drive)` 호출.
- 기존 `ohlc-backfill.yml`과 동일한 구조(checkout → setup-python → dependencies →
  실행 → 로그 업로드 → 자격증명 정리)를 따른다.

## 성능/트레이드오프

- daily job이 매번 Drive에서 해당 시장의 연도별 parquet 전체를 내려받아 티커를 스캔해야
  하므로, 기존 대비(현재는 당해 연도 파일만 받음) Drive 다운로드/디스크 사용이 늘어난다.
  현재 파일 크기가 연도당 수백 KB 수준이라 실질적 부담은 크지 않을 것으로 예상되지만,
  유니버스나 기간이 크게 늘어나면 재검토가 필요하다.
- 신규 종목이 다수 한꺼번에 추가되는 경우 `ohlc-daily.yml`의 `timeout-minutes: 30`을
  초과할 가능성이 있다. 이번 설계에서는 타임아웃을 늘리지 않고 그대로 두되, 실제
  운영 중 타임아웃이 발생하면 그때 값을 올린다 (YAGNI).

## 테스트 계획

이 프로젝트는 pytest 등 단위 테스트 프레임워크를 쓰지 않고(`requirements.txt`에도 없음),
`scripts/verify_kr.py`처럼 실행 스크립트로 직접 검증하는 방식을 쓰고 있다. 이번 기능도
동일한 관례를 따른다.

- 로컬 스모크 테스트: 임시 로컬 parquet(예: `us_2025.parquet`)에서 특정 티커 1개를
  제거한 사본을 만든 뒤 `ohlc_db.list_known_tickers("us")`를 호출해 그 티커가 빠져
  있는지 직접 확인.
- `python main.py --mode ohlc-new-backfill --market us --dry-run` — dry-run으로 실제
  Drive 접근/저장 없이 신규 종목 판별 로그만 확인.
- `input/us_universe.txt`에 테스트용 신규 티커 1개를 추가한 뒤 `--dry-run` 없이 실제
  실행해서, 해당 티커의 과거 연도 데이터가 로컬 parquet에 채워지는지 확인 (소규모 실측
  검증, Drive 업로드는 로컬 단계에서 `--upload-drive` 생략).
- 통합 확인: `run_ohlc_update()` 실행 로그에서 `backfill_new_tickers` 로그가
  `update_market` 로그보다 먼저 출력되는지 육안 확인.
