# US/Crypto 백필 완결성 검증 스크립트 설계

## 배경

[[backfill_pending 추적]](2026-07-11-backfill-pending-tracking-design.md) 기능은
"이번 실행에서 하드 실패한 티커"만 추적한다. 그런데 실제로 발견된 문제는 더
넓은 범위였다: 2026-07-10에 `[FEAT-NEW-TICKER-BACKFILL]`이 도입되기 **이전**에
유니버스(Luke Picks 워치리스트)에 추가된 종목들은, 자동 백필 안전장치가 아예
없던 시절이라 `update_market()`(일별 증분)만으로 조금씩 데이터가 쌓였다.

`list_known_tickers()`는 "어딘가 한 행이라도 있으면 완료"로 판단하므로, 이런
종목은 처음부터 "known" 상태로 시작해 `backfill_new_tickers()`의 후보에서
영구히 제외된다 — 하드 실패가 발생한 적이 없으니 `backfill_pending.json`에도
안 들어간다. TSM/ALAB/COHR/NBIS/STRL/AGX(6종목, 수동 시딩으로 해결)에 이어,
워치리스트가 28→332종목으로 커지며 DNLI/EDIT/KRYS 등 23종목이 동일한 패턴으로
추가 발견됐다: 전부 정확히 2026-01-02부터 시작하는 데이터만 존재.

수동으로 전수조사해서 찾아낸 방식은 반복 가능하지 않다. 자동으로 "최초
수집일이 의심스럽게 최근"인 티커를 찾아 리포트하고, 확인 후 pending에 반영해
기존 백필 파이프라인으로 흘려보내는 도구가 필요하다.

## 목표

- 시장별로 티커의 최초 수집일을 계산해, 지정한 기준일 이후에 데이터가
  시작되는 티커를 리포트한다.
- 리포트된 후보 중 사람이 확인 후, `--fix`로 `backfill_pending.json`에 반영해
  다음 `ohlc-new-ticker-backfill` 실행에서 자동으로 재시도되게 한다.

## 비목표

- 티커의 실제 상장일을 자동 판별하지 않는다 (기존 backfill_pending 설계와
  동일한 제약). 그래서 이 스크립트는 **리포트만** 하고, 실제 백필 데이터
  수집이나 pending 반영은 `--fix`를 사람이 명시적으로 실행할 때만 일어난다.
- `--fix`가 실제 yfinance fetch를 수행하지 않는다. pending 등록까지만 하고,
  실 수집은 기존 `backfill_new_tickers()`(daily 자동 통합 또는
  `ohlc-new-ticker-backfill.yml` 수동 트리거)에 위임한다 — 재시도/에러 처리
  로직을 한 곳에만 둔다.
- GHA 워크플로우로 자동 스케줄링하지 않는다. `verify_kr.py`와 동일하게 수동
  진단 도구로 둔다.

## 설계

### 1. `data/ohlc_db.py` — `first_seen_dates()` 신규 함수

`list_known_tickers()`(72~94번째 줄) 바로 다음에 추가한다. 같은 파일 순회
패턴을 재사용하되 `Ticker`뿐 아니라 `Date`도 읽어 티커별 최소값을 구한다.

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

연도 파일이 여러 개이므로 파일별로 계산한 min을 다시 전체 min으로 합친다
(한 파일 안에서만 min을 구하면 다른 연도 파일의 더 이른 날짜를 놓친다).

`list_known_tickers()`와 마찬가지로 개별 파일 읽기 실패는 경고 로그 후
스킵하고 전체 스캔은 계속한다.

### 2. `scripts/verify_ohlc.py` 신규 파일

`scripts/verify_kr.py`와 동일한 구조(분석 함수 → 리포트 출력 → 메인)를
따른다.

**CLI 인자:**
- `--market {us,crypto,all}` (기본 `all`) — `main.py`의 기존 `--market` 관례와
  동일
- `--after YYYY-MM-DD` (기본 `2024-01-01`) — 이 날짜 이후에 최초 데이터가
  시작되는 티커를 "의심 후보"로 분류
- `--drive` — 로컬 스캔 전에 Drive에서 최신 연도별 parquet +
  `backfill_pending.json` 다운로드 (`ohlc_db.download_all_years(market)`,
  `ohlc_db.download_pending()` 재사용)
- `--fix` — 새로 발견된 후보를 pending에 반영 (아래 상세)

**리포트 로직:** 시장별 결과를 `report[market] = {"new": [...], "already_pending": [...]}`
형태로 모아두고, 출력과 `--fix` 양쪽에서 재사용한다.

```python
report: dict[str, dict] = {}
for market in markets:
    first_dates = ohlc_db.first_seen_dates(market)
    pending = set(ohlc_db.load_pending().get(market, []))
    after = args.after  # date 객체

    suspects = {t: d for t, d in first_dates.items() if d > after}
    report[market] = {
        "new": sorted(set(suspects) - pending),
        "already_pending": sorted(set(suspects) & pending),
        "first_dates": suspects,
    }
```

출력 형식은 `verify_kr.py`의 표 스타일을 따른다: 티커, 최초 수집일, 상태
(`⚠️ 조치 필요` / `⏳ 이미 pending`)를 나열하고, 마지막에 요약 카운트와
`--fix` 실행 안내 문구를 출력한다.

**`--fix` 로직:** (기존 Task 1 함수만 재사용, 신규 fetch 로직 없음)

```python
if args.fix:
    for market in markets:
        new_candidates = report[market]["new"]
        if not new_candidates:
            continue
        all_pending = ohlc_db.load_pending()
        merged = sorted(set(all_pending.get(market, [])) | set(new_candidates))
        all_pending[market] = merged
        ohlc_db.save_pending(all_pending)
        ohlc_db.upload_pending()
    print("pending 반영 완료 — ohlc-new-ticker-backfill 워크플로우를 실행하면 다음 회차에 자동 재시도됩니다.")
```

기존 pending 항목은 덮어쓰지 않고 합집합으로 병합한다 (이미 진행 중인 다른
재시도 대상을 지우지 않기 위함).

## 데이터 흐름

```
(--drive 시) Drive → 로컬 parquet + backfill_pending.json 다운로드
  → first_seen_dates(market) 스캔
  → --after 기준으로 후보 필터링
  → pending과 대조해 신규/기존 구분
  → 리포트 출력
  → (--fix 시) pending 병합 → save_pending() → upload_pending()
```

실제 데이터 수집(yfinance fetch)은 이 스크립트가 하지 않는다. `--fix` 이후
사람이 `ohlc-new-ticker-backfill.yml`을 수동 실행하거나 다음 `ohlc-daily`
자동 실행을 기다려야 실제로 채워진다.

## 에러 처리

- 로컬에 해당 마켓 폴더가 없으면 (`--drive` 없이 실행한 경우 등) 안내 메시지
  출력 후 종료 — `verify_kr.py`와 동일한 패턴.
- 개별 parquet 파일 읽기 실패는 스킵 후 계속 (위 참고).
- `--fix` 중 `upload_pending()` 실패는 기존 관례대로 로그만 남기고 중단하지
  않음 — 로컬 `backfill_pending.json`은 이미 갱신됐으므로 사용자가 수동
  업로드하거나 다음 정상 실행 때 재시도 가능.

## 테스트 계획

pytest 없음 — 기존 관례대로 스크래치 스크립트 + `unittest.mock`으로 검증.

- `first_seen_dates()`: 서로 다른 연도 파일 2개(예: 2020년 파일엔 없고 2024년
  파일에만 있는 티커, 여러 연도에 걸쳐 있는 티커)를 가진 임시 parquet으로
  최소 날짜가 올바르게 계산되는지 확인.
- `verify_ohlc.py` 리포트: `first_seen_dates()`와 `load_pending()`을 모킹해
  (a) `--after` 이후 시작 + pending에 없음 → "신규 후보"로 분류되는지,
  (b) `--after` 이후 시작 + 이미 pending → "기존" 표시되는지, (c) `--after`
  이전 시작 → 후보에서 제외되는지 확인.
- `--fix`: 기존 pending에 다른 티커가 있는 상태에서 새 후보를 추가했을 때
  기존 항목이 지워지지 않고 합집합으로 저장되는지 확인.
- CLI 스모크: 로컬 데이터로 `python scripts/verify_ohlc.py --market us`
  실행해 에러 없이 리포트가 출력되는지 확인 (실제 Drive 접근 없이).
