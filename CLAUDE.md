# Daily-Market-Data-Crawling

## 프로젝트 개요
GitHub Actions로 매일 자동 실행되는 시장 데이터 크롤러.
US 주식/ETF, 크립토, KR(한국) 시장 OHLC + 재무데이터를 수집해 연도별 Parquet으로 Google Drive에 저장.

- GitHub: https://github.com/Lukedino/Daily-Market-Data-Crawling
- 브랜치: main

---

## Google Drive 구조

### 루트 폴더
- `GDRIVE_OHLC_FOLDER_ID` = `[Database] Market Crawling Data` 폴더 ID
- GCP 프로젝트: (GitHub Secrets / 로컬 .env 참조 — 공개 저장소라 기재하지 않음)
- Service Account: (동일 — `GOOGLE_APPLICATION_CREDENTIALS` Secret 의 client_email)

### 서브폴더 구조
```
[Database] Market Crawling Data/   ← GDRIVE_OHLC_FOLDER_ID
    ├─ kr/          ← KR OHLC + 시총 (marcap 스키마, 연도별 parquet)
    │   ├─ marcap-2025.parquet
    │   ├─ marcap-2026.parquet
    │   ├─ marcap-2027~2030.parquet  (빈 플레이스홀더, 수동 업로드)
    │   └─ financials/
    │       ├─ kr_financials_2025.parquet
    │       ├─ kr_financials_2026.parquet
    │       └─ kr_financials_2027.parquet  (빈 플레이스홀더, 수동 업로드)
    ├─ us/          ← US 주식/ETF OHLC (연도별 parquet)
    │   ├─ us_YYYY.parquet
    │   └─ us_sector_meta.parquet   ← 종목 메타 (Sector/Industry/Market, 주 1회 갱신)
    ├─ crypto/      ← 크립토 OHLC (연도별 parquet)
    │   ├─ crypto_YYYY.parquet
    │   └─ crypto_sector_meta.parquet  ← 종목 메타 (Market="Crypto", 주 1회 갱신)
    └─ _meta/       ← DB 상태 메타 (db_status.json)
```

> ⚠️ Service Account는 신규 파일 생성 불가 (storageQuotaExceeded). 신규 연도 파일은 수동 업로드 후 SA가 update만 가능.
> 2027~2030년 빈 플레이스홀더 파일은 이미 생성 완료 → Drive에 수동 업로드 필요.

---

## GitHub Secrets

| Secret | 용도 |
|--------|------|
| `GDRIVE_OHLC_FOLDER_ID` | `[Database] Market Crawling Data` 폴더 ID |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCP Service Account JSON 키 |
| `DART_API_KEY` | DART 재무데이터 API 키 |
| `GDRIVE_LUKE_PICKS_FILE_ID` | US 유니버스 개인 관심종목 리스트 연동용 |
| `TELEGRAM_TOKEN` | 워크플로우 실패 알림용 봇 토큰 (KIS-Trading과 같은 값) |
| `TELEGRAM_CHAT_ID` | 실패 알림 수신 채팅 ID (KIS-Trading과 같은 값) |

> 텔레그램 시크릿이 없으면 알림 스텝은 조용히 건너뛴다(워크플로우는 실패한 채로 끝난다).

---

## GitHub Actions 워크플로우

| 파일 | 스케줄 | 역할 |
|------|--------|------|
| `kr-daily.yml` | 평일 UTC 07:30 (KST 16:30) | KR 일별 스냅샷 수집 + 자동 갭 보정 |
| `kr-backfill.yml` | workflow_dispatch | KR 누락 구간 과거 수집 |
| `ohlc-daily.yml` | 월~토 UTC 22:00 (KST 07:00) | US/Crypto OHLC 일별 수집 |
| `ohlc-daily.yml` | **매일 UTC 00:30 (KST 09:30)** | **Crypto 전용** — 일봉 마감(UTC 00:00) 직후 확정 캔들 수집 |
| `ohlc-backfill.yml` | workflow_dispatch | US/Crypto OHLC 과거 수집 |
| `ohlc-new-ticker-backfill.yml` | workflow_dispatch | 유니버스에 새로 추가된 종목만 골라 과거 이력 백필 (daily에도 자동 통합됨) |
| `financials-update.yml` | 자동 | US/KR 재무데이터 수집 (KR은 DART, 시총 상위 1,000) |
| `sector-meta.yml` | 매주 일요일 UTC 01:00 (KST 10:00) | US/Crypto Sector/Industry/Market 태그 수집 |

> **실패 알림**: 예약 실행 4개(`kr-daily`·`ohlc-daily`·`financials-update`·`sector-meta`)는 실패 시
> 텔레그램으로 알린다. **성공하면 아무 메시지도 오지 않는다 — 조용한 게 정상이다.**
> 수동 전용(`*-backfill`, `ohlc-new-ticker-backfill`)은 사람이 보고 있으므로 알림이 없다.
> 시크릿(`TELEGRAM_TOKEN`·`TELEGRAM_CHAT_ID`)이 없으면 알림만 건너뛰고 워크플로우는 실패로 끝난다.

---

## 실행 방법 (main.py)

```bash
# KR 당일 스냅샷
python main.py --mode kr-daily --upload-drive

# KR 과거 백필
python main.py --mode kr-backfill --start-date 2026-01-02 --end-date 2026-03-31 --upload-drive

# 드라이런 (저장 없음)
python main.py --mode kr-daily --dry-run

# 신규 종목만 과거 이력 백필
python main.py --mode ohlc-new-backfill --market us --upload-drive
```

---

## 데이터 구조 및 주요 모듈

### KR 시장 (marcap 스키마)

**스키마 컬럼:**
```
Code | Name | Close | Dept | ChangeCode | Changes | ChangesRatio |
Volume | Amount | Open | High | Low | Marcap | Stocks |
Market | MarketId | Rank | Date
```

**수집 전략:**
- `[daily]` FinanceDataReader StockListing × KOSPI + KOSDAQ + KONEX
  - 당일 스냅샷: OHLCV + Marcap + Rank + Market 포함
  - Rank = 시장 내 시총 기준 내림차순
- `[backfill]` yfinance .KS/.KQ 배치 수집 (100종목씩)
  - 과거 OHLCV만 (Marcap/Rank = NaN)
  - pykrx 전종목 엔드포인트는 GHA 환경에서 차단됨 → yfinance 우회
- `[daily 폴백]` FDR이 0건이면 기존 parquet 유니버스 + yfinance로 당일 전종목 수집
  - ⚠️ FDR `StockListing`은 KRX가 아니라 제3자 GitHub 캐시 저장소의 **날짜별 CSV**를 읽는다.
    상류가 그날치를 안 올리면 세 시장 전부 404다 (2026-09-08 실제 사고)
  - 폴백으로 받은 날은 `Marcap`/`Rank`가 비어 있다 — 시총 기반 소비처는 이 점을 감안할 것

**주요 파일:**
- `data/kr_collector.py` — FDR daily + yfinance backfill 수집 로직
- `data/kr_db.py` — Parquet 저장/로드, Drive 업로드/다운로드
- `scripts/verify_kr.py` — DB 현황 검증 + 누락 구간 감지 + 자동 보정

### US / Crypto OHLC

- `data/ohlc_collector.py` — yfinance 기반 수집 + `collect_sector_meta()` (주 1회 메타 수집)
- `data/ohlc_db.py` — 연도별 Parquet 관리 + sector_meta 저장/업로드/다운로드

**OHLC 스키마:** `Ticker | Date | Open | High | Low | Close | Volume | Amount | ChangesRatio | MarketCap | Dividends | Splits`

**Sector Meta 스키마:** `Ticker | Market | Sector | Industry | updated_at`
- `Market`: ETF / DOW30 / S&P500 / NASDAQ100 / US / Crypto (우선순위 순)
- `Sector` / `Industry`: Yahoo Finance `.info` 기반 (US만, Crypto는 빈 값)
- 로컬 경로: `data/local/ohlc_db/{market}/{market}_sector_meta.parquet`
- 수집 주기: 매주 일요일 (sector-meta.yml)

---

## 자동 갭 보정 (kr-daily)

`run_kr_daily()`는 매일 실행 시:
1. Drive에서 현재 연도 parquet 다운로드
2. `last_date` 확인 → 어제까지 누락된 영업일 계산
3. 누락 구간이 있으면 yfinance backfill 자동 실행
4. FDR StockListing으로 오늘 스냅샷 수집 (0건이면 yfinance 전량 폴백)
5. Drive에 업로드

> 4번이 폴백까지 0건이면 `sys.exit(1)`로 끝난다 — 조용히 넘어가면 하류가 하루 늦은
> 데이터로 돌기 때문이다. 실패 시 `kr-daily.yml`이 텔레그램으로 알린다.

---

## 검증 스크립트

```bash
# 로컬 검증
python scripts/verify_kr.py

# Drive에서 다운로드 후 검증
python scripts/verify_kr.py --drive

# 누락 자동 보정
python scripts/verify_kr.py --drive --fix
```

---

## 로컬 파일 경로

| 경로 | 내용 | 갱신 주기 |
|------|------|---------|
| `data/local/ohlc_db/kr/marcap-YYYY.parquet` | KR OHLC + 시총 | 매일 |
| `data/local/ohlc_db/us/us_YYYY.parquet` | US OHLC + MarketCap | 매일 |
| `data/local/ohlc_db/crypto/crypto_YYYY.parquet` | Crypto OHLC + MarketCap | 매일 |
| `data/local/ohlc_db/us/us_sector_meta.parquet` | US Sector/Industry/Market 태그 | 주 1회 (일요일) |
| `data/local/ohlc_db/crypto/crypto_sector_meta.parquet` | Crypto Market 태그 | 주 1회 (일요일) |

---

## 주요 이력

| 날짜 | 변경 내용 |
|------|---------|
| 2026-09-14 | **[PERF-KR-FIN-CALENDAR-GATE]** financials-update 가 KR 재무 신설(09-03) 뒤 105~120분이 됐는데, **첫 수집 비용이 아니라 매달 반복되는 헛호출**이었다. 09-04(두 번째) 실행 로그 실측: US 33분 · Crypto 1분 · **KR 70분**(1,000종목 × 4.2초). 수집 대상은 작년+올해 2년뿐이고 증분 스킵도 있지만, 당해 연도의 **아직 공시되지 않은 분기**(9월엔 3Q·4Q)를 매번 목표로 삼아 차분에 필요한 반기·3Q·사업보고서 **3회를 종목마다 빈 응답으로 받고** 있었다 — 11월 3Q 공시 뒤엔 2회, 3월 사업보고서 뒤엔 다음 연도 4개가 전부 미공시라 4회(≈93분), 즉 영구 반복. → `target_quarters(year, today)`: 정기보고서 **법정 제출기한**(1Q 5/15·반기 8/14·3Q 11/14·사업보고서 익년 3/31, `_REPORT_DEADLINES`)이 차분 의존 보고서 전부에서 지난 분기만 목표. 기한 당일은 제외(다음 날부터). 효과(매월 1일 실행): 6·9·12·4월에만 새 분기 1개 수집(25~47분), 나머지 8개월은 **DART 호출 0건**(늦게 공시한 회사만 재시도). 실행 로그에 `{연도}년 목표 분기 [...]` 한 줄이 찍힌다. `collect_kr_financials(today=)` 주입 인자 추가(테스트 결정성). `timeout-minutes` 180 은 그대로 둔다. 테스트 +9(`TestCalendarGate` 5 + 통합 3 + 기존 4건에 today 고정). Personal Assistant GHA 감시의 DURATION_SPIKE 도 같은 날 "연속 두 번이면 새 기준" 규칙으로 보정됨 |
| 2026-09-13 | **[FIX-CRON-OFFPEAK]** `ohlc-daily.yml` 크론을 정각·30분에서 **`23 22 * * 1-6`·`41 0 * * *`** 로 이동. GitHub Actions 는 매시 정각 부근 부하로 예약 실행을 미루는데, 실측 22:00 크론은 23:29~23:54 에(1.5~2시간), 00:30 크론은 04:47~05:35 에(4.3~5시간) 시작해 "크립토 마감 30분 뒤 반영" 이라는 00:30 크론의 목적이 무너져 있었다(Personal Assistant GHA 감시의 OHLC 23시대 실패 조사 중 발견 — 실패 자체는 09-08 `b8d76f9` 로 이미 해결). ⚠️ 대상 시장 판별이 `github.event.schedule` **문자열 비교**라 크론을 바꾸면 두 비교(`if [ "$SCHEDULE" = "41 0 * * *" ]`, 실행 스텝·알림 스텝)도 같이 바꿔야 한다 — 놓치면 크립토 크론이 `market=all` 로 돌아 US 를 하루 두 번 저장한다. 효과 확인은 다음 예약 실행의 시작 시각(22:23·00:41 근처면 성공) → **측정 결과(09-14~15) 효과 없음**: 00:41 크론 → 05:23·05:15 시작(4.5시간 지연 그대로), 22:23 크론 → 다음날 00:42 시작(2.3시간). 지연의 원인은 분 단위 혼잡이 아니라 GitHub 예약 큐 자체(이 계정의 KIS-Trading·Trading-AI-Pipeline 도 동일). 정시성이 필요하면 크론 대신 외부 트리거(Cloud Scheduler → `workflow_dispatch` API)로 바꿔야 한다 — Personal Assistant 저장소에서 검토 |
| 2026-09-16 | **[DISPATCH-EXTERNAL]** `ohlc-daily.yml` 의 `schedule:` 블록 제거 — 예약 실행을 Personal Assistant 의 tick 디스패처(Cloud Scheduler 5분 → `POST /v1/internal/gha/tick` → GitHub `workflow_dispatch`)로 이관. 설정은 그쪽 `apps/orchestrator/config/gha-schedules.json` 의 `ohlc-us`(22:23Z 월~토, market=all)·`ohlc-crypto`(00:41Z, market=crypto). 사전 검증: 11:05Z 슬롯 dry-run 디스패치가 tick 후 2초 만에 실행 생성(run 35088412567, `DRY_RUN: true`). 실행 스텝의 `$SCHEDULE` 분기는 남겨 둠(`github.event.schedule` 이 빈 값이라 `inputs.market` 사용). 되돌리기: 그쪽 항목 `enabled:false` + 여기 schedule 블록 복원 |
| 2026-09-08 | **[FEAT-FAILURE-ALERT]** 예약 실행 워크플로우 4개 전부에 `if: failure()` 텔레그램 알림 추가(`kr-daily`·`ohlc-daily`·`financials-update`·`sector-meta`). 이 저장소는 **생산자**라 실패가 조용하면 하류(KIS-Trading·Mr.Market·ML 분석·ATR 모니터)가 하루 낡은 데이터로 돌거나 멈추는데, 알림이 없어 ohlc-daily의 09-06·09-08 US 실패를 며칠 뒤에야 알았다. 메시지에 **주기별 회복 비용**을 함께 싣는다 — 월 1회(financials)는 다음 기회가 한 달 뒤, 주 1회(sector-meta)는 다음 일요일이라 수동 재실행 판단이 달라진다. ⚠️ ohlc-daily의 대상 시장은 앞 스텝 출력이 아니라 `github.event.schedule`로 **다시** 판별한다(checkout·pip install에서 죽으면 그 스텝이 안 돌아 출력이 비고 전부 같은 시장으로 오인된다 — KIS-Trading에서 겪은 함정). 시크릿이 없으면 알림 스텝만 조용히 건너뛰고 워크플로우는 실패인 채로 끝난다. 수동 전용(`*-backfill`)은 사람이 보고 있으므로 제외. Secret: `TELEGRAM_TOKEN`·`TELEGRAM_CHAT_ID` (2026-09-08 등록 완료, Mr.Market 채널) |
| 2026-09-08 | **[BUG-FDR-UPSTREAM-CACHE]** kr-daily가 그날 수집 0건으로 끝났는데 GHA는 success였다(하류 KIS EOD 분석의 알림으로만 드러남). 원인은 KRX가 아니라 **FDR의 상류**: `fdr.StockListing("KOSPI"/"KOSDAQ"/"KONEX")`는 제3자 GitHub 저장소(`FinanceData/fdr_krx_data_cache`)의 **날짜별 CSV**(`data/listing/krx/{날짜}.csv`)를 `pandas.read_csv`로 읽는다 — 그 저장소 자동 업데이트가 09-08 04:33 KST 이후 멈춰 그날 파일이 없었고 세 시장 전부 HTTP 404. 그날은 실거래일이었다(삼성전자 269,500원). PyPI 최신이 이미 핀으로 박힌 0.9.202라 버전 상향으로는 못 푼다. ⚠️ 접미사 붙은 `StockListing("KRX-DESC")`는 KIND+KRX JSON을 직접 때리는 **다른 경로**라 무관(Mr.Market step7은 영향 없음). **두 번째 결함**: 폴백이어야 할 yfinance 백필도 종목 목록을 같은 `StockListing`으로 얻어(`_build_universe`) 같은 404에 막혀 "다음날 갭 backfill이 메운다"는 자가 치유 자체가 성립하지 않았다. → ① `run_kr_daily`가 수집 0건에 `sys.exit(1)`(기존 `return`) + `kr-daily.yml`에 `if: failure()` 텔레그램 알림(시크릿 없으면 조용히 건너뜀) ② `collect_daily_fallback()` 신설 — FDR 0건이면 **기존 parquet에서 뽑은 유니버스**(`_recent_code_meta`, 최근 7일 등장 종목의 Name/Market)로 당일 전종목을 yfinance에서 수집(Marcap/Rank는 NaN) ③ `_build_universe(fallback_meta=)` / `collect_backfill(fallback_meta=)`로 갭 백필도 FDR 없이 돌게 함 ④ `collect_missing_today`와 공용 `_collect_yfinance_day(label=)`로 통합해 로그로 경로 구분. 실물 검증: 09-08 폴백으로 삼성전자 269,500·SK하이닉스 1,793,000 수집 확인. 테스트 14건(155) | 신규 Secret 필요: `TELEGRAM_TOKEN`·`TELEGRAM_CHAT_ID` |
| 2026-09-08 | **[BUG-FOREIGN-CALENDAR]** ohlc-daily(us)가 2026-09-05 23:29Z부터 `CoverageGapError: us_2026 … -99.9% 급감 (2026-06-19). 총 4일`로 죽던 문제. [BUG-TICKER-SUFFIX]로 `U-UN.TO`(TSX) 야후 조회가 되기 시작한 첫 실행에서 백필이 2020~2026 US 파일 7개에 **미국 휴장일(MLK·현충일·준틴스·독립기념일·추수감사절, 2025-01-09 카터 추모 임시휴장 등)에 U-UN.TO 혼자 있는 날 34개**를 만들어 Drive에 올렸고(부분집합 게이트는 기존 파일에 없던 날짜라 통과), 곧이어 `update_market()`의 전체 저장이 그 날짜에서 "1,071→1종목"으로 연속성 게이트에 막힘 — 파일이 이미 오염돼 **이후 매 실행 반드시 재발**하는 구조(US 데이터 자체는 09-04까지 정상, 크립토 무관). → `ohlc_db.drop_foreign_calendar_rows()`: `save_year(market="us")`가 병합 결과에서 **거래소 접미사 종목의 행 중 같은 날짜에 미국 상장 종목이 하나도 없는 행을 제거**(US 파일은 미국 달력에만 맞춘다 — 달력 라이브러리 없이 데이터로 판정하므로 비정기 휴장 포함). 백필·증분 어느 경로든 병합 결과에 적용되므로 기존 오염도 다음 저장에서 함께 치유. 접미사 정규식은 `ohlc_db.EXCHANGE_SUFFIX_RE` 한 곳으로 통합(컬렉터가 import). Drive 7개 파일은 로컬에서 새 `save_year()`로 재저장해 치유·업로드(제거된 행 34개 전부 U-UN.TO 휴장일, 종목 수·마지막 날짜 무변경). 소비 측 ML_Market Data Analysis의 최근 연도 게이트(2025~2026)도 같은 파일에 걸리던 상태라 함께 해소. 테스트 `TestForeignCalendarRows` 7건(141) |
| 2026-09-05 | **[CLEANUP-2026-09-05]** 잔여 소소 결함 묶음 — ① KR 재무 유니버스는 **보통주만**(`build_universe`가 KRX 코드 끝자리 0만 채택: 5/7/9 우선주·영문 포함 특수코드는 corp_code 없어 매 실행 실패만 남기던 34종목 제거) ② 크립토 유니버스에서 **비ASCII 심볼 제외**(`_cmc_items_to_tickers`: `币安人生` 등은 야후에 없어 매일 신규 백필 404/레이트리밋 노이즈) ③ 레거시 `data/collector.get_dart_financials`의 `finstate(fs_div=)` TypeError(0.2.2 시그니처 위반) → 단일 호출 + `fs_div` 컬럼 선택 ④ `_save_financials_year` 병합 실패 시 덮어쓰기 → 예외 중단(ohlc_db 구멍 B와 동일 함정) ⑤ `kr_financials_placeholders/2028~2030` 추적 추가(Drive엔 이미 업로드됨). 테스트 +11(134). **알려진 한계(문서화)**: KR 재무 증분 스킵은 `(Ticker, Year, Quarter)`가 한 번 저장되면 다시 조회하지 않는다 — 일부 계정만 채워진 분기(예: 순이익만 결측)나 DART 정정공시는 재수집되지 않음. 필요 시 해당 연도 parquet에서 그 행을 지우고 재실행 |
| 2026-09-05 | **[BUG-TICKER-SUFFIX]** US 유니버스 `_normalize_ticker`가 모든 `.`을 `-`로 바꿔 Luke Picks의 `U-UN.TO`(TSX)가 `U-UN-TO`로 야후에 나가 매일 404 → 행이 없어 매일 "신규 종목"으로 재백필되던 결함(Mr.Stock-Market-Crawler 2026-05-06 [BUG-TICKER]와 동일). 끝의 2~3자 알파벳 접미사(.TO .HK .AS .PA .DE .TWO …)와 1자 거래소 코드(.L .V .F .T)는 보존, 그 외 1자(`BRK.B`→`BRK-B` 클래스)와 중간 `.`은 기존대로 변환. `.KS/.KQ`는 Luke Picks 로더가 사전에 걸러 US 파일에 섞이지 않음. `tests/test_ticker_normalize.py` 20건 |
| 2026-09-05 | **[BUG-SUBSET-GATE]** ohlc-daily(crypto)가 2026-09-03 23:44Z부터 3연속 `CoverageGapError`로 죽던 문제 — 결함 2개의 조합. ① `backfill_new_tickers()`가 신규 종목 몇 개(부분집합)를 연도 파일에 병합해 저장할 때 `save_year()`의 연속성 게이트가 병합 결과 전체를 보므로, 신규 종목만 행을 가진 날짜(유니버스 마지막 저장일 이후 후행일 + 기존 구멍)가 "203→3종목 = -98.5%"로 잡혀 **신규 종목이 나타나는 날마다 반드시 실패**(첫 발생 08-31 05:53Z, 총 6회). 백필이 `update_market()`보다 먼저 돌아 유니버스를 채울 기회도 없이 죽는 순환 실패 → `save_year(subset_merge=True)`: 기존 파일에 없던 날짜는 검사에서 빼고 기존 날짜 사이의 급감은 그대로 차단(allow_gap과 다름). ② 08-31 구멍의 원인: 08-31 새벽 실패로 그날 유니버스 수집이 안 됐고, 09-01 05:26Z 조회(08-30~09-02)에 야후가 08-31 봉을 아직 안 내서 199종목에 394행(종목당 2일)만 수신 — 커서가 하루만 물려(`start=last_date`) 이후 다시 조회되지 않아 영구 구멍. → `CRYPTO_LOOKBACK_DAYS=7`(`incremental_start_date()`): 크립토는 최근 1주를 매번 다시 받아 늦게 나온 봉·일시 결손이 다음 실행에서 자연 치유(현재 08-31 구멍도 이걸로 채워짐). US는 기존(+1일) 유지. 테스트 `TestSubsetMergeGate` 4건 + `test_incremental_start.py` 4건. 같은 날 보고된 sector-meta #24(08-31 10:45Z `_exempt` NameError)는 이미 `86f2177`(10:57Z)로 수정·재실행 성공한 건 |
| 2026-09-03 | **[FEAT-KR-FINANCIALS]** KR 분기 재무 파이프라인 (섹터리더 2단계 KR §A) — DART 주요계정(OpenDartReader)·누적 차분 분기화·EPS=순이익÷상장주식수 근사·시총 상위 1,000·증분 스킵. `kr/financials/kr_financials_YYYY.parquet` (placeholder 수동 업로드 필요 — SA 제약). financials-update에 market=kr 추가 |
| 2026-08-26 | **[BUG-COVERAGE-SHRINK]** 같은 형태의 데이터 소실이 세 번째 발생한 것을 확인하고 **저장 직전 축소 가드**를 넣었다. 이력: crypto 2026 197→172종목(`[BUG-BACKFILL-REPLACE]`, 2026-08-12), crypto 2022 161→33종목(`[BUG-PURGE-TOO-WIDE]`, 2026-08-13), 그리고 이번에 발견된 **us 2024 899→727종목**(2026-08-20 백필, 무징후로 통과해 기록조차 없었음 — 소비 측 ML_Market Data Analysis 의 무결성 스윕에서 뒤늦게 검출). `[BUG-BACKFILL-REPLACE]` 대응으로 `download_year()` 선행 호출을 넣었는데도 재발한 이유는 `save_year()` 에 구멍이 둘 남아서다. **구멍 A**: 로컬 파일이 없으면 병합 블록을 건너뛰고 그냥 쓴다 — `download_year()` 가 "Drive 에 없음"과 "다운로드 실패"를 모두 `False` 로 돌려줘 호출부가 구분할 수 없었다. **구멍 B**: 병합 중 예외가 나면 `logger.warning` 만 남기고 덮어쓴다. 둘 다 조용히 통과한다. → ①`save_year()` 에 `CoverageShrinkError` + `TICKER_SHRINK_TOLERANCE_PCT=5.0` 축소 가드(저장·업로드 전에 예외), ②병합 예외를 경고가 아니라 중단으로 승격, ③`download_year_state()` 신설로 `ok`/`absent`/`failed` 3상태 구분 — `backfill_market()` 이 `failed` 면 해당 연도를 건너뛴다, ④`replace_tickers` 로 의도적으로 비우는 종목은 양쪽에서 똑같이 빼고 세어 정당한 purge(ARB 2022 등)와 사고를 구분. 실제 us_2023(899)/us_2024(727) 파일로 검증 — 가드 작동 후 원본 899종목 보존 확인. 테스트 16개 추가(`tests/test_coverage_shrink_guard.py`) |
| 2026-08-14 | **[VERIFY-CRYPTO-CLEAN]** `[BUG-PURGE-TOO-WIDE]` 수정 후 재백필한 crypto 2020~2026 데이터 최종 검증. 접합 흔적(하루 300%+ 급변) 62→61→**21/228종목(9%)**으로 개선. 남은 21개를 CMC 현재가와 전수 대조한 결과 전부 자릿수 일치(1.0x) — 오염이 아니라 **실제 시장 이벤트**로 확인됨(AAVE 2020-10-03 LEND→AAVE 100:1 리디노미네이션, OP/WLD/TAO 상장일 초기 가격발견, TIA 본딩커브 등). 유일하게 여전히 오염 상태였던 4종목(COW/GMX/SIGN/TON — 예: `TON-USD`가 실제 Toncoin이 아니라 "TON Token"이라는 별개 잡코인)은 **전부 현재 200종목 유니버스 밖**(과거 한때 Top200이었다가 탈락)이라 오늘 전략·백테스트 대상이 아님을 확인. `resolve_symbol_overrides`가 CMC Top200에 없는 종목은 대조 기준이 없어 검증을 건너뛰기 때문 — 향후 이 종목들이 Top200에 재진입하면 재검증 필요(알려진 한계로 기록, 현재는 무해) |
| 2026-08-13 | **[BUG-PURGE-TOO-WIDE]** `[BUG-WRONG-TOKEN-2]`의 `replace_tickers`를 **유니버스 전체**로 넘긴 탓에 대량 데이터 소실. 배치 다운로드가 레이트리밋으로 실패한 종목까지 기존 행이 purge되어, crypto 백필 후 **2022년 161종목→33종목 / 전체 365,172행→185,259행**으로 반토막. → `purge_targets(symbol_overrides, failed)` 신설: ①심볼이 잘못됐다고 판명된 종목(override 존재)이면서 ②이번 조회가 하드 실패하지 않은 것만 대상. override 없는 종목은 기존 행 단위 병합이 그대로 적용되어 데이터가 보존된다. 유니버스 밖 종목(강등 코인)은 애초에 purge 대상이 아니라 이번 사고에서도 무사했음(29종목 확인). 소실분은 전체 백필이 Yahoo에서 재수집하므로 재실행으로 복구됨 |
| 2026-08-13 | **[BUG-WRONG-TOKEN-2]** `[BUG-WRONG-TOKEN]` 수정 후 재백필했는데도 오염이 61/228 종목 그대로 남아 원인 3건을 추가 수정. ①**재탐색 결과를 버림**: plain 심볼이 *최근* 시세를 안 주는 종목(옛 토큰이 이미 거래정지)은 프로브에서 `_retry_crypto_missing`이 CMC id 티커로 복구하는데, 복구 데이터가 원래 티커명으로 라벨링되니 CMC 가격과 일치해 `resolve_symbol_overrides`가 "교체 불필요"로 판정 → 연도별 조회가 다시 plain으로 나가 **옛 토큰의 과거 데이터**를 그대로 저장(UNI/COMP/APT/SUI). → `_discovered_symbols`에 기록하고 `build_symbol_overrides`가 override로 승격. ②**병합이 오염 행을 보존**: `[BUG-BACKFILL-REPLACE]`로 넣은 행 단위 병합 때문에, 재수집에서 해당 날짜 데이터가 없으면(ARB 2022 — Arbitrum은 2023년 출시) 옛 오염 행이 살아남음. → `save_year(replace_tickers=)` 추가로 **이번에 다시 받기로 한 종목만 통째 교체**하고 나머지는 보존(유니버스 밖 종목 데이터 손실 없음). ③**단일 티커 배치 실패**: `_extract_ticker_df`가 `batch_size==1`이면 무조건 flat 컬럼이라 가정했으나 최신 yfinance는 MultiIndex를 줌 → 유니버스 201종목이면 마지막 배치 1종목이 통째로 실패. 검증: UNI 2022:6.795/2024:7.801/2025:7.165, COMP 2022:58.56, ARB 2022:없음(상장 전 정확) — 테스트 18개 |
| 2026-08-13 | **[BUG-WRONG-TOKEN]** plain `{SYM}-USD` 조회가 **다른 토큰 시세를 주던** 데이터 오염 수정. Yahoo는 심볼이 겹칠 때 CMC id 접미사 티커를 새 코인에 배정하고 plain 심볼은 먼저 그 심볼을 쓰던 다른 토큰에 남겨둔다 — 그래서 조회가 **에러 없이 성공**하지만 완전히 다른 자산의 데이터가 들어온다. `[BUG-CRYPTO-SUFFIX]`(누락)와 달리 조용히 틀려서 훨씬 위험하다. 실측 CMC Top 200 중 **13종목 오염**: ARB(plain $0.000629 vs 실제 Arbitrum $0.0759, 121배), JUP(653배), AERO(11,744배), O(510,875배), WLFI(1,853억배), M/TRUMP/DATA/NEX/CHEEMS/MELANIA/EDGE/USDG. → `_cmc_listing()`이 id와 함께 **가격**도 보관하고, `resolve_symbol_overrides()`가 Yahoo 관측가와 CMC 가격이 5배 이상 어긋나면 `{SYM}{CMCid}-USD`로 교체한다. `build_symbol_overrides()`가 실행 시작 시 최근 7일 시세로 1회 판정해 모든 연도 수집에 재사용한다(과거 연도 가격은 오늘 CMC 가격과 당연히 달라 연도별 판정은 불가). `fetch_ohlc_range(symbol_overrides=)`는 **조회만 교체 심볼로 하고 저장 라벨은 원래 티커를 유지**한다. 검증: 7종목 교체 후 ARB $0.0758 / JUP $0.1691 / AERO $0.4112로 CMC와 일치, 정상 종목(LINK/SOL/SHIB)은 무변경. 테스트 7개 추가(`tests/test_crypto_symbol_resolution.py`) |
| 2026-08-12 | **[BUG-BACKFILL-REPLACE]** `backfill_market()`이 Drive 연도 파일을 병합이 아니라 **교체**하던 데이터 소실 버그 수정. `save_year()`는 "로컬 파일이 있으면" 병합하는데 GHA 러너는 매번 빈 상태로 시작하므로, 기존 연도 파일을 먼저 받지 않으면 이번에 수집한 종목만 담긴 파일이 Drive를 덮어쓴다. 실제로 crypto 2026 복구 백필에서 현재 유니버스(CMC Top 200)에 없는 종목들의 2026년 데이터가 소실됨(**197종목 → 172종목**). `update_market()`/`backfill_new_tickers()`는 이미 다운로드를 선행하는데 `backfill_market()`만 빠져 있었음 → `download_year()` 선행 추가 |
| 2026-08-12 | **[FIX-CRYPTO-RESOLVE]** 크립토 티커 해석을 `yf.Search` 단독 → **CMC id 기반 결정적 매핑 우선**으로 전환. Yahoo의 숫자 접미사가 CoinMarketCap id와 일치함을 확인(APT21794/GRT6719/COMP5692/POL28321/PI35697/HYPE32196 — CMC id와 6/6 일치)했고, 유니버스를 CMC에서 받아오므로 검색 없이 `{SYMBOL}{CMC_id}-USD`를 직접 구성할 수 있다. `yf.Search`는 주식 티커와 겹치는 심볼에서 신뢰 불가 — APT는 Alpha Pro Tech/Aptiv에 밀려 `max_results=30`에도 CRYPTOCURRENCY 결과가 0건이고, POL 검색은 Polkadot(`DOT-USD`)을 반환하는 오답을 낸다. 이제 CMC id 후보를 먼저 시도하고 Search는 CMC 목록에 없는 종목의 폴백으로만 사용(`max_results` 5→20). `_retry_crypto_with_search` → `_retry_crypto_missing` 개명. 검증: APT/GRT/COMP/POL/PI/HYPE/PENGU **7/7 복구** |
| 2026-08-12 | **[FEAT-CRYPTO-CRON]** `ohlc-daily.yml`에 UTC 00:30 크립토 전용 크론 추가. `[BUG-PARTIAL-CANDLE]` 수정 이후에도 parquet의 마지막 행은 항상 진행 중인 오늘 캔들이라(`end = today + 1`로 조회), 22:00 UTC 실행만으로는 D일 캔들이 확정값으로 반영되는 시점이 D+1일 22:00 UTC — 마감 후 22시간 지연이었음. 00:30 UTC 크론으로 지연을 약 30분으로 단축. `github.event.schedule`로 크론을 구분해 `--market crypto`로 고정하며, workflow_dispatch는 `github.event.schedule`이 빈 값이라 기존처럼 `inputs.market`이 그대로 쓰인다. 크립토는 주말에도 거래되므로 요일 제한 없이 매일 실행. 부수: `inputs`를 env로 옮기고 `${{ inputs.dry_run \|\| 'false' }} && ...`를 `[ "$DRY_RUN" = "true" ] && ...`로 교체 |
| 2026-08-12 | **[BUG-PARTIAL-CANDLE]** 크립토 일봉이 전부 미완성 캔들로 저장되던 문제 수정. 크립토 일봉 마감은 UTC 00:00인데 `ohlc-daily.yml`은 UTC 22:00에 돌아 수집 시점에 당일 캔들이 2시간 남은 진행 중 상태였고(Close=22시 시점 가격, High/Low 마지막 2시간 누락, Volume ~92%), `update_market()`이 `last_updated`를 그 날짜로 올려버려 다음 실행은 그 다음날부터 조회 → 미완성 값이 영구 확정되었음. `market == "crypto"`일 때 수집 커서를 하루 물려(`start_date = last_date`) 직전 수집일을 재조회, `save_year()`의 `(Ticker, Date)` `keep="last"` 중복제거로 확정값이 덮어씀. US는 장마감(UTC 20~21시)이 크롤 시각보다 앞서 캔들이 이미 확정이라 기존 동작 유지. 실증: `yf.download('BTC-USD')`는 UTC 08:41 시점에도 당일 캔들을 반환하나 `NVDA`는 반환하지 않음 |
| 2026-08-12 | **[BUG-CRYPTO-SUFFIX]** 숫자 접미사 크립토 티커가 조용히 누락되던 문제 수정. Yahoo는 신규/동명 코인에 숫자 접미사를 붙이는데(`HYPE-USD` 미존재, `HYPE32196-USD`로만 조회) `yf.download()`는 exact match만 하고 빈 응답은 예외가 아니라 `failed` 목록에도 안 남아 무징후로 사라졌음. `_search_crypto_yahoo_ticker()`(`yf.Search` + `^{SYMBOL}\d*-USD$` 정규식, 프로세스 캐시) + `_retry_crypto_with_search()` 추가, `fetch_ohlc_range()`가 배치 후 빈 응답 `-USD` 종목만 골라 재탐색·재수집. **저장되는 Ticker 값은 원래 이름(`HYPE-USD`)을 유지** — parquet의 Ticker가 유니버스와 달라지면 소비 측(Trading-AI-Pipeline `CustomChartFetcher` 등)이 종목을 못 찾음. Mr.Stock-Market-Crawler `step4_ohlc_atr_core.py`의 동일 대응을 이식. 검증: `HYPE-USD` 42행 복구 확인 |
| 2026-07-12 | **[FEAT-VERIFY-OHLC]** 백필 완결성 진단 스크립트 추가: `[FEAT-NEW-TICKER-BACKFILL]` 도입 이전(2026-07-10 이전)에 유니버스에 추가된 종목은 자동 백필 없이 daily 증분만으로 데이터가 쌓여 "known"인데 2020년 이력이 없는 경우가 다수 발견됨(DNLI/EDIT/KRYS 등 23종목). `ohlc_db.first_seen_dates()`(티커별 최초 수집일) + `scripts/verify_ohlc.py`(`--market`/`--after`/`--drive`로 리포트, `--fix`로 `backfill_pending.json`에 병합) 추가 — `verify_kr.py`와 동일하게 수동 실행 진단 도구, 실제 fetch는 기존 `backfill_new_tickers()`에 위임 |
| 2026-07-11 | **[FEAT-BACKFILL-PENDING]** 하드 실패 종목 재시도 추적 추가: `list_known_tickers()`가 "어디든 행 하나만 있으면 완료"로 오판해 TSM/ALAB/COHR/NBIS 등 일부 연도만 실패한 종목이 영구히 재시도 대상에서 빠지던 문제 해결. `ohlc_db.load/save/download/upload_pending()` (backfill_pending.json, db_status.json과 동일 패턴) + `fetch_ohlc_range()`가 하드 실패 티커 목록을 함께 반환 + `backfill_new_tickers()`가 신규 종목뿐 아니라 pending에 남은 종목도 재시도하고 결과에 따라 pending을 갱신 |
| 2026-07-10 | **[FEAT-NEW-TICKER-BACKFILL]** US/Crypto 신규 종목 자동 백필 추가: `ohlc_db.list_known_tickers()` + `ohlc_collector.backfill_new_tickers()` 신규 함수, `run_ohlc_update()`에 자동 통합 + `ohlc-new-backfill` CLI 모드 + `ohlc-new-ticker-backfill.yml` 신규 워크플로우 |
| 2026-05-25 | **[FEAT-KR-SUPPLEMENT]** kr-daily 누락 종목 yfinance 보완 수집 추가: FDR StockListing에서 빠진 종목 중 최근 7일 이내 거래 이력 있는 종목을 yfinance로 재시도. 상장폐지 확정 종목(7일 초과 미거래)은 자동 스킵. `kr_collector.collect_missing_today()` 신규 함수 + `main.run_kr_daily()` 3b 단계 추가 |
| 2026-03-25 | 레포명 변경: Quant-Korea-Data → Daily-Market-Data-Crawling |
| 2026-04-03 | KR 시장 수집 추가 (kr_collector.py, kr_db.py, kr-daily.yml, kr-backfill.yml) |
| 2026-04-03 | Drive 폴더 marcap/ → kr/ 로 표준화 |
| 2026-04-03 | verify_kr.py 추가 (누락 구간 감지 + 자동 보정) |
| 2026-04-03 | 자동 갭 보정 로직 main.py run_kr_daily()에 추가 |
| 2026-04-04 | sector-meta 추가: US/Crypto Sector/Industry/Market 태그 주 1회 수집 (sector-meta.yml) |
| 2026-04-04 | ohlc_collector.collect_sector_meta(), ohlc_db.save/upload/download_sector_meta() 추가 |
