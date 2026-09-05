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
4. FDR StockListing으로 오늘 스냅샷 수집
5. Drive에 업로드

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
