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
- GCP 프로젝트: `seraphic-jet-489008-b4`
- Service Account: `stock-crawler-bot@seraphic-jet-489008-b4.iam.gserviceaccount.com`

### 서브폴더 구조
```
[Database] Market Crawling Data/   ← GDRIVE_OHLC_FOLDER_ID
    ├─ kr/          ← KR OHLC + 시총 (marcap 스키마, 연도별 parquet)
    │   ├─ marcap-2025.parquet
    │   ├─ marcap-2026.parquet
    │   └─ marcap-2027~2030.parquet  (빈 플레이스홀더, 수동 업로드)
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
| `financials-update.yml` | 자동 | US 재무데이터 수집 |
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
