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

---

## GitHub Actions 워크플로우

| 파일 | 스케줄 | 역할 |
|------|--------|------|
| `kr-daily.yml` | 평일 UTC 07:30 (KST 16:30) | KR 일별 스냅샷 수집 + 자동 갭 보정 |
| `kr-backfill.yml` | workflow_dispatch | KR 누락 구간 과거 수집 |
| `ohlc-daily.yml` | 월~토 UTC 22:00 (KST 07:00) | US/Crypto OHLC 일별 수집 |
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
| 2026-07-10 | **[FEAT-NEW-TICKER-BACKFILL]** US/Crypto 신규 종목 자동 백필 추가: `ohlc_db.list_known_tickers()` + `ohlc_collector.backfill_new_tickers()` 신규 함수, `run_ohlc_update()`에 자동 통합 + `ohlc-new-backfill` CLI 모드 + `ohlc-new-ticker-backfill.yml` 신규 워크플로우 |
| 2026-05-25 | **[FEAT-KR-SUPPLEMENT]** kr-daily 누락 종목 yfinance 보완 수집 추가: FDR StockListing에서 빠진 종목 중 최근 7일 이내 거래 이력 있는 종목을 yfinance로 재시도. 상장폐지 확정 종목(7일 초과 미거래)은 자동 스킵. `kr_collector.collect_missing_today()` 신규 함수 + `main.run_kr_daily()` 3b 단계 추가 |
| 2026-03-25 | 레포명 변경: Quant-Korea-Data → Daily-Market-Data-Crawling |
| 2026-04-03 | KR 시장 수집 추가 (kr_collector.py, kr_db.py, kr-daily.yml, kr-backfill.yml) |
| 2026-04-03 | Drive 폴더 marcap/ → kr/ 로 표준화 |
| 2026-04-03 | verify_kr.py 추가 (누락 구간 감지 + 자동 보정) |
| 2026-04-03 | 자동 갭 보정 로직 main.py run_kr_daily()에 추가 |
| 2026-04-04 | sector-meta 추가: US/Crypto Sector/Industry/Market 태그 주 1회 수집 (sector-meta.yml) |
| 2026-04-04 | ohlc_collector.collect_sector_meta(), ohlc_db.save/upload/download_sector_meta() 추가 |
