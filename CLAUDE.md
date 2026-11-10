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
| `kr-daily.yml` | 외부 디스패처 → workflow_dispatch, 평일 UTC 14:00 (KST 23:00) | KR 일별 스냅샷 수집 + 자동 갭 보정 (2026-10-21 cron 16:30 KST 에서 이관 — FDR 당일 CSV 최종 갱신 ~22:30 KST 뒤, Mr.Market 크롤러 23:30 KST 앞) |
| `kr-backfill.yml` | workflow_dispatch | KR 누락 구간 과거 수집 |
| `ohlc-daily.yml` | 외부 디스패처 → workflow_dispatch, 월~토 UTC 22:23 (KST 다음 날 07:23) | US/Crypto OHLC 일별 수집 |
| `ohlc-daily.yml` | 외부 디스패처 → workflow_dispatch, 매일 UTC 00:41 (KST 09:41) | Crypto 전용 — 전일 확정 캔들 수집 |
| `ohlc-backfill.yml` | workflow_dispatch | US/Crypto OHLC 과거 수집 |
| `ohlc-new-ticker-backfill.yml` | workflow_dispatch | 유니버스에 새로 추가된 종목만 골라 과거 이력 백필 (daily에도 자동 통합됨) |
| `financials-update.yml` | 외부 디스패처(매월 1일 01:00 UTC) | US/KR 재무데이터 수집 (KR은 DART, 시총 상위 1,000) |
| `sector-meta.yml` | 매주 일요일 UTC 01:00 (KST 10:00) | US/Crypto Sector/Industry/Market 태그 수집 |

7개 워크플로는 Drive 연도 파일과 `_meta`를 공유하므로 같은 concurrency 그룹
`daily-market-drive-writer`에서 하나씩 실행한다. `cancel-in-progress: false`와
`queue: max`를 사용해 실행 중 작업을 유지하고 최대 100개까지 대기시킨다.
긴 백필·재무 작업은 후속 일간 수집을 지연시킬 수 있으며, 대기 순서는 디스패치 순서와
항상 같지는 않다. 이 제한은 같은 저장소의 같은 그룹에 참여하는 Actions 실행에만 적용된다.
로컬 CLI·유지보수 스크립트·다른 저장소의 Drive 쓰기는 별도로 실행 시간을 조율해야 한다.
사양: [GitHub workflow concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

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

# KR 공식값(marcap) 정정 — 계획만(운영 쓰기와 같은 검증 경로, 파일·Drive 무변경)
python main.py --mode kr-settle --plan --start-date 2026-01-01 --report plan.json
# 2026 일괄 정정(별도 승인 필요) — 검증된 휴장일 복사 행 삭제를 같은 원자 쓰기로
python main.py --mode kr-settle --start-date 2026-01-01 --drop-closed-dates 2026-05-01,... --upload-drive

# 신규 종목만 과거 이력 백필
python main.py --mode ohlc-new-backfill --market us --upload-drive
```

**로컬 테스트**: 전용 `.venv`(gitignore)에 고정 의존성을 설치해 돌린다. 전역 Python 의 FDR(0.9.110)로 돌리면 KR 원천 테스트 10건이 `fdr_version_unverified` 로 실패한다.
`requirements.txt` 는 ASCII 만 쓴다 — 이 PC 의 pip 이 cp949 로 읽어 한글 주석이 있으면 설치가 깨진다.
```bash
python -m venv .venv && .venv/Scripts/python.exe -m pip install -r requirements.txt pytest
.venv/Scripts/python.exe -m pytest tests -q   # 경로 인자 필수 — 폴더명의 [] 를 pytest 가 파라미터로 오인
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
- `[daily]` 고정 FDR 공급 경로의 날짜 선택 1회 → 같은 날짜 CSV 1회 → 세 시장 분리
  - 원천 세션 날짜의 스냅샷: OHLCV + Marcap + Rank + Market 포함 (요청 당일로 재표기하지 않음)
  - Rank = 시장 내 시총 기준 내림차순
- `[backfill]` yfinance .KS/.KQ 배치 수집 (100종목씩)
  - 과거 OHLCV만 (Marcap/Rank = NaN)
  - pykrx 전종목 엔드포인트는 GHA 환경에서 차단됨 → yfinance 우회
- `[daily 폴백·backfill]` Yahoo adjusted 후보와 FDR 기준의 일치를 입증하지 못하면 저장·게시를 보류한다.
  - 원천 날짜/응답 실패도 명시 오류다. FDR 장애를 새 날짜·정상 빈 결과로 변환하지 않는다.
  - 과거 가격의 raw/adjusted 정책·일괄 재작성은 이번 결함 수정에 포함하지 않는다. [현재 경계](docs/2026-09-22-data-preservation.md)를 따른다.

**주요 파일:**
- `data/kr_collector.py` — FDR daily + yfinance backfill 수집 로직
- `data/kr_db.py` — Parquet 저장/로드, Drive 업로드/다운로드
- `scripts/verify_kr.py` — DB 현황 검증 + 누락 구간 감지 + 자동 보정

### US / Crypto OHLC

- `data/ohlc_collector.py` — yfinance 기반 수집 + `collect_sector_meta()` (주 1회 메타 수집)
- `data/ohlc_db.py` — 연도별 Parquet 관리 + sector_meta 저장/업로드/다운로드

**OHLC 스키마:** `Ticker | Date | Open | High | Low | Close | Volume | Amount | ChangesRatio | MarketCap | Dividends | Splits`

> 가격은 yfinance `auto_adjust=True` 조정가다. 배당·분할이 관측된 종목은 그날 전체 이력을 다시 받아 그 종목 행만 교체한다([FEAT-EVENT-REBASE], `backfill_pending.json` 의 `"{market}:rebase"` 키가 재시도 목록). `Dividends`/`Splits` 열은 상수(0/1)이며 실제 액션은 수집 attrs 로만 판정한다.

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
3. 누락 세션은 원천 캐시의 그날 최종본(`collect_session`)으로 채워 바로 업로드하고, 못 채운 날부터 yfinance backfill 실행
   — 메울 수 없는 날이 생기면 kr-daily 가 매일 실패하고 공식 정정도 돌지 않는다. 그때는 수동 `kr-backfill`/`kr-settle` 이 복구 절차다
4. FDR StockListing으로 오늘 스냅샷 수집 — 0건이거나 **원천 typed 실패**(404·빈 응답·날짜 불일치, 2026-09-26)면 저장본과 종목별 대조를 통과한 행만 받는 **검증된 yfinance 전량 폴백**
5. Drive에 업로드

> 4번이 폴백까지 0건이면 `sys.exit(1)`(원천 typed 실패였으면 같은 오류 코드로 raise)로 끝난다 — 조용히 넘어가면 하류가 하루 늦은
> 데이터로 돌기 때문이다. 실패 시 `kr-daily.yml`이 텔레그램으로 알린다.
>
> **종목 범위 게이트(2026-10-10)**: 4번 뒤 저장 직전에 오늘 종목 수를 인접 거래일 5개의 종목 수 중앙값과 비교해
> 90% 미만이면 저장·게시 없이 `kr_coverage_insufficient` 로 실패한다(→ 텔레그램). 저장 계층 `kr_db.save_year`
> 도 일일·백필·폴백 모든 경로에서 **병합 후 실제로 저장될** 데이터의 이번 쓰기 대상 날짜를 같은 기준으로 판정해
> 미달이면 `kr_coverage_gap`(기존 파일 무변경). 공개 로그에 `kr_date`·`kr_count`·`kr_baseline`·`kr_ratio_pct`·
> `kr_source` 가 남는다. 의도한 예외는 `save_year(..., allow_gap=True)`.

---

## 공식값 정정 (kr-daily · kr-backfill · kr-settle)

당일 fdr 스냅숏은 **잠정값**이다(거래량은 공식보다 항상 작고, 일부 가격도 다르다). 공식 원천은
`FinanceData/marcap` 의 `data/marcap-{year}.parquet`(같은 18열 스키마, 약 2거래일 늦게 게시, 원주가 = yfinance 원가격).
`data/kr_official.py` 가 공식 파일이 **실제로 제공한 날짜만** 정정한다:

- **측정값**(Open·High·Low·Close·Volume·Amount·Changes·ChangesRatio·Marcap·Stocks): 공식값. 공식이 NaN 이면 저장값 유지
- **채움만**(Name·Dept·MarketId·Market): 저장값 우선, 비었을 때만 공식(Market 은 MarketId→KOSPI/KOSDAQ/KONEX — marcap 의 `KOSDAQ GLOBAL` 은 쓰지 않음)
- **ChangeCode**: 저장 코드의 방향(1·4 상승 / 2·5 하락 / 3·0 보합)이 공식 Changes 부호와 맞으면 유지, 다르면 1·2·3 로
- **Rank**: 시장 내 순위로 다시 매긴다(marcap 은 전체 시장 순위라 정의가 다름)
- 공식에만 있는 행은 추가(누락 날짜·종목 복원), 우리에만 있는 행은 그대로
- 정정 하한 `KR_OFFICIAL_SETTLE_FROM`(기본 `2026-01-01`, 사용자 결정 2026-10-10) 이전 날짜와 `KR_OFFICIAL_MIN_YEAR`(2026) 이전 연도는 건드리지 않는다 — 닫힌 2025 이하는 바이트 불변.
  하한은 "어디까지 대조하나" 다. 매 실행 공식 연도 파일 1개(약 20MB)를 받아 하한~공식 확정 경계 전체를 메모리에서 대조하므로 marcap 이 과거 날짜를 고쳐 다시 게시해도 잡힌다. 실제 쓰기는 공식값과 다른 행뿐이다(일괄 정정 뒤 매일은 새로 게시된 1~2거래일분)
- **자동 정정 모드 `KR_OFFICIAL_SETTLE_MODE`(기본 `apply` — 2026-10-13 일괄 정정·검증 뒤 전환, 그 전엔 `plan`)**: kr-daily·kr-backfill 은 plan 이면 받기·검증·계획·종목 범위 게이트까지 하고 **쓰지 않는다**(`kr_official_planned` 로그에 수치만, 공식 원천 장애도 알림 없이 로그만). 2026 일괄 정정이 승인·실행된 뒤 `apply` 로 바꾼다 — 하한 2026-01-01 인 채 apply 면 첫 kr-daily 가 30만 행을 바꾼다. 알 수 없는 값은 plan
- **`--plan`**: `settle_year(apply=False)` 가 운영 쓰기와 같은 함수·같은 방어(대량 변경 거부)·같은 게이트(`kr_db.preview_year` → `_prepare_year`)를 거친 뒤 쓰지 않는다. `--report` JSON 에 날짜·종목 집합 대조(`reconcile`: 누락·초과 날짜/행, 중복 키, 정렬, OHLCV 불일치, 최종 행 수)
- **휴장일 복사 행 삭제 `--drop-closed-dates`**(수동 kr-settle 만): 날짜마다 ① KRX 달력(`krx_calendar`, 수집기와 같은 출처)이 휴장 ② 공식 파일이 그 날짜 이후까지 게시됐는데 그날이 없음(게시 지연과 구분) ③ 직전 저장 거래일과 종가 99% 이상 일치 — 셋 다 만족해야 하고 하나라도 어긋나면 `kr_closed_session_unverified` 로 아무것도 안 쓴다. 정정과 한 번의 원자 쓰기로 처리하고 정정 이력 메타데이터에 `rows_dropped`·`dropped_dates` 를 남긴다. marcap 에 없다는 이유만으로는 지우지 않는다
- **상장 전 무거래 행 삭제 `--drop-prelisting-keys CODE:YYYY-MM-DD,...`**(수동 kr-settle 만, 명시한 키만): 키마다 저장본 1행·거래량 0, 공식 파일이 그 날짜를 게시했는데 그 종목은 없음, 그 종목의 공식 첫 등장일이 그 날짜 이후 — 셋 다 만족해야 하고 아니면 `kr_prelisting_row_unverified`. 상장일 근거(KIND 상장법인 목록)는 실행 보고서에 둔다. "공식에 없는 행은 전부 삭제" 같은 일반 규칙은 두지 않는다(2026-10-10 사용자 지시)
- **멱등**: 같은 공식 파일로 다시 돌면 추가 0·교체 0. 정정된 날짜는 이후 잠정 수집이 덮어쓰지 않는다(kr-daily 는 오늘만 쓰고, 정정은 그 뒤에 돈다)
- 상태·이력은 parquet 스키마 메타데이터(`kr_official_through`·`kr_official_source`·`kr_settlement_log` 최근 30건) — SA 가 Drive 새 파일을 못 만들어 별도 메타 파일을 두지 않는다

kr-daily 는 오늘 저장 뒤 현재 연도(1월엔 전년도도)를 정정하고, kr-backfill 은 갱신한 연도를 업로드 전에 정정한다.
공식 원천 실패(`kr_official_unavailable`·`kr_official_invalid`)는 잠정 데이터를 **보존한 채 업로드까지 끝낸 뒤** 같은 코드로
실패해 텔레그램이 울린다. 공식 경계 뒤 잠정 거래일이 10일을 넘으면 `kr_official_stale`. 공식 파일이 아직 없는 연도(404)는
`kr_official_absent` 경고만. 외부 원천 이상 방어: 압축 해제 전 행 수 200만 초과·거래 있는 행의 고가<저가는 `kr_official_invalid`, 저장 종가와 5% 넘게 다른 행이 한 날짜의 20% 를 넘으면 정정 전체를 거부(`kr_official_invalid`, 2026 일괄 실측 최대 3.4%). 공개 로그 지표: `kr_official_through`·`kr_dates`·`kr_rows_added`·`kr_rows_replaced`·`kr_provisional_sessions`.

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

## 불변식·실행 경계

### 2026-09-22 — 연도 기준본 보존 및 업로드 완료 확인
- OHLC 전체/신규 백필·여러 연도에 걸친 증분, KR 백필은 저장 대상 연도의 기준본을 먼저 확인한다. Drive의 `absent`와 `failed`를 구분하고 실패·손상은 저장/업로드를 중단한다.
- 기존 연도 파일은 strict 읽기 후 병합한다. 다운로드와 Parquet 저장은 임시 파일을 검증한 뒤 교체하여 중간 실패로 정상 파일을 훼손하지 않는다.
- KR OHLC 백필은 기존 `(Code, Date)` 이력을 유지하며, 다시 받을 수 없는 `Marcap`·`Stocks`·`Rank`와 기존 실측 `Amount`를 결측값이나 추정치로 지우지 않는다.
- OHLC 연도/상태/재시도 목록 업로드 실패는 호출자에게 전달한다. 확인되지 않은 업로드를 완료 커서나 재시도 목록 제거로 처리하지 않는다.
- 7개 Actions workflow의 Drive 쓰기를 공통 concurrency 그룹에서 직렬화한다. `sector-meta`의 boolean dry-run 입력도 올바르게 전달한다. 기존 예약/외부 디스패치 방식은 유지한다.
- 격리된 코드 사본에서 전체 테스트 **266 passed / 기존 3 skipped**를 확인했다(이전 186 passed에서 회귀 검사 80개 추가). 합성 parquet·가짜 Drive로 검사했으며 네트워크·운영 파일 접근 시도는 0건이다. 기존 CMC 테스트의 localhost 연결도 합성 오류로 대체했다. Workflow YAML 7개와 가짜 Python을 사용한 sector-meta 인수 전달 4개 사례도 통과했다.
- 이 변경은 실제 Drive 데이터의 복구나 전체 품질 검증이 아니다. 조정주가 정책, 부분 종목 수집과 날짜 완결성, Crypto `MarketCap` 결측 덮어쓰기, 재무/sector-meta의 나머지 실패 경로는 후속 검토 범위다. 수동 `resave_ohlc.py --upload`의 업로드 결과 처리와 `verify_ohlc.py --fix`의 원격 기준본/동시 실행도 별도 개선이 필요하다.

### 2026-09-20 — 증분 경로의 Drive 동기화 불변식 (D-01 · D-02)
2026-09-19 심층 검토에서 **매일 도는 증분 경로**에 백필 경로와 같은 구멍이 남아 있음이 재현됐다.
- **D-01** `update_market()`·`run_kr_daily()` 가 `download_year()` 의 False("없음" 과 "실패" 를 구분 못 함)를 무시하고 진행했다.
  다운로드는 실패하고 업로드는 성공하는 부분 장애 한 번이면 **당해 연도 파일이 증분 며칠 치로 교체**돼 올라간다.
  KR 은 1/1 부터 yfinance 로 재백필되지만 `Marcap`·`Rank`·`Stocks`·`Amount` 과거값은 다시 받을 수 없다.
  → `ohlc_db.download_year_state()` 3상태 가드를 증분 경로에도 적용, `kr_db.download_year_state()` 신설. `failed` 면 저장·업로드 없이 비정상 종료.
- **D-02** `upload_years()` 가 예외를 로그로 삼키고 아무것도 돌려주지 않아, 업로드가 실패해도 커서(`db_status.json`)가 전진했다.
  US 는 `last_date+1` 부터만 조회하므로 그날이 **영구 구멍**이 되고 워크플로는 초록, 알림 없음.
  → `upload_years()` 가 실패한 파일 이름 목록을 반환. 실패가 있으면 상태를 갱신하지 않고 `DriveSyncError`/`sys.exit(1)` → 기존 실패 알림이 울린다.
- 공개 저장소라 업로드·다운로드 실패 로그에는 예외 문자열(Drive ID 가 실릴 수 있음) 대신 예외 종류만 남긴다.
- 테스트 176 → 186 (`tests/test_incremental_drive_sync.py`).
- **남은 것**(같은 검토 — 2026-09-26 현재 코드 기준 재판정, 09-21 코덱스 번호 DM-xx 로 통일): 09-22 `b90caae`·`6121222` 와 09-24 관문 수정으로 **해결** — 백필 `upload_years` 실패 전파(DM-03), 크립토 재조회의 `MarketCap` 보존(DM-06 `_preserve_missing_marketcap`), 손상 파일 strict 읽기·원자 교체(DM-07), 공개 로그 `PublicFormatter`(DM-08), 워크플로 공통 `concurrency`·dry-run 조기 반환(DM-09), 문서·테스트 경로 오탐(DM-13).
  **DM-04 → 2026-09-26 [FEAT-EVENT-REBASE] 로 해소**(이벤트 종목만 전체 이력 재수집·교체, 스키마 불변). **DM-05 → [FIX-US-LOOKBACK] 로 해소**(US 도 1주 재조회, 종목별 실패 목록 대신 창으로 자가 치유). **DM-10 → [FIX-SECTOR-EMPTY-FIELD] 로 해소**. **DM-11 → 2026-11-10 해소**: 11-02 원천 캐시 자가 복구는 지난 갭만 채우고 오늘 폴백분(Marcap·Stocks 결측)은 남기므로 재현됐다 — 기준본의 마지막 저장일이 폴백일이면 스냅숏을 읽기도 전에 `universe_unverified`. 이제 그날 또는 그 뒤의 같은 원천 스냅숏이면 그 스냅숏으로 고르고, 더 오래된 스냅숏·원천 typed 실패·폴백일 뒤 휴장일 빈 스냅숏은 종전대로 차단(`tests/test_financials_preservation.py` `fallback_tail`). **DM-12 → 2026-11-10 종결**: 하류 정적 감사 — KIS `kr_eod_data` 는 Date·OHLCV 만, Mr.Market step3a 는 OHLCV+Marcap(`groupby.last()` 로 마지막 비결측), step4·step6 은 OHLCV 만, ML 은 Ticker·Date·OHLCV 만 골라 쓴다. Stocks·Rank 소비처 0, nullable 형식이 OHLCV·Marcap 으로 번지지 않음을 계약 테스트로 고정(`test_downstream_columns_stay_numpy_when_fallback_row_is_saved`). Stocks·Rank 를 하류가 쓰기 시작하면 pd.NA 처리를 다시 확인할 것.
  참고: "`financials-update` 가 ratios 이력을 매 실행 지운다" 는 2026-09-01 `15bfd12` 에서 **이미 수정**됐다.

날짜별 변경표는 `docs/HISTORY.md`.


### 2026-09-22 잔여 보존·실행 경계

- 재무 strict/atomic 저장·기준본 staging·게시 실패 전파와 KR 시총 유니버스 원천 날짜 검증을 적용했다.
- 증분 실패/빈 응답/누락·가격 기준 변경은 커서 전진 전 보류한다. 시총 결측은 이전 값을 보존하며 현재 시총을 과거 봉에 복사하지 않는다.
- sector는 기준본을 확인하고 실패한 관측 필드와 시각을 보존한다. 원본 없는 실패는 보류하고 실패 업로드를 완료로 보고하지 않는다.
- Drive는 read-only 조회/완전 pagination/중복·경로 검증/staging을 사용한다. 기존 슬롯 update와 유효한 0행 placeholder 계약을 유지한다.
- 모든 수집 mode의 dry-run은 수집·다운로드·데이터 저장·게시 0회다. main과 수동 writer는 같은 LOCAL_DATA_DIR의 OS 잠금으로 배타 실행한다. 다른 머신과의 분산 잠금은 아니다.
- 공개 수집 로그는 소스·줄·고정 코드로 제한하고 원문 예외·인수·Python progress 출력을 게시하지 않는다. 자세한 보류 원인·한계·운영 확인은 [수집·게시 보존 계약](docs/2026-09-22-data-preservation.md)을 본다.
