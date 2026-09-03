# 섹터 리더 후보 2단계 — KR 확장 설계 (2026-09-03)

**범위**: 2단계 조건(2026-09-01 스펙)의 KR 구현. US분(A·B·C)은 2026-09-02 전체 배포 완료 — 이 스펙은 그 위에 KR 값을 채우는 확장이다. 기존 시트 컬럼 8종을 **그대로 재사용**하며 신규 컬럼은 없다.
**아키텍처 결정(사용자 승인 2026-09-03)**: ① KR 분기 재무는 **Daily-Market에 KR 재무 파이프라인 신설**(US financials-update와 완전 대칭) ② 외국인소진율은 **주도섹터 KR 유니버스만 step7에서 주간 조회**(전 종목 수집은 차단 위험으로 제외) ③ 어닝서프라이즈 KR은 제외(무료 컨센서스 소스 부재).

## 0. 전제 (2026-09-03 확인 완료)

- `DART_API_KEY` Secret — Daily-Market 저장소에 재등록됨(2026-09-03 실측)
- KR OHLCV·시총·상장주식수: `kr/marcap-YYYY.parquet` (스키마에 `Stocks`·`Rank` 포함) — ②③·VCP 계산에 신규 데이터 불요
- 외국인 지분 데이터는 기존 어디에도 없음(marcap·FDR·Mr.Market 크롤러 전무), pykrx는 GHA에서 KRX 차단 → 실질 소스는 Naver뿐
- Pactolus 소비(§C)는 컬럼 단위 generic이라 KR 값이 채워지면 자동 동작

## A. Daily-Market — KR 재무 파이프라인 신설

### A-1. 수집기 (`data/kr_financials_collector.py` 신규)

- **corp_code 매핑**: DART `corpCode.xml` zip 다운로드 → 종목코드(6자리)↔corp_code 매핑. **매 실행 1회 다운로드**(단일 요청 ≈2MB — GHA 러너가 에페머럴이라 로컬 캐시 무의미, Drive 캐시는 불필요한 복잡도)
- **대상 유니버스**: marcap 최신 스냅샷 **시총(Marcap) 내림차순 상위 1,000종목** (KOSPI+KOSDAQ 통합 — `Rank` 컬럼이 시장별 랭크이므로 Marcap 정렬로 직접 산출). 전 종목(~2,700)은 DART 일 한도(20,000건) 부담. US 998종목과 규모 대칭
- **재무 조회**: DART **주요계정 API(`fnlttSinglAcnt`)**, 연결(CFS) 우선 → 없으면 개별(OFS) 폴백. reprt_code: 11013(1Q)·11012(반기)·11014(3Q)·11011(사업보고서)
- **분기화 규칙 — 누적 차분**: 손익 항목(매출액·영업이익·당기순이익)은 보고서별 **누적값을 수집한 뒤 차분**으로 분기값을 만든다: Q1=1Q누적, Q2=반기누적−1Q누적, Q3=3Q누적−반기누적, **Q4=연간−3Q누적**. 중간 보고서가 결측이면 해당 분기와 그 차분에 걸린 분기는 결측(판정불가) — 절대 0으로 채우지 않는다
- **EPS 산출 — 명시적 근사**: DART 주요계정 API에는 주당순이익 항목이 없다(전체재무제표 XBRL 파싱은 계정명 편차가 커서 채택하지 않음). `DilutedEPS = 분기 당기순이익 ÷ 상장주식수(marcap 최신 Stocks)`로 산출해 저장한다. 상장주식수가 3분기 창 안에서 사실상 상수이므로 **⑤의 성장 판정(e0<e1<e2)은 실질적으로 분기 순이익 연속 성장과 동치** — 이 근사를 컬럼 값의 정의로 명시한다. Stocks 결측 종목은 DilutedEPS 결측
- **레이트리밋**: 요청 간 sleep(0.1s~), 실패 재시도 1회, 종목별 실패는 스킵(해당 종목 판정불가). 1,000종목 × 보고서 최대 8건(당해+전년) ≈ 8,000요청/실행 — 일 한도 내. 이미 저장된 (Ticker, PeriodDate)는 재조회 스킵(증분)

### A-2. 저장 (기존 `data/financials_db.py` 재사용 — 이미 market 파라미터 구조. `config.DRIVE_PATHS`에 `kr_financials` 항목 추가만 필요)

- **스키마**: `kr/financials/kr_financials_YYYY.parquet` — `Ticker(6자리 str), PeriodDate, Year, Quarter, Revenue, OperatingIncome, NetIncome, DilutedEPS, SnapDate` (US 스키마 서브셋)
- **안전장치 그대로**: 저장 전 Drive 베이스라인 선다운로드(`ensure_drive_baseline` 패턴, failed면 중단) + (Ticker, PeriodDate) 중복 시 SnapDate 최신 우선 + 축소 가드
- **⚠️ 사용자 작업 1회**: Drive `kr/financials/` 폴더 생성 + 빈 placeholder parquet 수동 업로드(`kr_financials_2025.parquet`·`kr_financials_2026.parquet` 등) — SA 신규 파일 생성 불가 제약(기존 관례)

### A-3. 워크플로우

- 기존 `financials-update.yml`에 `market` 선택지 `kr` 추가(스케줄 동일 — 매월 1~7일 + 매주 월요일). main.py에 `--mode financials --market kr` 경로 연결
- 배포 직후 workflow_dispatch(market=kr) 수동 1회로 첫 수집(그래야 토요일 섹터리뷰가 바로 ⑤를 판정)

## B. Mr.Market step7 — KR 조건 활성화

### B-1. 시장별 게이트 확장 (`stage2_conditions.py` + `step7_sector_review_core.py`)

- `compute_stage2_columns`의 `market != 'US' → 전부 빈칸` 게이트를 **시장별 분기**로 교체: US·KR은 계산, 그 외(향후 신규 시장)는 빈칸
- **지수 off52를 시장별로**: ctx의 `index_off52`를 `{market: value}` 맵으로 확장 — US=`^GSPC`(기존), **KR=`^KS11`**(market_regime KR 경로 재사용). 분모 하한 1% 클램프·×2.5 배수 동일
- 필수 ②③·가산 VCP: 기존 순수함수 무변경 — KR 행의 OHLC(marcap parquet, `load_ticker_ohlc` KR 경로)로 그대로 계산
- 필수 ⑤: `kr/financials` parquet을 `build_eps_lookup` 재사용으로 룩업(티커 6자리 키 — zero-padding 정규화 주의). ETF 면제 로직은 무변경(KR 유니버스에 ETF 사실상 없음 — Type 부재 시 비ETF)
- `EarnBeat`: KR 항상 빈칸(제외 확정). 어닝 조회 대상은 기존대로 US 티커만

### B-2. 외국인소진율 추세 (`InstTrend` 컬럼의 KR 값)

- **의미 재정의(문서화)**: `InstTrend` = "주요 수급주체 보유 추세" — US는 기관보유(HeldPctInstitutions), **KR은 외국인소진율**
- **조회**: step7 실행 중 주도섹터 KR 유니버스만(수십 종목) Naver 조회 — 1순위 `m.stock.naver.com/api/stock/{code}/integration` JSON(외국인소진율 필드), 실패 시 `finance.naver.com/item/main.naver?code=` HTML 폴백. 종목당 0.3s sleep, 종목별 실패는 판정불가(예외 전파 없음). 필드 위치는 구현 첫 단계에서 실응답으로 확정한다
- **주간 스냅샷 누적**: `data/Foreign_Rate_History_1.csv` (컬럼: `데이터추출일자, Ticker, ForeignRate`) — Sector_History와 동일한 저장/로드/행수분할 패턴 + 같은 주 재실행 시 덮어쓰지 않음(dedup: 날짜+Ticker). drive_sync history 패턴에 파일명 매칭 추가(다운로드·업로드 왕복). **⚠️ 사용자 작업 1회**: Drive `data/history/`에 빈 `Foreign_Rate_History_1.csv` 수동 업로드(SA 제약)
- **판정**: 티커별 주간 값 시계열(오름차순)의 끝 3개 엄밀 증가 — `cond_inst_trend` 재사용. **스냅샷 3주 누적 후 자동 활성**(그 전엔 KR InstTrend 빈칸이 정상)

### B-3. 데이터 접근

- `drive_sync.download_financials()`에 `kr/financials` 서브폴더 추가(US와 동일 graceful degradation — 실패 시 해당 조건만 판정불가)
- `build_stage2_context()`: kr eps_lookup·kr 지수 off52·외인소진율 히스토리 로드를 소스별 독립 try/except로 추가(기존 다층 격리 유지)

## C. Pactolus — 최소 손질

- 가산 뱃지 tooltip 라벨 `기관보유증가` → `기관/외인 보유증가` (1줄, `sectorreview.js` 캐시버스팅 +1). 그 외 소비 로직 무변경 — KR 행의 ②③⑤ 게이트·가산 집계는 기존 generic 로직이 자동 처리

## 배포·순서

1. **A (Daily-Market)**: 구현·테스트 → push → **사용자: Drive `kr/financials/` placeholder 업로드** → workflow_dispatch(market=kr) 수동 1회 → parquet 생성 검증
2. **B (Mr.Market)**: 구현·테스트 → push → **사용자: Drive history에 `Foreign_Rate_History_1.csv` placeholder 업로드** → 토요일 정기 실행(또는 workflow_dispatch)으로 KR 컬럼 검증
3. **C (Pactolus)**: tooltip 1줄 — B 검증 후
4. 외인소진율 추세는 3주 후 자동 활성, ⑤는 A의 첫 수집 직후부터 판정 가능

## 범위 제외 (기록)

- KR 어닝서프라이즈 — 무료 컨센서스 소스 부재
- 외국인소진율 전 종목 상시 수집 — 차단 위험 대비 이득 없음(유니버스 주간 조회로 충분)
- 정밀 EPS(XBRL 전체재무제표 파싱) — 주요계정+주식수 근사로 대체(위 A-1 명시). 필요해지면 별도 과제
- SEPA Buy Point 자동 모니터링 — 별도 스펙 유지
