# Quant-Korea-Data

한국 주식 퀀트 팩터 원본 데이터 수집 & Google Drive 축적 자동화

> Phase 1: 데이터 수집 전용 (Public)
> Phase 2 (추후): Private 전환 후 백테스트 + 퀀트 로직 추가

---

## 구조

```
GitHub Actions (Azure IP)
  ↓ pykrx / DART API / CompanyGuide 수집
  ↓ Parquet/ZSTD 로컬 저장
  ↓ Google Drive 업로드

Google Drive /MyDrive/korea_factor/
  data/market/      YYYYMM.parquet  (월별 시장 스냅샷)
  data/financials/  YYYY.parquet    (연간 재무제표)
  data/prices/      YYYYMM.parquet  (월별 일별 OHLCV)
  data/progress/    collection_status.json
```

---

## GitHub Secrets 등록

| Secret | 설명 |
|--------|------|
| `DART_API_KEY` | [opendart.fss.or.kr](https://opendart.fss.or.kr) 무료 발급 |
| `GDRIVE_CREDENTIALS` | Service Account JSON을 base64 인코딩 |
| `GDRIVE_FOLDER_ID` | Google Drive 대상 폴더 ID |

**GDRIVE_CREDENTIALS 인코딩 방법:**
```bash
# Mac/Linux
base64 -i service-account.json | tr -d '\n'

# Windows (PowerShell)
[Convert]::ToBase64String([IO.File]::ReadAllBytes("service-account.json"))
```

---

## 실행 순서 (최초 데이터 구축)

GitHub → Actions 탭에서 순서대로 수동 실행:

1. **Bootstrap 1Y** → 2025년 수집 (가장 최근, 가장 중요)
2. **Bootstrap 2Y** → 2024년 수집
3. **Bootstrap 3Y** → 2023년 수집
4. **Bootstrap 4Y** → 2022년 수집
5. **Bootstrap 5Y** → 2021년 수집

완료 후 → **Daily Incremental**이 매일 18:30 KST 자동 실행

---

## 워크플로우 옵션

| 옵션 | 설명 |
|------|------|
| `skip_prices` | 일별 주가 수집 생략 (시장 스냅샷만) |
| `skip_financials` | 재무제표 수집 생략 (bootstrap) |
| `force` | 완료된 월도 재수집 |
| `dry_run` | 실제 저장 없이 테스트 |

---

## 로컬 실행

```bash
pip install -r requirements.txt

# 환경변수 설정
export DART_API_KEY=your_key
export GDRIVE_FOLDER_ID=your_folder_id
export GOOGLE_APPLICATION_CREDENTIALS=gdrive_creds.json

# 2022년 데이터 수집
python main.py --mode bootstrap --year 2022 --upload-drive

# 수집 현황 확인
python main.py --mode bootstrap --status
```

---

## 수집 데이터 명세

### market/ — 월별 시장 스냅샷
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | TEXT | 수집 기준일 YYYYMMDD |
| ticker | TEXT | 종목코드 6자리 |
| name | TEXT | 종목명 |
| market | TEXT | KOSPI / KOSDAQ |
| close | REAL | 종가 |
| volume | INTEGER | 거래량 |
| market_cap | INTEGER | 시가총액 |
| PER | REAL | 주가수익비율 |
| PBR | REAL | 주가순자산비율 |
| EPS | REAL | 주당순이익 |
| BPS | REAL | 주당순자산 |
| DIV | REAL | 배당수익률 |

### financials/ — 연간 재무제표
| 컬럼 | 타입 | 설명 |
|------|------|------|
| year | INTEGER | 사업연도 |
| ticker | TEXT | 종목코드 |
| fs_type | TEXT | CFS(연결) / OFS(별도) |
| 매출액 | REAL | |
| 영업이익 | REAL | |
| 당기순이익 | REAL | |
| 자산총계 | REAL | |
| 부채총계 | REAL | |
| 자본총계 | REAL | |
| 영업현금흐름 | REAL | |
| 매출총이익 | REAL | |

### prices/ — 월별 일별 OHLCV
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | TEXT | 거래일 YYYYMMDD |
| ticker | TEXT | 종목코드 |
| open | REAL | 시가 |
| high | REAL | 고가 |
| low | REAL | 저가 |
| close | REAL | 종가 |
| volume | INTEGER | 거래량 |

> ⚠️ prices 수집은 GitHub Actions(Azure IP) 전용. Colab(GCP)에서는 KRX 차단.
