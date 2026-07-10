# Luke Picks Drive 연동 설계

## 배경

US 유니버스(`data/ohlc_collector.py`)는 S&P500/NASDAQ100/DOW30/CoinMarketCap
Top200/`Luke Picks.xlsx`/수동 리스트를 합쳐서 구성된다. 이 중 `Luke Picks.xlsx`는
사용자가 개인적으로 관심 있는 종목을 추가하기 위한 override 파일인데, 실제로는
`input/Luke Picks.xlsx`라는 **로컬 파일**만 확인하는 코드였다
(`_load_luke_picks()`, `data/ohlc_collector.py:140-152`). 이 저장소는 `input/`을
커밋하지 않고(`.gitignore`에도 명시조차 없음), GitHub Actions 어디에도 이 파일을
Drive에서 내려받는 단계가 없었다. 따라서 실제 GHA 실행(신선한 Ubuntu 컨테이너)에서는
이 파일이 존재한 적이 없고, `_load_luke_picks()`는 항상 빈 리스트를 반환해왔다 —
즉 사용자가 개인적으로 추가하고 싶었던 종목(CRDO/STRL/AGX/AFRM/SEZL/DAVE/NET 등)은
한 번도 실제로 유니버스에 반영되지 않았다.

별도 프로젝트인 Portfolio_ATR_Monitor는 동일한 문제를 이미
`_load_portfolio_from_drive()`(`config.py:145-192`)로 해결해 두었다: Service
Account 인증으로 Drive의 특정 파일 ID를 직접 다운로드하고, Google Sheets
네이티브 포맷이면 CSV export, 아니면 raw 파일 다운로드 후 파싱한다. 이번
작업은 이 패턴을 그대로 이 저장소에 적용한다.

## 목표

1. Drive에 업로드해 둔 개인 관심종목 리스트 파일을 Service Account로 다운로드해서
   US 유니버스에 병합하는 기능을 추가한다.
2. 로컬 파일 fallback은 두지 않는다 (Drive 전용 소스).
3. 퍼블릭 저장소인 `CLAUDE.md`에는 새 Secret의 이름과 용도만 기존 다른 Secret과
   동일한 수준으로 기록하고, 실제 파일 ID나 종목명은 어디에도 남기지 않는다.

## 비목표

- KR/Crypto 유니버스는 변경하지 않는다 (US만 해당).
- Portfolio.xlsx(ATR Monitor용)와 파일을 공유하지 않는다 — Luke Picks는 별도의
  단순 티커 리스트 전용 파일이다 (계좌/구분 등 다중 컬럼 스키마 아님).
- 새 Drive 파일에 대한 업로드/쓰기 기능은 만들지 않는다 (읽기 전용).

## 파일 형식

Drive에 저장되는 Luke Picks 파일은 **단순 티커 리스트**다: `Ticker` (또는
`ticker`/`Symbol`) 컬럼 하나만 있으면 되고, 기존 로컬 리더가 쓰던 컬럼 탐색
로직(`data/ohlc_collector.py:145-149`)을 그대로 재사용한다. `.xlsx` 업로드나
Google Sheets 네이티브 파일 둘 다 지원한다 (ATR Monitor와 동일).

## 설계

### 1. `data/ohlc_collector.py` — `_load_luke_picks_from_drive() -> list[str]`

기존 `_LUKE_PICKS_PATH`/`_load_luke_picks()`(로컬 파일 전용, 140~152번째 줄)를
제거하고 아래 함수로 교체한다.

```python
def _load_luke_picks_from_drive() -> list[str]:
    """
    Google Drive에서 개인 관심종목 리스트(단순 Ticker 컬럼)를 다운로드하여 파싱.
    GOOGLE_SERVICE_ACCOUNT_JSON + GDRIVE_LUKE_PICKS_FILE_ID 둘 다 있어야 동작.
    둘 중 하나라도 없거나 실패하면 조용히 빈 리스트 반환 (필수 기능 아님).
    """
    import os

    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    file_id = os.environ.get("GDRIVE_LUKE_PICKS_FILE_ID", "")
    if not sa_json or not file_id:
        return []

    try:
        import io
        import json as _json
        import pandas as pd
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_info(
            _json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        meta = service.files().get(fileId=file_id, fields="mimeType").execute()
        mime = meta.get("mimeType", "")

        if mime == "application/vnd.google-apps.spreadsheet":
            raw = service.files().export(
                fileId=file_id, mimeType="text/csv"
            ).execute()
            df = pd.read_csv(io.BytesIO(raw))
        elif mime in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        ):
            raw = service.files().get_media(fileId=file_id).execute()
            df = pd.read_excel(io.BytesIO(raw))
        else:
            raw = service.files().get_media(fileId=file_id).execute()
            df = pd.read_csv(io.StringIO(raw.decode("utf-8")), sep=None, engine="python")

        for col in ("Ticker", "ticker", "Symbol"):
            if col in df.columns:
                tickers = df[col].dropna().astype(str).str.strip().tolist()
                tickers = [t for t in tickers if t]
                logger.info(f"[Universe] Drive LukePicks: {len(tickers)}종목")
                return tickers

        logger.warning("[Universe] Drive LukePicks: Ticker 컬럼 없음")
        return []

    except Exception as e:
        logger.warning(f"[Universe] Drive LukePicks 로드 실패: {e}")
        return []
```

`_build_us_universe()`(`data/ohlc_collector.py:155-173`)의
`raw.extend(_load_luke_picks())` 호출을 `raw.extend(_load_luke_picks_from_drive())`로
변경한다. 그 외 유니버스 구성 순서(S&P500 → NASDAQ100 → DOW30 → LukePicks →
MANUAL → 중복 제거)는 그대로 유지한다.

모듈 상단의 `_LUKE_PICKS_PATH = Path("input/Luke Picks.xlsx")` 상수(37번째 줄)는
제거한다. `Path`는 `load_tickers()`(233번째 줄, `_UNIVERSE_FILES` override 파일
경로)에서 계속 쓰이므로 `from pathlib import Path` import는 그대로 둔다.

### 2. GitHub Secrets

`GDRIVE_LUKE_PICKS_FILE_ID` 신규 추가. 값은 GitHub Secrets에만 저장하고
저장소 어디에도 실제 파일 ID를 남기지 않는다.

### 3. 영향받는 워크플로우

`load_tickers("us")`를 직접/간접 호출하는 아래 5개 워크플로우의 `env:` 블록에
`GOOGLE_SERVICE_ACCOUNT_JSON`(이미 있음)과 나란히 `GDRIVE_LUKE_PICKS_FILE_ID`를
추가한다:

- `ohlc-daily.yml`
- `ohlc-backfill.yml`
- `ohlc-new-ticker-backfill.yml`
- `sector-meta.yml`
- `financials-update.yml` (`data/financials_collector.py`가 `load_tickers("us")`를
  직접 호출하므로 포함)

`kr-daily.yml`/`kr-backfill.yml`은 KR 전용 수집기를 쓰므로 변경 불필요.

### 4. `CLAUDE.md` 문서화 (퍼블릭 저장소 — 노출 수위 제한)

"GitHub Secrets" 표에 기존 다른 항목과 동일한 수준으로 한 줄만 추가:

```markdown
| `GDRIVE_LUKE_PICKS_FILE_ID` | US 유니버스 개인 관심종목 리스트 연동용 |
```

실제 파일 ID, Drive 폴더 경로, 종목명은 이 문서 어디에도 기록하지 않는다.
(참고: 이 설계 문서 자체도 저장소에 커밋되므로 동일한 제약을 따른다 — 이미
지키고 있음.)

## 에러 처리

- `GOOGLE_SERVICE_ACCOUNT_JSON` 또는 `GDRIVE_LUKE_PICKS_FILE_ID`가 없으면
  (로컬 개발 환경 등) 조용히 빈 리스트 반환 — 이 기능은 선택사항이며 없어도
  기존 S&P500/NASDAQ100/DOW30/MANUAL 유니버스는 정상 동작해야 한다.
- Drive 인증 실패, 파일 없음, 파싱 실패 등은 모두 `except Exception`으로 잡아
  warning 로그 후 빈 리스트 반환 — 전체 수집 파이프라인을 중단시키지 않는다
  (ATR Monitor의 `_load_portfolio_from_drive()`와 동일한 원칙).

## 테스트 계획

이 프로젝트는 pytest를 쓰지 않으므로, 기존 관례대로 스크래치 스크립트 +
`unittest.mock`(표준 라이브러리)으로 검증한다.

- 환경변수 둘 다 없는 경우 → 빈 리스트, 네트워크 호출 없음 확인.
- `googleapiclient`를 모킹해서: (a) Google Sheets 네이티브 파일 mimeType일 때
  export 경로를 타는지, (b) `.xlsx` 업로드 파일 mimeType일 때 get_media +
  `pd.read_excel` 경로를 타는지, (c) Ticker 컬럼 파싱이 정상 동작하는지 확인.
- 예외 발생 시(예: `service.files().get()`이 예외를 던지는 경우) 빈 리스트로
  안전하게 폴백하는지 확인.
- 로컬에서 실제 Drive 자격증명이 있다면(선택) 실제 파일로 스모크 테스트 —
  이 프로젝트 개발 환경에 `GOOGLE_SERVICE_ACCOUNT_JSON` 파일이 없다는 것을
  이미 확인했으므로, 이번 작업에서는 모킹 기반 검증까지만 하고 실제 GHA 실행
  결과는 사용자가 별도로 확인한다.
