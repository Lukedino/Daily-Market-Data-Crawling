# Luke Picks Drive 연동 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** US 유니버스에 개인 관심종목을 반영하는 `Luke Picks` 소스를, 실제로는 한 번도 동작한 적 없는 로컬 파일 방식에서 Google Drive 다운로드 방식(ATR Monitor의 `_load_portfolio_from_drive()`와 동일 패턴)으로 교체한다.

**Architecture:** `data/ohlc_collector.py`의 로컬 파일 전용 `_load_luke_picks()`를 Service Account 기반 Drive 다운로드 함수로 교체하고, `load_tickers("us")`를 호출하는 5개 GitHub Actions 워크플로우에 신규 Secret을 배선한다.

**Tech Stack:** Python 3.12, google-api-python-client, google-auth, pandas, GitHub Actions

## Global Constraints

- US 유니버스만 대상 (KR/Crypto 변경 없음).
- 로컬 파일 fallback 없음 — Drive가 유일한 소스이며, 환경변수가 없으면 조용히 빈 리스트 반환 (필수 기능 아님, 파이프라인 중단 없음).
- Luke Picks 파일 형식은 단순 `Ticker`(또는 `ticker`/`Symbol`) 컬럼 하나짜리 리스트 — Portfolio.xlsx의 다중 컬럼 스키마(계좌/구분/종목/Ticker)와 다르다.
- 퍼블릭 저장소인 `CLAUDE.md`에는 새 Secret의 이름/용도만 다른 기존 Secret과 동일한 수준으로 기록한다 — 실제 파일 ID, Drive 폴더 경로, 종목명은 어디에도 남기지 않는다.
- 이 프로젝트는 pytest 등 테스트 프레임워크가 없다. 검증은 `unittest.mock`(표준 라이브러리)을 사용한 스크래치 스크립트로 하고, 커밋하지 않는다.

---

### Task 1: `_load_luke_picks_from_drive()` 구현 및 배선

**Files:**
- Modify: `data/ohlc_collector.py` (37번째 줄 상수 제거, 140~152번째 줄 함수 교체, 161번째 줄 호출부 변경)

**Interfaces:**
- Produces: `_load_luke_picks_from_drive() -> list[str]` — Drive에서 개인 관심종목 티커 리스트를 다운로드/파싱. 환경변수 없음/실패 시 빈 리스트.
- Consumes (기존 유지): `_build_us_universe()`가 이 함수를 호출.

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_drive.py` 파일을 아래 내용으로 작성한다.

```python
import io
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling")

from data import ohlc_collector

# --- Case 1: 환경변수 없음 → 빈 리스트, 네트워크 호출 없음 ---
with patch.dict(os.environ, {}, clear=True):
    result = ohlc_collector._load_luke_picks_from_drive()
assert result == [], f"FAIL case1: {result}"
print("OK case 1 (no env vars -> empty list):", result)

# --- Case 2: Google Sheets 네이티브 파일 → export 경로 ---
csv_bytes = b"Ticker\nCRDO\nSTRL\n"

mock_service = MagicMock()
mock_service.files.return_value.get.return_value.execute.return_value = {
    "mimeType": "application/vnd.google-apps.spreadsheet"
}
mock_service.files.return_value.export.return_value.execute.return_value = csv_bytes

with patch.dict(os.environ, {
    "GOOGLE_SERVICE_ACCOUNT_JSON": '{"type": "service_account"}',
    "GDRIVE_LUKE_PICKS_FILE_ID": "fake_file_id",
}), \
     patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
     patch("googleapiclient.discovery.build", return_value=mock_service):
    result2 = ohlc_collector._load_luke_picks_from_drive()

assert result2 == ["CRDO", "STRL"], f"FAIL case2: {result2}"
print("OK case 2 (Google Sheets export path):", result2)

# --- Case 3: .xlsx 업로드 파일 → get_media + read_excel 경로 ---
import pandas as pd
xlsx_buf = io.BytesIO()
pd.DataFrame({"Ticker": ["AFRM", "SEZL"]}).to_excel(xlsx_buf, index=False)
xlsx_bytes = xlsx_buf.getvalue()

mock_service2 = MagicMock()
mock_service2.files.return_value.get.return_value.execute.return_value = {
    "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}
mock_service2.files.return_value.get_media.return_value.execute.return_value = xlsx_bytes

with patch.dict(os.environ, {
    "GOOGLE_SERVICE_ACCOUNT_JSON": '{"type": "service_account"}',
    "GDRIVE_LUKE_PICKS_FILE_ID": "fake_file_id",
}), \
     patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
     patch("googleapiclient.discovery.build", return_value=mock_service2):
    result3 = ohlc_collector._load_luke_picks_from_drive()

assert result3 == ["AFRM", "SEZL"], f"FAIL case3: {result3}"
print("OK case 3 (.xlsx get_media path):", result3)

# --- Case 4: 예외 발생 시 빈 리스트로 안전하게 폴백 ---
mock_service3 = MagicMock()
mock_service3.files.return_value.get.return_value.execute.side_effect = Exception("drive error")

with patch.dict(os.environ, {
    "GOOGLE_SERVICE_ACCOUNT_JSON": '{"type": "service_account"}',
    "GDRIVE_LUKE_PICKS_FILE_ID": "fake_file_id",
}), \
     patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
     patch("googleapiclient.discovery.build", return_value=mock_service3):
    result4 = ohlc_collector._load_luke_picks_from_drive()

assert result4 == [], f"FAIL case4: {result4}"
print("OK case 4 (exception -> empty list fallback):", result4)

print("ALL OK")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_drive.py"`
Expected: `AttributeError: module 'data.ohlc_collector' has no attribute '_load_luke_picks_from_drive'`

- [ ] **Step 3: `data/ohlc_collector.py` 수정**

37번째 줄의 아래 상수를 삭제한다:

```python
_LUKE_PICKS_PATH = Path("input/Luke Picks.xlsx")
```

(`Path`는 233번째 줄 `load_tickers()`에서 계속 쓰이므로 `from pathlib import Path` import는 그대로 둔다.)

140~152번째 줄의 기존 함수:

```python
def _load_luke_picks() -> list[str]:
    if not _LUKE_PICKS_PATH.exists():
        return []
    try:
        df = pd.read_excel(_LUKE_PICKS_PATH)
        for col in ("Ticker", "ticker", "Symbol"):
            if col in df.columns:
                tickers = df[col].dropna().astype(str).str.strip().tolist()
                logger.info(f"[Universe] LukePicks: {len(tickers)}종목")
                return [t for t in tickers if t]
    except Exception as e:
        logger.warning(f"[Universe] LukePicks 로드 실패: {e}")
    return []
```

를 아래로 교체한다:

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

161번째 줄의 호출부를 변경한다:

```python
    raw.extend(_load_luke_picks())
```
→
```python
    raw.extend(_load_luke_picks_from_drive())
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_luke_picks_drive.py"`
Expected: 4개 케이스 모두 `OK` 출력, 마지막에 `ALL OK`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_luke_picks_drive.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add data/ohlc_collector.py
git commit -m "$(cat <<'EOF'
Load Luke Picks from Drive instead of a local-only file

_load_luke_picks() checked input/Luke Picks.xlsx, which is never
present in a GHA run (not committed, nothing downloaded it) — so any
manually-curated ticker was never actually reflected in the crawled US
universe. Mirrors Portfolio_ATR_Monitor's _load_portfolio_from_drive()
pattern: service-account download by file ID, with Google Sheets
export vs raw .xlsx handled by mimeType. Missing env vars or any
failure fall back to an empty list — this source stays optional.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: 5개 워크플로우에 `GDRIVE_LUKE_PICKS_FILE_ID` 배선

**Files:**
- Modify: `.github/workflows/ohlc-daily.yml`
- Modify: `.github/workflows/ohlc-backfill.yml`
- Modify: `.github/workflows/ohlc-new-ticker-backfill.yml`
- Modify: `.github/workflows/sector-meta.yml`
- Modify: `.github/workflows/financials-update.yml`

**Interfaces:**
- Consumes: `data.ohlc_collector._load_luke_picks_from_drive()`(Task 1)가 읽는 `GDRIVE_LUKE_PICKS_FILE_ID` 환경변수.

이 5개 워크플로우는 모두 (직접 또는 `data/financials_collector.py`를 통해 간접적으로) `load_tickers("us")`를 호출하므로, 이미 있는 `GOOGLE_SERVICE_ACCOUNT_JSON` 줄 바로 아래에 신규 줄을 추가한다. `kr-daily.yml`/`kr-backfill.yml`은 KR 전용 수집기라 변경하지 않는다.

- [ ] **Step 1: 스크래치 검증 스크립트 작성 (구현 전 — 실패 확인용)**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_workflow_secrets.py`:

```python
import yaml

files = [
    r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\.github\workflows\ohlc-daily.yml",
    r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\.github\workflows\ohlc-backfill.yml",
    r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\.github\workflows\ohlc-new-ticker-backfill.yml",
    r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\.github\workflows\sector-meta.yml",
    r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\.github\workflows\financials-update.yml",
]

for path in files:
    with open(path, encoding="utf-8") as f:
        text = f.read()
        f.seek(0)
        doc = yaml.safe_load(f)

    assert "GDRIVE_LUKE_PICKS_FILE_ID" in text, f"FAIL missing secret ref in {path}"
    assert "${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}" in text, f"FAIL not wired to secrets in {path}"

    # 실제 secret 값이나 파일 ID 문자열이 하드코딩되지 않았는지 확인
    assert "GDRIVE_LUKE_PICKS_FILE_ID:" in text, f"FAIL env key not found in {path}"

    print(f"OK: {path}")

print("ALL OK")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_workflow_secrets.py"`
Expected: 첫 파일(`ohlc-daily.yml`)에서 `AssertionError: FAIL missing secret ref in ...`

- [ ] **Step 3: 5개 워크플로우 파일 수정**

`ohlc-daily.yml`의 `env:` 블록(33~36번째 줄, `env:` 줄 포함)을 아래로 교체:

```yaml
        env:
          GDRIVE_FOLDER_ID:            ${{ secrets.GDRIVE_FOLDER_ID }}
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GDRIVE_LUKE_PICKS_FILE_ID:   ${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}
```

`ohlc-backfill.yml`의 `env:` 블록(41~48번째 줄, `env:` 줄 포함, 이번 세션에서 이미 보안 하드닝된 상태)을 아래로 교체:

```yaml
        env:
          MARKET:                     ${{ inputs.market }}
          START_YEAR:                 ${{ inputs.start_year }}
          END_YEAR:                   ${{ inputs.end_year }}
          DRY_RUN:                    ${{ inputs.dry_run }}
          GDRIVE_FOLDER_ID:            ${{ secrets.GDRIVE_FOLDER_ID }}
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GDRIVE_LUKE_PICKS_FILE_ID:   ${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}
```

`ohlc-new-ticker-backfill.yml`의 `env:` 블록(37~43번째 줄, `env:` 줄 포함)을 아래로 교체:

```yaml
        env:
          MARKET:                     ${{ inputs.market }}
          START_YEAR:                 ${{ inputs.start_year }}
          DRY_RUN:                    ${{ inputs.dry_run }}
          GDRIVE_FOLDER_ID:            ${{ secrets.GDRIVE_FOLDER_ID }}
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GDRIVE_LUKE_PICKS_FILE_ID:   ${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}
```

`sector-meta.yml`의 `env:` 블록(35~37번째 줄, `env:` 줄 포함)을 아래로 교체:

```yaml
        env:
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GDRIVE_LUKE_PICKS_FILE_ID:   ${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}
```

`financials-update.yml`의 `env:` 블록(33~36번째 줄, `env:` 줄 포함)을 아래로 교체:

```yaml
        env:
          GDRIVE_FOLDER_ID:            ${{ secrets.GDRIVE_FOLDER_ID }}
          GDRIVE_OHLC_FOLDER_ID:       ${{ secrets.GDRIVE_OHLC_FOLDER_ID }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
          GDRIVE_LUKE_PICKS_FILE_ID:   ${{ secrets.GDRIVE_LUKE_PICKS_FILE_ID }}
```

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_workflow_secrets.py"`
Expected: 5개 파일 모두 `OK: ...` 출력, 마지막에 `ALL OK`

추가로 각 파일의 YAML 문법이 깨지지 않았는지 별도 확인:

Run: `python -c "import yaml, glob; [yaml.safe_load(open(f, encoding='utf-8')) for f in glob.glob('.github/workflows/*.yml')]; print('YAML OK')"` (프로젝트 루트에서 실행)
Expected: `YAML OK`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_workflow_secrets.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add .github/workflows/ohlc-daily.yml .github/workflows/ohlc-backfill.yml .github/workflows/ohlc-new-ticker-backfill.yml .github/workflows/sector-meta.yml .github/workflows/financials-update.yml
git commit -m "$(cat <<'EOF'
Wire GDRIVE_LUKE_PICKS_FILE_ID into all US-universe workflows

Every workflow that calls load_tickers("us") (directly or via
financials_collector) needs this secret alongside the existing
GOOGLE_SERVICE_ACCOUNT_JSON for _load_luke_picks_from_drive() to have
any effect. kr-daily.yml/kr-backfill.yml are unaffected (KR uses a
separate collector).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `CLAUDE.md` 문서화 (퍼블릭 저장소 — 노출 수위 제한)

**Files:**
- Modify: `CLAUDE.md`

**Interfaces:** 없음 (문서만 변경)

- [ ] **Step 1: 반영 확인용 스크래치 체크 작성**

`%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_claude_md_secret_row.py`:

```python
path = r"<USER_HOME>\Desktop\Luke 작업공간\[Claude Code] Daily-Market-Data-Crawling\CLAUDE.md"
with open(path, encoding="utf-8") as f:
    text = f.read()

assert "GDRIVE_LUKE_PICKS_FILE_ID" in text, "FAIL: secret row missing"

# 민감 정보(실제 파일 ID, 종목명)가 실수로 들어가지 않았는지 최소 확인
forbidden = ["CRDO", "STRL", "AGX", "AFRM", "SEZL", "DAVE", "NET"]
leaked = [t for t in forbidden if t in text]
assert not leaked, f"FAIL: 종목명 노출 발견: {leaked}"

print("OK: secret row present, no ticker names leaked")
```

- [ ] **Step 2: 실행해서 실패 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_claude_md_secret_row.py"`
Expected: `AssertionError: FAIL: secret row missing`

- [ ] **Step 3: `CLAUDE.md` 수정**

"GitHub Secrets" 표(42~46번째 줄)를 아래로 교체:

```markdown
| Secret | 용도 |
|--------|------|
| `GDRIVE_OHLC_FOLDER_ID` | `[Database] Market Crawling Data` 폴더 ID |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | GCP Service Account JSON 키 |
| `DART_API_KEY` | DART 재무데이터 API 키 |
| `GDRIVE_LUKE_PICKS_FILE_ID` | US 유니버스 개인 관심종목 리스트 연동용 |
```

실제 파일 ID, Drive 폴더 경로, 종목명은 이 파일 어디에도 추가하지 않는다.

- [ ] **Step 4: 실행해서 통과 확인**

Run: `python "%LOCALAPPDATA%\..\AppData\Local\Temp\claude\C--Users-Luke-Skywalker\c1a1746b-776d-4dbc-978e-8e7d8265329c\scratchpad\test_claude_md_secret_row.py"`
Expected: `OK: secret row present, no ticker names leaked`

- [ ] **Step 5: 스크래치 스크립트 삭제 후 커밋**

```bash
rm "<SCRATCHPAD>/c1a1746b-776d-4dbc-978e-8e7d8265329c/scratchpad/test_claude_md_secret_row.py"
cd "<WORKSPACE>/[Claude Code] Daily-Market-Data-Crawling"
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
Document GDRIVE_LUKE_PICKS_FILE_ID secret at the same disclosure level as existing secrets

Name and purpose only, matching how GDRIVE_OHLC_FOLDER_ID/DART_API_KEY
are already documented in this public repo — no file ID, Drive path,
or ticker names.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
