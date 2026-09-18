> 이 문서는 Codex 등 외부 에이전트용 안내문이다. 2026-09-18 작성.

# AGENTS.md — Daily-Market-Data-Crawling

**정본은 같은 폴더의 `CLAUDE.md` 다. 이 파일은 포인터다.**
작업·검토 전에 `CLAUDE.md` 를 먼저 읽어라. 두 파일이 어긋나면 `CLAUDE.md` 가 옳다.
프로젝트 지식을 이 파일에 복제하지 말 것 — 여기에는 읽는 순서와 검토 규칙만 둔다.

> ⚠️ 이 저장소는 **PUBLIC** 이다. 보고서·커밋·이 파일에 Drive 폴더 ID, GCP 프로젝트, 서비스 계정 이메일, chat id 를 적지 말 것.

## 읽는 순서
1. `CLAUDE.md` — `## 프로젝트 개요`, `## GitHub Actions 워크플로우`, `## 실행 방법 (main.py)`, `## 자동 갭 보정 (kr-daily)`, `## 주요 이력`
2. `main.py` → `scripts/` → `.github/workflows/` (7개)
3. `tests/`

## 구조 한 줄 요약
GitHub Actions 가 US/Crypto OHLC, KR 일별 스냅샷, US/KR 재무를 수집해 연도별 Parquet 으로 Google Drive 에 저장한다. 저장소에는 데이터를 커밋하지 않는다. `ohlc-daily` 의 정시 트리거는 외부(비공개) 디스패처가 `workflow_dispatch` 로 호출한다.

## 검토(크로스체크) 규칙
- 요청이 없으면 **파일을 고치지 말고** 보고서만 쓴다. 커밋·push·배포·워크플로 실행 금지.
- 지적마다 `파일:줄` · 재현 시나리오 · 심각도(P0~P3) · 제안을 적는다. 확신이 없으면 "추정" 이라고 표시한다.
- 비밀값(`.env`, 토큰, 키, ID)은 읽더라도 보고서에 옮기지 않는다. 변수 이름만 쓴다.
- `CLAUDE.md` 의 "주요 이력"·"완료" 절에 이미 기록된 수정은 다시 지적하지 말고, 그 수정이 **실제로 코드에 남아 있는지**만 확인한다.

## 의도된 설계 — 결함으로 오인하지 말 것
- Drive 의 빈 플레이스홀더 파일(`kr_financials_placeholders/` 등)은 서비스 계정이 **새 파일을 만들 수 없고 덮어쓰기만 가능**해서 사람이 미리 올려 두는 것이다.
- KR 재무 수집은 법정 공시 기한이 지난 분기만 조회한다(달력 게이트). 대부분의 달에 DART 호출이 0건인 것은 정상이다.
- `ohlc-daily.yml` 의 `on:` 에 `schedule:` 블록이 없고 `workflow_dispatch` 만 있는 것은 의도된 이관 결과다(파일 상단 주석에 근거와 복원 방법이 있다). 본문의 `github.event.schedule` 참조는 복원에 대비해 남긴 것이다.
- `upload-artifact` 단계의 `continue-on-error` 는 아티팩트 403 이 "작업 실패" 오보를 내던 것을 막는 장치다.

## 집중할 위험 지점
- 공개 로그에 새는 값(폴더 ID, 이메일, 예외 메시지 속 경로).
- 워크플로 `permissions` 최소화, 서드파티 액션 버전 고정.
- 저장 전 Drive 기존 파일을 내려받지 않아 이력을 덮어쓰는 경로(`financials-update` 의 ratios 스냅샷은 **알려진 미수정** 항목).
- 부분 실패를 성공으로 기록하는 경로, 수집 종목 수 급감 가드, 휴장일·시간대 처리, 상류(FDR·yfinance) 장애 폴백.
