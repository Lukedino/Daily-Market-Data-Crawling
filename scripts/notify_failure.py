"""예약 워크플로 실패를 텔레그램으로 알린다 (공통 양식: HTML, KST 시각, 실행 로그 링크).

워크플로의 `if: failure()` 스텝이 부른다. pip 설치 전에 죽어도 돌아야 하므로 표준 라이브러리만 쓰고
프로젝트 모듈을 import 하지 않는다. 공개 저장소라 메시지 본문을 로그로 찍지 않는다.

    python3 scripts/notify_failure.py --job kr-daily [--target "..."]

환경변수: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, RUN_URL
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))  # 한국은 서머타임이 없어 고정 오프셋이면 충분
_WEEKDAYS = "월화수목금토일"

# 작업별 고정 문구. 사용자 입력(대상)만 이스케이프해 끼운다.
JOBS: dict[str, dict[str, str]] = {
    "kr-daily": {
        "title": "KR 일별 수집 실패",
        "target": "한국 시장",
        "impact": "직전 정상본 유지 · 오늘 KR 시세 없음 → Mr.Market·KIS 가 하루 늦은 데이터로 돎",
        "action": "로그의 <code>code=</code> 값으로 원인 확인 후 KR Daily 재실행",
    },
    "ohlc-daily": {
        "title": "OHLC 일별 수집 실패",
        "target": "",
        "impact": "US·크립토 시세 미갱신 → ML 분석·TAP·ATR 모니터가 하루 늦은 데이터로 돎",
        "action": "로그의 <code>code=</code> 값으로 원인 확인 후 OHLC Daily 재실행",
    },
    "financials-update": {
        "title": "재무 수집 실패",
        "target": "",
        "impact": "이번 달 재무·ratios 스냅샷 미수집 · 다음 자동 실행은 다음 달 1일",
        "action": "원인 확인 후 Financials Update 수동 재실행",
    },
    "sector-meta": {
        "title": "섹터 태그 수집 실패",
        "target": "",
        "impact": "섹터·산업 태그 이번 주 미갱신 · 다음 자동 실행은 다음 일요일",
        "action": "급하지 않으면 다음 주 자동 실행을 기다려도 됨",
    },
}


_MARKETS = {"all": "전체", "us": "미국", "crypto": "크립토", "kr": "한국"}


def _korean_target(target: str) -> str:
    """첫 단어가 시장 코드면 한글로(`all (수동 실행)` → `전체 (수동 실행)`)."""
    head, sep, rest = target.partition(" ")
    return _MARKETS.get(head, head) + sep + rest


def esc(value: str) -> str:
    """HTML 본문·속성 값 이스케이프 (`&` `<` `>` `"`)."""
    return html.escape(value, quote=True)


def fmt_kst(now: datetime | None = None) -> str:
    """`11/14(금) 16:28` — 항상 한국 시각."""
    now = (now or datetime.now(timezone.utc)).astimezone(KST)
    return f"{now:%m/%d}({_WEEKDAYS[now.weekday()]}) {now:%H:%M}"


def build_message(job: str, target: str, run_url: str, now: datetime | None = None) -> str:
    spec = JOBS[job]
    target = _korean_target(target) if target else spec["target"]
    second = fmt_kst(now) + (f" · 대상 {esc(target)}" if target else "")
    lines = [
        f"🔴 <b>[Daily-Market] {spec['title']}</b>",
        second,
        "",
        f"영향: {spec['impact']}",
        f"👉 {spec['action']}",
    ]
    if run_url:
        lines.append(f'<a href="{esc(run_url)}">실행 로그 보기</a>')
    return "\n".join(lines)


def to_plain(message: str) -> str:
    """HTML 파싱 실패 시 재전송용 평문: 링크는 `텍스트: 주소`, 태그 제거, 엔티티 복원."""
    text = re.sub(r'<a href="([^"]*)">([^<]*)</a>',
                  lambda m: f"{m.group(2)}: {m.group(1)}", message)
    text = re.sub(r"</?[a-z]+>", "", text)
    return html.unescape(text)


def _post(token: str, payload: dict) -> None:
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20):
        pass


def send(token: str, chat_id: str, message: str, post=_post) -> str:
    """HTML 로 보내고, 400 엔티티 파싱 오류면 평문으로 한 번 더. 결과 키워드만 반환(본문은 안 돌려줌)."""
    base = {"chat_id": chat_id, "disable_web_page_preview": True}
    try:
        post(token, {**base, "text": message, "parse_mode": "HTML"})
        return "sent"
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")
        except Exception:
            pass
        if exc.code != 400 or "parse entities" not in body:
            raise
    post(token, {**base, "text": to_plain(message)})
    return "sent_plain"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=sorted(JOBS))
    parser.add_argument("--target", default="")
    args = parser.parse_args(argv)

    token = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("텔레그램 시크릿 미설정 - 실패 알림 생략")
        return 0
    message = build_message(args.job, args.target, os.environ.get("RUN_URL", ""))
    try:
        result = send(token, chat_id, message)
    except Exception as exc:  # 알림 실패가 워크플로 결과를 바꾸지 않게, 원인 종류만 남긴다
        print(f"텔레그램 실패 알림 전송 실패: {type(exc).__name__}")
        return 0
    print(f"텔레그램 실패 알림: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
