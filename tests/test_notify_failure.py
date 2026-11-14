"""실패 알림 메시지 양식(HTML·KST·평문 재전송) 고정 테스트. 네트워크 호출 없음."""
import io
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import notify_failure as nf  # noqa: E402

# 2026-11-13 07:28 UTC = 11/13(금) 16:28 KST
NOW = datetime(2026, 11, 13, 7, 28, tzinfo=timezone.utc)
URL = "https://github.com/o/r/actions/runs/1"


def test_fmt_kst_converts_utc_and_uses_korean_weekday():
    assert nf.fmt_kst(NOW) == "11/13(금) 16:28"
    # UTC 일요일 20:00 → KST 월요일 05:00 (날짜·요일이 함께 넘어간다)
    assert nf.fmt_kst(datetime(2026, 11, 15, 20, 0, tzinfo=timezone.utc)) == "11/16(월) 05:00"


def test_message_shape_is_fixed():
    msg = nf.build_message("ohlc-daily", "crypto (일봉 확정 사이클)", URL, NOW)
    assert msg.split("\n") == [
        "🔴 <b>[Daily-Market] OHLC 일별 수집 실패</b>",
        "11/13(금) 16:28 · 대상 크립토 (일봉 확정 사이클)",
        "",
        "영향: US·크립토 시세 미갱신 → ML 분석·TAP·ATR 모니터가 하루 늦은 데이터로 돎",
        "👉 로그의 <code>code=</code> 값으로 원인 확인 후 OHLC Daily 재실행",
        f'<a href="{URL}">실행 로그 보기</a>',
    ]


@pytest.mark.parametrize("job", sorted(nf.JOBS))
def test_every_job_has_title_impact_action_and_link(job):
    lines = nf.build_message(job, "", URL, NOW).split("\n")
    assert lines[0].startswith("🔴 <b>[Daily-Market] ") and lines[0].endswith(" 실패</b>")
    assert lines[2] == ""
    assert lines[3].startswith("영향: ")
    assert lines[4].startswith("👉 ")
    assert lines[5] == f'<a href="{URL}">실행 로그 보기</a>'
    assert "http" not in "\n".join(lines[:5])  # 원시 URL 노출 없음


def test_kr_daily_default_target():
    assert nf.build_message("kr-daily", "", URL, NOW).split("\n")[1] == "11/13(금) 16:28 · 대상 한국 시장"


def test_user_input_and_url_are_escaped():
    msg = nf.build_message("sector-meta", "<b>x</b> & y", 'https://e/?a=1&b="2"', NOW)
    assert "대상 &lt;b&gt;x&lt;/b&gt; &amp; y" in msg
    assert '<a href="https://e/?a=1&amp;b=&quot;2&quot;">' in msg


def test_to_plain_strips_tags_and_restores_entities():
    msg = nf.build_message("sector-meta", "a & b", URL, NOW)
    plain = nf.to_plain(msg)
    assert "<" not in plain.replace("&lt;", "")
    assert plain.split("\n")[0] == "🔴 [Daily-Market] 섹터 태그 수집 실패"
    assert "대상 a & b" in plain
    assert plain.endswith(f"실행 로그 보기: {URL}")


def _http_error(code, body):
    return urllib.error.HTTPError(URL, code, "err", {}, io.BytesIO(body.encode()))


def test_send_uses_html_and_disables_preview():
    calls = []
    assert nf.send("t", "c", "<b>x</b>", post=lambda tok, p: calls.append(p)) == "sent"
    assert calls == [{"chat_id": "c", "disable_web_page_preview": True,
                      "text": "<b>x</b>", "parse_mode": "HTML"}]


def test_send_retries_plain_once_on_parse_error():
    calls = []

    def post(tok, payload):
        calls.append(payload)
        if len(calls) == 1:
            raise _http_error(400, '{"description":"Bad Request: can\'t parse entities"}')

    assert nf.send("t", "c", "<b>a &amp; b</b>", post=post) == "sent_plain"
    assert len(calls) == 2
    assert "parse_mode" not in calls[1] and calls[1]["text"] == "a & b"


def test_send_does_not_retry_other_errors():
    calls = []

    def post(tok, payload):
        calls.append(payload)
        raise _http_error(401, "Unauthorized")

    with pytest.raises(urllib.error.HTTPError):
        nf.send("t", "c", "x", post=post)
    assert len(calls) == 1


def test_main_skips_without_secrets_and_never_prints_message(monkeypatch, capsys):
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert nf.main(["--job", "kr-daily"]) == 0
    assert "생략" in capsys.readouterr().out


def test_main_swallows_send_failure_without_echoing(monkeypatch, capsys):
    monkeypatch.setenv("TELEGRAM_TOKEN", "t")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "c")
    monkeypatch.setenv("RUN_URL", URL)

    def boom(*a, **k):
        raise OSError("network down")

    monkeypatch.setattr(nf, "send", boom)
    assert nf.main(["--job", "ohlc-daily", "--target", "secret-target"]) == 0
    out = capsys.readouterr().out
    assert "OSError" in out and "secret-target" not in out and "영향" not in out


@pytest.mark.parametrize("name,job", [("kr-daily", "kr-daily"), ("ohlc-daily", "ohlc-daily"),
                                      ("financials-update", "financials-update"),
                                      ("sector-meta", "sector-meta")])
def test_workflow_calls_script_with_fallback(name, job):
    text = (ROOT / ".github" / "workflows" / f"{name}.yml").read_text(encoding="utf-8")
    assert "if: failure()" in text
    assert f"python3 scripts/notify_failure.py --job {job}" in text
    # 체크아웃 전 실패에도 알림이 가도록 고정 평문 curl 폴백을 유지한다
    assert "[ -f scripts/notify_failure.py ]" in text and "api.telegram.org" in text
    assert "MSG=\"🚨" not in text and "echo \"$MSG\"" not in text


def test_market_code_target_is_shown_in_korean():
    line = nf.build_message("financials-update", "all (수동 실행)", URL, NOW).split("\n")[1]
    assert line == "11/13(금) 16:28 · 대상 전체 (수동 실행)"
    assert nf.build_message("sector-meta", "us", URL, NOW).split("\n")[1].endswith("대상 미국")
