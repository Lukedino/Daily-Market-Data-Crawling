"""
아티팩트 업로드 실패가 "본 작업 실패"로 둔갑하지 않게 막는 가드.

2026-09-09 실측: `EOD Order (US)` 스텝은 **success** 였는데 `Upload log` 스텝이
`Failed to FinalizeArtifact: (403) Forbidden` 으로 죽으면서 잡 전체가 failure 가
됐고, `if: failure()` 알림이 `🚨 EOD Order (US) 실패 - 미국주식 주문 실행이
실패했습니다` 를 보냈다. 그날 주문은 애초에 나가지도 않았고(창 밖 도착) 아무
문제도 없었다.

로그 보존은 **부수 기능**이다. 그 실패가 매매·수집의 성패를 대변하면 알림을
믿을 수 없게 되고, 진짜 실패가 오보에 묻힌다.

규칙 두 가지:
  1. 실패 알림(`if: failure()`)이 있는 워크플로우의 `upload-artifact` 스텝은
     반드시 `continue-on-error: true` 를 단다.
  2. **알림 스텝 자신에는 절대 달지 않는다** — 달면 진짜 실패를 삼킨다.

⚠️ PyYAML 을 쓰지 않는다. 이 저장소 requirements 에 없고 전이 의존으로만 깔려
있어, 테스트가 import 하면 KIS-Trading 의 pandas_ta 사고(로컬 통과, CI에서
ModuleNotFoundError)와 같은 함정을 새로 만든다. 정규식도 쓰지 않고 들여쓰기로만
스텝을 가른다 — 이 저장소 워크플로우는 전부 같은 형식이다.
"""
from pathlib import Path

import pytest

WORKFLOW_DIR = Path(__file__).resolve().parent.parent / ".github" / "workflows"


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _steps(text: str) -> list[list[str]]:
    """`steps:` 아래를 스텝 단위 블록으로 쪼갠다 (YAML 파서 없이 들여쓰기로만)."""
    lines = text.splitlines()
    start = next((i for i, l in enumerate(lines) if l.strip() == "steps:"), None)
    if start is None:
        return []

    step_indent = None
    blocks: list[list[str]] = []
    cur: list[str] | None = None
    for line in lines[start + 1:]:
        stripped = line.strip()
        is_item = stripped.startswith("- ")
        if is_item and (step_indent is None or _indent(line) == step_indent):
            step_indent = _indent(line)
            if cur:
                blocks.append(cur)
            cur = [line]
        elif cur is not None:
            if stripped and _indent(line) <= step_indent:
                break                      # steps 블록을 벗어났다
            cur.append(line)
    if cur:
        blocks.append(cur)
    return blocks


def _files() -> list[Path]:
    return sorted(WORKFLOW_DIR.glob("*.yml"))


def test_workflow_dir_is_found():
    """경로가 어긋나면 아래 검사가 통째로 빈 통과가 된다 — 그걸 먼저 막는다."""
    assert _files(), f"워크플로우를 못 찾았다: {WORKFLOW_DIR}"


def test_step_splitter_actually_finds_steps():
    """쪼개기가 망가지면 모든 검사가 조용히 통과한다."""
    for path in _files():
        assert _steps(path.read_text(encoding="utf-8")), f"{path.name}: 스텝을 못 찾았다"


@pytest.mark.parametrize("path", _files(), ids=lambda p: p.name)
def test_artifact_upload_cannot_fake_a_failure_alert(path):
    text = path.read_text(encoding="utf-8")
    if "if: failure()" not in text:
        pytest.skip("실패 알림이 없는 워크플로우 — 오보가 날 구조가 아니다")

    uploads = [b for b in _steps(text) if "upload-artifact" in " ".join(b)]
    assert uploads or "upload-artifact" not in text, f"{path.name}: 업로드 스텝을 못 찾았다"
    for block in uploads:
        assert "continue-on-error: true" in " ".join(block), (
            f"{path.name}: 아티팩트 업로드 스텝에 continue-on-error 가 없다. "
            f"업로드가 403 한 번 나면 '작업 실패' 오보가 나간다."
        )


@pytest.mark.parametrize("path", _files(), ids=lambda p: p.name)
def test_notify_step_never_swallows_its_own_failure(path):
    """알림 스텝에 continue-on-error 가 붙으면 진짜 실패가 조용히 지나간다."""
    for block in _steps(path.read_text(encoding="utf-8")):
        joined = " ".join(block)
        if "Notify failure" in joined:
            assert "continue-on-error" not in joined, f"{path.name}: 알림 스텝에 붙어 있다"
