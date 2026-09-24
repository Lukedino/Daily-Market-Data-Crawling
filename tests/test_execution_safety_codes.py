"""공개 artifact 의 실패 코드 허용목록이 소스와 어긋나지 않게 한다.

2026-09-22 OHLC 장애가 이틀 동안 원인 불명이었던 직접적인 이유다. `collection_incomplete`
가 raise 되는데 `_ERROR_CODES` 에 없어 로그에 `code=` 가 한 번도 찍히지 않았고, 개명하며
버려진 `incremental_incomplete` 는 아무도 raise 하지 않는데 목록에 남아 있었다. 실패한
실행의 artifact 전체가 `[ERROR] execution_safety.py:155 operation_failed` 한 줄이었다.
"""
import re
from pathlib import Path

from data import execution_safety

_ROOT = Path(__file__).resolve().parents[1]
# PublicFormatter 가 읽는 코드는 전부 `raise X("snake_case")` 형태로만 들어온다.
# 사람이 읽는 메시지(`ImportError("...를 설치하세요")`)는 코드가 아니라 대상이 아니다.
_RAISE = re.compile(r'raise\s+[A-Za-z_][A-Za-z0-9_.]*\(\s*"([a-z0-9_]+)"')


def _sources():
    yield _ROOT / "main.py"
    for folder in ("data", "scripts"):
        yield from sorted((_ROOT / folder).rglob("*.py"))


def _raised_codes():
    found = set()
    for path in _sources():
        found.update(_RAISE.findall(path.read_text(encoding="utf-8")))
    return found


def test_every_raised_code_survives_into_the_public_artifact():
    unlisted = sorted(_raised_codes() - execution_safety._ERROR_CODES)
    assert not unlisted, (
        "_ERROR_CODES 에 없는 코드는 artifact 에서 지워져 원인 없는 operation_failed 가 된다: "
        f"{unlisted}"
    )


def test_no_allowed_code_has_lost_its_raise_site():
    dead = sorted(execution_safety._ERROR_CODES - _raised_codes())
    assert not dead, (
        f"개명·삭제 뒤 목록에만 남은 죽은 코드다(실제로는 절대 찍히지 않는다): {dead}"
    )


def test_the_scan_actually_finds_codes():
    """정규식이 깨져 0건이 되면 위 두 검사가 조용히 통과한다."""
    found = _raised_codes()
    assert len(found) > 50 and "collection_incomplete" in found
