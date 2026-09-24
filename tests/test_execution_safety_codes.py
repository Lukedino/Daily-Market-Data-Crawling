"""공개 artifact 의 실패 코드 허용목록이 소스와 어긋나지 않게 한다.

2026-09-22 OHLC 장애가 이틀 동안 원인 불명이었던 직접적인 이유다. `collection_incomplete`
가 raise 되는데 `_ERROR_CODES` 에 없어 로그에 `code=` 가 한 번도 찍히지 않았고, 개명하며
버려진 `incremental_incomplete` 는 아무도 raise 하지 않는데 목록에 남아 있었다. 실패한
실행의 artifact 전체가 `[ERROR] execution_safety.py:155 operation_failed` 한 줄이었다.

스캔은 정규식이 아니라 AST 로 한다. 처음엔 `raise X("code")` 만 찾는 정규식이었는데
`FinancialsPublishError` 가 코드를 `super().__init__("financials_publish_failed")` 로
박아 두는 바람에 **테스트는 초록인데 목록엔 없는** 상태를 그대로 통과시켰다.
"""
import ast
from pathlib import Path

import pytest

from data import execution_safety

_ROOT = Path(__file__).resolve().parents[1]


def _sources():
    yield _ROOT / "main.py"
    for folder in ("data", "scripts"):
        yield from sorted((_ROOT / folder).rglob("*.py"))


def _first_literal(call):
    """호출의 첫 인자가 문자열 리터럴이면 그 값. `cli_entry` 가 읽는 것이 args[0] 이다."""
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return None


class _CodeScan(ast.NodeVisitor):
    def __init__(self):
        self.codes = set()

    def visit_Raise(self, node):
        if isinstance(node.exc, ast.Call):
            self._collect(_first_literal(node.exc))
        self.generic_visit(node)

    def visit_Call(self, node):
        # 예외 클래스가 스스로 코드를 박는 경우: super().__init__("code")
        if isinstance(node.func, ast.Attribute) and node.func.attr == "__init__":
            self._collect(_first_literal(node))
        self.generic_visit(node)

    def _collect(self, value):
        # 사람이 읽는 문장(`"FinanceDataReader를 설치하세요"`)은 코드가 아니다.
        if value and value.replace("_", "").isalnum() and value == value.lower() and " " not in value:
            self.codes.add(value)


def _raised_codes():
    found = set()
    for path in _sources():
        scan = _CodeScan()
        scan.visit(ast.parse(path.read_text(encoding="utf-8")))
        found.update(scan.codes)
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
    """스캔이 깨져 0건이 되면 위 두 검사가 조용히 통과한다."""
    found = _raised_codes()
    assert len(found) > 50
    assert {"collection_incomplete", "financials_publish_failed"} <= found


@pytest.mark.parametrize("source, expected", [
    ('raise KrStateError("kr_source_failed")', {"kr_source_failed"}),
    ('class E(X):\n    def __init__(self):\n        super().__init__("publish_failed")', {"publish_failed"}),
    ('raise ImportError("yfinance를 설치하세요: pip install yfinance")', set()),
    ('raise ValueError()', set()),
    ('raise DriveSyncError(f"업로드 실패 {files}")', set()),
])
def test_the_scan_separates_codes_from_human_messages(source, expected):
    scan = _CodeScan()
    scan.visit(ast.parse(source))
    assert scan.codes == expected
