"""공개 시장 스냅숏(PA 아침 브리핑 숫자 줄) — 오프라인 테스트.

이 저장소는 public 이다. 스냅숏은 고정 지수·ETF·환율·원자재·크립토 대표 목록만 담고,
보유·관심 종목(Drive Portfolio·LUKE_PICKS)에는 닿지 않는다 — 아래 정적 테스트가 강제한다.
"""
import ast
import json
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from scripts import market_snapshot as ms

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "market_snapshot.py"
WORKFLOW = ROOT / ".github" / "workflows" / "market-snapshot.yml"
REQUIREMENTS = ROOT / "requirements-market-snapshot.txt"

# PA 저장소 packages/schemas/src/market-snapshot.ts 의 MARKET_KEYS 와 같은 순서(두 저장소 테스트가 각자 고정)
EXPECTED = [
    ("SPX", "^GSPC"), ("COMP", "^IXIC"), ("DJI", "^DJI"), ("RUT", "^RUT"), ("RSP", "RSP"),
    ("VIX", "^VIX"), ("US10Y", "^TNX"), ("DXY", "DX-Y.NYB"),
    ("XLK", "XLK"), ("XLF", "XLF"), ("XLE", "XLE"), ("XLV", "XLV"), ("XLI", "XLI"), ("XLC", "XLC"),
    ("XLRE", "XLRE"), ("XLU", "XLU"), ("XLP", "XLP"), ("XLB", "XLB"), ("XLY", "XLY"),
    ("KOSPI", "^KS11"), ("KOSDAQ", "^KQ11"), ("USDKRW", "KRW=X"),
    ("GOLD", "GC=F"), ("WTI", "CL=F"), ("BTC", "BTC-USD"), ("ETH", "ETH-USD"),
]
NOW = datetime(2026, 11, 24, 21, 16, 0, tzinfo=timezone.utc)


def test_instrument_list_is_the_fixed_public_allowlist():
    assert [(i.key, i.symbol) for i in ms.INSTRUMENTS] == EXPECTED


def test_instrument_from_closes_uses_last_two_sessions():
    rows = [(date(2026, 11, 20), 100.0), (date(2026, 11, 21), 101.0), (date(2026, 11, 24), 101.505)]
    got = ms.instrument_from_closes("SPX", rows)
    assert got == {"key": "SPX", "sessionDate": "2026-11-24", "close": 101.505, "changePct": 0.5}


@pytest.mark.parametrize("rows", [
    [],
    [(date(2026, 11, 24), 100.0)],
    [(date(2026, 11, 21), 0.0), (date(2026, 11, 24), 100.0)],
    [(date(2026, 11, 21), 100.0), (date(2026, 11, 24), float("nan"))],
    [(date(2026, 11, 21), 100.0), (date(2026, 11, 24), -3.0)],
])
def test_instrument_from_closes_missing_values(rows):
    assert ms.instrument_from_closes("VIX", rows) == {"key": "VIX", "sessionDate": None, "close": None, "changePct": None}


def test_instrument_from_closes_skips_nan_rows_before_taking_last_two():
    rows = [(date(2026, 11, 20), 50.0), (date(2026, 11, 21), float("nan")), (date(2026, 11, 24), 55.0)]
    assert ms.instrument_from_closes("BTC", rows)["changePct"] == 10.0


def test_kimchi_from_state_takes_latest_entry_in_utc():
    state = {"history": [
        {"time": "2026-11-24T08:29:53.367136+09:00", "usdt_kimp": 2.024, "gold_kimp": 1.0346, "usd_krw": 1331.2},
        {"time": "2026-11-24T17:24:40.300207+09:00", "usdt_kimp": 1.8491, "gold_kimp": 0.6086, "usd_krw": 1330.4},
    ]}
    assert ms.kimchi_from_state(state) == {"usdtPct": 1.85, "goldPct": 0.61, "observedAt": "2026-11-24T08:24:40Z"}


@pytest.mark.parametrize("state", [
    None, {}, {"history": []}, {"history": "x"},
    {"history": [{"time": "bad", "usdt_kimp": 1, "gold_kimp": 1}]},
    {"history": [{"time": "2026-11-24T08:29:53+09:00", "usdt_kimp": "1", "gold_kimp": 1}]},
    {"history": [{"time": "2026-11-24T08:29:53+09:00", "usdt_kimp": 99, "gold_kimp": 1}]},
    {"history": [{"time": "2026-11-24T08:29:53", "usdt_kimp": 1, "gold_kimp": 1}]},
])
def test_kimchi_from_state_rejects_bad_shapes(state):
    assert ms.kimchi_from_state(state) is None


def test_build_snapshot_keeps_going_when_one_symbol_fails():
    def fetch_closes(symbols):
        out = {s: [(date(2026, 11, 21), 10.0), (date(2026, 11, 24), 11.0)] for s in symbols}
        del out["^VIX"]
        return out

    snap = ms.build_snapshot(fetch_closes, lambda: {"history": []}, NOW)
    assert snap["v"] == 1
    assert snap["generatedAt"] == "2026-11-24T21:16:00Z"
    assert [i["key"] for i in snap["instruments"]] == [k for k, _ in EXPECTED]
    vix = next(i for i in snap["instruments"] if i["key"] == "VIX")
    assert vix["close"] is None and vix["changePct"] is None
    assert next(i for i in snap["instruments"] if i["key"] == "SPX")["changePct"] == 10.0
    assert snap["kimchi"] is None
    assert len(json.dumps(snap).encode("utf-8")) <= ms.MAX_BYTES


def test_build_snapshot_survives_total_fetch_failure():
    def boom(_symbols):
        raise RuntimeError("yahoo down")

    def state_boom():
        raise OSError("no network")

    snap = ms.build_snapshot(boom, state_boom, NOW)
    assert all(i["close"] is None for i in snap["instruments"])
    assert snap["kimchi"] is None


def test_closes_from_frame_reads_close_column_per_symbol():
    pd = pytest.importorskip("pandas")
    idx = pd.to_datetime(["2026-11-21", "2026-11-24"])
    cols = pd.MultiIndex.from_tuples([("^GSPC", "Close"), ("^GSPC", "Open"), ("BTC-USD", "Close")])
    frame = pd.DataFrame([[1.0, 0.5, 2.0], [1.1, 0.6, float("nan")]], index=idx, columns=cols)
    got = ms.closes_from_frame(frame, ["^GSPC", "BTC-USD", "^VIX"])
    assert got["^GSPC"] == [(date(2026, 11, 21), 1.0), (date(2026, 11, 24), 1.1)]
    assert len(got["BTC-USD"]) == 2 and math.isnan(got["BTC-USD"][1][1])
    assert "^VIX" not in got


def test_write_snapshot_writes_json(tmp_path):
    out = tmp_path / "snapshot.json"
    ms.write_snapshot({"v": 1, "generatedAt": "x", "instruments": [], "kimchi": None}, out)
    assert json.loads(out.read_text(encoding="utf-8"))["v"] == 1


# ── 정적 개인정보 검사 ────────────────────────────────────────────────────────

def test_script_imports_nothing_that_can_reach_private_data():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add((node.module or "").split(".")[0])
    assert names <= {"__future__", "argparse", "dataclasses", "datetime", "json", "math", "pathlib", "sys", "typing", "requests", "yfinance"}, names


def test_script_has_no_private_source_markers():
    src = SCRIPT.read_text(encoding="utf-8")
    for marker in ("GDRIVE", "PICKS", "Portfolio", "environ", "getenv", "secrets", "drive", "gspread"):
        assert marker.lower() not in src.lower(), marker


def test_script_ticker_like_strings_are_only_the_allowlist():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    allowed = {s for _, s in EXPECTED} | {k for k, _ in EXPECTED}
    ticker = re.compile(r"^(\^[A-Z0-9]{2,6}|[A-Z]{2,6}(-USD|=X|=F|\.NYB)?)$")
    found = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and ticker.match(n.value)}
    # 티커 모양이지만 종목이 아닌 상수(열 이름·HTTP 메서드 등)만 예외로 둔다
    assert found - allowed <= {"Close", "GET"}, found - allowed


def test_workflow_uses_no_secrets_and_read_only_permissions():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "secrets." not in text
    assert re.search(r"^permissions:\n  contents: read\n(?!  )", text, re.M)
    assert "id-token" not in text and "write" not in text.replace("write_snapshot", "")
    assert "daily-market-drive-writer" not in text
    assert re.search(r"^on:\n  workflow_dispatch:", text, re.M) and "schedule:" not in text
    assert "name: market-snapshot\n" in text and "path: snapshot.json" in text
    assert "requirements-market-snapshot.txt" in text and "requirements.txt" not in text.replace("requirements-market-snapshot.txt", "")


def test_requirements_are_pinned_and_ascii():
    raw = REQUIREMENTS.read_bytes()
    raw.decode("ascii")
    lines = [l for l in raw.decode().splitlines() if l and not l.startswith("#")]
    assert lines and all(re.match(r"^[A-Za-z0-9_.-]+==[0-9][0-9A-Za-z.]*$", l) for l in lines), lines
    assert not any(l.split("==")[0].lower().startswith(("google", "gspread", "pydrive")) for l in lines)
