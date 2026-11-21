"""공개 시장 스냅숏 — Personal Assistant 아침 브리핑의 숫자 줄 원천.

고정 지수·ETF·환율·원자재·크립토 대표 목록의 마지막 두 일봉 종가와 공개 김치 프리미엄 모니터의
최신 값만 snapshot.json 으로 쓴다. 자격 증명·저장소 데이터 모듈(data/·config)·클라우드 저장소를 쓰지 않는다.
보유·관심 종목은 이 스크립트가 닿을 수 없다 — tests/test_market_snapshot.py 가 정적으로 강제한다.

계약(PA packages/schemas/src/market-snapshot.ts 와 같다):
  {"v": 1, "generatedAt": "YYYY-MM-DDTHH:MM:SSZ",
   "instruments": [{"key", "sessionDate"|null, "close"|null, "changePct"|null}, ...],
   "kimchi": {"usdtPct", "goldPct", "observedAt"} | null}
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

MAX_BYTES = 16 * 1024
KIMCHI_STATE_URL = "https://raw.githubusercontent.com/Lukedino/Kimchi-Premium-Monitor/main/state.json"
KIMCHI_MAX_BYTES = 1024 * 1024
KIMCHI_ABS_LIMIT = 30.0
HTTP_TIMEOUT_SEC = 20


@dataclass(frozen=True)
class Instrument:
    key: str
    symbol: str


# 순서·키는 PA 의 MARKET_KEYS 와 같아야 한다(두 저장소 테스트가 각자 고정)
INSTRUMENTS: tuple[Instrument, ...] = (
    Instrument("SPX", "^GSPC"), Instrument("COMP", "^IXIC"), Instrument("DJI", "^DJI"), Instrument("RUT", "^RUT"),
    Instrument("RSP", "RSP"), Instrument("VIX", "^VIX"), Instrument("US10Y", "^TNX"), Instrument("DXY", "DX-Y.NYB"),
    Instrument("XLK", "XLK"), Instrument("XLF", "XLF"), Instrument("XLE", "XLE"), Instrument("XLV", "XLV"),
    Instrument("XLI", "XLI"), Instrument("XLC", "XLC"), Instrument("XLRE", "XLRE"), Instrument("XLU", "XLU"),
    Instrument("XLP", "XLP"), Instrument("XLB", "XLB"), Instrument("XLY", "XLY"),
    Instrument("KOSPI", "^KS11"), Instrument("KOSDAQ", "^KQ11"), Instrument("USDKRW", "KRW=X"),
    Instrument("GOLD", "GC=F"), Instrument("WTI", "CL=F"), Instrument("BTC", "BTC-USD"), Instrument("ETH", "ETH-USD"),
)

Closes = list[tuple[date, float]]


def _usable(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def instrument_from_closes(key: str, rows: Closes) -> dict:
    """결측(NaN·0 이하)을 뺀 뒤 마지막 두 종가로 등락률을 만든다. 두 개가 안 되면 값 전부 None."""
    clean = [(d, float(c)) for d, c in sorted(rows, key=lambda r: r[0]) if _usable(c)]
    if len(clean) < 2:
        return {"key": key, "sessionDate": None, "close": None, "changePct": None}
    (_, prev), (last_date, last) = clean[-2], clean[-1]
    return {
        "key": key,
        "sessionDate": last_date.isoformat(),
        "close": round(last, 4),
        "changePct": round((last / prev - 1.0) * 100.0, 2),
    }


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value) if abs(value) <= KIMCHI_ABS_LIMIT else None


def kimchi_from_state(state: object) -> dict | None:
    """김치 프리미엄 모니터 state.json 의 마지막 이력 항목. 형식이 하나라도 어긋나면 None."""
    if not isinstance(state, dict):
        return None
    history = state.get("history")
    if not isinstance(history, list) or not history or not isinstance(history[-1], dict):
        return None
    last = history[-1]
    usdt, gold = _number(last.get("usdt_kimp")), _number(last.get("gold_kimp"))
    raw_time = last.get("time")
    if usdt is None or gold is None or not isinstance(raw_time, str):
        return None
    try:
        observed = datetime.fromisoformat(raw_time)
    except ValueError:
        return None
    if observed.tzinfo is None:
        return None
    observed = observed.astimezone(timezone.utc).replace(microsecond=0)
    return {"usdtPct": round(usdt, 2), "goldPct": round(gold, 2), "observedAt": observed.strftime("%Y-%m-%dT%H:%M:%SZ")}


def closes_from_frame(frame, symbols: list[str]) -> dict[str, Closes]:
    """yfinance download(group_by='ticker') 결과에서 심볼별 (날짜, 종가) 목록. 열이 없는 심볼은 뺀다."""
    out: dict[str, Closes] = {}
    for symbol in symbols:
        column = (symbol, "Close")
        if column not in frame.columns:
            continue
        series = frame[column]
        out[symbol] = [(ts.date(), float(v)) for ts, v in series.items()]
    return out


def fetch_closes_yahoo(symbols: list[str]) -> dict[str, Closes]:
    import yfinance as yf

    frame = yf.download(symbols, period="10d", interval="1d", group_by="ticker", auto_adjust=False,
                        progress=False, threads=False)
    return closes_from_frame(frame, symbols)


def fetch_kimchi_state() -> object:
    import requests

    response = requests.get(KIMCHI_STATE_URL, timeout=HTTP_TIMEOUT_SEC)
    response.raise_for_status()
    if len(response.content) > KIMCHI_MAX_BYTES:
        return None
    return response.json()


def build_snapshot(fetch_closes: Callable[[list[str]], dict[str, Closes]], fetch_state: Callable[[], object], now: datetime) -> dict:
    symbols = [i.symbol for i in INSTRUMENTS]
    try:
        closes = fetch_closes(symbols)
    except Exception as exc:  # 원천 전면 실패도 스냅숏은 만든다(PA 가 UNUSABLE 로 판정)
        print(f"market_snapshot: closes unavailable ({type(exc).__name__})")
        closes = {}
    try:
        kimchi = kimchi_from_state(fetch_state())
    except Exception as exc:
        print(f"market_snapshot: kimchi unavailable ({type(exc).__name__})")
        kimchi = None
    instruments = [instrument_from_closes(i.key, closes.get(i.symbol, [])) for i in INSTRUMENTS]
    snapshot = {
        "v": 1,
        "generatedAt": now.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "instruments": instruments,
        "kimchi": kimchi,
    }
    if len(json.dumps(snapshot).encode("utf-8")) > MAX_BYTES:
        raise ValueError("snapshot too large")
    return snapshot


def write_snapshot(snapshot: dict, path: Path) -> None:
    path.write_text(json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="public market snapshot for the PA morning briefing")
    parser.add_argument("--out", default="snapshot.json")
    args = parser.parse_args(argv)
    snapshot = build_snapshot(fetch_closes_yahoo, fetch_kimchi_state, datetime.now(timezone.utc))
    write_snapshot(snapshot, Path(args.out))
    filled = sum(1 for i in snapshot["instruments"] if i["close"] is not None)
    # 공개 로그에는 개수만 남긴다(값은 artifact 에만)
    print(f"market_snapshot: instruments={len(snapshot['instruments'])} filled={filled} kimchi={'yes' if snapshot['kimchi'] else 'no'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
