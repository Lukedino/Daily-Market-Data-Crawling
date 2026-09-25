"""KRX 정규장 개장일 판정 — 휴장일을 '갭'으로 세지 않기 위한 오프라인 달력.

2026-09-25(추석) 실행이 실패했다: 저장본 마지막 날짜 09-23, 어제 09-24 가 평일이라
`pd.bdate_range` 가 갭으로 판정 → yfinance 갭 백필 → 야후는 휴장 구간을 물으면
**직전 거래일(09-23) 봉을 돌려주므로** 결과가 비어 있지 않다 → `validate_price_basis`
가 yfinance 출처를 거부 → exit 1(실패 알림). 09-24 는 어제(09-23)가 저장본과 같아
갭이 없었고 FDR 경로로 성공했다. 휴장일은 갭이 아니다.

KIS-Trading `trader/krx_calendar.py` 와 같은 출처를 쓴다 — `holidays.financial_holidays("XKRX")`.
법정·대체 공휴일, 선거일, 연말 휴장(12/31)을 포함하고 네트워크·키가 필요 없다.
한계: 정부가 늦게 지정하는 임시공휴일은 고정된 버전이 모른다. 그날은 FDR 원천이
직전 세션을 돌려주고 갭 백필이 지금처럼 걸리므로 실패 알림으로 드러난다.
달력 조회 자체가 실패하면 "unknown" 을 돌려주고 호출부는 평일 기준으로 진행한다.
"""
from datetime import date, timedelta
from functools import lru_cache
from typing import Iterator, Optional


@lru_cache(maxsize=8)
def _xkrx(year: int):
    import holidays
    return holidays.financial_holidays("XKRX", years=year, language="ko")


def session_status(day: date) -> tuple[str, str]:
    """("open"|"closed"|"unknown", 휴장 사유). 판정 불가면 ("unknown", "")."""
    if day.weekday() >= 5:
        return "closed", "주말"
    try:
        name = _xkrx(day.year).get(day)
    except Exception:
        return "unknown", ""
    return ("closed", str(name)) if name else ("open", "")


def trading_days(start: date, end: date) -> list[date]:
    """start~end(포함)의 KRX 거래일. 달력을 모르는 해는 평일을 거래일로 본다(기존 동작)."""
    out: list[date] = []
    day = start
    while day <= end:
        status, _ = session_status(day)
        if status != "closed":
            out.append(day)
        day += timedelta(days=1)
    return out
