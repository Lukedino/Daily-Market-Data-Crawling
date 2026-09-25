"""증분 업데이트 시작일 — 크립토는 최근 1주를 매번 재조회한다 (2026-09-05).

배경: 2026-08-31 크립토 일봉이 Drive 파일에서 통째로 빠졌다(08-30 203종목 → 08-31 0
→ 09-01 202). 09-01 05:26Z 실행이 08-30~09-02 를 조회했는데 199종목에 394행(종목당
2일)만 왔다 — 그 시각 야후에 08-31 봉이 아직 없었다. 이후 실행은 커서를 하루만
물리므로(`start = last_date`) 08-31 은 다시 조회되지 않아 영구 구멍이 됐고, 신규
종목 백필이 그 날짜에 행을 넣자 연속성 게이트가 발동해 daily 가 3일 연속 죽었다.

→ 크립토 증분 조회를 `last_date - 6` 부터로 넓힌다. 200종목 × 7일이라 비용은
미미하고, 야후가 늦게 낸 봉·일시 결손은 다음 실행이 (Ticker, Date) keep="last"
병합으로 자연 치유한다. US도 마지막 저장 세션을 다시 받아 조정 기준을 비교한다.
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data import ohlc_collector as oc


def test_crypto_lookback_is_one_week():
    assert oc.CRYPTO_LOOKBACK_DAYS == 7


def test_crypto_start_covers_last_week():
    assert oc.incremental_start_date("crypto", date(2026, 9, 3)) == date(2026, 8, 28)


def test_crypto_lookback_covers_the_2026_08_31_hole():
    start = oc.incremental_start_date("crypto", date(2026, 9, 3))
    assert start <= date(2026, 8, 31) < date(2026, 9, 3)


def test_us_start_covers_last_week_like_crypto():
    """DM-05 (2026-09-26): 허용된 누락 종목의 날짜가 종목 단위 영구 구멍이 되지 않도록
    US 도 1주를 다시 받는다. 직전 저장 세션은 여전히 창 안이라 기준 비교도 그대로다."""
    assert oc.US_LOOKBACK_DAYS == 7
    start = oc.incremental_start_date("us", date(2026, 9, 3))
    assert start == date(2026, 8, 28) and start < date(2026, 9, 3)


def test_us_window_recovers_a_tolerated_miss_from_the_previous_run():
    """어제 5% 안에서 빠진 종목의 봉(어제 날짜)이 오늘 창 안에 있어야 다시 요청된다."""
    yesterday, today = date(2026, 9, 2), date(2026, 9, 3)
    assert oc.incremental_start_date("us", today) <= yesterday
