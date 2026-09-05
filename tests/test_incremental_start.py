"""증분 업데이트 시작일 — 크립토는 최근 1주를 매번 재조회한다 (2026-09-05).

배경: 2026-08-31 크립토 일봉이 Drive 파일에서 통째로 빠졌다(08-30 203종목 → 08-31 0
→ 09-01 202). 09-01 05:26Z 실행이 08-30~09-02 를 조회했는데 199종목에 394행(종목당
2일)만 왔다 — 그 시각 야후에 08-31 봉이 아직 없었다. 이후 실행은 커서를 하루만
물리므로(`start = last_date`) 08-31 은 다시 조회되지 않아 영구 구멍이 됐고, 신규
종목 백필이 그 날짜에 행을 넣자 연속성 게이트가 발동해 daily 가 3일 연속 죽었다.

→ 크립토 증분 조회를 `last_date - 6` 부터로 넓힌다. 200종목 × 7일이라 비용은
미미하고, 야후가 늦게 낸 봉·일시 결손은 다음 실행이 (Ticker, Date) keep="last"
병합으로 자연 치유한다. US 는 장마감 후 크롤이라 기존(+1일) 유지.
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


def test_us_start_is_next_day():
    assert oc.incremental_start_date("us", date(2026, 9, 3)) == date(2026, 9, 4)
