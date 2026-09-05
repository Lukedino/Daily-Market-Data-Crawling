"""US 유니버스 티커 정규화 — 거래소 접미사 보존 (2026-09-05).

배경: `_normalize_ticker` 가 모든 '.' 을 '-' 로 바꿔 Luke Picks 의 `U-UN.TO`(TSX)가
`U-UN-TO` 로 야후에 나가 매일 404 → 행이 없으니 매일 "신규 종목"으로 백필을 다시
시도했다(Mr.Stock-Market-Crawler 가 2026-05-06 [BUG-TICKER] 로 고친 것과 같은 결함).

규칙: 끝의 `.XX`/`.XXX`(2~3자 알파벳 — .TO .HK .AS .PA .DE .SW .TWO …)와 1자 거래소
코드 `.L`(런던) `.V`(TSX-V) `.F`(프랑크푸르트) `.T`(도쿄)는 보존한다. 그 외 1자 접미사
(`BRK.B`, `BF.B` 같은 클래스 구분자)와 종목 중간의 '.' 은 기존대로 '-' 로 바꾼다 —
야후가 클래스주는 `BRK-B` 형식만 받기 때문이다. 한국 `.KS/.KQ` 는 Luke Picks 로더가
그 전에 걸러내므로(`_KR_TICKER_PATTERN`) US 파일에 섞이지 않는다.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.ohlc_collector import _normalize_ticker


@pytest.mark.parametrize("raw, expected", [
    ("U-UN.TO", "U-UN.TO"),      # 실제 사고 종목 — TSX
    ("SHOP.TO", "SHOP.TO"),
    ("0700.HK", "0700.HK"),
    ("ASML.AS", "ASML.AS"),
    ("MC.PA", "MC.PA"),
    ("SAP.DE", "SAP.DE"),
    ("2330.TWO", "2330.TWO"),    # 3자 접미사
    ("HSBA.L", "HSBA.L"),        # 1자 거래소 — 런던
    ("7203.T", "7203.T"),        # 1자 거래소 — 도쿄
    ("u-un.to ", "U-UN.TO"),     # 대문자화·공백 제거 뒤에도 접미사 유지
])
def test_exchange_suffix_preserved(raw, expected):
    assert _normalize_ticker(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("BRK.B", "BRK-B"),          # 클래스 구분자 — 야후는 하이픈만 받는다
    ("BF.B", "BF-B"),
    ("brk.b", "BRK-B"),
    ("HEI.A", "HEI-A"),
])
def test_class_share_dot_converted(raw, expected):
    assert _normalize_ticker(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("$AAPL", "AAPL"),
    ("  nvda ", "NVDA"),
    ("BRK-B", "BRK-B"),          # 이미 야후 형식이면 그대로
    ("ABC.DEFG", "ABC-DEFG"),    # 4자 이상은 거래소 코드가 아니다 — 기존 동작
    ("A.B.TO", "A-B.TO"),        # 중간 '.' 만 변환, 끝 접미사 유지
    ("AB#C!", "ABC"),
])
def test_other_cleanup_unchanged(raw, expected):
    assert _normalize_ticker(raw) == expected
