"""
크립토 심볼 해석 테스트 (Daily-Market-Data-Crawling).

Yahoo는 심볼이 겹칠 때 CMC id 접미사를 붙인 티커를 새 코인에 배정하고,
plain {SYM}-USD는 먼저 그 심볼을 쓰던 **다른 토큰**에 남겨둔다. 그래서 plain
티커 조회는 에러 없이 성공하지만 완전히 다른 자산의 시세를 준다.

실측 (2026-08-13, CMC Top 200 중 13종목):
  ARB   plain $0.000629 vs 실제 Arbitrum $0.0759   (121배)
  JUP   plain $0.000260 vs 실제 Jupiter  $0.1692   (8280배)
  AERO  plain $0.000036 vs 실제 Aerodrome $0.4112  (11538배)
누락은 시끄럽게 드러나지만 이건 조용히 틀린 값이라 더 위험하다.
"""
import sys

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from data.ohlc_collector import resolve_symbol_overrides


CMC = {
    "ARB":  (11841, 0.0759),
    "JUP":  (29210, 0.1692),
    "SHIB": (5994,  0.0000044),
    "LINK": (1975,  8.65),
    "USDG": (33793, 0.9997),
}


def test_replaces_symbol_whose_price_is_wildly_off():
    """plain 티커가 CMC 가격과 자릿수부터 다르면 다른 토큰이다."""
    observed = {"ARB-USD": 0.000629}
    assert resolve_symbol_overrides(["ARB-USD"], observed, CMC) == {"ARB-USD": "ARB11841-USD"}


def test_keeps_symbol_when_price_matches_cmc():
    """LINK/SHIB처럼 plain 티커가 정상인 종목은 건드리지 않는다."""
    observed = {"LINK-USD": 8.6499, "SHIB-USD": 0.00000439}
    assert resolve_symbol_overrides(["LINK-USD", "SHIB-USD"], observed, CMC) == {}


def test_tolerates_ordinary_exchange_price_differences():
    """거래소·시점 차이로 몇 % 벌어지는 것은 정상이므로 교체하지 않는다."""
    observed = {"LINK-USD": 8.65 * 1.15}
    assert resolve_symbol_overrides(["LINK-USD"], observed, CMC) == {}


def test_detects_mismatch_in_either_direction():
    """관측가가 CMC보다 훨씬 높은 경우(NEX 사례)도 잡아야 한다."""
    observed = {"USDG-USD": 5.4465}
    assert resolve_symbol_overrides(["USDG-USD"], observed, CMC) == {"USDG-USD": "USDG33793-USD"}


def test_skips_when_no_observation():
    """조회 자체가 안 된 종목은 여기서 다루지 않는다 (Search 폴백 담당)."""
    assert resolve_symbol_overrides(["ARB-USD"], {}, CMC) == {}


def test_skips_when_symbol_absent_from_cmc():
    """CMC에 없으면 비교 기준이 없으므로 그대로 둔다."""
    assert resolve_symbol_overrides(["FOO-USD"], {"FOO-USD": 1.0}, CMC) == {}


def test_skips_non_positive_prices():
    assert resolve_symbol_overrides(["ARB-USD"], {"ARB-USD": 0.0}, CMC) == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
