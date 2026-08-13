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


# ── 중복 조회 방지 ────────────────────────────────────────────────────────────
# 상장 전 구간(예: 2021년에 SUI/ONDO/TAO)은 정상적으로 빈 응답이 온다. 이때
# _retry_crypto_missing이 "빈 응답 = 심볼이 틀렸다"로 보고 재탐색에 들어가는데,
# 이미 override로 {SYM}{id}-USD를 조회했다면 같은 심볼을 또 받는 것은 순수 낭비다.
# 백필은 연도 × 종목으로 도는 탓에 이 낭비가 곱해진다.

def test_retry_skips_symbol_already_queried_in_main_batch(monkeypatch):
    import data.ohlc_collector as oc

    calls = []

    def _fake_download(symbol, **kwargs):
        calls.append(symbol)
        import pandas as pd
        return pd.DataFrame()

    import yfinance as yf
    monkeypatch.setattr(yf, "download", _fake_download)
    monkeypatch.setattr(oc, "_cmc_listing", lambda: {"M": (35491, 1.09)})
    monkeypatch.setattr(oc, "_search_crypto_yahoo_ticker", lambda sym: None)

    # 본 배치에서 이미 M35491-USD로 조회했고 (상장 전이라) 빈 응답을 받은 상황
    oc._retry_crypto_missing(
        ["M-USD"], "2021-01-01", "2022-01-01",
        already_queried={"M-USD": "M35491-USD"},
    )

    assert "M35491-USD" not in calls, "이미 조회한 심볼을 재조회했다"


def test_retry_still_tries_cmc_id_when_main_batch_used_plain_symbol(monkeypatch):
    """override가 없었던 종목은 기존대로 CMC id 후보를 시도해야 한다."""
    import data.ohlc_collector as oc

    calls = []

    def _fake_download(symbol, **kwargs):
        calls.append(symbol)
        import pandas as pd
        return pd.DataFrame()

    import yfinance as yf
    monkeypatch.setattr(yf, "download", _fake_download)
    monkeypatch.setattr(oc, "_cmc_listing", lambda: {"HYPE": (32196, 54.0)})
    monkeypatch.setattr(oc, "_search_crypto_yahoo_ticker", lambda sym: None)

    oc._retry_crypto_missing(
        ["HYPE-USD"], "2026-08-01", "2026-08-13",
        already_queried={"HYPE-USD": "HYPE-USD"},
    )

    assert "HYPE32196-USD" in calls


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
