"""
_extract_ticker_df의 단일 티커 배치 처리 테스트.

배치 크기가 1이면 무조건 flat 컬럼이라고 가정하고 raw를 그대로 돌려줬는데,
최신 yfinance는 티커가 하나여도 MultiIndex 컬럼을 준다. 그러면 이후
_finalize_ticker_frame이 'Close'를 못 찾아 예외가 나고 그 종목이 통째로 실패한다.

_BATCH_SIZE가 50이라 배치가 1이 되는 경우는 드물어 보이지만, 유니버스가
201종목이면 마지막 배치가 정확히 1종목이다 (크립토 유니버스가 실제로 200~202개).
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.ohlc_collector import _extract_ticker_df


def _multiindex_raw(ticker: str) -> pd.DataFrame:
    idx = pd.date_range("2026-08-01", periods=3, freq="D", name="Date")
    cols = pd.MultiIndex.from_product([[ticker], ["Open", "High", "Low", "Close", "Volume"]])
    return pd.DataFrame(
        [[1.0, 1.1, 0.9, 1.05, 1e6]] * 3, index=idx, columns=cols
    )


def _flat_raw() -> pd.DataFrame:
    idx = pd.date_range("2026-08-01", periods=3, freq="D", name="Date")
    return pd.DataFrame(
        {"Open": [1.0] * 3, "High": [1.1] * 3, "Low": [0.9] * 3,
         "Close": [1.05] * 3, "Volume": [1e6] * 3},
        index=idx,
    )


def test_single_ticker_batch_with_multiindex_columns_is_flattened():
    out = _extract_ticker_df(_multiindex_raw("UNI7083-USD"), "UNI7083-USD", batch_size=1)

    assert out is not None
    assert not isinstance(out.columns, pd.MultiIndex), "MultiIndex가 그대로 남았다"
    assert "Close" in out.columns


def test_single_ticker_batch_with_flat_columns_still_works():
    out = _extract_ticker_df(_flat_raw(), "UNI-USD", batch_size=1)

    assert out is not None
    assert "Close" in out.columns


def test_multi_ticker_batch_unchanged():
    idx = pd.date_range("2026-08-01", periods=3, freq="D", name="Date")
    cols = pd.MultiIndex.from_product([["AAA-USD", "BBB-USD"],
                                       ["Open", "High", "Low", "Close", "Volume"]])
    raw = pd.DataFrame([[1.0, 1.1, 0.9, 1.05, 1e6] * 2] * 3, index=idx, columns=cols)

    out = _extract_ticker_df(raw, "BBB-USD", batch_size=2)

    assert out is not None
    assert "Close" in out.columns
