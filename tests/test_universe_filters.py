"""유니버스 필터 2건 (2026-09-05 바로 착수 묶음).

1) KR 재무 유니버스는 보통주만 — KRX 코드 끝자리 0 = 보통주, 5/7/9 = 우선주, 영문 포함 =
   특수코드. 우선주·특수코드는 발행사 corp_code 가 없어 DART 조회가 매 실행 실패만 남긴다
   (2026-09-03 첫 수집 실패 34종목 전부 이 부류).
2) 크립토 유니버스에서 비ASCII 심볼('币安人生' 등)은 야후에 존재할 수 없어 제외 —
   남겨두면 매 실행 신규 종목 백필을 시도해 404/레이트리밋 노이즈만 남긴다.
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.kr_financials_collector import build_universe
from data.ohlc_collector import _cmc_items_to_tickers


def _marcap(codes, marcaps):
    return pd.DataFrame({
        "Code": codes, "Marcap": marcaps, "Stocks": [100] * len(codes),
        "Date": [date(2026, 9, 5)] * len(codes),
    })


class TestKrCommonStockOnly:
    def test_preferred_and_special_codes_excluded(self):
        df = _marcap(["005930", "005935", "0126Z0", "000660", "00680K", "051905"],
                     [900, 800, 700, 600, 500, 400])
        uni = build_universe(df, top_n=10)
        assert list(uni["Code"]) == ["005930", "000660"]

    def test_top_n_counts_common_stocks_only(self):
        # 우선주가 시총 상위에 끼어도 top_n 슬롯을 잡아먹지 않는다
        df = _marcap(["005935", "005930", "000660", "035420"], [950, 900, 800, 700])
        uni = build_universe(df, top_n=2)
        assert list(uni["Code"]) == ["005930", "000660"]

    def test_short_code_is_padded_then_filtered(self):
        df = _marcap(["660", "5935"], [10, 20])           # zfill 후 000660(보통주)·005935(우선주)
        assert list(build_universe(df, 10)["Code"]) == ["000660"]


class TestCmcNonAsciiSymbols:
    def test_non_ascii_symbol_excluded(self):
        items = [{"symbol": "BTC"}, {"symbol": "币安人生"}, {"symbol": "eth"}, {"symbol": ""}]
        assert _cmc_items_to_tickers(items) == ["BTC-USD", "ETH-USD"]

    def test_top_n_applies_to_raw_list(self):
        items = [{"symbol": f"C{i}"} for i in range(5)]
        assert _cmc_items_to_tickers(items, top_n=3) == ["C0-USD", "C1-USD", "C2-USD"]

    def test_empty_and_missing_symbol(self):
        assert _cmc_items_to_tickers([]) == []
        assert _cmc_items_to_tickers([{"name": "x"}]) == []
