"""
save_year의 종목 단위 교체 테스트.

기존 save_year는 (Ticker, Date) 행 단위로 병합한다. 그래서 잘못된 토큰 데이터가
이미 저장돼 있고 재수집에서 그 날짜에 데이터가 안 나오면(예: Arbitrum은 2023년
출시라 ARB11841-USD에 2022년 데이터가 없음) **옛 오염 행이 그대로 살아남는다.**
실제로 crypto 2020~2026 재백필 후에도 61/228 종목의 접합이 그대로였다.

행 단위 병합 자체는 필요하다 — 그게 없으면 이번 유니버스에 없는 종목의 과거
데이터가 통째로 날아간다([BUG-BACKFILL-REPLACE] 참조). 그래서 "이번에 다시
받기로 한 종목만" 통째로 교체하고 나머지는 건드리지 않는 방식이어야 한다.
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _rows(ticker: str, dates: list[str], price: float) -> pd.DataFrame:
    return pd.DataFrame({
        "Ticker": [ticker] * len(dates),
        "Date": [date.fromisoformat(d) for d in dates],
        "Open": [price] * len(dates), "High": [price] * len(dates),
        "Low": [price] * len(dates), "Close": [price] * len(dates),
        "Volume": [1e6] * len(dates),
    })


@pytest.fixture
def db(tmp_path, monkeypatch):
    from data import ohlc_db
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    return ohlc_db


def test_replace_tickers_purges_stale_rows_even_without_new_data(db):
    """
    교체 대상 종목은 새 데이터가 그 날짜에 없더라도 옛 행이 남으면 안 된다.
    ARB 2022년(상장 전) 오염 행이 정확히 이 경우다.
    """
    db.save_year(_rows("ARB-USD", ["2022-03-01", "2022-03-02"], 0.000345), "crypto", 2022)
    db.save_year(_rows("BTC-USD", ["2022-03-01"], 40000.0), "crypto", 2022)

    # 재수집: ARB는 올바른 심볼에 2022년 데이터가 없어 빈 결과
    db.save_year(pd.DataFrame(columns=db._SCHEMA_COLS), "crypto", 2022,
                 replace_tickers=["ARB-USD"])

    out = db.load_year("crypto", 2022)
    assert "ARB-USD" not in set(out["Ticker"]), "오염된 옛 행이 남았다"
    assert "BTC-USD" in set(out["Ticker"]), "교체 대상이 아닌 종목까지 지웠다"


def test_replace_tickers_keeps_untouched_tickers(db):
    """이번 유니버스에 없는 종목의 과거 데이터는 보존해야 한다."""
    db.save_year(_rows("OLD-USD", ["2022-01-05"], 1.23), "crypto", 2022)

    db.save_year(_rows("BTC-USD", ["2022-01-05"], 41000.0), "crypto", 2022,
                 replace_tickers=["BTC-USD"])

    out = db.load_year("crypto", 2022)
    assert set(out["Ticker"]) == {"OLD-USD", "BTC-USD"}


def test_replace_tickers_overwrites_with_new_values(db):
    db.save_year(_rows("UNI-USD", ["2025-06-02"], 0.000189), "crypto", 2025)

    db.save_year(_rows("UNI-USD", ["2025-06-02"], 7.16), "crypto", 2025,
                 replace_tickers=["UNI-USD"])

    out = db.load_year("crypto", 2025)
    assert len(out) == 1
    assert float(out["Close"].iloc[0]) == pytest.approx(7.16)


def test_purge_scope_excludes_failed_and_non_overridden(db):
    """
    purge 대상은 "심볼이 잘못됐다고 판명된 종목" 중 "이번 조회가 하드 실패하지
    않은 것"으로만 좁혀야 한다.

    ⚠️ 유니버스 전체를 무조건 purge하면 배치 다운로드가 레이트리밋으로 실패했을 때
       그 50종목이 기존 데이터까지 통째로 잃는다. 실제로 이렇게 crypto 2022년이
       161종목 → 33종목으로 줄었다.
    """
    from data.ohlc_collector import purge_targets

    overrides = {"UNI-USD": "UNI7083-USD", "ARB-USD": "ARB11841-USD"}
    failed = ["ARB-USD"]

    targets = purge_targets(overrides, failed)

    assert targets == ["UNI-USD"]


def test_purge_scope_is_empty_without_overrides(db):
    """교체 판정이 없으면 아무것도 지우지 않는다 — 기존 병합 동작 유지."""
    from data.ohlc_collector import purge_targets

    assert purge_targets({}, []) == []


def test_without_replace_tickers_behaviour_is_unchanged(db):
    """기존 호출부(행 단위 병합)는 그대로 동작해야 한다."""
    db.save_year(_rows("BTC-USD", ["2022-01-05", "2022-01-06"], 40000.0), "crypto", 2022)
    db.save_year(_rows("BTC-USD", ["2022-01-06"], 42000.0), "crypto", 2022)

    out = db.load_year("crypto", 2022).sort_values("Date")
    assert len(out) == 2
    assert float(out["Close"].iloc[-1]) == pytest.approx(42000.0)   # 최신 우선
