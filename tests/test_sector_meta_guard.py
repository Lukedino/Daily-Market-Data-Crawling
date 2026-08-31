"""save_sector_meta 축소 가드 (2026-08-31).

배경: 2026-08-26 커밋(040e7eb)이 save_year() 의 축소 가드를 save_sector_meta() 에 복붙하면서
그 함수에만 존재하는 지역변수(_exempt, prior_n, allow_shrink, year, allow_gap)를 그대로 참조 —
비어 있지 않은 df 를 저장하려는 순간 NameError 로 죽어 **sector-meta 주간 수집이 08-30부터 전멸**했다.
sector_meta 는 날짜 축이 없는 종목 메타이므로 (1) 축소 가드는 파일 전체 종목 수 기준으로 자체 구현,
(2) 날짜별 커버리지 연속성 게이트는 적용 대상이 아니다.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _meta(tickers) -> pd.DataFrame:
    return pd.DataFrame({
        "Ticker": list(tickers),
        "Sector": ["Technology"] * len(tickers),
        "Industry": ["Software"] * len(tickers),
    })


@pytest.fixture
def db(tmp_path, monkeypatch):
    from data import ohlc_db
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    return ohlc_db


def test_first_write_roundtrip(db):
    """첫 저장은 가드 없이 통과 — 현재 코드는 여기서부터 NameError 로 죽는다(회귀 재현)."""
    db.save_sector_meta(_meta(["AAPL", "MSFT", "NVDA"]), "us")
    out = pd.read_parquet(db.sector_meta_path("us"))
    assert sorted(out["Ticker"]) == ["AAPL", "MSFT", "NVDA"]


def test_overwrite_same_size_passes(db):
    db.save_sector_meta(_meta([f"T{i}" for i in range(100)]), "us")
    db.save_sector_meta(_meta([f"T{i}" for i in range(99)]), "us")   # -1% — 허용 범위
    assert pd.read_parquet(db.sector_meta_path("us"))["Ticker"].nunique() == 99


def test_shrink_beyond_tolerance_blocked(db):
    db.save_sector_meta(_meta([f"T{i}" for i in range(100)]), "us")
    with pytest.raises(db.CoverageShrinkError):
        db.save_sector_meta(_meta([f"T{i}" for i in range(50)]), "us")
    # 기존 파일은 보존
    assert pd.read_parquet(db.sector_meta_path("us"))["Ticker"].nunique() == 100


def test_allow_shrink_escape_hatch(db):
    db.save_sector_meta(_meta([f"T{i}" for i in range(100)]), "us")
    db.save_sector_meta(_meta([f"T{i}" for i in range(50)]), "us", allow_shrink=True)
    assert pd.read_parquet(db.sector_meta_path("us"))["Ticker"].nunique() == 50


def test_empty_df_is_skipped(db):
    db.save_sector_meta(pd.DataFrame(), "us")
    assert not db.sector_meta_path("us").exists()
