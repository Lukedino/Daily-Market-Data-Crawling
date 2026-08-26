"""
커버리지 축소 가드 — 작은 파일이 큰 파일을 덮어쓰는 것을 막는다.

이 사고는 두 번 일어났다.

  1. 2026-08-12 crypto 2026 재백필 — 유니버스(CMC Top 200)에서 빠진 종목의
     데이터가 소실됐다 (197종목 → 172종목). 대응으로 backfill_market 에
     download_year() 선행 호출을 넣었다.
  2. 2026-08-20 us 2024 백필 — 899종목 → **727종목**. 가드가 들어간 뒤인데도
     재발했다. 199종목의 2024년치가 통째로 사라졌고, 그 종목들은 2023 과
     2025 에는 전부 존재한다.

1번의 대응이 부족했던 이유는 save_year() 에 구멍이 두 개 남아 있었기 때문이다.

  구멍 A — 로컬 파일이 없으면 병합 블록을 통째로 건너뛰고 그냥 쓴다.
           download_year() 는 실패해도 False 만 반환하고 호출부가 이를 보지
           않으므로, Drive 다운로드가 실패하면 이번에 받은 것만 저장된다.

  구멍 B — 병합 중 예외가 나면 logger.warning 만 남기고 덮어쓴다.

둘 다 **조용히 통과**한다. 그래서 저장 직전에 종목 수가 줄어드는지 보고
줄면 예외를 던져 중단하는 가드를 넣는다. 임계는 사전 등록값이다.
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _rows(ticker: str, dates: list[str], price: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame({
        "Ticker": [ticker] * len(dates),
        "Date": [date.fromisoformat(d) for d in dates],
        "Open": [price] * len(dates), "High": [price] * len(dates),
        "Low": [price] * len(dates), "Close": [price] * len(dates),
        "Volume": [1e6] * len(dates),
    })


def _universe(tickers, dates) -> pd.DataFrame:
    return pd.concat([_rows(t, dates) for t in tickers], ignore_index=True)


@pytest.fixture
def db(tmp_path, monkeypatch):
    from data import ohlc_db
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    return ohlc_db


DATES = ["2024-01-02", "2024-01-03", "2024-01-04"]


class TestThresholdIsPreRegistered:
    def test_tolerance_value(self, db):
        assert db.TICKER_SHRINK_TOLERANCE_PCT == 5.0


class TestShrinkGuard:
    def test_us_2024_pattern_is_blocked(self, db):
        """
        실제 사고 재현: 899종목 파일 위에 727종목만 담긴 프레임을 저장한다.
        행 단위 병합이 정상 동작하면 899가 유지돼야 하고, 병합이 깨져
        727만 남는 상황이면 저장 자체가 막혀야 한다.
        """
        big = [f"T{i:04d}" for i in range(899)]
        db.save_year(_universe(big, DATES), "us", 2024)
        assert db.load_year("us", 2024)["Ticker"].nunique() == 899

        # 병합이 깨진 상황을 강제한다 — 기존 파일을 못 읽는 경우
        small = _universe(big[:727], DATES)
        with pytest.raises(db.CoverageShrinkError) as e:
            db.save_year(small, "us", 2024, _existing_override=pd.DataFrame())
        assert "727" in str(e.value) and "899" in str(e.value)

        # 파일은 그대로여야 한다
        assert db.load_year("us", 2024)["Ticker"].nunique() == 899

    def test_normal_merge_keeps_everything_and_passes(self, db):
        """정상 경로 — 병합되면 종목이 줄지 않으므로 통과한다."""
        big = [f"T{i:04d}" for i in range(899)]
        db.save_year(_universe(big, DATES), "us", 2024)
        db.save_year(_universe(big[:727], DATES), "us", 2024)   # 병합 정상
        assert db.load_year("us", 2024)["Ticker"].nunique() == 899

    def test_small_shrink_within_tolerance_passes(self, db):
        """5% 이내 축소는 통과한다 (상장폐지 등 정상 사유)."""
        big = [f"T{i:04d}" for i in range(100)]
        db.save_year(_universe(big, DATES), "us", 2024)
        db.save_year(_universe(big[:97], DATES), "us", 2024,
                     _existing_override=pd.DataFrame())
        assert db.load_year("us", 2024)["Ticker"].nunique() == 97

    def test_shrink_beyond_tolerance_blocked(self, db):
        big = [f"T{i:04d}" for i in range(100)]
        db.save_year(_universe(big, DATES), "us", 2024)
        with pytest.raises(db.CoverageShrinkError):
            db.save_year(_universe(big[:94], DATES), "us", 2024,
                         _existing_override=pd.DataFrame())

    def test_allow_shrink_escape_hatch(self, db):
        """의도적 축소는 명시적으로만 허용한다."""
        big = [f"T{i:04d}" for i in range(100)]
        db.save_year(_universe(big, DATES), "us", 2024)
        db.save_year(_universe(big[:50], DATES), "us", 2024,
                     allow_shrink=True, _existing_override=pd.DataFrame())
        assert db.load_year("us", 2024)["Ticker"].nunique() == 50

    def test_first_write_is_not_blocked(self, db):
        """기존 파일이 없으면 비교 대상이 없으므로 통과한다."""
        db.save_year(_universe([f"T{i:04d}" for i in range(50)], DATES),
                     "us", 2099)
        assert db.load_year("us", 2099)["Ticker"].nunique() == 50

    def test_replace_tickers_path_is_not_broken(self, db):
        """
        replace_tickers 는 의도적 교체다. 같은 종목을 다시 넣으므로 종목 수가
        줄지 않고, 따라서 가드에 걸리지 않아야 한다.
        """
        base = [f"T{i:04d}" for i in range(100)]
        db.save_year(_universe(base, DATES), "us", 2024)
        db.save_year(_rows("T0000", DATES, price=999.0), "us", 2024,
                     replace_tickers=["T0000"])
        out = db.load_year("us", 2024)
        assert out["Ticker"].nunique() == 100
        assert out[out.Ticker == "T0000"]["Close"].eq(999.0).all()


class TestMergeFailureDoesNotOverwrite:
    """구멍 B — 병합 중 예외가 나면 덮어쓰지 말고 중단해야 한다."""

    def test_merge_exception_raises_instead_of_overwriting(self, db, monkeypatch):
        big = [f"T{i:04d}" for i in range(100)]
        db.save_year(_universe(big, DATES), "us", 2024)

        def boom(*a, **k):
            raise OSError("parquet 손상")

        monkeypatch.setattr(db, "load_year", boom)
        with pytest.raises(db.CoverageShrinkError, match="병합 실패"):
            db.save_year(_universe(big[:10], DATES), "us", 2024)


class TestBackfillRespectsDownloadFailure:
    """
    구멍 A — download_year 가 실패하면 백필이 그 연도를 건너뛰어야 한다.

    "Drive 에 파일이 없다"(최초 백필)와 "다운로드가 실패했다"(네트워크·권한)를
    구분해야 한다. 전자는 정상적으로 새로 써야 하고, 후자는 덮어쓰면 안 된다.
    기존 download_year() 는 둘 다 False 를 돌려줘 구분할 수 없었다.
    """

    def _patch_common(self, monkeypatch, tmp_path, saved):
        from data import ohlc_collector, ohlc_db
        monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
        monkeypatch.setattr(ohlc_db, "save_year",
                            lambda *a, **k: saved.append(a))
        monkeypatch.setattr(ohlc_db, "update_status", lambda *a, **k: None)
        monkeypatch.setattr(ohlc_collector, "load_tickers", lambda m: ["AAA"])
        monkeypatch.setattr(ohlc_collector, "build_symbol_overrides",
                            lambda t: {})
        monkeypatch.setattr(ohlc_collector, "purge_targets",
                            lambda *a, **k: [])
        monkeypatch.setattr(ohlc_collector, "fetch_ohlc_range",
                            lambda *a, **k: (_rows("AAA", DATES), []))
        return ohlc_collector, ohlc_db

    def test_backfill_skips_year_when_download_fails(self, monkeypatch, tmp_path):
        saved = []
        oc, db = self._patch_common(monkeypatch, tmp_path, saved)
        monkeypatch.setattr(db, "download_year_state", lambda *a, **k: "failed")
        oc.backfill_market("us", 2024, 2024, upload=False)
        assert saved == [], "Drive 다운로드 실패 시 저장하면 안 된다"

    def test_backfill_proceeds_when_remote_is_absent(self, monkeypatch, tmp_path):
        """최초 백필 — Drive 에 아직 파일이 없으면 정상적으로 새로 쓴다."""
        saved = []
        oc, db = self._patch_common(monkeypatch, tmp_path, saved)
        monkeypatch.setattr(db, "download_year_state", lambda *a, **k: "absent")
        oc.backfill_market("us", 2024, 2024, upload=False)
        assert len(saved) == 1

    def test_backfill_proceeds_when_download_succeeds(self, monkeypatch, tmp_path):
        saved = []
        oc, db = self._patch_common(monkeypatch, tmp_path, saved)
        monkeypatch.setattr(db, "download_year_state", lambda *a, **k: "ok")
        oc.backfill_market("us", 2024, 2024, upload=False)
        assert len(saved) == 1


class TestDownloadYearState:
    """download_year_state 는 세 상태를 구분한다."""

    def test_returns_absent_when_not_on_drive(self, db, monkeypatch):
        class U:
            def download(self, *a, **k):
                raise FileNotFoundError()
        monkeypatch.setattr(db, "_get_uploader", lambda u=None: U())
        assert db.download_year_state("us", 2024) == "absent"

    def test_returns_failed_on_other_errors(self, db, monkeypatch):
        class U:
            def download(self, *a, **k):
                raise ConnectionError("network down")
        monkeypatch.setattr(db, "_get_uploader", lambda u=None: U())
        assert db.download_year_state("us", 2024) == "failed"

    def test_returns_failed_when_no_uploader(self, db, monkeypatch):
        monkeypatch.setattr(db, "_get_uploader", lambda u=None: None)
        assert db.download_year_state("us", 2024) == "failed"

    def test_returns_ok_on_success(self, db, monkeypatch):
        class U:
            def download(self, *a, **k):
                return None
        monkeypatch.setattr(db, "_get_uploader", lambda u=None: U())
        assert db.download_year_state("us", 2024) == "ok"


class TestCoverageContinuityGate:
    """
    ④ 생산자 게이트 — 파일 **안**에서 유니버스가 하루 만에 꺼지는 것을 막는다.

    축소 가드(TICKER_SHRINK_TOLERANCE_PCT)와 서로를 대체하지 않는다.
    us_2024 사고는 파일 전체로는 727종목이 멀쩡히 들어 있었고,
    2024-01-02 에 899 -> 704 로 꺼지는 형태였다. 재수집 결과가 727 이면
    "원래 727이었다"와 구분되지 않아 축소 가드는 통과한다.
    """

    def _panel(self, dates, tickers, price=100.0):
        rows = []
        for t in tickers:
            rows.append(_rows(t, dates, price))
        return pd.concat(rows, ignore_index=True)

    def test_threshold_is_ten_percent(self, db):
        assert db.COVERAGE_DROP_PCT == 10.0

    def test_stable_universe_passes(self, db):
        d = ["2024-01-02", "2024-01-03", "2024-01-04"]
        u = [f"T{i:04d}" for i in range(200)]
        db.save_year(self._panel(d, u), "us", 2024)
        assert db.load_year("us", 2024)["Ticker"].nunique() == 200

    def test_us_2024_intra_file_drop_is_blocked(self, db):
        """실제 사고 형태 — 파일 안에서 899 -> 704."""
        d1 = ["2023-12-27", "2023-12-28", "2023-12-29"]
        d2 = ["2024-01-02", "2024-01-03"]
        keep = [f"K{i:04d}" for i in range(704)]
        gone = [f"G{i:04d}" for i in range(195)]
        df = pd.concat([self._panel(d1, keep + gone), self._panel(d2, keep)],
                       ignore_index=True)
        with pytest.raises(db.CoverageGapError) as e:
            db.save_year(df, "us", 2024)
        assert "2024-01-02" in str(e.value)

    def test_shrink_guard_would_have_passed_this(self, db):
        """
        같은 프레임이 축소 가드는 통과한다는 것을 명시적으로 고정한다.
        두 검사가 서로를 대체하지 않는 이유다.
        """
        d1 = ["2023-12-27", "2023-12-28", "2023-12-29"]
        d2 = ["2024-01-02", "2024-01-03"]
        keep = [f"K{i:04d}" for i in range(704)]
        gone = [f"G{i:04d}" for i in range(195)]
        df = pd.concat([self._panel(d1, keep + gone), self._panel(d2, keep)],
                       ignore_index=True)
        db.save_year(df, "us", 2024, allow_gap=True)      # 게이트만 끈다
        assert db.load_year("us", 2024)["Ticker"].nunique() == 899

    def test_allow_gap_escape_hatch(self, db):
        d1 = ["2024-01-02", "2024-01-03"]
        d2 = ["2024-01-04"]
        keep = [f"K{i:04d}" for i in range(150)]
        gone = [f"G{i:04d}" for i in range(100)]
        df = pd.concat([self._panel(d1, keep + gone), self._panel(d2, keep)],
                       ignore_index=True)
        db.save_year(df, "us", 2024, allow_gap=True)
        assert db.load_year("us", 2024)["Ticker"].nunique() == 250

    def test_small_universe_is_skipped(self, db):
        d1 = ["2024-01-02"]
        d2 = ["2024-01-03"]
        df = pd.concat([self._panel(d1, ["A", "B", "C"]),
                        self._panel(d2, ["A"])], ignore_index=True)
        db.save_year(df, "us", 2024)
        assert db.load_year("us", 2024)["Ticker"].nunique() == 3
