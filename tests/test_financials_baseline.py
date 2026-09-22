"""financials/ratios Drive 선다운로드 베이스라인 (2026-09-01).

배경: save_*() 는 로컬 파일하고만 병합하는데, GHA 러너는 매 실행 빈 파일시스템에서
시작하고 수집 경로 어디에도 Drive 선다운로드가 없어 upload_*() 가 이번 실행분만으로
Drive 기존 파일을 덮어썼다. ratios 는 실행당 SnapDate 1개라 스냅샷 이력이 매번 전멸
(기관보유 추세 판정 불가 — 섹터리더 2단계 선결 결함). ohlc 의 [BUG-BACKFILL-REPLACE]
와 동일 패턴이며, 수정도 ohlc_db.download_year_state 의 3상태 철학을 따른다:
absent(원격에 없음) = 최초 실행 정상 진행 / failed(조회·다운로드 실패) = 덮어쓰기
방지를 위해 중단.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class StubUploader:
    """download_all_state 호출을 기록하고 지정된 상태를 돌려주는 스텁."""

    def __init__(self, states):
        # states: {remote_subfolder: "ok"|"absent"|"failed"}
        self.states = states
        self.calls = []

    def download_all_state(self, remote_subfolder, local_dir, extensions=(".parquet",)):
        self.calls.append((remote_subfolder, local_dir))
        state = self.states.get(remote_subfolder, "absent")
        if state == "ok":
            from data import financials_db
            market, kind = remote_subfolder.split("/")
            # ok는 검증 가능한 파일을 실제로 staging에 내려받았다는 뜻이다.
            pd.DataFrame(columns=financials_db._columns(kind)).to_parquet(
                Path(local_dir) / f"{market}_{kind}_2026.parquet", index=False)
        return state


@pytest.fixture
def fdb(tmp_path, monkeypatch):
    from data import financials_db
    monkeypatch.setattr(financials_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    return financials_db


# ── ensure_drive_baseline — 3상태 판정 ─────────────────────────────


def test_baseline_us_downloads_both_folders(fdb):
    u = StubUploader({"us/financials": "ok", "us/ratios": "absent"})
    fdb.ensure_drive_baseline("us", uploader=u)          # absent 는 정상 (최초 실행)
    remotes = [c[0] for c in u.calls]
    assert remotes == ["us/financials", "us/ratios"]
    # 로컬 대상 폴더가 실제 저장 경로와 일치해야 병합이 성립한다
    assert u.calls[0][1].endswith(str(Path("us") / "financials"))
    assert u.calls[1][1].endswith(str(Path("us") / "ratios"))


def test_baseline_crypto_ratios_only(fdb):
    u = StubUploader({"crypto/ratios": "ok"})
    fdb.ensure_drive_baseline("crypto", kinds=("ratios",), uploader=u)
    assert [c[0] for c in u.calls] == ["crypto/ratios"]


def test_baseline_failed_raises(fdb):
    """failed 인 채 진행하면 이번 실행분만으로 Drive 를 덮어쓴다 — 반드시 중단."""
    u = StubUploader({"us/financials": "ok", "us/ratios": "failed"})
    with pytest.raises(RuntimeError):
        fdb.ensure_drive_baseline("us", uploader=u)


def test_baseline_no_uploader_is_explicit_failure(fdb, monkeypatch):
    """게시 요청에서 client 부재는 성공이 아니며 수집 전에 중단한다."""
    monkeypatch.setattr(fdb, "_get_uploader", lambda uploader=None: None)
    with pytest.raises(fdb.FinancialsStateError, match="uploader_unavailable"):
        fdb.ensure_drive_baseline("us")


# ── 회귀 재현 — 베이스라인 파일이 있으면 스냅샷이 누적된다 ─────────


def _ratio_row(ticker, snap):
    return {"Ticker": ticker, "SnapDate": snap, "Name": ticker,
            "Sector": "Technology", "Industry": "Software"}


def test_ratios_accumulate_after_baseline(fdb):
    """선다운로드된 기존 스냅샷 위에 새 스냅샷을 저장하면 둘 다 남아야 한다."""
    fdb.save_ratios(pd.DataFrame([_ratio_row("AAPL", "2026-08-01")]), "us")   # = 다운로드된 베이스라인
    fdb.save_ratios(pd.DataFrame([_ratio_row("AAPL", "2026-09-01")]), "us")   # 이번 실행 스냅샷
    df = fdb.load_ratios_year("us", 2026)
    assert sorted(str(d) for d in df["SnapDate"]) == ["2026-08-01", "2026-09-01"]


# ── 수집 함수 배선 — upload=True 일 때만 베이스라인 선행 ───────────


def test_collector_us_calls_baseline_before_save(monkeypatch):
    from data import financials_collector, financials_db
    calls = []
    monkeypatch.setattr(financials_db, "ensure_drive_baseline",
                        lambda market, kinds=("financials", "ratios"), uploader=None:
                        calls.append((market, tuple(kinds))))
    financials_collector.collect_us_financials(tickers=[], upload=True)
    assert calls == [("us", ("financials", "ratios"))]


def test_collector_us_skips_baseline_without_upload(monkeypatch):
    from data import financials_collector, financials_db
    calls = []
    monkeypatch.setattr(financials_db, "ensure_drive_baseline",
                        lambda *a, **k: calls.append(a))
    financials_collector.collect_us_financials(tickers=[], upload=False)
    assert calls == []


def test_collector_crypto_calls_baseline(monkeypatch):
    import requests
    from data import financials_collector, financials_db
    calls = []
    monkeypatch.setattr(financials_db, "ensure_drive_baseline",
                        lambda market, kinds=("financials", "ratios"), uploader=None:
                        calls.append((market, tuple(kinds))))
    # Simulate failure without opening even a loopback connection.
    def unavailable(*args, **kwargs):
        assert calls == [("crypto", ("ratios",))]
        raise requests.ConnectionError("synthetic CMC outage")
    monkeypatch.setattr(requests.Session, "get", unavailable)
    with pytest.raises(financials_db.FinancialsStateError, match="collection_empty"):
        financials_collector.collect_crypto_ratios(tickers=[], upload=True)
    assert calls == [("crypto", ("ratios",))]


# ── financials Drive 경로 market 키 일반화 (kr 지원, Task 2) ───────────────


class _UploadRecordingUploader:
    """upload() 호출의 remote 인자를 기록하는 스텁 (StubUploader 는 download_all_state 전용)."""

    def __init__(self):
        self.uploaded = []

    def upload(self, local, remote):
        self.uploaded.append(remote)
        return True


def test_baseline_kr_financials_uses_kr_drive_path(fdb):
    """market='kr' + kinds=('financials',) → DRIVE_PATHS['kr_financials'] 경로로 다운로드."""
    u = StubUploader({"kr/financials": "absent"})
    fdb.ensure_drive_baseline("kr", kinds=("financials",), uploader=u)
    assert [c[0] for c in u.calls] == ["kr/financials"]


def test_upload_financials_kr_drive_path(fdb):
    """upload_financials('kr', ...)가 kr/financials 원격 경로를 쓴다."""
    p = fdb.local_financials_path("kr", 2026)
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=fdb._FINANCIALS_COLS).to_parquet(p, index=False)

    u = _UploadRecordingUploader()
    fdb.upload_financials("kr", [2026], uploader=u)
    assert u.uploaded == ["kr/financials"]


# ── 수집 함수 배선 — KR (Task 3, 최종 리뷰 반영) ────────────────────


def _kr_baseline_setup(monkeypatch, tmp_path):
    """collect_kr_financials가 실제 I/O 없이 돌게 하는 공통 스텁 (financials_db._LOCAL_ROOT
    격리 + kr_db.download_year/load_year 1종목 marcap 스텁 + upload_financials no-op)."""
    from data import financials_db, kr_db, kr_collector

    monkeypatch.setattr(financials_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    monkeypatch.setattr(financials_db, "upload_financials", lambda *a, **k: None)
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **kw: {y: "ok" for y in years})
    from datetime import date as _d
    marcap = pd.DataFrame({
        "Code": ["005930"], "Marcap": [500], "Stocks": [100],
        "Date": [_d(2026, 9, 3)],
    })
    monkeypatch.setattr(kr_db, "load_year", lambda year, strict=False: marcap if year == 2026 else marcap.iloc[:0])
    marcap.attrs["krx_snapshot"] = {"version": 1, "provider": "fdr_krx_cache", "source_date": "2026-09-03"}
    monkeypatch.setattr(kr_collector, "read_krx_snapshot", lambda: marcap)


class _EmptyDart:
    """finstate가 항상 응답 없음 — 실네트워크·실계정 없이 배선만 검증."""

    def finstate(self, code, year, reprt_code="11011"):
        return None


def test_collector_kr_calls_baseline_before_save(monkeypatch, tmp_path):
    from data import kr_financials_collector as kfc, financials_db

    calls = []
    _kr_baseline_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(financials_db, "ensure_drive_baseline",
                        lambda market, kinds=("financials", "ratios"), uploader=None:
                        calls.append((market, tuple(kinds))))
    monkeypatch.setattr(kfc, "_SLEEP_SEC", 0)

    kfc.collect_kr_financials(top_n=10, upload=True, dart=_EmptyDart(), years=[2026])
    assert calls == [("kr", ("financials",))]


def test_collector_kr_skips_baseline_without_upload(monkeypatch, tmp_path):
    from data import kr_financials_collector as kfc, financials_db

    calls = []
    _kr_baseline_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(financials_db, "ensure_drive_baseline",
                        lambda *a, **k: calls.append(a))
    monkeypatch.setattr(kfc, "_SLEEP_SEC", 0)

    kfc.collect_kr_financials(top_n=10, upload=False, dart=_EmptyDart(), years=[2026])
    assert calls == []
