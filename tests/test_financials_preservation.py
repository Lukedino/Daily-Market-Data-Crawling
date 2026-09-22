"""실제 임시 Parquet와 가짜 Drive로 재무 손실·거짓 성공 경계를 고정한다."""
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data import financials_db as db
from data import financials_collector as fc
from data import kr_financials_collector as kr


def financial(ticker="AAPL", year=2026, value=10):
    return pd.DataFrame([{"Ticker": ticker, "PeriodDate": date(year, 6, 30),
                         "Year": year, "Quarter": 2, "SnapDate": date(2026, 9, 22),
                         "Revenue": value, "NetIncome": value}])


def ratio(ticker="AAPL", year=2026, value=10):
    return pd.DataFrame([{"Ticker": ticker, "SnapDate": date(year, 9, 22), "PE": value}])


def write_frame(path, frame):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_LOCAL_ROOT", tmp_path / "db")
    monkeypatch.setattr(db.config, "DRIVE_PATHS", {
        "us_financials": "us/financials", "us_ratios": "us/ratios",
        "kr_financials": "kr/financials", "crypto_ratios": "crypto/ratios"})
    monkeypatch.setattr(fc.time, "sleep", lambda n: None)


class Drive:
    def __init__(self, files=None, states=None, upload_result="synthetic_file_id"):
        self.files = files or {}
        self.states = states or {}
        self.upload_result = upload_result
        self.downloads, self.uploads = [], []

    def download_all_state(self, remote, local, extensions=(".parquet",)):
        self.downloads.append((remote, local))
        for name, value in self.files.get(remote, {}).items():
            destination = Path(local) / name
            if isinstance(value, bytes):
                destination.write_bytes(value)
            else:
                write_frame(destination, value)
        return self.states.get(remote, "ok" if self.files.get(remote) else "absent")

    def upload(self, local, remote):
        self.uploads.append((Path(local).name, remote))
        if isinstance(self.upload_result, Exception):
            raise self.upload_result
        return self.upload_result


@pytest.mark.parametrize("kind", ["financials", "ratios"])
@pytest.mark.parametrize("bad", [b"", b"not parquet"])
def test_existing_corrupt_is_not_empty(kind, bad):
    path = db._LOCAL_ROOT / "us" / kind / f"us_{kind}_2026.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(bad)
    with pytest.raises(db.FinancialsStateError):
        db._load("us", kind, 2026)
    with pytest.raises(db.FinancialsStateError):
        db._save(financial() if kind == "financials" else ratio(), "us", kind)
    assert path.read_bytes() == bad


@pytest.mark.parametrize("mutation", ["year", "duplicate", "date", "key", "type", "period", "infinity"])
def test_existing_invalid_parquet_is_rejected(mutation):
    frame = financial()
    if mutation == "year": frame["PeriodDate"] = date(2025, 6, 30)
    if mutation == "duplicate": frame = pd.concat([frame, frame])
    if mutation == "date": frame["PeriodDate"] = "not-a-date"
    if mutation == "key": frame = frame.drop(columns="Ticker")
    if mutation == "type": frame["Revenue"] = "SYNTHETIC_SECRET"
    if mutation == "period": frame["Quarter"] = 3
    if mutation == "infinity": frame["Revenue"] = float("inf")
    path = db.local_financials_path("us", 2026)
    write_frame(path, frame)
    before = path.read_bytes()
    with pytest.raises(db.FinancialsStateError) as error:
        db.save_financials(financial("MSFT"), "us")
    assert "SYNTHETIC_SECRET" not in str(error.value)
    assert path.read_bytes() == before


def test_absent_and_valid_empty_placeholder():
    assert db.load_financials_year("kr", 2026).empty
    path = db.local_financials_path("kr", 2026)
    write_frame(path, financial().iloc[:0])
    assert db.load_financials_year("kr", 2026).empty
    db.save_financials(financial("005930"), "kr")
    assert len(db.load_financials_year("kr", 2026)) == 1


def test_all_year_baselines_before_any_staging(monkeypatch):
    db.save_financials(financial(year=2025), "us")
    first = db.local_financials_path("us", 2025)
    second = db.local_financials_path("us", 2026)
    second.write_bytes(b"broken")
    before = first.read_bytes()
    writes = []
    monkeypatch.setattr(db, "_stage_frame", lambda *a: writes.append(a))
    with pytest.raises(db.FinancialsStateError):
        db.save_financials(pd.concat([financial("MSFT", 2025), financial("MSFT", 2026)]), "us")
    assert writes == [] and first.read_bytes() == before and second.read_bytes() == b"broken"


@pytest.mark.parametrize("fault", ["write", "reread", "fsync", "replace"])
def test_atomic_save_preserves_original_on_fault(monkeypatch, fault):
    db.save_financials(financial(), "us")
    path = db.local_financials_path("us", 2026)
    before = path.read_bytes()
    if fault == "write":
        def partial(table, destination, **kw):
            Path(destination).write_bytes(b"partial")
            raise OSError("SYNTHETIC_SECRET")
        monkeypatch.setattr(db.pq, "write_table", partial)
    elif fault == "reread":
        write = db.pq.write_table
        monkeypatch.setattr(db.pq, "write_table", lambda table, target, **kw: write(table.slice(0, 1), target, **kw))
    else:
        def fail(*a): raise OSError("SYNTHETIC_SECRET")
        monkeypatch.setattr(db.os, fault, fail)
    with pytest.raises(db.FinancialsStateError) as error:
        db.save_financials(financial("MSFT"), "us")
    assert str(error.value).startswith("financials_")
    assert path.read_bytes() == before
    assert not list(path.parent.glob(".financials-*"))


def test_same_directory_temp_and_all_candidate_verification_before_replace(monkeypatch):
    replace = db.os.replace
    seen = []
    def record(source, target):
        assert Path(source).parent == Path(target).parent
        assert len(list(Path(source).parent.glob(".financials-*"))) == 2 - len(seen)
        seen.append(Path(target).name)
        replace(source, target)
    monkeypatch.setattr(db.os, "replace", record)
    db.save_financials(pd.concat([financial(year=2025), financial(year=2026)]), "us")
    assert seen == ["us_financials_2025.parquet", "us_financials_2026.parquet"]


def test_baseline_is_staged_and_keeps_local_only_history_and_nonmissing_values():
    db.save_financials(pd.concat([financial("AAPL"), financial("LOCAL")]), "us")
    incoming = financial("AAPL", value=None)
    incoming["NetIncome"] = 20
    u = Drive({"us/financials": {"us_financials_2026.parquet": incoming}})
    assert db.ensure_drive_baseline("us", uploader=u) is u
    result = db.load_financials_year("us", 2026).set_index("Ticker")
    assert set(result.index) == {"AAPL", "LOCAL"}
    assert result.loc["AAPL", "Revenue"] == 10 and result.loc["AAPL", "NetIncome"] == 20
    assert all(not str(path).startswith(str(db._LOCAL_ROOT)) for _, path in u.downloads)


@pytest.mark.parametrize("fault", ["failed", "corrupt", "duplicate", "missing_ok", "unknown"])
def test_second_kind_baseline_failure_does_not_replace_first(fault):
    db.save_financials(financial(), "us")
    path = db.local_financials_path("us", 2026)
    before = path.read_bytes()
    files = {"us/financials": {"us_financials_2026.parquet": financial(value=20)}}
    states = {}
    if fault == "corrupt": files["us/ratios"] = {"us_ratios_2026.parquet": b"bad"}
    elif fault == "duplicate": files["us/ratios"] = {"us_ratios_2026.parquet": pd.concat([ratio(), ratio()])}
    else: states["us/ratios"] = {"failed": "failed", "missing_ok": "ok", "unknown": "surprise"}[fault]
    with pytest.raises(db.FinancialsStateError):
        db.ensure_drive_baseline("us", uploader=Drive(files, states))
    assert path.read_bytes() == before


@pytest.mark.parametrize("result", [False, None, "", {}, 1, "invalid id", OSError("SYNTHETIC_SECRET")])
def test_upload_failure_is_explicit_and_preserves_local(result):
    db.save_financials(financial(), "us")
    path = db.local_financials_path("us", 2026)
    before = path.read_bytes()
    with pytest.raises(db.FinancialsPublishError) as error:
        db.upload_financials("us", [2026], uploader=Drive(upload_result=result))
    assert error.value.failed == ("us_financials_2026.parquet",)
    assert "SYNTHETIC_SECRET" not in str(error.value)
    assert path.read_bytes() == before


@pytest.mark.parametrize("operation", ["baseline", "upload"])
def test_missing_uploader_is_failure(monkeypatch, operation):
    monkeypatch.setattr(db, "_get_uploader", lambda *a: None)
    with pytest.raises(db.FinancialsStateError, match="uploader_unavailable"):
        db.ensure_drive_baseline("us") if operation == "baseline" else db.upload_ratios("us", [2026])


def test_missing_upload_file_fails_before_other_uploads():
    db.save_ratios(ratio(year=2025), "us")
    u = Drive()
    with pytest.raises(db.FinancialsStateError, match="file_missing"):
        db.upload_ratios("us", [2025, 2026], u)
    assert u.uploads == []


def test_missing_drive_path_is_failure(monkeypatch):
    monkeypatch.setattr(db.config, "DRIVE_PATHS", {})
    with pytest.raises(db.FinancialsStateError, match="path_missing"):
        db.ensure_drive_baseline("us", uploader=Drive())


@pytest.mark.parametrize("upload", [True, False])
def test_collector_checks_all_baselines_before_fetch(monkeypatch, upload):
    bad = db.local_ratios_path("us", 2025)
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"corrupt")
    monkeypatch.setattr(db, "_get_uploader", lambda *a: Drive())
    calls = []
    monkeypatch.setattr(fc, "_fetch_quarterly_financials", lambda *a: calls.append(a))
    with pytest.raises(db.FinancialsStateError):
        fc.collect_us_financials(["AAPL"], upload=upload)
    assert calls == []


def test_us_reuses_baseline_client_and_stops_after_failed_first_batch(monkeypatch):
    u = Drive(upload_result=False)
    created = []
    def get_uploader(supplied=None):
        if supplied is not None: return supplied
        created.append(1)
        return u
    monkeypatch.setattr(db, "_get_uploader", get_uploader)
    calls = []
    monkeypatch.setattr(fc, "_fetch_quarterly_financials", lambda ticker: calls.append(ticker) or financial(ticker))
    monkeypatch.setattr(fc, "_fetch_ratios_snapshot", lambda ticker, snap: ratio(ticker).iloc[0].to_dict())
    with pytest.raises(db.FinancialsPublishError):
        fc.collect_us_financials([f"T{i}" for i in range(20)], upload=True)
    assert created == [1] and len(calls) == 10 and len(u.uploads) == 1
    assert len(db.load_financials_year("us", 2026)) == 10
    assert len(db.load_ratios_year("us", 2026)) == 10


def marcap(day=date(2026, 9, 22), stocks=100, cap=500):
    return pd.DataFrame([{"Code": "005930", "Stocks": stocks, "Marcap": cap, "Date": day}])


def setup_kr(monkeypatch, frame=None, states=None, observed=date(2026, 9, 22)):
    from data import kr_db, kr_collector
    frame = marcap() if frame is None else frame
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **kw: states or {y: "ok" for y in years})
    monkeypatch.setattr(kr_db, "load_year", lambda y, strict=False: frame if y == 2026 else frame.iloc[:0])
    monkeypatch.setattr(kr_collector, "read_krx_snapshot", lambda: marcap(), raising=False)
    monkeypatch.setattr(kr_collector, "snapshot_source_date", lambda f: observed, raising=False)


def test_ohlc_only_latest_day_does_not_hide_valid_snapshot():
    frame = pd.concat([marcap(date(2026, 9, 21)), marcap(cap=0, stocks=0)])
    result = kr.build_universe(frame)
    assert list(result["Code"]) == ["005930"]
    assert result.attrs["source_date"] == date(2026, 9, 21)
    assert result.attrs["latest_known_date"] == date(2026, 9, 22)


@pytest.mark.parametrize("stocks", [0, None, float("nan"), float("inf"), -1, True])
def test_missing_stocks_is_not_silently_removed_from_top_n(stocks):
    with pytest.raises(db.FinancialsStateError, match="universe_unverified"):
        kr.build_universe(marcap(stocks=stocks))


@pytest.mark.parametrize("fault", ["download", "empty", "stale", "ohlc", "previous", "future"])
def test_unverified_universe_blocks_dart_and_publish(monkeypatch, fault):
    frame, states, observed = marcap(), None, date(2026, 9, 22)
    if fault == "download": states = {2025: "ok", 2026: "failed"}
    if fault == "empty": frame = marcap().iloc[:0]
    if fault == "stale": observed = date(2026, 9, 23)
    if fault == "ohlc": frame = pd.concat([marcap(date(2026, 9, 21)), marcap(cap=0, stocks=0)])
    if fault == "previous": frame = marcap(date(2025, 12, 31))
    if fault == "future": frame = marcap(date(2026, 9, 23))
    setup_kr(monkeypatch, frame, states, observed)
    u = Drive()
    monkeypatch.setattr(db, "_get_uploader", lambda *a: u)
    class NoDart:
        def finstate(self, *a, **kw): pytest.fail("DART must not run")
    with pytest.raises(db.FinancialsStateError, match="universe_unverified"):
        kr.collect_kr_financials(upload=True, dart=NoDart(), years=[2026], today=date(2026, 9, 22))
    assert u.uploads == []


def test_calendar_no_reports_never_constructs_dart(monkeypatch):
    setup_kr(monkeypatch, marcap(date(2026, 2, 2)), observed=date(2026, 2, 2))
    monkeypatch.setattr(kr, "_get_dart", lambda: pytest.fail("no DART construction"))
    result = kr.collect_kr_financials(upload=False, years=[2026], today=date(2026, 2, 2))
    assert result == {"source_date": "2026-02-02", "valid_count": 1, "failed_count": 0}


def test_previous_unpublished_quarter_republished_without_dart(monkeypatch):
    setup_kr(monkeypatch)
    rows = pd.concat([financial("005930").assign(PeriodDate=date(2026, 3, 31), Quarter=1), financial("005930")])
    db.save_financials(rows, "kr")
    u = Drive()
    monkeypatch.setattr(db, "_get_uploader", lambda supplied=None: supplied or u)
    monkeypatch.setattr(kr, "_get_dart", lambda: pytest.fail("saved quarters are skipped"))
    kr.collect_kr_financials(upload=True, years=[2026], today=date(2026, 9, 22))
    assert u.uploads == [("kr_financials_2026.parquet", "kr/financials")]
    assert len(db.load_financials_year("kr", 2026)) == 2


def test_empty_remote_placeholder_preserves_local_unpublished_rows():
    db.save_financials(financial("005930"), "kr")
    u = Drive({"kr/financials": {"kr_financials_2026.parquet": financial().iloc[:0]}})
    db.ensure_drive_baseline("kr", kinds=("financials",), uploader=u)
    assert list(db.load_financials_year("kr", 2026)["Ticker"]) == ["005930"]


def test_second_candidate_reread_failure_replaces_no_files(monkeypatch):
    db.save_financials(pd.concat([financial(year=2025), financial(year=2026)]), "us")
    before = {year: db.local_financials_path("us", year).read_bytes() for year in [2025, 2026]}
    original = db._read_path
    def bad_second(path, kind, year):
        if path.suffix == ".tmp" and year == 2026:
            return original(path, kind, year).iloc[:0]
        return original(path, kind, year)
    monkeypatch.setattr(db, "_read_path", bad_second)
    with pytest.raises(db.FinancialsStateError):
        db.save_financials(pd.concat([financial("MSFT", 2025), financial("MSFT", 2026)]), "us")
    assert before == {year: db.local_financials_path("us", year).read_bytes() for year in before}


def test_crypto_publish_false_propagates_after_local_save(monkeypatch):
    import requests
    import yfinance
    u = Drive(upload_result=False)
    constructed = []
    def uploader(supplied=None):
        if supplied is None: constructed.append(1)
        return supplied or u
    monkeypatch.setattr(db, "_get_uploader", uploader)
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return {"data": {"cryptoCurrencyList": [{"symbol": "BTC", "name": "Bitcoin",
                    "circulatingSupply": 100, "quotes": [{"marketCap": 200}]}]}}
    monkeypatch.setattr(requests.Session, "get", lambda *a, **k: Response())
    monkeypatch.setattr(yfinance, "Ticker", lambda *a: type("Ticker", (), {"info": {}})())
    with pytest.raises(db.FinancialsPublishError):
        fc.collect_crypto_ratios(upload=True)
    assert constructed == [1]
    assert len(db.load_ratios_year("crypto", date.today().year)) == 1
    assert len(u.uploads) == 1


def test_kr_failed_fifty_ticker_flush_keeps_local_and_stops(monkeypatch):
    codes = [f"{i:05d}0" for i in range(1, 52)]
    universe = pd.DataFrame({"Code": codes, "Stocks": [100] * len(codes)})
    universe.attrs.update(source_date=date(2026, 9, 22), valid_count=len(codes))
    monkeypatch.setattr(kr, "_load_verified_universe", lambda *a, **kw: universe)
    u = Drive(upload_result=False)
    monkeypatch.setattr(db, "_get_uploader", lambda supplied=None: supplied or u)
    calls = []
    def report(dart, code, year, reprt_code):
        calls.append((code, reprt_code))
        return pd.DataFrame([{"sj_div": "IS", "account_nm": "당기순이익",
            "thstrm_amount": "10" if reprt_code == "11013" else "20"}])
    monkeypatch.setattr(kr, "_fetch_report", report)
    with pytest.raises(db.FinancialsPublishError):
        kr.collect_kr_financials(upload=True, dart=object(), years=[2026], today=date(2026, 9, 22))
    assert len(calls) == 100 and codes[-1] not in {code for code, _ in calls}
    assert len(db.load_financials_year("kr", 2026)) == 100
    assert len(u.uploads) == 1


def test_real_drive_upload_string_contract_is_accepted(monkeypatch):
    import sys
    import types
    from data.drive_uploader import DriveUploader
    db.save_financials(financial(), "us")
    class Response:
        def execute(self): return {"id": "synthetic_file_id"}
    class Files:
        def update(self, **kwargs):
            assert kwargs["fileId"] == "synthetic_file_id"
            return Response()
        def create(self, **kwargs): pytest.fail("existing slot must be updated")
    service = types.SimpleNamespace(files=lambda: Files())
    uploader = DriveUploader.__new__(DriveUploader)
    monkeypatch.setattr(uploader, "_get_service", lambda: service)
    monkeypatch.setattr(uploader, "_get_or_create_folder", lambda folder: "synthetic_folder")
    monkeypatch.setattr(uploader, "_find_file", lambda folder, name: "synthetic_file_id")
    http = types.ModuleType("googleapiclient.http")
    http.MediaFileUpload = lambda *a, **kw: object()
    monkeypatch.setitem(sys.modules, "googleapiclient.http", http)
    assert db.upload_financials("us", [2026], uploader) == []
