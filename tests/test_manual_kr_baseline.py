"""수동 writer 기준본의 합성 bulk·가격 보류·실패 전파 회귀."""
import ast
from datetime import date, datetime, timedelta
import json
import logging
import re
from pathlib import Path
import tempfile
from types import SimpleNamespace

import pandas as pd
import pytest
from data import kr_db, kr_collector, ohlc_db


def frame(year=2026, code="005930", close=100, cap=1000, stocks=100):
    return pd.DataFrame([{"Code": code, "Date": pd.Timestamp(year, 9, 1),
        "Name": "Synthetic", "Market": "KOSPI", "Close": close,
        "Open": close, "High": close, "Low": close, "Volume": 10,
        "Marcap": cap, "Stocks": stocks, "Rank": 1}])


def save_raw(year, data):
    path = kr_db.local_path(year)
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes): path.write_bytes(data)
    else: data.to_parquet(path, index=False)
    return path, path.read_bytes()


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "_LOCAL_ROOT", tmp_path / "kr")
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc")
    monkeypatch.setattr(ohlc_db, "_PENDING_PATH", tmp_path / "pending.json")
    monkeypatch.setattr(kr_db.config, "DRIVE_PATHS", {
        "ohlc_kr": "kr", "ohlc_us": "us", "ohlc_crypto": "crypto", "ohlc_meta": "meta"})


class Bulk:
    def __init__(self, files=None, state=None):
        self.files = files or {}
        self.state = state or ("ok" if self.files else "absent")
        self.destinations = []
    def download_all_state(self, remote, destination, extensions=(".parquet",)):
        self.destinations.append(Path(destination))
        for name, data in self.files.items():
            path = Path(destination) / name
            if isinstance(data, bytes): path.write_bytes(data)
            else: data.to_parquet(path, index=False)
        return self.state


def test_kr_bulk_preserves_local_only_partitions_and_rows():
    save_raw(2025, frame(2025, "LOCAL25"))
    save_raw(2026, pd.concat([frame(), frame(code="LOCAL26")]))
    remote = Bulk({"marcap-2026.parquet": frame(close=120, cap=None, stocks=0)})
    assert kr_db.download_all(remote) == "ok"
    result = kr_db.load_year(2026, strict=True).set_index("Code")
    assert set(result.index) == {"005930", "LOCAL26"}
    assert result.loc["005930", "Close"] == 120
    assert result.loc["005930", "Marcap"] == 1000 and result.loc["005930", "Stocks"] == 100
    assert list(kr_db.load_year(2025, strict=True)["Code"]) == ["LOCAL25"]
    assert remote.destinations[0] != kr_db._LOCAL_ROOT


@pytest.mark.parametrize("bad", ["corrupt", "wrongyear", "duplicate", "key", "date", "empty_schema"])
def test_second_remote_partition_invalid_promotes_nothing(bad, monkeypatch):
    first, before = save_raw(2025, frame(2025))
    incoming = frame()
    if bad == "corrupt": incoming = b"broken"
    if bad == "wrongyear": incoming = frame(2025)
    if bad == "duplicate": incoming = pd.concat([frame(), frame()])
    if bad == "key": incoming = frame().drop(columns="Code")
    if bad == "date": incoming = frame().assign(Date="not-date")
    if bad == "empty_schema": incoming = pd.DataFrame({"garbage": []})
    calls = []
    monkeypatch.setattr(kr_db.os, "replace", lambda *args: calls.append(args))
    with pytest.raises(RuntimeError):
        kr_db.download_all(Bulk({"marcap-2025.parquet": frame(2025, close=200),
                                "marcap-2026.parquet": incoming}))
    assert calls == [] and first.read_bytes() == before
    assert not kr_db.local_path(2026).exists()


def test_local_only_corrupt_partition_blocks_all_promotions(monkeypatch):
    first, before = save_raw(2025, frame(2025))
    bad, bad_bytes = save_raw(2026, b"broken")
    calls = []
    monkeypatch.setattr(kr_db.os, "replace", lambda *args: calls.append(args))
    with pytest.raises(RuntimeError): kr_db.download_all(Bulk({"marcap-2025.parquet": frame(2025, close=200)}))
    assert calls == [] and first.read_bytes() == before and bad.read_bytes() == bad_bytes


@pytest.mark.parametrize("state", ["failed", "unknown", None])
def test_bulk_nonconfirmed_outcome_is_failure(state):
    path, before = save_raw(2026, frame())
    u = Bulk()
    u.state = state
    with pytest.raises(RuntimeError): kr_db.download_all(u)
    assert path.read_bytes() == before


def test_absent_preserves_local_and_still_rejects_local_corruption():
    path, before = save_raw(2026, frame())
    assert kr_db.download_all(Bulk()) == "absent"
    assert path.read_bytes() == before
    path.write_bytes(b"corrupt")
    with pytest.raises(RuntimeError): kr_db.download_all(Bulk())
    assert path.read_bytes() == b"corrupt"


@pytest.mark.parametrize("case", ["ok_empty", "absent_with_file", "wrong_filename"])
def test_bulk_status_and_file_set_must_agree(case):
    path, before = save_raw(2026, frame())
    if case == "ok_empty": u = Bulk(state="ok")
    elif case == "absent_with_file": u = Bulk({"marcap-2026.parquet": frame(close=200)}, "absent")
    else: u = Bulk({"unexpected.parquet": frame()})
    with pytest.raises(RuntimeError): kr_db.download_all(u)
    assert path.read_bytes() == before


@pytest.mark.parametrize("fault", ["candidate", "fsync", "replace"])
def test_bulk_staging_fault_preserves_good_partition(fault, monkeypatch):
    path, before = save_raw(2026, frame())
    if fault == "candidate":
        original = kr_db._write_staged_frame
        def corrupt(data, year, destination):
            original(data.iloc[:0], year, destination)
            raise OSError("synthetic candidate interrupted")
        monkeypatch.setattr(kr_db, "_write_staged_frame", corrupt)
    else:
        def fail(*args): raise OSError("synthetic storage failure")
        monkeypatch.setattr(kr_db.os, fault, fail)
    with pytest.raises(Exception): kr_db.download_all(Bulk({"marcap-2026.parquet": frame(close=200)}))
    assert path.read_bytes() == before


def functions(relative, names, **bindings):
    tree = ast.parse((Path(__file__).parents[1] / relative).read_text(encoding="utf-8-sig"))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    scope = {"logger": logging.getLogger("manual-synthetic"), "pd": pd, "date": date,
             "datetime": datetime, "timedelta": timedelta, "Path": Path,
             "json": json, "tempfile": tempfile, "re": re, "ohlc_db": ohlc_db, "kr_db": kr_db}
    scope.update(bindings)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<manual-synthetic>", "exec"), scope)
    return scope


def test_verify_kr_corrupt_local_is_non_success():
    save_raw(2026, b"broken")
    scope = functions("scripts/verify_kr.py", {"verify", "analyze_year"},
                      print_report=lambda analyses: pytest.fail("corrupt file must stop before report"))
    with pytest.raises(RuntimeError, match="kr_baseline_invalid"):
        scope["verify"](SimpleNamespace(drive=False, fix=False))


def test_verify_kr_fix_validates_price_before_any_append(monkeypatch):
    path, before = save_raw(2026, frame())
    scope = functions("scripts/verify_kr.py", {"verify", "analyze_year"},
                      print_report=lambda analyses: [(2026, date(2026, 9, 2), date(2026, 9, 3), 2)])
    candidate = frame()
    candidate.attrs["kr_price_basis"] = {"provider": "yfinance", "auto_adjust": True}
    monkeypatch.setattr(kr_collector, "collect_backfill", lambda *args: candidate)
    monkeypatch.setattr(kr_db, "append_rows", lambda *args, **kwargs: pytest.fail("unverified price append"))
    with pytest.raises(kr_collector.KrCollectionError, match="price_basis_unverified"):
        scope["verify"](SimpleNamespace(drive=False, fix=True))
    assert path.read_bytes() == before


def test_resave_requested_year_baselines_all_precede_save(monkeypatch):
    scope = functions("scripts/resave_ohlc.py", {"resave"})
    first = ohlc_db.local_path("us", 2025)
    first.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"Ticker": ["SYN"], "Date": [date(2025, 9, 1)], "Close": [1]}).to_parquet(first, index=False)
    second = ohlc_db.local_path("us", 2026)
    second.write_bytes(b"corrupt")
    before = first.read_bytes()
    monkeypatch.setattr(ohlc_db, "download_all_years", lambda *args: "ok")
    monkeypatch.setattr(ohlc_db, "save_year", lambda *args: pytest.fail("second baseline failed"))
    with pytest.raises(ohlc_db.DriveSyncError): scope["resave"](SimpleNamespace(market="us", years=[2025, 2026], upload=False))
    assert first.read_bytes() == before


def test_resave_upload_failure_propagates(monkeypatch):
    scope = functions("scripts/resave_ohlc.py", {"resave"})
    path = ohlc_db.local_path("us", 2026)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"Ticker": ["SYN"], "Date": [date(2026, 9, 1)], "Close": [1]}).to_parquet(path, index=False)
    monkeypatch.setattr(ohlc_db, "download_all_years", lambda *args: "ok")
    monkeypatch.setattr(ohlc_db, "check_coverage_continuity", lambda frame: {"n_bad": 0})
    monkeypatch.setattr(ohlc_db, "save_year", lambda *args: None)
    monkeypatch.setattr(ohlc_db, "upload_years", lambda *args: ["us_2026.parquet"])
    with pytest.raises(ohlc_db.DriveSyncError, match="resave_publication_failed"):
        scope["resave"](SimpleNamespace(market="us", years=[2026], upload=True))


def test_verify_ohlc_fix_strict_local_preflight_before_pending_publish(monkeypatch):
    good = ohlc_db.local_path("us", 2026)
    good.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"Ticker": ["SYN"], "Date": [date(2026, 9, 1)], "Close": [1]}).to_parquet(good, index=False)
    ohlc_db.local_path("us", 2025).write_bytes(b"corrupt")
    scope = functions("scripts/verify_ohlc.py", {"verify", "analyze_market"}, print_report=lambda *a: None,
                      apply_fix=lambda results: pytest.fail("incomplete local history cannot create pending"))
    with pytest.raises(ohlc_db.DriveSyncError):
        scope["verify"](SimpleNamespace(market="us", drive=False, fix=True, after="2024-01-01"), date(2024, 1, 1))


@pytest.mark.parametrize("suffix", ["invalid", "02026", "+2026"])
def test_verify_kr_invalid_partition_name_is_not_empty_success(suffix):
    kr_db._LOCAL_ROOT.mkdir(parents=True)
    (kr_db._LOCAL_ROOT / f"marcap-{suffix}.parquet").write_bytes(b"not a partition")
    scope = functions("scripts/verify_kr.py", {"verify", "analyze_year"},
                      print_report=lambda analyses: pytest.fail("invalid file name cannot disappear"))
    with pytest.raises(RuntimeError, match="kr_local_filename_invalid"):
        scope["verify"](SimpleNamespace(drive=False, fix=False))


def test_pending_fix_unions_remote_and_local_then_one_confirmed_publication(monkeypatch):
    ohlc_db.save_pending({"us": ["LOCAL"]})
    class PendingDrive:
        def download(self, remote, name, destination):
            Path(destination).write_text(json.dumps({"us": ["REMOTE"], "crypto": ["BTC-USD"]}), encoding="utf-8")
            return True
    monkeypatch.setattr(ohlc_db, "_get_uploader", lambda *a: PendingDrive())
    calls = []
    monkeypatch.setattr(ohlc_db, "publish_pending", lambda pending, upload: calls.append((pending, upload)))
    scope = functions("scripts/verify_ohlc.py", {"pending_baseline", "apply_fix"})
    scope["apply_fix"]([{"market": "us", "new": ["NEW"]}])
    assert calls == [({"us": ["LOCAL", "NEW", "REMOTE"], "crypto": ["BTC-USD"]}, True)]
