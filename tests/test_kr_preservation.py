"""KR baseline/metadata preservation with synthetic Parquet and fake Drive only."""
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import main
from data import kr_collector, kr_db


def rows(*items, **overrides):
    records = []
    for code, day in items:
        record = dict(Code=code, Date=pd.Timestamp(day), Name="Synthetic", Close=100.0,
                      Open=99.0, High=101.0, Low=98.0, Volume=10, Amount=987.0,
                      Marcap=100000.0, Stocks=1000, Rank=1, Dept="sector",
                      ChangeCode="1", Changes=1.0, ChangesRatio=1.0,
                      Market="KOSPI", MarketId="STK")
        record.update(overrides)
        records.append(record)
    return pd.DataFrame(records, columns=kr_db.SCHEMA_COLS)


class FakeDrive:
    def __init__(self, baselines=None, *, events=None, fail_upload=False):
        self.baselines = baselines or {}
        self.events = events if events is not None else []
        self.uploaded = []
        self.fail_upload = fail_upload

    def download(self, remote, name, destination):
        year = int(Path(name).stem.split("-")[-1])
        self.events.append(("download", year))
        outcome = self.baselines.get(year, FileNotFoundError("synthetic absent"))
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, bytes):
            Path(destination).write_bytes(outcome)
        else:
            outcome.to_parquet(destination, index=False)

    def upload(self, local, remote):
        self.events.append(("upload", Path(local).name))
        if self.fail_upload:
            raise OSError("synthetic upload failure")
        self.uploaded.append(pd.read_parquet(local))


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "_LOCAL_ROOT", tmp_path / "kr")
    monkeypatch.setattr(kr_db, "_STATUS_PATH", tmp_path / "meta" / "kr_status.json")
    monkeypatch.setitem(kr_db.config.DRIVE_PATHS, "ohlc_kr", "synthetic-kr")
    monkeypatch.setattr(kr_collector, "collect_backfill", lambda *a, **k: pytest.fail("unexpected collector"))
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: pytest.fail("unexpected daily collector"))
    monkeypatch.setattr(kr_db, "_get_uploader", lambda uploader=None: uploader)
    return kr_db


def args(**kwargs):
    return SimpleNamespace(**(dict(start_date="2026-01-05", end_date="2026-01-06",
                                   dry_run=False, upload_drive=True) | kwargs))


def setup_run(db, monkeypatch, collected, drive):
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: uploader or drive)

    def collect(*a, **k):
        drive.events.append(("collect",))
        return collected.copy()

    monkeypatch.setattr(kr_collector, "collect_backfill", collect)


def test_backfill_downloads_all_years_before_collection(db, monkeypatch):
    baselines = {year: rows(("000001", f"{year}-01-02")) for year in (2024, 2025, 2026)}
    drive = FakeDrive(baselines)
    collected = rows(("000002", "2025-12-31"), ("000002", "2026-01-02"),
                     Marcap=float("nan"), Stocks=0, Rank=0)
    setup_run(db, monkeypatch, collected, drive)
    main.run_kr_backfill(args(start_date="2024-12-31", end_date="2026-01-02"))
    assert drive.events[:4] == [("download", 2024), ("download", 2025), ("download", 2026), ("collect",)]
    assert len(drive.uploaded) == 2
    assert set(db.load_year(2025, strict=True)["Code"]) == {"000001", "000002"}


@pytest.mark.parametrize("bad", [OSError("synthetic"), b"broken parquet",
    rows(("000001", "2024-01-02")), pd.DataFrame({"Wrong": [1]}),
    rows(("000001", "2025-01-02"), ("000001", "2025-01-02"))])
def test_failed_baseline_blocks_collection_save_upload_and_status(db, monkeypatch, bad):
    db.save_year(rows(("000009", "2025-01-02")), 2025)
    before = db.local_path(2025).read_bytes()
    drive = FakeDrive({2025: bad, 2026: rows(("000001", "2026-01-02"))})
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: drive)
    monkeypatch.setattr(db, "append_rows", lambda *a, **k: pytest.fail("unexpected save"))
    with pytest.raises(SystemExit) as error:
        main.run_kr_backfill(args(start_date="2025-12-31", end_date="2026-01-06"))
    assert error.value.code == 1
    assert drive.events == [("download", 2025), ("download", 2026)]
    assert not drive.uploaded
    assert not db._STATUS_PATH.exists()
    assert db.local_path(2025).read_bytes() == before


def test_local_only_corrupt_baseline_stops_before_collection(db):
    db._LOCAL_ROOT.mkdir()
    path = db.local_path(2026)
    path.write_bytes(b"corrupt baseline")
    with pytest.raises(SystemExit) as error:
        main.run_kr_backfill(args(upload_drive=False))
    assert error.value.code == 1
    assert path.read_bytes() == b"corrupt baseline"


@pytest.mark.parametrize("baseline", [FileNotFoundError("absent"), pd.DataFrame(columns=kr_db.SCHEMA_COLS)])
def test_absent_or_empty_placeholder_allows_new_history(db, monkeypatch, baseline):
    drive = FakeDrive({2026: baseline})
    setup_run(db, monkeypatch, rows(("000001", "2026-01-05")), drive)
    main.run_kr_backfill(args())
    assert len(drive.uploaded) == 1
    assert db.load_status()["last_updated"] == "2026-01-05"


def test_local_presence_does_not_skip_remote_failure(db, monkeypatch):
    db.save_year(rows(("000001", "2026-01-02")), 2026)
    drive = FakeDrive({2026: OSError("synthetic")})
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: drive)
    with pytest.raises(SystemExit):
        main.run_kr_backfill(args())
    assert drive.events == [("download", 2026)]


def test_two_day_backfill_preserves_other_keys_and_metadata(db, monkeypatch):
    baseline = rows(("000001", "2026-01-02"), ("000001", "2026-01-05"),
                    ("000002", "2026-01-05"))
    drive = FakeDrive({2026: baseline})
    collected = rows(("000001", "2026-01-05"), ("000001", "2026-01-06"),
                     Close=105.0, Marcap=float("nan"), Stocks=0, Rank=0,
                     Amount=1050.0, Dept=None, ChangeCode=None)
    setup_run(db, monkeypatch, collected, drive)
    main.run_kr_backfill(args())
    result = db.load_year(2026, strict=True).set_index(["Code", "Date"])
    assert len(result) == 4
    overlap = result.loc[("000001", pd.Timestamp("2026-01-05"))]
    assert overlap["Close"] == 105
    for column in ("Marcap", "Stocks", "Rank", "Amount", "Dept", "ChangeCode"):
        assert overlap[column] == baseline.iloc[1][column]
    assert result.loc[("000001", pd.Timestamp("2026-01-06")), "Amount"] == 1050
    assert pd.isna(result.loc[("000001", pd.Timestamp("2026-01-06")), "Marcap"])
    pd.testing.assert_frame_equal(drive.uploaded[0], result.reset_index()[db.SCHEMA_COLS], check_dtype=False)


@pytest.mark.parametrize("missing", [None, float("nan"), 0])
def test_backfill_sentinels_do_not_erase_metadata(db, missing):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    db.append_rows(rows(("000001", "2026-01-05"), Marcap=missing, Stocks=missing,
                        Rank=missing, Amount=missing), ohlc_only=True)
    result = db.load_year(2026, strict=True).iloc[0]
    assert [result[c] for c in ("Marcap", "Stocks", "Rank", "Amount")] == [100000, 1000, 1, 987]


def test_daily_genuine_zero_turnover_and_new_metadata_update(db):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    db.append_rows(rows(("000001", "2026-01-05"), Marcap=120000, Stocks=1200, Rank=2, Amount=0))
    result = db.load_year(2026, strict=True).iloc[0]
    assert [result[c] for c in ("Marcap", "Stocks", "Rank", "Amount")] == [120000, 1200, 2, 0]


def test_backfill_keeps_existing_zero_turnover(db):
    db.save_year(rows(("000001", "2026-01-05"), Amount=0), 2026)
    db.append_rows(rows(("000001", "2026-01-05"), Amount=1500), ohlc_only=True)
    assert db.load_year(2026, strict=True).iloc[0]["Amount"] == 0


def test_corrupt_baseline_save_preserves_bytes(db):
    db._LOCAL_ROOT.mkdir()
    db.local_path(2026).write_bytes(b"corrupt")
    with pytest.raises(db.KrStateError):
        db.save_year(rows(("000001", "2026-01-05")), 2026)
    assert db.local_path(2026).read_bytes() == b"corrupt"


def test_later_corrupt_year_blocks_earlier_year_save(db):
    db.save_year(rows(("000001", "2025-01-02")), 2025)
    before = db.local_path(2025).read_bytes()
    db.local_path(2026).write_bytes(b"broken")
    with pytest.raises(db.KrStateError):
        db.append_rows(rows(("000002", "2025-01-03"), ("000002", "2026-01-05")))
    assert db.local_path(2025).read_bytes() == before
    assert db.local_path(2026).read_bytes() == b"broken"


@pytest.mark.parametrize("failure", ["write", "fsync", "replace", "invalid_candidate"])
def test_failed_atomic_save_preserves_prior_file(db, monkeypatch, failure):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    before = db.local_path(2026).read_bytes()

    def fail(*a, **k):
        raise OSError("synthetic failure")

    if failure == "write":
        def partial(table, where, **kwargs):
            Path(where).write_bytes(b"partial")
            fail()
        monkeypatch.setattr(db.pq, "write_table", partial)
    elif failure == "invalid_candidate":
        monkeypatch.setattr(db.pq, "write_table", lambda table, where, **kwargs: Path(where).write_bytes(b"invalid"))
    else:
        monkeypatch.setattr(db.os, failure, fail)
    with pytest.raises(db.KrStateError):
        db.save_year(rows(("000002", "2026-01-06")), 2026)
    assert db.local_path(2026).read_bytes() == before
    assert list(db._LOCAL_ROOT.glob(".marcap-*.parquet")) == []


def test_validated_roundtrip_catches_silent_row_loss(db, monkeypatch):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    before = db.local_path(2026).read_bytes()
    real_write = db.pq.write_table
    monkeypatch.setattr(db.pq, "write_table", lambda table, where, **kwargs: real_write(table.slice(0, 1), where, **kwargs))
    with pytest.raises(db.KrStateError):
        db.save_year(rows(("000002", "2026-01-06")), 2026)
    assert db.local_path(2026).read_bytes() == before


@pytest.mark.parametrize("invalid", [rows(("000001", "2025-01-05")),
    rows(("", "2026-01-05")), rows((None, "2026-01-05")),
    pd.DataFrame({"Code": ["000001"], "Date": [None]}), pd.DataFrame({"Other": [1]})])
def test_invalid_keys_or_year_never_replace_baseline(db, invalid):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    before = db.local_path(2026).read_bytes()
    with pytest.raises((db.KrStateError, KeyError, ValueError)):
        db.save_year(invalid, 2026)
    assert db.local_path(2026).read_bytes() == before


def test_partial_download_failure_preserves_local_baseline(db):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    before = db.local_path(2026).read_bytes()

    class PartialDrive:
        def download(self, remote, name, destination):
            Path(destination).write_bytes(b"partial")
            raise OSError("synthetic failure")

    assert db.download_year_state(2026, uploader=PartialDrive()) == "failed"
    assert db.local_path(2026).read_bytes() == before
    assert list(db._LOCAL_ROOT.glob(".baseline-*.parquet")) == []


def test_absent_remote_does_not_make_corrupt_local_safe(db):
    db._LOCAL_ROOT.mkdir()
    db.local_path(2026).write_bytes(b"corrupt")
    assert db.ensure_year_baselines([2026], download=True, uploader=FakeDrive()) == {2026: "failed"}


def test_remote_refresh_preserves_unpublished_local_keys_and_metadata(db):
    local = rows(("000001", "2026-01-05"), ("000001", "2026-01-06"),
                 ("000002", "2026-01-06"), Marcap=123456, Stocks=1234, Rank=7, Amount=765)
    db.save_year(local, 2026)
    remote = rows(("000001", "2026-01-05"), ("000003", "2026-01-05"),
                  Close=120, Marcap=float("nan"), Stocks=0, Rank=0, Amount=float("nan"))
    assert db.download_year_state(2026, uploader=FakeDrive({2026: remote})) == "ok"
    result = db.load_year(2026, strict=True).set_index(["Code", "Date"])
    assert len(result) == 4
    assert result.loc[("000001", pd.Timestamp("2026-01-05")), "Close"] == 120
    for key in [("000001", pd.Timestamp("2026-01-05")), ("000001", pd.Timestamp("2026-01-06")),
                ("000002", pd.Timestamp("2026-01-06"))]:
        assert [result.loc[key, col] for col in ("Marcap", "Stocks", "Rank", "Amount")] == [123456, 1234, 7, 765]


def test_remote_valid_overlap_fields_take_precedence(db):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    remote = rows(("000001", "2026-01-05"), Close=111, Marcap=111000, Stocks=1110, Rank=3, Amount=0)
    assert db.download_year_state(2026, uploader=FakeDrive({2026: remote})) == "ok"
    result = db.load_year(2026, strict=True).iloc[0]
    assert [result[c] for c in ("Close", "Marcap", "Stocks", "Rank", "Amount")] == [111, 111000, 1110, 3, 0]


def test_empty_remote_placeholder_preserves_local_rows(db):
    local = rows(("000001", "2026-01-05"))
    db.save_year(local, 2026)
    assert db.download_year_state(2026, uploader=FakeDrive({2026: pd.DataFrame(columns=db.SCHEMA_COLS)})) == "ok"
    pd.testing.assert_frame_equal(db.load_year(2026, strict=True), local)


def test_corrupt_local_is_not_overwritten_by_remote_refresh(db):
    db._LOCAL_ROOT.mkdir()
    db.local_path(2026).write_bytes(b"corrupt local evidence")
    drive = FakeDrive({2026: rows(("000001", "2026-01-05"))})
    assert db.download_year_state(2026, uploader=drive) == "failed"
    assert drive.events == []
    assert db.local_path(2026).read_bytes() == b"corrupt local evidence"


def test_upload_missing_or_corrupt_files_is_failure(db):
    db._LOCAL_ROOT.mkdir()
    db.local_path(2026).write_bytes(b"broken")
    drive = FakeDrive()
    assert db.upload_years([2025, 2026], uploader=drive) == ["marcap-2025.parquet", "marcap-2026.parquet"]
    assert drive.events == []


def test_explicit_false_upload_result_is_failure(db):
    db.save_year(rows(("000001", "2026-01-05")), 2026)
    uploader = SimpleNamespace(upload=lambda local, remote: False)
    assert db.upload_years([2026], uploader=uploader) == ["marcap-2026.parquet"]


@pytest.mark.parametrize("failure", ["fsync", "replace", "serialization"])
def test_status_save_failure_is_atomic_and_observable(db, monkeypatch, failure):
    db.save_status(date(2026, 1, 5), 10)
    before = db._STATUS_PATH.read_bytes()

    def fail(*a, **k):
        raise OSError("synthetic status failure")

    count = 11
    if failure == "serialization":
        count = float("nan")
    else:
        monkeypatch.setattr(db.os, failure, fail)
    with pytest.raises(db.KrStateError):
        db.save_status(date(2026, 1, 6), count)
    assert db._STATUS_PATH.read_bytes() == before
    assert list(db._STATUS_PATH.parent.glob(".kr-status-*.json")) == []


def test_successful_status_write_is_valid_json(db):
    db.save_status(date(2026, 1, 5), 10)
    db.save_status(date(2026, 1, 6), 11)
    status = db.load_status()
    assert status["last_updated"] == "2026-01-06"
    assert status["trading_days_total"] == 11
    assert list(db._STATUS_PATH.parent.glob(".kr-status-*.json")) == []


def test_backfill_upload_failure_does_not_advance_status(db, monkeypatch):
    drive = FakeDrive(fail_upload=True)
    setup_run(db, monkeypatch, rows(("000001", "2026-01-05")), drive)
    db.save_status(date(2025, 12, 31), 10)
    before = db._STATUS_PATH.read_bytes()
    with pytest.raises(SystemExit) as error:
        main.run_kr_backfill(args())
    assert error.value.code == 1
    assert db._STATUS_PATH.read_bytes() == before


def test_daily_checks_remote_even_with_local_baseline(db, monkeypatch):
    year = date.today().year
    db.save_year(rows(("000001", f"{year}-01-02")), year)
    drive = FakeDrive({year: OSError("synthetic")})
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: drive)
    with pytest.raises(SystemExit) as error:
        main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True))
    assert error.value.code == 1
    assert drive.events == [("download", year)]
    assert not db._STATUS_PATH.exists()


def test_daily_upload_failure_does_not_advance_status(db, monkeypatch):
    today = date.today()
    baseline = rows(("000001", today - timedelta(days=1)))
    drive = FakeDrive({today.year: baseline}, fail_upload=True)
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: drive)
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: rows(("000001", today)))
    db.save_status(today - timedelta(days=1), 10)
    before = db._STATUS_PATH.read_bytes()
    with pytest.raises(SystemExit) as error:
        main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True))
    assert error.value.code == 1
    assert db._STATUS_PATH.read_bytes() == before


def test_daily_retry_keeps_local_only_snapshot_instead_of_ohlc_reconstruction(db, monkeypatch):
    today = date.today()
    year_start = today.replace(month=1, day=1)
    local_day = max(year_start, today - timedelta(days=1))
    remote_day = max(year_start, today - timedelta(days=2))
    local = rows(("000002", local_day), Marcap=234567, Stocks=2345, Rank=6, Amount=888)
    db.save_year(local, today.year)
    drive = FakeDrive({today.year: rows(("000001", remote_day))})
    monkeypatch.setattr(db, "_get_uploader", lambda uploader=None: drive)
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: rows(("000001", today), ("000002", today)))
    main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True))
    result = drive.uploaded[0]
    if local_day < today:
        preserved = result[(result["Code"] == "000002") & (result["Date"] == pd.Timestamp(local_day))].iloc[0]
        assert [preserved[col] for col in ("Marcap", "Stocks", "Rank", "Amount")] == [234567, 2345, 6, 888]
    assert not any(event[0] == "collect" for event in drive.events)


def test_dry_run_does_not_download_collect_save_or_upload(db, monkeypatch):
    monkeypatch.setattr(db, "ensure_year_baselines", lambda *a, **k: pytest.fail("unexpected baseline I/O"))
    main.run_kr_backfill(args(dry_run=True))
    assert not db._LOCAL_ROOT.exists()


@pytest.mark.parametrize("options", [dict(start_date=None), dict(end_date="invalid"),
                                     dict(start_date="2026-01-07", end_date="2026-01-06")])
def test_invalid_range_fails_before_baseline(db, monkeypatch, options):
    monkeypatch.setattr(db, "ensure_year_baselines", lambda *a, **k: pytest.fail("unexpected baseline"))
    with pytest.raises(SystemExit) as error:
        main.run_kr_backfill(args(**options))
    assert error.value.code == 1


@pytest.mark.parametrize("collected", [pd.DataFrame(), rows(("000001", "2027-01-05")),
                                       rows(("000001", "2026-01-07"))])
def test_empty_or_out_of_range_collection_cannot_save(db, monkeypatch, collected):
    drive = FakeDrive()
    setup_run(db, monkeypatch, collected, drive)
    monkeypatch.setattr(db, "append_rows", lambda *a, **k: pytest.fail("unexpected save"))
    with pytest.raises(SystemExit) as error:
        main.run_kr_backfill(args())
    assert error.value.code == 1
    assert not drive.uploaded
    assert not db._STATUS_PATH.exists()
