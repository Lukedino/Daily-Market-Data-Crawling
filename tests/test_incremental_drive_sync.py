"""매일 도는 증분 경로의 Drive 동기화 불변식 (2026-09-19 심층 검토 D-01·D-02).

D-01  Drive 다운로드 **실패**를 "파일 없음" 으로 취급하면 당해 연도 파일이 증분 며칠 치로 교체돼 올라간다.
      3상태 가드는 백필 경로에만 있었다.
D-02  업로드가 실패해도 커서(db_status.json)가 전진하면 그날은 다시 수집되지 않는 영구 구멍이 된다.
"""
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import main as entry
from data import ohlc_collector as oc
from data import ohlc_db


def _new_rows():
    day = date.today() - timedelta(days=1)
    return pd.DataFrame({"Date": [day], "Ticker": ["AAA"], "Open": [1.0], "High": [1.0], "Low": [1.0],
                         "Close": [1.0], "Volume": [1.0]})


@pytest.fixture
def harness(tmp_path, monkeypatch):
    calls = {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    monkeypatch.setattr(ohlc_db, "_STATUS_PATH", tmp_path / "db_status.json")
    monkeypatch.setattr(ohlc_db, "_PENDING_PATH", tmp_path / "backfill_pending.json")
    monkeypatch.setattr(ohlc_db, "download_status", lambda *a, **k: None)
    monkeypatch.setattr(ohlc_db, "load_status",
                        lambda: {"us": {"last_updated": str(date.today() - timedelta(days=3))}})
    monkeypatch.setattr(oc, "load_tickers", lambda m: ["AAA"])
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda t: {})
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (_new_rows(), []))
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda df: df)

    def count(name, result=None):
        def inner(*a, **k):
            calls[name] += 1
            return result
        return inner
    monkeypatch.setattr(ohlc_db, "append_rows", count("append", [date.today().year]))
    monkeypatch.setattr(ohlc_db, "upload_years", count("upload_years", []))
    monkeypatch.setattr(ohlc_db, "update_status", count("update_status"))
    monkeypatch.setattr(ohlc_db, "upload_status", count("upload_status"))
    return calls, monkeypatch


def test_failed_baseline_download_stops_before_anything_is_written(harness):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "failed")
    with pytest.raises(ohlc_db.DriveSyncError):
        oc.update_market("us", upload=True)
    assert calls == {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}


@pytest.mark.parametrize("state", ["ok", "absent"])
def test_healthy_baseline_proceeds(harness, state):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: state)
    oc.update_market("us", upload=True)
    assert calls == {"append": 1, "upload_years": 1, "update_status": 1, "upload_status": 1}


def test_failed_upload_does_not_advance_the_cursor(harness):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")

    def failing_upload(market, years, uploader=None):
        calls["upload_years"] += 1
        return [f"{market}_{years[0]}.parquet"]              # 실패한 파일 목록
    monkeypatch.setattr(ohlc_db, "upload_years", failing_upload)
    with pytest.raises(ohlc_db.DriveSyncError):
        oc.update_market("us", upload=True)
    assert calls["update_status"] == 0 and calls["upload_status"] == 0   # 다음 실행이 같은 날을 다시 수집


@pytest.mark.parametrize("fault", ["empty", "exception"])
def test_unusable_collection_writes_nothing_and_leaks_no_provider_text(harness, fault):
    calls, monkeypatch = harness
    tickers = [f"T{i:03}" for i in range(100)]
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")
    if fault == "exception":
        def collect(*a, **k):
            raise OSError("SYNTHETIC_SECRET")
    else:
        collect = lambda *a, **k: (pd.DataFrame(), [])
    monkeypatch.setattr(oc, "fetch_ohlc_range", collect)
    with pytest.raises(oc.CollectionIncompleteError) as caught:
        oc.update_market("us", tickers=tickers, upload=True)
    assert "SYNTHETIC_SECRET" not in str(caught.value)
    assert calls == {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}


@pytest.mark.parametrize("fault", ["reported", "silent"])
def test_one_of_hundred_missing_still_publishes_the_verified_ninety_nine(harness, fault):
    """공급자는 매일 소수 종목을 돌려주지 않는다 — US 실측이 1,058 요청 중 1,053 수집이다.
    검증된 행까지 버리면 수집이 영구히 멈춘다(2026-09-22 실제 장애). 못 받은 종목의
    날짜는 게시되지 않고, 커서는 검증된 집합 기준으로만 전진한다."""
    calls, monkeypatch = harness
    tickers = [f"T{i:03}" for i in range(100)]
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")
    rows = pd.concat([_new_rows().assign(Ticker=t) for t in tickers[:99]], ignore_index=True)
    monkeypatch.setattr(oc, "fetch_ohlc_range",
                        lambda *a, **k: (rows, [tickers[-1]] if fault == "reported" else []))
    oc.update_market("us", tickers=tickers, upload=True)
    assert calls == {"append": 1, "upload_years": 1, "update_status": 1, "upload_status": 1}


def test_one_market_failure_does_not_stop_the_other(monkeypatch):
    """market=all 은 시장마다 독립이다 — 2026-09-22 실행은 US 에서 죽어 크립토가
    시작조차 못 했다. 실패는 여전히 잡을 실패시키되 남은 시장은 수집을 마친다."""
    started = []

    def update(market, upload):
        started.append(market)
        if market == "us":
            raise oc.CollectionIncompleteError("collection_incomplete", {"T099"})
    monkeypatch.setattr(oc, "backfill_new_tickers", lambda **kwargs: [])
    monkeypatch.setattr(oc, "update_market", update)
    with pytest.raises(oc.CollectionIncompleteError, match="collection_incomplete"):
        entry.run_ohlc_update(SimpleNamespace(dry_run=False, market="all", upload_drive=True))
    assert started == ["us", "crypto"]


def test_mass_missing_is_a_collection_fault_and_still_writes_nothing(harness):
    """소수 누락은 공급자 사정이지만 대량 누락은 수집 자체의 고장이다.
    연도 파일 축소 가드와 같은 임계(5%)를 넘으면 기존처럼 중단한다."""
    calls, monkeypatch = harness
    tickers = [f"T{i:03}" for i in range(100)]
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")
    rows = pd.concat([_new_rows().assign(Ticker=t) for t in tickers[:90]], ignore_index=True)
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (rows, []))
    with pytest.raises(oc.CollectionIncompleteError, match="collection_incomplete"):
        oc.update_market("us", tickers=tickers, upload=True)
    assert calls == {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}


def test_lagging_foreign_calendar_does_not_publish_the_other_tickers_maximum(harness):
    calls, monkeypatch = harness
    old = date.today() - timedelta(days=3)
    newer = date.today() - timedelta(days=1)
    frame = pd.concat([_new_rows().assign(Ticker="AAA", Date=newer),
                       _new_rows().assign(Ticker="U-UN.TO", Date=old)], ignore_index=True)
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (frame, []))
    cursors = []
    monkeypatch.setattr(ohlc_db, "publish_status", lambda market, last, *a, **k: cursors.append(last))
    oc.update_market("us", tickers=["AAA", "U-UN.TO"], upload=True)
    assert cursors == [old] and calls["append"] == 1


def test_same_calendar_internal_hole_does_not_certify_that_ticker():
    """구멍 난 종목은 커서를 확정해 주지 못한다 — 휴장/정지라고 추정하지 않는다.
    다만 깨끗한 종목이 남아 있으면 그 기준으로 게시한다(계약 변경 2026-09-24)."""
    frame = pd.DataFrame({"Ticker": ["AAA"] * 3 + ["BBB"] * 2,
                          "Date": [date(2026, 1, n) for n in (5, 6, 7, 5, 7)]})
    assert oc._incremental_cursor(frame, ["AAA", "BBB"], [], date(2026, 1, 5), "us") == date(2026, 1, 7)


def test_year_end_incomplete_retries_the_same_cursor_then_advances(tmp_path, monkeypatch):
    class Clock(date):
        @classmethod
        def today(cls):
            return cls(2027, 1, 2)
    monkeypatch.setattr(oc, "date", Clock)
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path)
    monkeypatch.setattr(ohlc_db, "_STATUS_PATH", tmp_path / "status.json")
    monkeypatch.setattr(ohlc_db, "_PENDING_PATH", tmp_path / "pending.json")
    monkeypatch.setattr(oc, "build_symbol_overrides", lambda *a: {})
    monkeypatch.setattr(oc, "_enrich_us_marketcap", lambda frame: frame)
    ohlc_db.save_status({"us": {"last_updated": "2026-12-31"}})
    before = ohlc_db._STATUS_PATH.read_bytes()
    starts, complete = [], [False]
    def collect(tickers, start, end, **kwargs):
        starts.append(start)
        result = pd.concat([_new_rows().assign(Ticker=t, Date=date(2027, 1, 2))
                            for t in (tickers if complete[0] else tickers[:1])], ignore_index=True)
        return result, [] if complete[0] else [tickers[-1]]
    monkeypatch.setattr(oc, "fetch_ohlc_range", collect)
    with pytest.raises(oc.CollectionIncompleteError):
        oc.update_market("us", ["AAA", "BBB"], upload=False)
    assert ohlc_db._STATUS_PATH.read_bytes() == before
    assert not ohlc_db.local_path("us", 2027).exists()
    complete[0] = True
    oc.update_market("us", ["AAA", "BBB"], upload=False)
    assert starts == ["2026-12-31", "2026-12-31"]
    assert ohlc_db.load_status()["us"]["last_updated"] == "2027-01-02"


def test_upload_years_reports_which_files_failed(tmp_path, monkeypatch):
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path / "ohlc_db")
    for year in (2025, 2026):
        path = ohlc_db.local_path("us", year)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = _new_rows()
        data["Date"] = date(year, 1, 2)
        data.to_parquet(path, index=False)

    class Uploader:
        def upload(self, local, remote):
            if "2026" in Path(local).name:
                raise OSError("503")
            return True
    monkeypatch.setitem(ohlc_db.config.DRIVE_PATHS, "ohlc_us", "data/ohlc/us")
    assert ohlc_db.upload_years("us", [2025, 2026], uploader=Uploader()) == ["us_2026.parquet"]


@pytest.mark.parametrize("receipt,success", [(None, False), ("", False), ("  ", False),
                                            (False, False), (0, False), (1, False),
                                            ({"id": "fake"}, False), (True, True),
                                            ("synthetic-file-id", True)])
def test_upload_requires_explicit_receipt_for_year_and_metadata(tmp_path, monkeypatch, receipt, success):
    monkeypatch.setattr(ohlc_db, "_LOCAL_ROOT", tmp_path)
    path = ohlc_db.local_path("us", 2026)
    path.parent.mkdir(parents=True, exist_ok=True)
    _new_rows().assign(Date=date(2026, 1, 2)).to_parquet(path, index=False)
    metadata = tmp_path / "status.json"
    metadata.write_text("{}", encoding="utf-8")
    monkeypatch.setitem(ohlc_db.config.DRIVE_PATHS, "ohlc_us", "synthetic/us")
    monkeypatch.setitem(ohlc_db.config.DRIVE_PATHS, "ohlc_meta", "synthetic/meta")
    uploader = SimpleNamespace(upload=lambda *a: receipt)
    assert ohlc_db.upload_years("us", [2026], uploader) == ([] if success else ["us_2026.parquet"])
    if success:
        assert ohlc_db._upload_metadata(metadata, uploader) is True
    else:
        with pytest.raises(ohlc_db.DriveSyncError):
            ohlc_db._upload_metadata(metadata, uploader)


@pytest.mark.parametrize("fault", ["mismatch", "incomplete"])
def test_unverified_basis_stops_before_save_upload_and_cursor(harness, fault):
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "absent")
    original = _new_rows()
    ohlc_db.save_year(original, "us", date.today().year)
    path = ohlc_db.local_path("us", date.today().year)
    before = path.read_bytes()
    candidate = original.copy()
    if fault == "mismatch":
        candidate["Close"] = 2.
    else:
        # actions 를 못 받은 경우는 계속 보류한다 — 기준 변경을 판정할 근거가 없다.
        candidate.attrs["ohlc_request"] = {"actions_complete": False, "action_tickers": []}
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (candidate, []))
    with pytest.raises(ohlc_db.PriceBasisError):
        oc.update_market("us", upload=True)
    assert path.read_bytes() == before
    assert calls == {"append": 0, "upload_years": 0, "update_status": 0, "upload_status": 0}


def test_dividend_in_the_window_no_longer_holds_the_whole_market(harness):
    """배당·분할이 관측된 종목이 하나라도 있으면 시장 전체를 보류하던 계약을 바꿨다.
    80거래일 표본에서 2거래일 이상 창에 배당이 없던 적이 0회라 US 는 영구 보류였다.
    재조정된 값이 저장본과 달라도 그 종목만 overlap 비교에서 빠지고 게시는 진행된다."""
    calls, monkeypatch = harness
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "absent")
    ohlc_db.save_year(_new_rows(), "us", date.today().year)
    candidate = _new_rows()
    candidate["Close"] = 0.998                      # auto_adjust 소급 재조정
    candidate.attrs["ohlc_request"] = {"actions_complete": True, "action_tickers": ["AAA"]}
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (candidate, []))
    oc.update_market("us", upload=True)
    assert calls == {"append": 1, "upload_years": 1, "update_status": 1, "upload_status": 1}


# ── KR 일별: 같은 불변식. KR 은 Marcap·Rank 과거값을 다시 받을 길이 없어 더 나쁘다 ──────────
from types import SimpleNamespace

import main as kr_main
from data import kr_db


def _kr_rows(year=2026):
    return pd.DataFrame({"Code": ["000001"], "Date": [date(year, 1, 2)],
                         "Open": [100], "High": [101], "Low": [99], "Close": [100],
                         "Volume": [1000], "Marcap": [100000], "Rank": [1], "Stocks": [1000]})


class _Uploader:
    def __init__(self, download=None, upload=None):
        self._download, self._upload = download, upload

    def download(self, remote, name, dest):
        if isinstance(self._download, Exception):
            raise self._download
        year = int(name.removeprefix("marcap-").removesuffix(".parquet"))
        _kr_rows(year).to_parquet(dest, index=False)

    def upload(self, local, remote):
        if isinstance(self._upload, Exception):
            raise self._upload
        return True


@pytest.fixture
def kr_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "local_path", lambda year: tmp_path / f"marcap-{year}.parquet")
    monkeypatch.setitem(kr_db.config.DRIVE_PATHS, "ohlc_kr", "data/ohlc/kr")
    return tmp_path


@pytest.mark.parametrize("outcome, expected", [(None, "ok"), (FileNotFoundError("none"), "absent"),
                                               (OSError("503"), "failed")])
def test_kr_download_distinguishes_absent_from_failed(kr_paths, outcome, expected):
    assert kr_db.download_year_state(2026, uploader=_Uploader(download=outcome)) == expected


def test_kr_upload_reports_failed_files(kr_paths):
    _kr_rows().to_parquet(kr_paths / "marcap-2026.parquet", index=False)
    assert kr_db.upload_years([2026], uploader=_Uploader(upload=OSError("503"))) == ["marcap-2026.parquet"]
    assert kr_db.upload_years([2026], uploader=_Uploader()) == []


def test_kr_daily_stops_when_the_baseline_download_failed(kr_paths, monkeypatch):
    monkeypatch.setattr(kr_db, "download_year_state", lambda year, uploader=None: "failed")
    monkeypatch.setattr(kr_db, "append_rows", lambda df: pytest.fail("기준 파일 없이 저장하면 연도 파일이 교체된다"))
    monkeypatch.setattr(kr_db, "upload_years", lambda years, uploader=None: pytest.fail("업로드 금지"))
    with pytest.raises(SystemExit) as stopped:
        kr_main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True))
    assert stopped.value.code == 1


def _holed_frame(tickers, holed, days):
    """holed 종목만 가운데 날짜를 비운 프레임."""
    rows = []
    for t in tickers:
        for d in days:
            if t in holed and d == days[1]:
                continue
            rows.append({"Ticker": t, "Date": d})
    return pd.DataFrame(rows)


def test_one_ticker_with_an_internal_hole_does_not_discard_the_market():
    """그 날짜를 통째로 못 받은 종목은 이미 허용한다. 일부라도 받은 종목을 더
    엄하게 다루면 계약이 서로 어긋나고, 창이 넓어질수록 반드시 재발한다
    (2026-09-24 실제로 US·크립토 둘 다 이 게이트에서 멈췄다)."""
    days = [date(2026, 1, n) for n in (5, 6, 7)]
    tickers = [f"T{i:03}" for i in range(100)]
    frame = _holed_frame(tickers, {"T042"}, days)
    # 구멍 난 종목은 커서를 확정해 주지 못하므로 나머지 99종목 기준으로 잡는다.
    assert oc._incremental_cursor(frame, tickers, [], date(2026, 1, 4), "us") == days[-1]


def test_many_internal_holes_still_publish_from_the_clean_tickers():
    """구멍 개수로 잡을 죽이지 않는다 — 공개 로그가 값을 싣지 않아 임계를 맞출
    수가 없고(2026-09-24 두 번 연속 이 게이트에서 멈췄다), 대량 고장은 이미
    collection_incomplete·empty_unverified 가 잡는다."""
    days = [date(2026, 1, n) for n in (5, 6, 7)]
    tickers = [f"T{i:03}" for i in range(100)]
    frame = _holed_frame(tickers, {f"T{i:03}" for i in range(40)}, days)
    assert oc._incremental_cursor(frame, tickers, [], date(2026, 1, 4), "us") == days[-1]


def test_every_ticker_holed_is_a_collection_fault():
    """커서를 확정해 줄 종목이 하나도 남지 않으면 수집 자체의 고장이다.
    ⚠️ 모든 종목이 **같은** 날을 빼면 그건 구멍이 아니라 휴장이다(그 날짜가
    아무에게도 관측되지 않으므로). 서로 다른 날을 빼야 전부 구멍이 된다."""
    days = [date(2026, 1, n) for n in (5, 6, 7, 8)]
    frame = pd.DataFrame(
        [{"Ticker": "AAA", "Date": d} for d in days if d != days[1]] +
        [{"Ticker": "BBB", "Date": d} for d in days if d != days[2]])
    with pytest.raises(oc.CollectionIncompleteError, match="session_unverified"):
        oc._incremental_cursor(frame, ["AAA", "BBB"], [], date(2026, 1, 4), "us")


def _coverage_frame(days_by_ticker):
    return pd.DataFrame([{"Ticker": t, "Date": d} for t, days in days_by_ticker.items() for d in days])


def test_a_date_most_tickers_lack_holds_the_cursor_in_front_of_it():
    """공급자가 특정 날짜를 대부분 종목에 주지 않는 일이 있다 — 2026-09-24 실측으로
    US 09-22 봉이 중형주 28/28 에 없고 메가캡에만 있었다. 커서가 그 날짜를 지나면
    다시 요청되지 않아 영구 결손이 된다."""
    days = [date.today() - timedelta(days=n) for n in (3, 2, 1)]   # 오래된 순
    full, sparse_day = days[0], days[1]
    by = {f"T{i:03}": ([full, days[2]] if i else [full, sparse_day, days[2]]) for i in range(20)}
    cursor = oc._incremental_cursor(_coverage_frame(by), list(by), [], full, "us")
    assert cursor == full, "결손 날짜 앞에서 멈춰야 한다"


def test_the_hold_gives_up_after_a_week_so_the_window_cannot_grow_forever():
    """공급자가 끝내 복구하지 않으면 창이 무한히 커진다 — 그 날짜는 잃지만
    수집은 계속되는 편이 낫다."""
    days = [date.today() - timedelta(days=n) for n in (3, 2, 1)]
    stale = date.today() - timedelta(days=oc.MAX_CURSOR_HOLD_DAYS + 1)
    by = {f"T{i:03}": ([days[0], days[2]] if i else days) for i in range(20)}
    cursor = oc._incremental_cursor(_coverage_frame(by), list(by), [], stale, "us")
    assert cursor == days[2], "탈출 후에는 정상 커서로 전진한다"


def test_sparse_dates_are_kept_out_of_what_gets_published(harness):
    """커버리지 연속성 게이트는 제대로 동작하는 것이다 — 12% 짜리 날짜를 파일에
    넣지 않는다. 그 날짜 행만 빼고 나머지는 정상 게시한다."""
    calls, monkeypatch = harness
    tickers = [f"T{i:03}" for i in range(20)]
    monkeypatch.setattr(ohlc_db, "download_year_state", lambda *a, **k: "ok")
    good, thin = date.today() - timedelta(days=2), date.today() - timedelta(days=1)
    rows = pd.concat(
        [_new_rows().assign(Ticker=t, Date=good) for t in tickers] +
        [_new_rows().assign(Ticker=tickers[0], Date=thin)], ignore_index=True)
    published = {}
    monkeypatch.setattr(ohlc_db, "append_rows",
                        lambda df, market: published.setdefault("days", set(df["Date"])) or [date.today().year])
    monkeypatch.setattr(oc, "fetch_ohlc_range", lambda *a, **k: (rows, []))
    oc.update_market("us", tickers=tickers, upload=True)
    assert thin not in published["days"] and good in published["days"]


def test_sparse_session_detection_counts_tickers_not_rows():
    days = [date(2026, 1, 5), date(2026, 1, 6)]
    by = {f"T{i:02}": ([days[0], days[1]] if i < 2 else [days[0]]) for i in range(10)}
    assert oc.sparse_session_dates(_coverage_frame(by)) == [days[1]]   # 2/10 = 20% < 50%
    full = {f"T{i:02}": days for i in range(10)}
    assert oc.sparse_session_dates(_coverage_frame(full)) == []
