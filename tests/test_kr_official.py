"""
KR 잠정 일봉 → 공식 일봉 정정 (data/kr_official.py, 2026-10-10).

kr-daily 의 1순위 원천(FDR 캐시 최종본)은 2026년 실측에서 공식 일봉과 종가 11만 행·거래량
11만 행이 달랐다(다르면 거래량은 100% 적음). FinanceData/marcap 과 yfinance 원시가는 서로
독립인데 정확히 일치해 공식값으로 쓴다. marcap 은 하루~이틀 늦게 게시되므로 당일은 기존
원천으로 잠정 수집하고, 게시 직전마다 marcap 이 제공한 날짜를 공식값으로 정정한다.

여기서 잠그는 것
  - 잠정 행을 공식 행 전체(OHLCV 포함 18열)로 교체하고, 빠진 종목·날짜를 복구한다
  - 같은 입력이면 두 번째 실행은 아무것도 쓰지 않는다(멱등, 파일 바이트 동일)
  - 같은 실행 안에서 잠정 수집이 공식 날짜를 덮어도 게시본엔 공식값이 남는다
  - 공식 원천 지연(제공 날짜까지만)·장애·빈 응답·형식 오류에 데이터가 손상되지 않는다
  - 하한 이전 날짜와 KR_OFFICIAL_MIN_YEAR 이전 연도는 건드리지 않는다
  - 공식 확정 경계·정정 이력은 parquet 메타데이터에 남고, 잠정 저장이 그것을 지우지 않는다
"""
import io
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import main as kr_main
from data import kr_collector, kr_db, kr_official, krx_calendar

YEAR = date.today().year
CODES = [f"{i:06d}" for i in range(200000, 200150)]                 # 150종목 (게이트 하한 이상)
DAYS = list(pd.bdate_range(f"{YEAR}-03-02", periods=8))


def _rows(day, codes=CODES, close=1000.0, volume=100):
    n = len(codes)
    return pd.DataFrame({
        "Code": list(codes), "Name": ["n"] * n, "Close": [close] * n, "Dept": ["d"] * n,
        "ChangeCode": ["1"] * n, "Changes": [0.0] * n, "ChangesRatio": [0.0] * n,
        "Volume": [volume] * n, "Amount": [close * volume] * n, "Open": [close] * n,
        "High": [close + 5] * n, "Low": [close - 5] * n, "Marcap": [close * 1e6] * n,
        "Stocks": pd.array([10 ** 6] * n, dtype="Int64"), "Market": ["KOSPI"] * n,
        "MarketId": ["STK"] * n, "Rank": pd.array(range(1, n + 1), dtype="Int64"),
        "Date": [pd.Timestamp(day)] * n,
    })


def _official(days, codes=CODES, close=1010.0, volume=120):
    frame = pd.concat([_rows(d, codes, close, volume) for d in days], ignore_index=True)
    return kr_official.validate_official(frame, YEAR)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """대역 주입이 빠지면 실제 marcap(20MB)을 받는다 — 처음 작성 때 실제로 그랬다. 막아 둔다."""
    def refuse(*a, **k):
        raise AssertionError("테스트가 실제 네트워크에 접근했다")
    monkeypatch.setattr(kr_official.requests, "get", refuse)


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(kr_db, "_LOCAL_ROOT", tmp_path / "kr")
    monkeypatch.setattr(kr_db, "_STATUS_PATH", tmp_path / "meta" / "kr_status.json")
    monkeypatch.setattr(config, "KR_OFFICIAL_SETTLE_FROM", DAYS[0].date().isoformat())
    monkeypatch.setattr(config, "KR_OFFICIAL_MIN_YEAR", YEAR)
    return kr_db


def _provisional(db, days=DAYS[:5], codes=CODES):
    db.save_year(pd.concat([_rows(d, codes) for d in days], ignore_index=True), YEAR)


def _saved():
    return kr_db.load_year(YEAR, strict=True)


# ── 정정 자체 ────────────────────────────────────────────────────────────────

def test_provisional_ohlcv_is_replaced_by_official_values(db):
    _provisional(db)
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:4]))
    saved = _saved()
    settled = saved[saved.Date <= DAYS[3]]
    for column, value in {"Open": 1010.0, "High": 1015.0, "Low": 1005.0, "Close": 1010.0,
                          "Volume": 120, "Amount": 1010.0 * 120}.items():
        assert settled[column].eq(value).all(), column
    assert saved[saved.Date == DAYS[4]].Close.eq(1000.0).all(), "공식이 아직 없는 날은 잠정값 그대로"
    assert report["rows_replaced"] == 4 * len(CODES) and report["rows_added"] == 0
    assert report["fields"]["Close"] == 4 * len(CODES) and report["fields"]["Volume"] == 4 * len(CODES)


def test_missing_tickers_and_missing_dates_are_restored(db):
    """10-01 같은 결손일(일부 종목만)과 통째로 빠진 날짜를 공식 데이터로 채운다."""
    stored = pd.concat([_rows(DAYS[0]), _rows(DAYS[1], CODES[:20]), _rows(DAYS[3])])
    db.save_year(stored, YEAR, allow_gap=True)                       # 결손일·빠진 날을 그대로 심는다
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:4]))
    counts = kr_db.session_counts(_saved())
    assert [counts[d] for d in DAYS[:4]] == [len(CODES)] * 4


def test_second_settlement_writes_nothing_and_keeps_the_bytes(db):
    _provisional(db)
    official = _official(DAYS[:4])
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    before = kr_db.local_path(YEAR).read_bytes()
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert (report["rows_added"], report["rows_replaced"], report["written"]) == (0, 0, False)
    assert kr_db.local_path(YEAR).read_bytes() == before


def test_columns_the_merge_preserves_do_not_cause_rewrites_forever(db):
    """공식이 시총을 비워 두면 병합이 기존 값을 남긴다 — 그걸 매번 차이로 세면 영원히 다시 쓴다."""
    _provisional(db)
    official = _official(DAYS[:2])
    official.loc[official.Code == CODES[0], "Marcap"] = float("nan")
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert report["written"] is False and report["fields"]["Marcap"] == 0


def test_dates_before_the_floor_are_left_alone(db):
    _provisional(db)
    kr_official.settle_year(YEAR, settle_from=DAYS[2], official=_official(DAYS[:4]))
    saved = _saved()
    assert saved[saved.Date < DAYS[2]].Close.eq(1000.0).all()
    assert saved[saved.Date.isin(DAYS[2:4])].Close.eq(1010.0).all()


def test_official_lag_is_read_from_the_data_not_assumed(db):
    """게시 지연을 2일로 가정하지 않는다 — 공식이 제공한 마지막 날짜까지만 정정한다."""
    _provisional(db)
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:1]))
    assert report["official_through"] == DAYS[0]
    saved = _saved()
    assert saved[saved.Date > DAYS[0]].Close.eq(1000.0).all()
    assert kr_official.official_through(YEAR) == DAYS[0]


def test_tickers_only_we_have_are_not_deleted(db):
    _provisional(db, codes=CODES + ["999999"])
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:2]))
    saved = _saved()
    assert ((saved.Code == "999999") & (saved.Date == DAYS[0])).any()


# ── 메타데이터: 잠정/공식 경계와 이력 ─────────────────────────────────────────

def test_boundary_and_history_live_in_parquet_metadata_without_new_columns(db):
    _provisional(db)
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:3]))
    assert list(_saved().columns) == kr_db.SCHEMA_COLS
    assert kr_official.official_through(YEAR) == DAYS[2]
    import json
    log = json.loads(kr_db.file_metadata(YEAR)[kr_official.META_LOG])
    assert log[-1]["rows_replaced"] == 3 * len(CODES) and log[-1]["last_date"] == DAYS[2].date().isoformat()


def test_provisional_save_keeps_the_official_boundary(db):
    _provisional(db)
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:3]))
    db.save_year(_rows(DAYS[5]), YEAR)                               # 다음 날 잠정 수집
    assert kr_official.official_through(YEAR) == DAYS[2]


def test_history_is_bounded(db, monkeypatch):
    _provisional(db)
    monkeypatch.setattr(kr_official, "SETTLEMENT_LOG_LIMIT", 2)
    for close in (1001.0, 1002.0, 1003.0):
        kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:1], close=close))
    import json
    assert len(json.loads(kr_db.file_metadata(YEAR)[kr_official.META_LOG])) == 2


# ── 마감 연도·최소 연도 ────────────────────────────────────────────────────────

def test_years_before_the_minimum_are_never_touched(db, monkeypatch):
    monkeypatch.setattr(config, "KR_OFFICIAL_MIN_YEAR", YEAR + 1)
    _provisional(db)
    before = kr_db.local_path(YEAR).read_bytes()
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:4]))
    assert report.get("skipped") == "closed_year"
    assert kr_db.local_path(YEAR).read_bytes() == before


# ── 공식 원천 받기·검증 ────────────────────────────────────────────────────────

class _Response:
    def __init__(self, status, body=b""):
        self.status_code, self.body = status, body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, size):
        yield self.body


def _parquet_bytes(frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


@pytest.mark.real_official_fetch
def test_fetch_validates_and_normalizes_the_official_file():
    body = _parquet_bytes(pd.concat([_rows(d) for d in DAYS[:2]]))
    frame = kr_official.fetch_official_year(YEAR, request_get=lambda url, **k: _Response(200, body))
    assert list(frame.columns) == kr_db.SCHEMA_COLS and len(frame) == 2 * len(CODES)


@pytest.mark.real_official_fetch
def test_missing_year_file_is_absent_not_a_failure():
    with pytest.raises(kr_official.OfficialAbsent):
        kr_official.fetch_official_year(YEAR, request_get=lambda url, **k: _Response(404))


@pytest.mark.real_official_fetch
@pytest.mark.parametrize("response, code", [
    (lambda url, **k: _Response(500), "kr_official_unavailable"),
    (lambda url, **k: _Response(200, b""), "kr_official_unavailable"),
    (lambda url, **k: _Response(200, b"not a parquet"), "kr_official_invalid"),
])
def test_fetch_failures_are_typed(response, code):
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_official.fetch_official_year(YEAR, request_get=response)
    assert caught.value.args[0] == code


@pytest.mark.real_official_fetch
def test_oversized_official_file_is_rejected_before_it_is_loaded(monkeypatch):
    monkeypatch.setattr(kr_official, "OFFICIAL_MAX_ROWS", len(CODES))
    body = _parquet_bytes(pd.concat([_rows(d) for d in DAYS[:2]]))
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_official.fetch_official_year(YEAR, request_get=lambda url, **k: _Response(200, body))
    assert caught.value.args[0] == "kr_official_invalid"


@pytest.mark.real_official_fetch
def test_network_exception_is_typed():
    def boom(url, **k):
        raise ConnectionError("synthetic")
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_official.fetch_official_year(YEAR, request_get=boom)
    assert caught.value.args[0] == "kr_official_unavailable"


@pytest.mark.parametrize("mutate", [
    lambda f: f.assign(Date=pd.Timestamp(f"{YEAR - 1}-12-30")),          # 다른 연도
    lambda f: pd.concat([f, f.iloc[:1]]),                                  # 중복 키
    lambda f: f.drop(columns=["Volume"]),                                  # 필수 열 없음
    lambda f: f.assign(Close=-1.0),                                        # 음수 가격
    lambda f: f.assign(Code="12"),                                         # 잘못된 코드
    lambda f: f.assign(MarketId="XXX"),                                    # 알 수 없는 시장
    lambda f: f.iloc[0:0],                                                 # 빈 파일
    lambda f: f.assign(High=900.0),                                        # 거래 있는데 고가 < 저가
])
def test_malformed_official_frames_are_rejected(mutate):
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_official.validate_official(mutate(_rows(DAYS[0])), YEAR)
    assert caught.value.args[0] == "kr_official_invalid"


# ── 게시 경로: kr-daily ───────────────────────────────────────────────────────

@pytest.fixture
def daily(db, monkeypatch):
    record = SimpleNamespace(uploads=[], status=None)
    monkeypatch.setattr(krx_calendar, "session_status", lambda day: ("open", ""))
    monkeypatch.setattr(krx_calendar, "trading_days", lambda a, b: [])
    monkeypatch.setattr(kr_db, "ensure_year_baselines", lambda years, **k: {y: "ok" for y in years})
    monkeypatch.setattr(kr_db, "upload_years", lambda years, **k: record.uploads.append(sorted(years)) or [])
    monkeypatch.setattr(kr_db, "save_status", lambda d, n: setattr(record, "status", d))
    monkeypatch.setattr(kr_collector, "collect_missing_today", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(kr_collector, "validate_price_basis", lambda frame: None)
    _provisional(db)
    return record


def _run_daily(monkeypatch, today_rows, official):
    monkeypatch.setattr(kr_collector, "collect_daily", lambda: today_rows)
    monkeypatch.setattr(kr_official, "fetch_official_year", official)
    kr_main.run_kr_daily(SimpleNamespace(dry_run=False, upload_drive=True),
                         today=today_rows.Date.max().date())


def test_daily_publishes_provisional_today_and_official_history_once(daily, monkeypatch):
    _run_daily(monkeypatch, _rows(DAYS[5], close=1000.0), lambda year, **k: _official(DAYS[:4]))
    saved = _saved()
    assert saved[saved.Date <= DAYS[3]].Close.eq(1010.0).all()
    assert saved[saved.Date.isin([DAYS[4], DAYS[5]])].Close.eq(1000.0).all()
    assert daily.uploads == [[YEAR]], "잠정+정정을 한 번에 게시"


def test_provisional_recollection_cannot_overwrite_an_official_day(daily, monkeypatch):
    """같은 실행에서 잠정 수집이 이미 공식인 날을 덮어도 게시 전에 공식값으로 돌아간다."""
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:4]))
    _run_daily(monkeypatch, _rows(DAYS[2], close=999.0), lambda year, **k: _official(DAYS[:4]))
    saved = _saved()
    assert saved[saved.Date == DAYS[2]].Close.eq(1010.0).all()


def test_official_outage_keeps_data_publishes_provisional_then_fails(daily, monkeypatch):
    def outage(year, **k):
        raise kr_collector.KrCollectionError("kr_official_unavailable")
    before = _saved()
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        _run_daily(monkeypatch, _rows(DAYS[5]), outage)
    assert caught.value.args[0] == "kr_official_unavailable"
    saved = _saved()
    pd.testing.assert_frame_equal(saved[saved.Date < DAYS[5]].reset_index(drop=True),
                                  before.reset_index(drop=True), check_dtype=False)
    assert daily.uploads == [[YEAR]] and daily.status is not None, "오늘 잠정분은 게시됐다"


def test_absent_official_year_is_quiet(daily, monkeypatch):
    def absent(year, **k):
        raise kr_official.OfficialAbsent()
    _run_daily(monkeypatch, _rows(DAYS[5]), absent)
    assert daily.uploads == [[YEAR]]


def test_stale_official_source_fails_after_publishing(daily, monkeypatch):
    """공식 확정이 잠정 거래일 한도(10)를 넘게 밀리면 알린다 — 상류가 멈춘 것을 조용히 넘기지 않는다."""
    monkeypatch.setattr(kr_official, "STALE_AFTER_SESSIONS", 3)
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        _run_daily(monkeypatch, _rows(DAYS[5]), lambda year, **k: _official(DAYS[:1]))
    assert caught.value.args[0] == "kr_official_stale"
    assert daily.uploads == [[YEAR]]


# ── 게시 경로: kr-backfill / kr-settle ─────────────────────────────────────────

def test_manual_backfill_cannot_leave_provisional_values_on_official_days(daily, monkeypatch):
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official(DAYS[:4]))
    yahoo = pd.concat([_rows(d, close=990.0) for d in DAYS[1:3]], ignore_index=True)
    monkeypatch.setattr(kr_collector, "collect_backfill", lambda *a, **k: yahoo)
    monkeypatch.setattr(kr_official, "fetch_official_year", lambda year, **k: _official(DAYS[:4]))
    kr_main.run_kr_backfill(SimpleNamespace(dry_run=False, upload_drive=True,
                                            start_date=DAYS[1].date().isoformat(),
                                            end_date=DAYS[2].date().isoformat()))
    saved = _saved()
    assert saved[saved.Date.isin(DAYS[1:3])].Close.eq(1010.0).all()


def test_manual_settle_lowers_the_floor_only_when_asked(daily, monkeypatch):
    monkeypatch.setattr(config, "KR_OFFICIAL_SETTLE_FROM", DAYS[3].date().isoformat())
    monkeypatch.setattr(kr_official, "fetch_official_year", lambda year, **k: _official(DAYS[:4]))
    kr_main.run_kr_settle(SimpleNamespace(dry_run=False, upload_drive=True, start_date=None))
    saved = _saved()
    assert saved[saved.Date < DAYS[3]].Close.eq(1000.0).all(), "기본 하한 이전은 그대로"
    kr_main.run_kr_settle(SimpleNamespace(dry_run=False, upload_drive=True,
                                          start_date=DAYS[0].date().isoformat()))
    assert _saved()[lambda f: f.Date <= DAYS[3]].Close.eq(1010.0).all(), "명시적 하한이면 일괄 정정"


def test_manual_settle_never_touches_closed_years(daily, monkeypatch, tmp_path):
    previous = pd.concat([_rows(d) for d in pd.bdate_range(f"{YEAR - 1}-12-21", periods=5)])
    kr_db.save_year(previous, YEAR - 1)
    before = kr_db.local_path(YEAR - 1).read_bytes()
    monkeypatch.setattr(kr_official, "fetch_official_year",
                        lambda year, **k: _official(DAYS[:4]) if year == YEAR else pytest.fail("closed year fetched"))
    kr_main.run_kr_settle(SimpleNamespace(dry_run=False, upload_drive=True,
                                          start_date=f"{YEAR - 1}-01-02"))
    assert kr_db.local_path(YEAR - 1).read_bytes() == before


def test_settle_dry_run_touches_nothing(daily, monkeypatch):
    monkeypatch.setattr(kr_official, "fetch_official_year", lambda *a, **k: pytest.fail("fetched"))
    before = kr_db.local_path(YEAR).read_bytes()
    kr_main.run_kr_settle(SimpleNamespace(dry_run=True, upload_drive=True, start_date=None))
    assert kr_db.local_path(YEAR).read_bytes() == before and daily.uploads == []


# ── 열별 정책: 측정값은 공식으로, 분류·파생 열은 크롤러 정의로 ─────────────────
# 2026 실데이터 대조: marcap Rank 는 전 시장 통합 순위(크롤러는 시장 안), Market 은
# KOSDAQ GLOBAL 을 따로 두고, ChangeCode 는 1·2·3 뿐(크롤러는 4상한·5하한·0거래없음 포함).
# 그대로 가져오면 정정된 날과 잠정인 날의 기준이 섞인다.

KOSPI_CODES, KOSDAQ_CODES = CODES[:100], CODES[100:]


def _two_markets(day, close=1000.0, kosdaq_market="KOSDAQ"):
    frame = _rows(day, close=close)
    kosdaq = frame.Code.isin(KOSDAQ_CODES)
    frame.loc[kosdaq, "Market"], frame.loc[kosdaq, "MarketId"] = kosdaq_market, "KSQ"
    frame["Marcap"] = [float(10 ** 12 - i) for i in range(len(frame))]   # 시총 서로 다르게
    return frame


def _official_two_markets(days, close=1010.0):
    frame = pd.concat([_two_markets(d, close, kosdaq_market="KOSDAQ GLOBAL") for d in days])
    frame["Rank"] = pd.array(list(range(1, len(CODES) + 1)) * len(days), dtype="Int64")  # 통합 순위
    return kr_official.validate_official(frame, YEAR)


def _settle_two_markets(db, stored_days=DAYS[:2], official_days=DAYS[:2]):
    db.save_year(pd.concat([_two_markets(d) for d in stored_days]), YEAR)
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official_two_markets(official_days))
    return _saved()


def test_rank_stays_within_market_like_the_crawler(db):
    saved = _settle_two_markets(db)
    day = saved[saved.Date == DAYS[0]]
    assert day.groupby("Market").Rank.min().eq(1).all(), "시장마다 1위부터"
    assert int(day[day.Market == "KOSDAQ"].Rank.max()) == len(KOSDAQ_CODES), "전 시장 통합 순위가 아니다"


def test_kosdaq_global_label_is_not_imported(db):
    saved = _settle_two_markets(db, stored_days=DAYS[:1], official_days=DAYS[:2])   # 둘째 날은 통째 추가
    assert set(saved.Market) == {"KOSPI", "KOSDAQ"}, "추가된 행도 MarketId 로 크롤러 이름을 만든다"


def test_limit_codes_survive_when_direction_matches(db):
    stored = _two_markets(DAYS[0])
    stored["ChangeCode"] = "4"                                               # 상한
    stored["Changes"] = 10.0
    db.save_year(stored, YEAR)
    official = _official_two_markets(DAYS[:1]).assign(Changes=12.0, ChangeCode="1")
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert _saved().ChangeCode.eq("4").all()


def test_change_code_follows_the_official_direction_when_it_flips(db):
    stored = _two_markets(DAYS[0]).assign(ChangeCode="1", Changes=5.0)        # 잠정: 상승
    db.save_year(stored, YEAR)
    official = _official_two_markets(DAYS[:1]).assign(Changes=-5.0, ChangeCode="2")   # 공식: 하락
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert _saved().ChangeCode.eq("2").all()


def test_blank_classification_is_filled_but_present_values_are_kept(db):
    stored = _two_markets(DAYS[0])
    stored.loc[stored.Code == CODES[0], "Dept"] = None                         # 1~3월 yfinance 행처럼
    stored.loc[stored.Code == CODES[1], "Name"] = "옛이름"
    db.save_year(stored, YEAR)
    official = _official_two_markets(DAYS[:1]).assign(Dept="공식부", Name="새이름")
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    saved = _saved().set_index("Code")
    assert saved.loc[CODES[0], "Dept"] == "공식부" and saved.loc[CODES[2], "Dept"] == "d"
    assert saved.loc[CODES[1], "Name"] == "옛이름"


def test_policy_settlement_is_idempotent(db):
    _settle_two_markets(db)
    before = kr_db.local_path(YEAR).read_bytes()
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=_official_two_markets(DAYS[:2]))
    assert report["written"] is False and kr_db.local_path(YEAR).read_bytes() == before


def test_rows_whose_rank_cannot_be_computed_do_not_rewrite_forever(db):
    """시총이 없어 순위를 못 매기는 종목(실측 0132G0): 저장 계층은 빈 순위를 받으면 기존 값을
    남기므로, 비교도 그 규칙대로 해야 멱등이다."""
    stored = _two_markets(DAYS[0])
    stored["Rank"] = stored["Rank"].astype("Int64")
    stored.loc[stored.Code == CODES[0], "Rank"] = 0
    db.save_year(stored, YEAR)
    official = _official_two_markets(DAYS[:1])
    official.loc[official.Code == CODES[0], "Marcap"] = float("nan")
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    report = kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert report["written"] is False and report["rows_replaced"] == 0


# ── 외부 원천 이상 방어 ─────────────────────────────────────────────────────────

def test_official_file_that_rewrites_most_of_a_day_is_refused_and_nothing_changes(db):
    _provisional(db)
    before = kr_db.local_path(YEAR).read_bytes()
    official = _official(DAYS[:2])
    official.loc[official.Date == DAYS[1], "Close"] = 5000.0       # 그날 전 종목 종가가 5배
    with pytest.raises(kr_collector.KrCollectionError) as caught:
        kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    assert caught.value.args[0] == "kr_official_invalid"
    assert kr_db.local_path(YEAR).read_bytes() == before


def test_a_few_large_corrections_per_day_are_still_applied(db):
    _provisional(db)
    official = _official(DAYS[:2])
    few = official.Code.isin(CODES[:15])                            # 하루 10% — 기업행동 수준
    official.loc[few, "Close"] = 5000.0
    kr_official.settle_year(YEAR, settle_from=DAYS[0], official=official)
    saved = _saved()
    assert saved[saved.Code.isin(CODES[:15]) & saved.Date.isin(DAYS[:2])].Close.eq(5000.0).all()
