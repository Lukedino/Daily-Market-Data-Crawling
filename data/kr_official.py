"""
data/kr_official.py — KR 잠정 일봉을 공식 일봉으로 정정한다 (2026-10-10).

배경
  kr-daily 의 1순위 원천(FDR 캐시 `fdr_krx_data_cache/listing/krx/{날짜}.csv`)은 원천 일자의
  마감 후 최종 제공본을 받지만, 그 최종본이 KRX 공식 일봉과 다른 날이 많다 — 2026년
  실측(공통 키 530,200행)에서 종가 11만 행·거래량 11만 행이 공식값과 달랐고, 거래량은
  다른 경우 100% 우리 쪽이 적었다(9~10월 종가 35~55%·거래량 52~54% 불일치).
  `FinanceData/marcap` 연도 파일과 yfinance 원시가는 서로 독립인데 종가·거래량이
  정확히 일치했다(대형주 표본 38/42, 10-01 폴백 234/234) — 이것을 공식값으로 쓴다.
  marcap 은 원천 일자 뒤 약 하루~이틀 늦게 게시되므로 당일 수집 원천으로는 못 쓴다.

정책
  - 당일 데이터는 기존 원천으로 **잠정** 수집한다(시각·하류 일정 그대로).
  - 게시 직전마다 marcap 이 **실제로 제공한 날짜**를 읽어, 그 날짜의 측정값(OHLCV·거래대금·
    전일대비·등락률·시총·상장주식수)을 공식값으로 바꾸고 빠진 종목·날짜를 복구한다. 분류·파생
    열(Market·ChangeCode·Rank)은 marcap 과 정의가 달라 크롤러 정의로 다시 만든다(MEASURED 주석).
    지연을 고정 일수로 가정하지 않는다.
  - 하한 `config.KR_OFFICIAL_SETTLE_FROM` 이전 날짜는 건드리지 않는다 — 그 이전 이력의
    일괄 정정은 별도 승인을 받아 `--mode kr-settle --start-date` 로 실행한다.
  - 같은 입력이면 두 번째 실행에서 쓰기가 0건이다(멱등). 공식이 비운 측정값은 기존 값을 둔다.
  - 잠정 수집이 공식 날짜를 건드려도 같은 실행의 정정이 게시 전에 되돌린다.
  - 공식 확정 경계와 최근 정정 이력은 parquet 스키마 메타데이터에 남긴다(열 무변경 —
    서비스 계정은 Drive 에 새 파일을 못 만든다). 하류는 `kr_official_through` 이후를 잠정으로 본다.
"""
import io
import json
import logging
import math
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests

import config
from data import kr_db
from data.kr_collector import KrCollectionError, _normalize_schema

logger = logging.getLogger(__name__)

OFFICIAL_URL = ("https://raw.githubusercontent.com/FinanceData/marcap/"
                "master/data/marcap-{year}.parquet")
OFFICIAL_MAX_BYTES = 128 * 1024 * 1024
OFFICIAL_MAX_ROWS = 2_000_000      # 한 해 약 70만 행 — 압축을 푼 뒤 커지는 파일은 읽기 전에 거절
# 외부 원천이 저장본을 대량으로 바꾸려 하면 정정하지 않는다. 2026 일괄 정정 실측의 날짜별 최대
# 비율은 3.4%(1월, 조정가→원가) — 그 몇 배인 20% 를 넘는 날이 하나라도 있으면 원천 이상으로 본다.
CLOSE_SHIFT_PCT = 5.0
MAX_SHIFTED_SHARE_PCT = 20.0
STALE_AFTER_SESSIONS = 10          # 공식 확정이 우리 최신일보다 거래일 10개 넘게 뒤처지면 알린다
SETTLEMENT_LOG_LIMIT = 30          # parquet 메타데이터에 남기는 최근 정정 기록 수

META_THROUGH = b"kr_official_through"
META_SOURCE = b"kr_official_source"
META_LOG = b"kr_settlement_log"
SOURCE_NAME = "FinanceData/marcap"

_REQUIRED = {"Code", "Date", "Open", "High", "Low", "Close", "Volume", "Amount",
             "Marcap", "Stocks", "MarketId"}
_NON_NEGATIVE = ("Open", "High", "Low", "Close", "Volume", "Amount", "Marcap", "Stocks")
_COMPARE = [c for c in kr_db.SCHEMA_COLS if c not in ("Code", "Date")]

# 열별 정정 정책 — 2026 실데이터 대조(2026-10-10)로 정했다. marcap 은 측정값은 KRX 공식과
# 같지만 분류·파생 열의 **정의가 크롤러와 다르다**. 그 열을 그대로 가져오면 정정된 날과
# 잠정인 날의 기준이 섞이므로, 측정값만 공식으로 바꾸고 나머지는 크롤러 정의로 만든다.
#
# 측정값 — KRX 공식 일봉의 같은 양(등락률도 둘 다 round(전일대비/전일종가*100, 2), 99.9%).
MEASURED = ["Open", "High", "Low", "Close", "Volume", "Amount", "Changes", "ChangesRatio",
            "Marcap", "Stocks"]
# 같은 개념 — 기존 값이 있으면 두고, 비었을 때만 공식으로 채운다(1~3월 yfinance 행의 빈 칸).
FILL_ONLY = ["Name", "Dept", "MarketId"]
# 크롤러 정의로 다시 만드는 열:
#   Market     크롤러는 MarketId → KOSPI/KOSDAQ/KONEX. marcap 은 KOSDAQ GLOBAL 을 따로 둔다.
#   ChangeCode 크롤러(KRX 원천)는 1상승·2하락·3보합·4상한·5하한·0거래없음, marcap 은 1·2·3 뿐.
#              기존 코드의 방향이 공식 전일대비 부호와 맞으면 둔다(상한·하한 보존), 아니면 1/2/3.
#   Rank       크롤러는 **시장 안** 시총 순위, marcap 은 **전 시장 통합** 순위(가격이 같은 날도
#              1.5% 일치). 정정된 시총으로 같은 날·같은 시장 안에서 다시 매긴다.
_MARKET_BY_ID = {"STK": "KOSPI", "KSQ": "KOSDAQ", "KNX": "KONEX"}
_DIRECTION = {"1": 1, "4": 1, "2": -1, "5": -1, "3": 0, "0": 0}
_BASE_CODE = {1.0: "1", -1.0: "2", 0.0: "3"}
# kr_db._merge_year 가 비어 들어오면 기존 값을 남기는 열(ohlc_only=False 기준).
_MERGE_PRESERVED = ("Name", "Dept", "ChangeCode", "Market", "MarketId", "Marcap", "Stocks",
                    "Rank", "Amount")
_ZERO_PRESERVED = {"Marcap", "Stocks", "Rank"}


class OfficialAbsent(Exception):
    """그 연도의 공식 파일이 아직 없다(연초). 실패가 아니다."""


class OfficialStale(KrCollectionError):
    """공식 확정이 지나치게 늦다 — 잠정 거래일이 STALE_AFTER_SESSIONS 를 넘었다."""

    def __init__(self):
        super().__init__("kr_official_stale")


def fetch_official_year(year: int, *, request_get=None) -> pd.DataFrame:
    """marcap 연도 파일을 받아 검증·정규화한다. 없으면 OfficialAbsent, 그 밖의 실패는 typed 오류."""
    get = request_get or requests.get
    try:
        with get(OFFICIAL_URL.format(year=year), headers={"User-Agent": "Mozilla/5.0"},
                 timeout=(5, 60), stream=True, allow_redirects=False) as response:
            if response.status_code == 404:
                raise OfficialAbsent()
            if response.status_code != 200:
                raise KrCollectionError("kr_official_unavailable")
            body = bytearray()
            for chunk in response.iter_content(1 << 20):
                body.extend(chunk)
                if len(body) > OFFICIAL_MAX_BYTES:
                    raise KrCollectionError("kr_official_invalid")
    except (OfficialAbsent, KrCollectionError):
        raise
    except Exception:
        raise KrCollectionError("kr_official_unavailable") from None
    if not body:
        raise KrCollectionError("kr_official_unavailable")
    try:
        if pq.ParquetFile(io.BytesIO(bytes(body))).metadata.num_rows > OFFICIAL_MAX_ROWS:
            raise ValueError
        frame = pd.read_parquet(io.BytesIO(bytes(body)))
    except Exception:
        raise KrCollectionError("kr_official_invalid") from None
    return validate_official(frame, year)


def validate_official(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    """공식 프레임의 형식·연도·키·값 범위를 확인하고 저장 스키마로 맞춘다."""
    try:
        if frame.empty or frame.columns.has_duplicates or not _REQUIRED.issubset(frame.columns):
            raise ValueError
        work = frame.copy()
        work["Code"] = work["Code"].astype(str)
        if not work["Code"].str.fullmatch(r"[0-9A-Z]{6}").all():
            raise ValueError
        dates = pd.to_datetime(work["Date"], errors="coerce")
        if dates.isna().any() or not dates.dt.year.eq(year).all():
            raise ValueError
        work["Date"] = dates.dt.normalize()
        if work.duplicated(["Code", "Date"]).any():
            raise ValueError
        if not work["MarketId"].isin(["STK", "KSQ", "KNX"]).all():
            raise ValueError
        for column in _NON_NEGATIVE:
            values = pd.to_numeric(work[column], errors="coerce")
            present = values.dropna()
            if not present.map(math.isfinite).all() or (present < 0).any():
                raise ValueError
            work[column] = values
        if work["Close"].isna().any():
            raise ValueError
        if (work["Volume"].gt(0) & work["High"].lt(work["Low"])).any():
            raise ValueError
        return _normalize_schema(work)
    except KrCollectionError:
        raise
    except Exception:
        raise KrCollectionError("kr_official_invalid") from None


def _same(a: pd.Series, b: pd.Series) -> pd.Series:
    both_missing = a.isna() & b.isna()
    an, bn = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
    numeric = an.notna() & bn.notna()
    equal_number = numeric & (an == bn)
    equal_text = ~numeric & a.notna() & b.notna() & (a.astype(str) == b.astype(str))
    return (both_missing | equal_number | equal_text).fillna(False).astype(bool)


def plan_settlement(stored: pd.DataFrame, official: pd.DataFrame, *, settle_from):
    """공식 제공 날짜(하한 이후)의 저장본을 열별 정책대로 만든 행과 비교해, 달라지는 행만 돌려준다.

    반환: (쓸 행, 보고 dict). 하한 이전 날짜와 공식이 제공하지 않은 날짜는 대상이 아니다.
    공식에 없는 우리 쪽 종목 행은 지우지 않는다(순위 재계산엔 참여한다).
    """
    floor = pd.Timestamp(settle_from)
    report = {"official_through": None, "dates": 0, "rows_added": 0, "rows_replaced": 0,
              "fields": {}, "changed_dates": []}
    empty = official.iloc[0:0].reindex(columns=kr_db.SCHEMA_COLS)
    if official.empty:
        return empty, report
    report["official_through"] = pd.Timestamp(official["Date"].max())
    window = official[official["Date"] >= floor]
    if window.empty:
        return empty, report
    report["dates"] = int(window["Date"].nunique())

    mine = stored.reindex(columns=kr_db.SCHEMA_COLS).copy()
    if not mine.empty:
        mine["Date"] = pd.to_datetime(mine["Date"])
        mine["Code"] = mine["Code"].astype(str)
    mine = mine[mine["Date"].isin(set(window["Date"]))]

    joined = window.merge(mine, on=["Code", "Date"], how="left", suffixes=("", "_s"))
    _require_plausible(joined)
    target = pd.DataFrame({"Code": joined["Code"], "Date": joined["Date"]})
    for column in MEASURED:
        target[column] = joined[column].where(joined[column].notna(), joined[f"{column}_s"])
    for column in FILL_ONLY:
        target[column] = joined[f"{column}_s"].where(joined[f"{column}_s"].notna(), joined[column])
    target["Market"] = joined["Market_s"].where(joined["Market_s"].notna(),
                                                target["MarketId"].map(_MARKET_BY_ID))
    sign = np.sign(pd.to_numeric(target["Changes"], errors="coerce"))
    stored_code = joined["ChangeCode_s"].astype("string")
    consistent = stored_code.notna() & stored_code.map(_DIRECTION).eq(sign).fillna(False).astype(bool)
    target["ChangeCode"] = stored_code.where(consistent, sign.map(_BASE_CODE)).astype("object")

    # 공식에 없는 우리 종목 행도 그날 순위 계산에 넣는다(값은 그대로).
    keys = window[["Code", "Date"]].assign(_official=True)
    extra = mine.merge(keys, on=["Code", "Date"], how="left")
    extra = extra[extra["_official"].isna()].drop(columns="_official")
    columns = [c for c in kr_db.SCHEMA_COLS if c != "Rank"]          # 순위는 아래에서 매긴다
    parts = [target.reindex(columns=columns)] + ([extra[columns]] if not extra.empty else [])
    day_rows = pd.concat(parts, ignore_index=True)
    marcap = pd.to_numeric(day_rows["Marcap"], errors="coerce")
    day_rows["Rank"] = (marcap.groupby([day_rows["Date"], day_rows["Market"]])
                        .rank(ascending=False, method="min").astype("Int64"))
    day_rows = day_rows[kr_db.SCHEMA_COLS]

    compared = day_rows.merge(mine, on=["Code", "Date"], how="left", suffixes=("", "_s"),
                              indicator=True)
    # 저장 계층(kr_db._merge_year)은 들어오는 값이 비었으면(시총·주식수·순위는 0 도) 기존
    # 값을 남긴다. 비교도 "실제로 저장될 값" 으로 해야 멱등이다 — 시총이 없어 순위를 못
    # 매기는 종목(실측 0132G0)이 매 실행 다시 쓰이던 것을 막는다.
    for column in _MERGE_PRESERVED:
        kept = compared[column].isna()
        if column in _ZERO_PRESERVED:
            kept |= pd.to_numeric(compared[column], errors="coerce").eq(0)
        compared[column] = compared[column].where(~kept.fillna(False), compared[f"{column}_s"])
    added = compared["_merge"].eq("left_only")
    changed = pd.Series(False, index=compared.index)
    for column in _COMPARE:
        differs = ~_same(compared[column], compared[f"{column}_s"]) & ~added
        report["fields"][column] = int(differs.sum())
        changed |= differs
    write = compared.loc[added | changed, kr_db.SCHEMA_COLS].reset_index(drop=True)
    report["rows_added"] = int(added.sum())
    report["rows_replaced"] = int((changed & ~added).sum())
    report["changed_dates"] = sorted({d.date().isoformat() for d in write["Date"]})
    return write, report


def _require_plausible(joined: pd.DataFrame) -> None:
    """공식 종가가 저장 종가와 5% 넘게 다른 행이 한 날짜의 20% 를 넘으면 정정하지 않는다.

    정상적인 정정(잠정→공식, 조정가→원가)은 날짜마다 몇 %에 그친다. 그보다 훨씬 넓게 바뀌면
    외부 원천의 오염·형식 변경을 의심해 저장본을 지키고 typed 실패로 드러낸다.
    """
    stored = pd.to_numeric(joined["Close_s"], errors="coerce")
    official = pd.to_numeric(joined["Close"], errors="coerce")
    both = stored.gt(0) & official.notna()
    if not both.any():
        return
    shifted = ((official / stored - 1).abs() * 100 >= CLOSE_SHIFT_PCT) & both
    per_day = shifted.groupby(joined["Date"]).sum() * 100 / both.groupby(joined["Date"]).sum()
    worst = per_day[per_day > MAX_SHIFTED_SHARE_PCT]
    if not worst.empty:
        logger.error("kr_official_failed", extra={"public_metrics": {
            "kr_date": worst.index[0].date().isoformat(), "kr_ratio_pct": float(worst.iloc[0])}})
        raise KrCollectionError("kr_official_invalid")


def _metadata(previous: dict, report: dict) -> dict:
    """parquet 스키마 메타데이터 — 공식 확정 경계와 최근 정정 기록(열은 그대로)."""
    log = []
    raw = previous.get(META_LOG)
    if raw:
        try:
            log = json.loads(raw.decode("utf-8"))
        except Exception:
            log = []
    if report["rows_added"] or report["rows_replaced"]:
        log.append({
            "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "official_through": report["official_through"].date().isoformat(),
            "rows_added": report["rows_added"], "rows_replaced": report["rows_replaced"],
            "first_date": report["changed_dates"][0] if report["changed_dates"] else None,
            "last_date": report["changed_dates"][-1] if report["changed_dates"] else None,
            "fields": {k: v for k, v in report["fields"].items() if v},
        })
    return {META_THROUGH: report["official_through"].date().isoformat().encode(),
            META_SOURCE: SOURCE_NAME.encode(),
            META_LOG: json.dumps(log[-SETTLEMENT_LOG_LIMIT:], ensure_ascii=True).encode()}


def official_through(year: int) -> Optional[pd.Timestamp]:
    """저장본 메타데이터의 공식 확정 경계. 기록이 없으면 None."""
    value = kr_db.file_metadata(year).get(META_THROUGH)
    try:
        return pd.Timestamp(value.decode()) if value else None
    except Exception:
        return None


def settle_year(year: int, *, settle_from, official: Optional[pd.DataFrame] = None,
                fetch=None) -> dict:
    """저장본 한 연도를 공식값으로 정정한다. 공식 파일이 아직 없으면 아무것도 쓰지 않는다.

    `config.KR_OFFICIAL_MIN_YEAR` 이전 연도는 건드리지 않는다. 마감 연도(올해가 아닌 해)는
    바뀐 행이 있을 때만 쓴다 — 메타데이터만 바꾸려고 파일을 다시 쓰면 바이트가 달라진다.
    """
    if year < config.KR_OFFICIAL_MIN_YEAR:
        return {"year": year, "absent": False, "written": False, "skipped": "closed_year"}
    if official is None:
        fetch = fetch or fetch_official_year         # 호출 시점에 찾는다(대역 주입이 먹도록)
        try:
            official = fetch(year)
        except OfficialAbsent:
            logger.info("kr_official_absent")
            return {"year": year, "absent": True, "written": False}
    stored = kr_db.load_year(year, strict=True)
    rows, report = plan_settlement(stored, official, settle_from=settle_from)
    report.update({"year": year, "absent": False, "written": False})
    if report["official_through"] is None:
        return report
    through = official_through(year)
    open_year = year >= datetime.now(timezone.utc).year
    if not rows.empty or (open_year and through != report["official_through"]):
        # 쓸 행이 없어도 경계가 앞으로 갔으면 메타데이터만 갱신한다(같은 데이터를 다시 쓰는 비용).
        target = rows if not rows.empty else stored.iloc[0:0]
        kr_db.save_year(target, year, file_metadata=_metadata(kr_db.file_metadata(year), report))
        report["written"] = True
    logger.info("kr_official_settled", extra={"public_metrics": {
        "kr_official_through": report["official_through"].date().isoformat(),
        "kr_dates": report["dates"], "kr_rows_added": report["rows_added"],
        "kr_rows_replaced": report["rows_replaced"], "kr_source": "official_marcap"}})
    return report


def provisional_sessions(year: int, *, settle_from) -> int:
    """하한 이후 저장 거래일 중 공식 확정 경계 뒤에 남은(=아직 잠정인) 거래일 수.

    하한 이전은 이 정책의 대상이 아니라 세지 않는다(첫 배포 때 과거 이력이 '지연'으로
    잡히지 않게).
    """
    counts = kr_db.session_counts(kr_db.load_year(year, strict=True))
    dates = counts.index[counts.index >= pd.Timestamp(settle_from)]
    through = official_through(year)
    return int(len(dates) if through is None else (dates > through).sum())
