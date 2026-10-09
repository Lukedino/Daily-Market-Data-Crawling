"""저장소 공통 테스트 설정.

KR 게시 경로(kr-daily·kr-backfill)는 게시 직전에 공식 일봉(FinanceData/marcap)으로 정정한다
(data/kr_official.py, 2026-10-10). 테스트가 그 단계에서 실제 원천(20MB)을 받지 않도록
기본값을 "공식 파일 없음"으로 둔다 — 정정을 다루는 테스트는 직접 대역을 넣는다.
받기 함수 자체(request_get 주입)를 시험할 때만 `real_official_fetch` 표시로 이 기본값을 끈다.
"""
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "real_official_fetch: kr_official.fetch_official_year 를 대역 없이 쓴다(요청은 주입)")


@pytest.fixture(autouse=True)
def _official_source_absent_by_default(monkeypatch, request):
    if request.node.get_closest_marker("real_official_fetch"):
        return
    from data import kr_official

    def absent(year, **kwargs):
        raise kr_official.OfficialAbsent()

    monkeypatch.setattr(kr_official, "fetch_official_year", absent)
