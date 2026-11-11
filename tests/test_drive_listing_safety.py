"""Complete Drive listings and real staged-file preservation with synthetic media only."""
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from data import drive_uploader as drive


def item(identity, name, mime="application/octet-stream"):
    return {"id": identity, "name": name, "mimeType": mime}


def parquet_bytes(label="REMOTE", *, empty=False):
    frame = pd.DataFrame({"Ticker": pd.Series([] if empty else [label], dtype="str"),
                          "Close": pd.Series([] if empty else [12.5], dtype="float64")})
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


class Files:
    def __init__(self, pages=(), payloads=None):
        self.pages = list(pages)
        self.payloads = payloads or {}
        self.list_calls = []
        self.media_calls = []
        self.create_calls = []
        self.execute_kwargs = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        response = self.pages.pop(0)
        def execute(**kwargs):
            self.execute_kwargs.append(kwargs)
            if isinstance(response, Exception):
                raise response
            return response
        return SimpleNamespace(execute=execute)

    def get_media(self, **kwargs):
        self.media_calls.append(kwargs)
        return SimpleNamespace(payload=self.payloads[kwargs["fileId"]])

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        raise AssertionError("Read-only lookup must never create a folder or file")


@pytest.fixture
def make_drive(monkeypatch):
    import googleapiclient.http

    class SyntheticDownload:
        def __init__(self, stream, request, **kwargs):
            self.stream, self.payload = stream, request.payload
            self.calls = 0

        def next_chunk(self, **kwargs):
            self.calls += 1
            if isinstance(self.payload, Exception):
                self.stream.write(b"incomplete synthetic transfer")
                raise self.payload
            midpoint = len(self.payload) // 2
            self.stream.write(self.payload[:midpoint] if self.calls == 1 else self.payload[midpoint:])
            return None, self.calls == 2

    monkeypatch.setattr(googleapiclient.http, "MediaIoBaseDownload", SyntheticDownload)

    def factory(pages=(), payloads=None):
        api = Files(pages, payloads)
        uploader = drive.DriveUploader(root_folder_id="synthetic-root")
        uploader._service = SimpleNamespace(files=lambda: api)
        return uploader, api
    return factory


def baseline_pages(*rows):
    return [{"files": [item("synthetic-folder", "financials", drive.MIME_FOLDER)]},
            {"files": list(rows)}]


def local_files(directory):
    return {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}


def test_list_consumes_every_page_before_returning(make_drive):
    uploader, api = make_drive([
        {"files": [item("one", "2025.parquet")], "nextPageToken": "page-two"},
        {"files": [], "nextPageToken": "page-three"},
        {"files": [item("two", "2026.parquet")]},
    ])
    assert [row["id"] for row in uploader._list("synthetic query")] == ["one", "two"]
    assert [call.get("pageToken") for call in api.list_calls] == [None, "page-two", "page-three"]
    assert all("nextPageToken" in call["fields"] and "incompleteSearch" in call["fields"]
               for call in api.list_calls)


@pytest.mark.parametrize("pages,code", [
    ([{"files": [], "nextPageToken": "repeat"}, {"files": [], "nextPageToken": "repeat"}],
     "drive_pagination_invalid"),
    ([{"files": [], "nextPageToken": ""}], "drive_pagination_invalid"),
    ([{"files": [], "nextPageToken": 123}], "drive_pagination_invalid"),
    ([{"files": [], "incompleteSearch": True}], "drive_listing_incomplete"),
    ([{"files": [], "incompleteSearch": "false"}], "drive_listing_incomplete"),
    ([{}], "drive_listing_incomplete"),
    ([{"files": {}}], "drive_listing_incomplete"),
    ([{"files": [item("one", "a.parquet")], "nextPageToken": "next"},
      {"files": [item("one", "b.parquet")]}], "drive_listing_duplicate"),
    ([{"files": [item("unsafe/id", "a.parquet")]}], "drive_id_invalid"),
    ([{"files": [{"id": "one"}]}], "drive_listing_invalid"),
])
def test_incomplete_or_ambiguous_listing_never_returns_partial_success(make_drive, pages, code):
    uploader, api = make_drive(pages)
    with pytest.raises(drive.DriveStateError, match="^" + code + "$"):
        uploader._list("synthetic query")
    assert api.create_calls == api.media_calls == []


def test_lookup_absence_never_creates_folder(make_drive, tmp_path):
    uploader, api = make_drive([{"files": []}])
    assert uploader.download_all_state("financials", str(tmp_path / "absent")) == "absent"
    assert not (tmp_path / "absent").exists()
    assert api.create_calls == api.media_calls == []


def test_nested_lookup_uses_each_existing_parent_without_create(make_drive):
    uploader, api = make_drive([
        {"files": [item("parent", "market", drive.MIME_FOLDER)]},
        {"files": [item("child", "financials", drive.MIME_FOLDER)]},
    ])
    assert uploader._lookup_folder("market/financials") == "child"
    assert "'parent' in parents" in api.list_calls[1]["q"]
    assert api.create_calls == []


def test_duplicate_folder_on_later_page_is_not_absence(make_drive):
    uploader, api = make_drive([
        {"files": [item("first", "financials", drive.MIME_FOLDER)], "nextPageToken": "next"},
        {"files": [item("second", "financials", drive.MIME_FOLDER)]},
    ])
    with pytest.raises(drive.DriveStateError, match="drive_folder_ambiguous"):
        uploader._lookup_folder("financials")
    assert api.create_calls == []


@pytest.mark.parametrize("unsafe", ["../escape.parquet", "sub/file.parquet", "sub\\file.parquet",
                                    "C:evil.parquet", "CON.parquet", "LPT1.parquet", "x..parquet"])
def test_selected_unsafe_name_blocks_all_downloads(make_drive, tmp_path, unsafe):
    uploader, api = make_drive(baseline_pages(item("good", "2025.parquet"), item("bad", unsafe)))
    target = tmp_path / "baseline"
    target.mkdir()
    (target / "2025.parquet").write_bytes(parquet_bytes("LOCAL"))
    before = local_files(target)
    assert uploader.download_all_state("financials", str(target)) == "failed"
    assert api.media_calls == api.create_calls == []
    assert local_files(target) == before


def test_windows_case_alias_is_rejected_before_first_download(make_drive, tmp_path):
    uploader, api = make_drive(baseline_pages(item("one", "Annual.parquet"), item("two", "annual.parquet")))
    assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    assert api.media_calls == []


def test_later_listing_page_failure_preserves_baseline_without_downloading(make_drive, tmp_path, caplog):
    (tmp_path / "2025.parquet").write_bytes(parquet_bytes("LOCAL"))
    before = local_files(tmp_path)
    uploader, api = make_drive([
        {"files": [item("folder", "financials", drive.MIME_FOLDER)]},
        {"files": [item("one", "2025.parquet")], "nextPageToken": "unavailable-page"},
        OSError("SYNTHETIC_PROVIDER_SECRET"),
    ])
    assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    assert len(api.list_calls) == 3
    assert api.media_calls == []
    assert local_files(tmp_path) == before
    assert "SYNTHETIC_PROVIDER_SECRET" not in caplog.text


def test_folder_named_like_selected_file_is_rejected(make_drive, tmp_path):
    uploader, api = make_drive(baseline_pages(item("folder", "annual.parquet", drive.MIME_FOLDER)))
    assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    assert api.media_calls == []


@pytest.mark.parametrize("second", [b"corrupt synthetic parquet", b"", OSError("SYNTHETIC_PROVIDER_SECRET")])
def test_invalid_second_staged_file_preserves_every_old_byte(make_drive, tmp_path, caplog, second):
    target = tmp_path / "baseline"
    target.mkdir()
    for name in ("2025.parquet", "2026.parquet", "local-only.parquet"):
        (target / name).write_bytes(parquet_bytes("LOCAL-" + name))
    before = local_files(target)
    uploader, api = make_drive(baseline_pages(item("one", "2025.parquet"), item("two", "2026.parquet")),
                               {"one": parquet_bytes("NEW"), "two": second})
    assert uploader.download_all_state("financials", str(target)) == "failed"
    assert len(api.media_calls) == 2
    assert local_files(target) == before
    assert not list(target.glob(".drive-stage-*"))
    assert "SYNTHETIC_PROVIDER_SECRET" not in caplog.text


@pytest.mark.parametrize("first_existed", [True, False])
def test_late_promotion_error_restores_existing_and_removes_new_files(make_drive, tmp_path, monkeypatch, first_existed):
    target = tmp_path / "baseline"
    target.mkdir()
    if first_existed:
        (target / "2025.parquet").write_bytes(parquet_bytes("OLD-FIRST"))
    (target / "2026.parquet").write_bytes(parquet_bytes("OLD-SECOND"))
    before = local_files(target)
    uploader, _ = make_drive(baseline_pages(item("one", "2025.parquet"), item("two", "2026.parquet")),
                             {"one": parquet_bytes("NEW-FIRST"), "two": parquet_bytes("NEW-SECOND")})
    replace = drive.os.replace
    def fail_second(source, destination):
        if Path(source).parent.name == "incoming" and Path(source).name == "2026.parquet":
            raise OSError("synthetic promotion failure")
        return replace(source, destination)
    monkeypatch.setattr(drive.os, "replace", fail_second)
    assert uploader.download_all_state("financials", str(target)) == "failed"
    assert local_files(target) == before
    assert not list(target.glob(".drive-stage-*"))


def test_backup_copy_failure_stops_before_any_promotion(make_drive, tmp_path, monkeypatch):
    for name in ("2025.parquet", "2026.parquet"):
        (tmp_path / name).write_bytes(parquet_bytes("LOCAL"))
    before = local_files(tmp_path)
    uploader, _ = make_drive(baseline_pages(item("one", "2025.parquet"), item("two", "2026.parquet")),
                             {"one": parquet_bytes("NEW"), "two": parquet_bytes("NEW")})
    copy = drive.shutil.copyfile
    def fail_second(source, destination):
        if Path(source).name == "2026.parquet":
            raise OSError("synthetic backup failure")
        return copy(source, destination)
    monkeypatch.setattr(drive.shutil, "copyfile", fail_second)
    assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    assert local_files(tmp_path) == before


def test_rollback_failure_retains_old_bytes_for_recovery(make_drive, tmp_path, monkeypatch, caplog):
    old = parquet_bytes("OLD")
    (tmp_path / "2025.parquet").write_bytes(old)
    (tmp_path / "2026.parquet").write_bytes(old)
    uploader, _ = make_drive(baseline_pages(item("one", "2025.parquet"), item("two", "2026.parquet")),
                             {"one": parquet_bytes("NEW"), "two": parquet_bytes("NEW")})
    replace = drive.os.replace
    def fail_promotion_and_rollback(source, destination):
        source = Path(source)
        if (source.parent.name == "incoming" and source.name == "2026.parquet") or source.parent.name == "backup":
            raise OSError("synthetic unavailable destination")
        return replace(source, destination)
    monkeypatch.setattr(drive.os, "replace", fail_promotion_and_rollback)
    assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    stages = list(tmp_path.glob(".drive-stage-*"))
    assert len(stages) == 1
    assert (stages[0] / "backup/2025.parquet").read_bytes() == old
    assert (stages[0] / "backup/2026.parquet").read_bytes() == old
    assert (tmp_path / "2026.parquet").read_bytes() == old
    assert "local_recovery_required" in caplog.text


@pytest.mark.parametrize("method", ["single", "all", "strict_all"])
def test_credential_file_missing_cannot_mean_remote_absence(make_drive, tmp_path, monkeypatch, method):
    uploader, api = make_drive()
    def missing():
        raise FileNotFoundError("SYNTHETIC_CREDENTIAL_PATH")
    monkeypatch.setattr(uploader, "_get_service", missing)
    if method == "single":
        with pytest.raises(drive.DriveStateError, match="^drive_setup_unavailable$"):
            uploader.download("financials", "2026.parquet", str(tmp_path / "2026.parquet"))
    elif method == "strict_all":
        with pytest.raises(drive.DriveStateError, match="^drive_baseline_failed$"):
            uploader.download_all("financials", str(tmp_path))
    else:
        assert uploader.download_all_state("financials", str(tmp_path)) == "failed"
    assert api.create_calls == api.media_calls == []


def test_valid_zero_row_parquet_placeholder_is_distinct_from_zero_bytes(make_drive, tmp_path):
    empty = parquet_bytes(empty=True)
    uploader, _ = make_drive(baseline_pages(item("one", "2026.parquet")), {"one": empty})
    assert uploader.download_all_state("financials", str(tmp_path)) == "ok"
    assert (tmp_path / "2026.parquet").read_bytes() == empty
    assert pd.read_parquet(tmp_path / "2026.parquet").empty


def test_success_promotes_all_valid_files_and_preserves_unselected_local(make_drive, tmp_path):
    (tmp_path / "local-only.parquet").write_bytes(parquet_bytes("KEEP"))
    keep = (tmp_path / "local-only.parquet").read_bytes()
    first, second = parquet_bytes("FIRST"), parquet_bytes("SECOND")
    uploader, api = make_drive(baseline_pages(item("one", "2025.parquet"), item("two", "2026.parquet")),
                               {"one": first, "two": second})
    assert uploader.download_all("financials", str(tmp_path)) == "ok"
    assert (tmp_path / "2025.parquet").read_bytes() == first
    assert (tmp_path / "2026.parquet").read_bytes() == second
    assert (tmp_path / "local-only.parquet").read_bytes() == keep
    assert [call["fileId"] for call in api.media_calls] == ["one", "two"]
    assert api.create_calls == []
    assert not list(tmp_path.glob(".drive-stage-*"))



class _Media:
    def __init__(self, path, **kwargs):
        self.path, self.kwargs = path, kwargs


class _UploadFiles(Files):
    """목록 응답은 매 시도 같은 상태를 돌려주고, update/create 결과는 스크립트대로."""
    def __init__(self, existing, outcomes):
        super().__init__()
        self.existing, self.outcomes, self.writes = existing, list(outcomes), []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        query = kwargs["q"]
        if "mimeType=" in query:
            files = [item("synthetic-folder", "us", drive.MIME_FOLDER)]
        else:
            files = [item(self.existing, "us_2026.parquet")] if self.existing else []
        def execute(**options):
            self.execute_kwargs.append(options)
            return {"files": files}
        return SimpleNamespace(execute=execute)

    def _write(self, kind, kwargs):
        self.writes.append((kind, kwargs.get("fileId")))
        outcome = self.outcomes.pop(0)
        def execute(**options):
            assert options == {}   # 조각 PUT 은 라이브러리 재시도에 맡기지 않는다
            if isinstance(outcome, BaseException):
                raise outcome
            if outcome == "created-but-response-lost":
                self.existing = "synthetic-file"
                raise ConnectionResetError("synthetic")
            return {"id": outcome}
        return SimpleNamespace(execute=execute)

    def update(self, **kwargs):
        return self._write("update", kwargs)

    def create(self, **kwargs):
        return self._write("create", kwargs)


@pytest.fixture
def upload_env(tmp_path, monkeypatch):
    import googleapiclient.http
    monkeypatch.setattr(googleapiclient.http, "MediaFileUpload", _Media)
    sleeps = []
    monkeypatch.setattr(drive, "_sleep", sleeps.append)
    source = tmp_path / "us_2026.parquet"
    source.write_bytes(parquet_bytes())

    def run(existing, outcomes):
        api = _UploadFiles(existing, outcomes)
        uploader = drive.DriveUploader(root_folder_id="synthetic-root")
        uploader._service = SimpleNamespace(files=lambda: api)
        return uploader, api, str(source)
    run.sleeps = sleeps
    return run


def _http_error(status, reason=None):
    import json
    from googleapiclient.errors import HttpError
    body = {"error": {"errors": [{"reason": reason}]}} if reason else {}
    return HttpError(SimpleNamespace(status=status, reason="synthetic"), json.dumps(body).encode())


def test_upload_retries_transient_failures_then_succeeds(upload_env):
    """2026-11-11: 연도 Parquet 업로드 1회 실패로 OHLC 실행 전체가 실패했다 — 재시도 0회였다."""
    uploader, api, source = upload_env("synthetic-file",
                                       [TimeoutError(), _http_error(503), "synthetic-file"])
    assert uploader.upload(source, "us") == "synthetic-file"
    assert api.writes == [("update", "synthetic-file")] * 3
    assert upload_env.sleeps == [2, 4]
    assert api.execute_kwargs and all(k == {"num_retries": 3} for k in api.execute_kwargs)


def test_upload_gives_up_after_three_retries(upload_env):
    uploader, api, source = upload_env("synthetic-file", [ConnectionResetError()] * 4)
    with pytest.raises(ConnectionResetError):
        uploader.upload(source, "us")
    assert len(api.writes) == 4 and upload_env.sleeps == [2, 4, 8]


@pytest.mark.parametrize("error", [_http_error(403), _http_error(403, "insufficientFilePermissions"),
                                   _http_error(404), ValueError("bad")])
def test_upload_does_not_retry_permanent_failures(upload_env, error):
    uploader, api, source = upload_env("synthetic-file", [error, "synthetic-file"])
    with pytest.raises(type(error)):
        uploader.upload(source, "us")
    assert len(api.writes) == 1 and upload_env.sleeps == []


def test_lost_create_response_converges_to_update_without_duplicate(upload_env):
    uploader, api, source = upload_env(None, ["created-but-response-lost", "synthetic-file"])
    assert uploader.upload(source, "us") == "synthetic-file"
    assert api.writes == [("create", None), ("update", "synthetic-file")]


@pytest.mark.parametrize("error", [_http_error(403, "userRateLimitExceeded"),
                                   _http_error(403, "rateLimitExceeded")])
def test_upload_retries_drive_rate_limit_403(upload_env, error):
    uploader, api, source = upload_env("synthetic-file", [error, "synthetic-file"])
    assert uploader.upload(source, "us") == "synthetic-file"
    assert len(api.writes) == 2 and upload_env.sleeps == [2]


def test_upload_retries_token_refresh_transport_error(upload_env):
    from google.auth.exceptions import TransportError
    uploader, api, source = upload_env("synthetic-file", [TransportError("synthetic"), "synthetic-file"])
    assert uploader.upload(source, "us") == "synthetic-file"


def test_folder_is_resolved_once_outside_the_retry_loop(upload_env, monkeypatch):
    uploader, api, source = upload_env("synthetic-file", [TimeoutError(), "synthetic-file"])
    resolved = []
    original = uploader._get_or_create_folder
    monkeypatch.setattr(uploader, "_get_or_create_folder",
                        lambda path: resolved.append(path) or original(path))
    assert uploader.upload(source, "us") == "synthetic-file"
    assert resolved == ["us"]


def test_folder_creation_failure_is_not_retried(upload_env, monkeypatch):
    uploader, api, source = upload_env("synthetic-file", ["synthetic-file"])
    calls = []

    def failing(path):
        calls.append(path)
        raise TimeoutError()
    monkeypatch.setattr(uploader, "_get_or_create_folder", failing)
    with pytest.raises(TimeoutError):
        uploader.upload(source, "us")
    assert calls == ["us"] and api.writes == []
