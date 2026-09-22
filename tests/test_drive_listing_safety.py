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

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        response = self.pages.pop(0)
        def execute():
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
