"""
data/drive_uploader.py — Google Drive 업로드/다운로드

인증 우선순위:
  1. OAuth2 사용자 토큰 (GDRIVE_TOKEN_PATH 환경변수) — 개인 계정, 할당량 사용
  2. Service Account (GOOGLE_APPLICATION_CREDENTIALS 환경변수) — Shared Drive 전용

⚠️  Service Account는 일반 My Drive 폴더에 새 파일을 생성할 수 없습니다.
    개인 구글 계정 사용자는 OAuth2 토큰을 사용하세요.
    최초 설정: py -3.12 scripts/setup_oauth.py

대상 폴더: GDRIVE_FOLDER_ID 환경변수 (루트 폴더)

폴더 구조 (루트 폴더 하위):
  quant-korea-data/market/      ← YYYYMM.parquet
  quant-korea-data/financials/  ← YYYY.parquet
  quant-korea-data/prices/      ← YYYYMM.parquet
  quant-korea-data/progress/    ← collection_status.json
"""

import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
MIME_PARQUET = "application/octet-stream"
MIME_JSON    = "application/json"
MIME_FOLDER  = "application/vnd.google-apps.folder"


class DriveStateError(RuntimeError):
    """Fixed-code failures; provider exception text must not enter public logs."""


def _safe_name(value: str) -> str:
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,199}", value)
            or value.endswith((".", " ")) or ".." in value
            or value.split(".")[0].upper() in {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}):
        raise DriveStateError("drive_name_invalid")
    return value


def _safe_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise DriveStateError("drive_id_invalid")
    return value


class DriveUploader:
    """Google Drive 업로드/다운로드 클라이언트."""

    def __init__(self, root_folder_id: Optional[str] = None):
        """
        root_folder_id: Drive 루트 폴더 ID. None이면 config.GDRIVE_FOLDER_ID 사용.
        OHLC/재무 DB처럼 별도 폴더에 저장할 때 config.GDRIVE_OHLC_FOLDER_ID 전달.
        """
        self._service = None
        self._root_folder_id = root_folder_id or config.GDRIVE_FOLDER_ID
        self._folder_cache: dict[str, str] = {}  # path → folder_id 캐시

    def _get_service(self):
        if self._service is not None:
            return self._service

        from googleapiclient.discovery import build

        # [1] OAuth2 사용자 토큰 우선 사용 (개인 계정 Drive 접근)
        token_path = config.GDRIVE_TOKEN_PATH
        if token_path and Path(token_path).exists():
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            import json

            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
                logger.debug("[Drive] OAuth2 토큰 갱신 완료")
            self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
            logger.info("[Drive] OAuth2 사용자 인증 사용")
            return self._service

        # [2] Service Account — 환경변수 우선, 없으면 파일 fallback
        from google.oauth2 import service_account
        import json as _json

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if sa_json:
            creds = service_account.Credentials.from_service_account_info(
                _json.loads(sa_json), scopes=SCOPES
            )
            self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
            logger.info("[Drive] Service Account (env) 인증 사용")
            return self._service

        creds_path = config.GDRIVE_CREDS_PATH
        if not Path(creds_path).exists():
            raise FileNotFoundError("drive_credentials_absent")

        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info("[Drive] Service Account (file) 인증 사용")
        return self._service

    def _list(self, query: str) -> list[dict]:
        """A complete listing is required before absence or uniqueness is known."""
        service = self._get_service()
        result, seen_ids, seen_tokens = [], set(), set()
        token = None
        for _ in range(1000):
            kwargs = dict(q=query, fields="nextPageToken,incompleteSearch,files(id,name,mimeType)",
                          spaces="drive", supportsAllDrives=True, includeItemsFromAllDrives=True,
                          pageSize=1000)
            if token:
                kwargs["pageToken"] = token
            response = service.files().list(**kwargs).execute()
            if (not isinstance(response, dict) or response.get("incompleteSearch", False) is not False
                    or not isinstance(response.get("files"), list)):
                raise DriveStateError("drive_listing_incomplete")
            for item in response["files"]:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                    raise DriveStateError("drive_listing_invalid")
                identity = _safe_id(item.get("id"))
                if identity in seen_ids:
                    raise DriveStateError("drive_listing_duplicate")
                seen_ids.add(identity)
                result.append(item)
            token = response.get("nextPageToken")
            if token is None:
                return result
            if not isinstance(token, str) or not token or token in seen_tokens:
                raise DriveStateError("drive_pagination_invalid")
            seen_tokens.add(token)
        raise DriveStateError("drive_listing_limit")

    def _resolve_folder(self, path: str, parent_id=None, *, create=False):
        current = _safe_id(parent_id or self._root_folder_id)
        if not isinstance(path, str):
            raise DriveStateError("drive_path_invalid")
        parts = path.split("/") if path else []
        for part in parts:
            _safe_name(part)
            files = self._list(f"name='{part}' and mimeType='{MIME_FOLDER}' and '{current}' in parents and trashed=false")
            if len(files) > 1 or any(item["name"] != part for item in files):
                raise DriveStateError("drive_folder_ambiguous")
            if files:
                current = _safe_id(files[0]["id"])
            elif not create:
                return None
            else:
                response = self._get_service().files().create(
                    body={"name": part, "mimeType": MIME_FOLDER, "parents": [current]},
                    fields="id", supportsAllDrives=True).execute()
                current = _safe_id(response.get("id"))
        return current

    def _lookup_folder(self, path: str, parent_id=None):
        return self._resolve_folder(path, parent_id, create=False)

    def _get_or_create_folder(self, path: str, parent_id=None):
        return self._resolve_folder(path, parent_id, create=True)

    def _find_file(self, folder_id: str, filename: str):
        _safe_id(folder_id)
        _safe_name(filename)
        files = self._list(f"name='{filename}' and '{folder_id}' in parents and trashed=false")
        if len(files) > 1 or any(item["name"] != filename or item.get("mimeType") == MIME_FOLDER for item in files):
            raise DriveStateError("drive_file_ambiguous")
        return files[0]["id"] if files else None

    def upload(self, local_path: str, remote_subfolder: str) -> str:
        """Update an existing slot when present; retain the existing create policy."""
        from googleapiclient.http import MediaFileUpload
        local = Path(local_path)
        if not local.is_file() or local.is_symlink():
            raise DriveStateError("drive_upload_source_invalid")
        filename = _safe_name(local.name)
        service = self._get_service()
        folder_id = self._get_or_create_folder(remote_subfolder)
        existing_id = self._find_file(folder_id, filename)
        media = MediaFileUpload(str(local), mimetype=MIME_JSON if filename.endswith(".json") else MIME_PARQUET,
                                resumable=True)
        if existing_id:
            response = service.files().update(fileId=existing_id, media_body=media,
                                               fields="id", supportsAllDrives=True).execute()
        else:
            response = service.files().create(body={"name": filename, "parents": [folder_id]},
                                               media_body=media, fields="id", supportsAllDrives=True).execute()
        identity = _safe_id(response.get("id"))
        if existing_id and identity != existing_id:
            raise DriveStateError("drive_upload_identity_mismatch")
        logger.info("[Drive] file_upload_confirmed")
        return identity

    def upload_directory(self, local_dir: str, remote_subfolder: str,
                         extensions: tuple = (".parquet", ".json")):
        local = Path(local_dir)
        if not local.is_dir():
            raise DriveStateError("drive_upload_directory_absent")
        files = sorted(f for f in local.iterdir() if f.is_file() and f.suffix in extensions)
        if not files:
            raise DriveStateError("drive_upload_files_absent")
        for file in files:
            if not self.upload(str(file), remote_subfolder):
                raise DriveStateError("drive_upload_unconfirmed")
        return len(files)

    @staticmethod
    def _validate_download(path: Path, filename: str):
        if filename.endswith(".parquet"):
            import pyarrow.parquet as pq
            pq.read_table(path)  # valid schema-bearing zero-row placeholders are allowed
        elif filename.endswith(".json"):
            import json
            json.loads(path.read_text(encoding="utf-8"))

    def _download_id(self, file_id: str, filename: str, destination: Path):
        from googleapiclient.http import MediaIoBaseDownload
        if destination.is_symlink():
            raise DriveStateError("drive_destination_invalid")
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix="drive-", suffix=".tmp", dir=destination.parent)
        temporary = Path(temporary)
        try:
            with os.fdopen(fd, "w+b") as stream:
                request = self._get_service().files().get_media(fileId=_safe_id(file_id), supportsAllDrives=True)
                downloader = MediaIoBaseDownload(stream, request, chunksize=4 * 1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk(num_retries=2)
                stream.flush()
                os.fsync(stream.fileno())
            self._validate_download(temporary, filename)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def download(self, remote_subfolder: str, filename: str, local_path: str):
        _safe_name(filename)
        try:
            folder_id = self._lookup_folder(remote_subfolder)
            file_id = self._find_file(folder_id, filename) if folder_id else None
        except FileNotFoundError:
            # Credential/setup failures are never evidence of remote absence.
            raise DriveStateError("drive_setup_unavailable") from None
        if file_id is None:
            raise FileNotFoundError("drive_file_absent")
        self._download_id(file_id, filename, Path(local_path))
        logger.info("[Drive] file_download_validated")
        return True

    def download_all(self, remote_subfolder: str, local_dir: str,
                     extensions: tuple = (".parquet", ".json")):
        state = self.download_all_state(remote_subfolder, local_dir, extensions)
        if state == "failed":
            raise DriveStateError("drive_baseline_failed")
        return state

    def download_all_state(self, remote_subfolder: str, local_dir: str,
                           extensions: tuple = (".parquet",)) -> str:
        """Complete listing and staged validation precede local promotion.

        This is not a multi-file transaction or a cross-host lock. Financial and
        yearly callers stage again before domain validation and baseline merging.
        """
        stage = None
        retain_recovery = False
        try:
            folder_id = self._lookup_folder(remote_subfolder)
            if folder_id is None:
                return "absent"
            files = self._list(f"'{_safe_id(folder_id)}' in parents and trashed=false")
            selected, names = [], set()
            for item in files:
                if not item["name"].endswith(extensions):
                    continue
                name = _safe_name(item["name"])
                if name.casefold() in names or item.get("mimeType") == MIME_FOLDER:
                    raise DriveStateError("drive_file_ambiguous")
                names.add(name.casefold())
                selected.append(item)
            if not selected:
                return "absent"
            destination = Path(local_dir)
            if destination.is_symlink():
                raise DriveStateError("drive_destination_invalid")
            destination.mkdir(parents=True, exist_ok=True)
            stage = Path(tempfile.mkdtemp(prefix=".drive-stage-", dir=destination))
            incoming, backup = stage / "incoming", stage / "backup"
            incoming.mkdir()
            backup.mkdir()
            for item in selected:
                target = destination / item["name"]
                if target.is_symlink() or (target.exists() and not target.is_file()):
                    raise DriveStateError("drive_destination_invalid")
                self._download_id(item["id"], item["name"], incoming / item["name"])
            # Preserve every old file before the first replacement.
            old_names = set()
            for item in selected:
                target = destination / item["name"]
                if target.exists():
                    shutil.copyfile(target, backup / item["name"])
                    old_names.add(item["name"])
            promoted = []
            try:
                for item in selected:
                    name = item["name"]
                    os.replace(incoming / name, destination / name)
                    promoted.append(name)
            except Exception:
                for name in reversed(promoted):
                    try:
                        if name in old_names:
                            os.replace(backup / name, destination / name)
                        else:
                            (destination / name).unlink()
                    except OSError:
                        retain_recovery = True
                raise DriveStateError("drive_promotion_failed") from None
            return "ok"
        except Exception:
            logger.error("[Drive] baseline_failed")
            return "failed"
        finally:
            if stage is not None and not retain_recovery:
                shutil.rmtree(stage)
            elif retain_recovery:
                logger.error("[Drive] local_recovery_required")

    def sync_all_local(self):
        local_root = Path(config.LOCAL_DATA_DIR)
        count = 0
        for dtype, remote_path in config.DRIVE_PATHS.items():
            local_subdir = local_root / dtype
            if local_subdir.exists():
                count += self.upload_directory(str(local_subdir), remote_path)
        if not count:
            raise DriveStateError("drive_upload_files_absent")
        logger.info("[Drive] full_sync_confirmed")
