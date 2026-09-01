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
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
MIME_PARQUET = "application/octet-stream"
MIME_JSON    = "application/json"
MIME_FOLDER  = "application/vnd.google-apps.folder"


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
            raise FileNotFoundError(
                f"Google 자격증명 파일이 없습니다.\n"
                f"GOOGLE_SERVICE_ACCOUNT_JSON 환경변수 또는 파일: {creds_path}"
            )

        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info("[Drive] Service Account (file) 인증 사용")
        return self._service

    # ── 폴더 탐색 & 생성 ───────────────────────────────────────────────────────

    def _get_or_create_folder(self, path: str, parent_id: Optional[str] = None) -> str:
        """
        중첩 경로(예: "data/market")를 루트 폴더 하위에 자동 생성.
        존재하면 ID 반환, 없으면 생성 후 ID 반환.
        """
        if parent_id is None:
            parent_id = self._root_folder_id

        cache_key = f"{parent_id}/{path}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        service = self._get_service()
        parts = [p for p in path.split("/") if p]
        current_parent = parent_id

        for part in parts:
            # 이미 있는지 검색
            resp = service.files().list(
                q=(f"name='{part}' and mimeType='{MIME_FOLDER}' "
                   f"and '{current_parent}' in parents and trashed=false"),
                fields="files(id, name)",
                spaces="drive",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            files = resp.get("files", [])

            if files:
                current_parent = files[0]["id"]
            else:
                # 없으면 생성
                folder = service.files().create(
                    body={"name": part, "mimeType": MIME_FOLDER,
                          "parents": [current_parent]},
                    fields="id",
                    supportsAllDrives=True,
                ).execute()
                current_parent = folder["id"]
                logger.debug(f"[Drive] 폴더 생성: {part} (id={current_parent})")

        self._folder_cache[cache_key] = current_parent
        return current_parent

    def _find_file(self, folder_id: str, filename: str) -> Optional[str]:
        """폴더 내 파일 ID 검색. 없으면 None."""
        service = self._get_service()
        resp = service.files().list(
            q=(f"name='{filename}' and '{folder_id}' in parents and trashed=false"),
            fields="files(id, name)",
            spaces="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = resp.get("files", [])
        return files[0]["id"] if files else None

    # ── 업로드 ─────────────────────────────────────────────────────────────────

    def upload(self, local_path: str, remote_subfolder: str) -> str:
        """
        로컬 파일을 Drive의 remote_subfolder에 업로드.
        기존 파일 있으면 update, 없으면 create.
        대용량 파일 안전을 위해 resumable=True 사용.

        Returns: 업로드된 파일의 Drive file ID
        """
        from googleapiclient.http import MediaFileUpload

        local = Path(local_path)
        if not local.exists():
            raise FileNotFoundError(f"업로드할 파일 없음: {local_path}")

        service    = self._get_service()
        folder_id  = self._get_or_create_folder(remote_subfolder)
        filename   = local.name
        mime       = MIME_JSON if filename.endswith(".json") else MIME_PARQUET
        media      = MediaFileUpload(str(local), mimetype=mime, resumable=True)
        existing_id = self._find_file(folder_id, filename)

        if existing_id:
            file = service.files().update(
                fileId=existing_id,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            logger.info(f"[Drive] 업데이트: {remote_subfolder}/{filename}")
        else:
            file = service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            logger.info(f"[Drive] 업로드: {remote_subfolder}/{filename}")

        return file["id"]

    def upload_directory(self, local_dir: str, remote_subfolder: str,
                         extensions: tuple = (".parquet", ".json")):
        """
        로컬 디렉터리 내 파일 전체 업로드.
        extensions에 해당하는 확장자만 업로드.
        """
        local = Path(local_dir)
        if not local.exists():
            logger.warning(f"[Drive] 디렉터리 없음: {local_dir}")
            return

        files = [f for f in local.iterdir()
                 if f.is_file() and f.suffix in extensions]
        logger.info(f"[Drive] 디렉터리 업로드: {len(files)}개 파일 → {remote_subfolder}")

        for f in sorted(files):
            try:
                self.upload(str(f), remote_subfolder)
            except Exception as e:
                logger.error(f"[Drive] {f.name} 업로드 실패: {e}")

    # ── 다운로드 ───────────────────────────────────────────────────────────────

    def download(self, remote_subfolder: str, filename: str, local_path: str):
        """
        Drive에서 파일 다운로드.
        remote_subfolder: 예) "data/progress"
        filename:         예) "collection_status.json"
        local_path:       로컬 저장 경로
        """
        import io
        from googleapiclient.http import MediaIoBaseDownload

        service   = self._get_service()
        folder_id = self._get_or_create_folder(remote_subfolder)
        file_id   = self._find_file(folder_id, filename)

        if file_id is None:
            raise FileNotFoundError(
                f"Drive에 파일 없음: {remote_subfolder}/{filename}"
            )

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        with open(local_path, "wb") as f:
            f.write(buf.getvalue())

        logger.info(f"[Drive] 다운로드 완료: {remote_subfolder}/{filename} → {local_path}")

    def download_all(self, remote_subfolder: str, local_dir: str,
                     extensions: tuple = (".parquet", ".json")):
        """Drive 서브폴더의 모든 파일을 로컬로 다운로드."""
        service   = self._get_service()
        folder_id = self._get_or_create_folder(remote_subfolder)

        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)",
            spaces="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = resp.get("files", [])

        Path(local_dir).mkdir(parents=True, exist_ok=True)
        for f in files:
            if any(f["name"].endswith(ext) for ext in extensions):
                try:
                    self.download(
                        remote_subfolder=remote_subfolder,
                        filename=f["name"],
                        local_path=str(Path(local_dir) / f["name"]),
                    )
                except Exception as e:
                    logger.error(f"[Drive] {f['name']} 다운로드 실패: {e}")

    def download_all_state(self, remote_subfolder: str, local_dir: str,
                           extensions: tuple = (".parquet",)) -> str:
        """download_all 과 같되 결과를 세 상태로 구분한다 (2026-09-01, financials 베이스라인용).

          "ok"      하나 이상 내려받았다
          "absent"  원격 폴더에 대상 파일이 없다 (최초 실행 — 새로 쓰는 것이 정상)
          "failed"  목록 조회·다운로드 중 실패 (**이대로 저장→업로드하면 덮어쓴다**)

        download_all() 은 파일별 실패를 로그만 남기고 삼켜 두 경우를 구분할 수 없다 —
        ohlc_db.download_year_state 가 같은 이유로 존재한다 (us 2024 사고 구멍 A).
        """
        try:
            service   = self._get_service()
            folder_id = self._get_or_create_folder(remote_subfolder)
            resp = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name)",
                spaces="drive",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            files = [f for f in resp.get("files", [])
                     if any(f["name"].endswith(ext) for ext in extensions)]
        except Exception as e:
            logger.error(f"[Drive] {remote_subfolder} 목록 조회 실패: {e}")
            return "failed"

        if not files:
            return "absent"

        Path(local_dir).mkdir(parents=True, exist_ok=True)
        for f in files:
            try:
                self.download(
                    remote_subfolder=remote_subfolder,
                    filename=f["name"],
                    local_path=str(Path(local_dir) / f["name"]),
                )
            except Exception as e:
                logger.error(f"[Drive] {f['name']} 다운로드 실패: {e}")
                return "failed"
        return "ok"

    # ── 전체 업로드 (수집 완료 후 일괄) ──────────────────────────────────────

    def sync_all_local(self):
        """
        data/local/ 하위 모든 Parquet/JSON 파일을 Drive에 동기화.
        bootstrap 완료 후 일괄 업로드 시 사용.
        """
        local_root = Path(config.LOCAL_DATA_DIR)

        for dtype, remote_path in config.DRIVE_PATHS.items():
            local_subdir = local_root / dtype
            if local_subdir.exists():
                self.upload_directory(str(local_subdir), remote_path)

        logger.info("[Drive] 전체 동기화 완료")
