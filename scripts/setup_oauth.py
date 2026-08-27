"""
scripts/setup_oauth.py -- Google Drive OAuth2 토큰 최초 발급 스크립트

사용법:
  py -3.12 scripts/setup_oauth.py

사전 준비:
  1. GCP Console (https://console.cloud.google.com) 접속
     프로젝트: <GCP_PROJECT_ID>
  2. APIs & Services -> Credentials -> CREATE CREDENTIALS -> OAuth client ID
  3. Application type: Desktop App (이름 임의)
  4. 다운로드한 JSON 파일을 이 프로젝트 루트에 oauth_client_secret.json 으로 저장
  5. py -3.12 scripts/setup_oauth.py 실행 -> 브라우저에서 구글 계정 로그인 허용
  6. 완료되면 data/oauth_token.json 생성됨
  7. .env 에 다음 줄 추가:
       GDRIVE_TOKEN_PATH=data/oauth_token.json

GitHub Actions 사용 시:
  data/oauth_token.json 내용을 base64 인코딩 후 GDRIVE_TOKEN 시크릿으로 등록
  PowerShell: [Convert]::ToBase64String([IO.File]::ReadAllBytes("data/oauth_token.json"))
"""

import json
import sys
from pathlib import Path

CLIENT_SECRET_FILE = "oauth_client_secret.json"
TOKEN_OUTPUT_FILE  = "data/oauth_token.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]


def main():
    if not Path(CLIENT_SECRET_FILE).exists():
        print(f"[ERROR] {CLIENT_SECRET_FILE} 파일이 없습니다.")
        print()
        print("GCP Console에서 OAuth2 클라이언트 ID를 생성하세요:")
        print("  1. https://console.cloud.google.com/apis/credentials")
        print("  2. CREATE CREDENTIALS -> OAuth client ID")
        print("  3. Application type: Desktop App")
        print("  4. 다운로드 JSON을 oauth_client_secret.json 으로 저장")
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("[ERROR] google-auth-oauthlib 패키지가 필요합니다.")
        print("  pip install google-auth-oauthlib")
        sys.exit(1)

    print("[OAuth2] 브라우저에서 구글 계정 인증을 진행합니다...")
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    Path(TOKEN_OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_OUTPUT_FILE, "w") as f:
        f.write(creds.to_json())

    print(f"[OAuth2] 토큰 저장 완료: {TOKEN_OUTPUT_FILE}")
    print()
    print(".env 파일에 다음 줄을 추가하세요:")
    print(f"  GDRIVE_TOKEN_PATH={TOKEN_OUTPUT_FILE}")
    print()
    print("GitHub Actions 시크릿 등록 방법:")
    print("  PowerShell:")
    print(f"  [Convert]::ToBase64String([IO.File]::ReadAllBytes(\"{TOKEN_OUTPUT_FILE}\"))")
    print("  -> 결과를 GDRIVE_TOKEN 시크릿으로 등록")


if __name__ == "__main__":
    main()
