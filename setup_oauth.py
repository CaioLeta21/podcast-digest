#!/usr/bin/env python3
"""One-time OAuth setup for Podcast Digest.

Handles the full OAuth device flow automatically:
- Requests a device code from Google
- Opens the browser for authorization
- Polls until the user completes auth
- Saves the token for pytubefix to reuse
"""

import json
import sys
import time
import urllib.request
import urllib.parse
import webbrowser
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
OAUTH_CACHE = PROJECT_ROOT / "__cache__"
TOKEN_FILE = OAUTH_CACHE / "tokens.json"

# Same credentials pytubefix uses internally
CLIENT_ID = "861556708454-d6dlm3lh05idd8npek18k6be8ba3oc68.apps.googleusercontent.com"
CLIENT_SECRET = "SboVhoG9s0rNafixCSGGKXAT"
SCOPE = "https://www.googleapis.com/auth/youtube"


def _post_json(url, data):
    """POST JSON and return parsed response."""
    encoded = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def run_device_flow():
    """Run the full OAuth 2.0 device flow."""
    # Step 1: Request device code
    print("Solicitando codigo de autorizacao...")
    device_resp = _post_json(
        "https://oauth2.googleapis.com/device/code",
        {"client_id": CLIENT_ID, "scope": SCOPE},
    )

    verification_url = device_resp["verification_url"]
    user_code = device_resp["user_code"]
    device_code = device_resp["device_code"]
    interval = device_resp.get("interval", 5)
    expires_in = device_resp.get("expires_in", 1800)

    # Step 2: Show code and open browser
    print(f"\n  Codigo: {user_code}")
    print(f"  Link:   {verification_url}")
    print()
    print("Abrindo o navegador...")
    webbrowser.open(verification_url)
    print("Digite o codigo acima no navegador e autorize.")
    print("Aguardando voce completar...\n")

    # Step 3: Poll for token
    start = time.time()
    while time.time() - start < expires_in:
        time.sleep(interval)
        try:
            token_resp = _post_json(
                "https://oauth2.googleapis.com/token",
                {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
            )
            # Success
            return token_resp
        except urllib.error.HTTPError as e:
            body = json.loads(e.read().decode("utf-8"))
            error = body.get("error", "")
            if error == "authorization_pending":
                sys.stdout.write(".")
                sys.stdout.flush()
                continue
            elif error == "slow_down":
                interval += 2
                continue
            else:
                print(f"\nErro na autorizacao: {body}")
                return None

    print("\nTimeout: voce demorou demais. Tente novamente.")
    return None


def save_token(token_resp):
    """Save token in the format pytubefix expects."""
    OAUTH_CACHE.mkdir(exist_ok=True)
    data = {
        "access_token": token_resp["access_token"],
        "refresh_token": token_resp["refresh_token"],
        "expires": int(time.time()) + token_resp.get("expires_in", 3600),
        "visitorData": None,
        "po_token": None,
    }
    TOKEN_FILE.write_text(json.dumps(data))
    return data


def test_transcript():
    """Test that transcripts actually work with the saved token."""
    try:
        from pytubefix import YouTube
    except ImportError:
        print("pytubefix nao encontrado.")
        return False

    yt = YouTube(
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        use_oauth=True,
        allow_oauth_cache=True,
        token_file=str(TOKEN_FILE),
    )

    captions = yt.captions
    for code in ["en", "a.en", "pt", "a.pt", "pt-BR"]:
        if code in captions:
            srt = captions[code].generate_srt_captions()
            if srt and len(srt) > 50:
                return True
    return False


def main():
    if TOKEN_FILE.exists():
        print(f"Token existente encontrado em: {TOKEN_FILE}")
        print("Testando se funciona...\n")
        if test_transcript():
            print("Transcricoes funcionando! Nada a fazer.")
            return
        print("Token expirado ou invalido. Renovando...\n")
        TOKEN_FILE.unlink()

    token_resp = run_device_flow()
    if not token_resp:
        sys.exit(1)

    print("\n\nAutorizacao concluida!")
    save_token(token_resp)
    print(f"Token salvo em: {TOKEN_FILE}")

    print("\nTestando transcricao...")
    if test_transcript():
        print("Tudo funcionando! As transcricoes vao funcionar automaticamente agora.")
    else:
        print("Token salvo, mas o teste de transcricao falhou.")
        print("Tente rodar o digest mesmo assim, pode funcionar.")


if __name__ == "__main__":
    main()
