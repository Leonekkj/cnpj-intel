"""Gmail API service — OAuth flow, email sending, reply polling."""
import os
import base64
import logging
import asyncio
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

log = logging.getLogger("gmail_service")

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APP_URL              = os.environ.get("APP_URL", "http://localhost:8000").rstrip("/")
REDIRECT_URI         = f"{APP_URL}/api/auth/google/gmail/callback"


def get_gmail_auth_url(state: str) -> str:
    """Return the Google OAuth URL the user must visit to grant Gmail access."""
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [REDIRECT_URI],
            }
        },
        scopes=GMAIL_SCOPES,
        redirect_uri=REDIRECT_URI,
    )
    url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        state=state,
        prompt="consent",
    )
    return url


def exchange_code_for_tokens(code: str) -> dict:
    """Exchange an OAuth authorization code for access+refresh tokens.
    Returns dict with access_token, refresh_token, expiry (ISO string), email.
    """
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [REDIRECT_URI],
            }
        },
        scopes=GMAIL_SCOPES,
        redirect_uri=REDIRECT_URI,
    )
    flow.fetch_token(code=code)
    creds = flow.credentials
    # Get the Gmail address
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    email = profile.get("emailAddress", "")
    expiry = (datetime.utcnow() + timedelta(seconds=3600)).isoformat()
    return {
        "access_token": creds.token,
        "refresh_token": creds.refresh_token,
        "expiry": expiry,
        "email": email,
    }


def build_gmail_service(access_token: str, refresh_token: str, expiry_iso: str):
    """Build an authenticated Gmail API service, refreshing token if needed.
    Returns (service, new_access_token, new_expiry_iso).
    new values if refreshed, originals if not.
    """
    creds = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=GMAIL_SCOPES,
    )
    # Treat token as expired if within 5 minutes of expiry
    try:
        exp = datetime.fromisoformat(expiry_iso)
        if datetime.utcnow() >= exp - timedelta(minutes=5):
            creds.refresh(Request())
            new_expiry = (datetime.utcnow() + timedelta(seconds=3600)).isoformat()
            return build("gmail", "v1", credentials=creds), creds.token, new_expiry
    except Exception as e:
        log.warning(f"Token refresh failed: {e}")
        raise
    return build("gmail", "v1", credentials=creds), access_token, expiry_iso


def send_email_gmail(service, to: str, subject: str, body_text: str) -> str:
    """Send an email via Gmail API. Returns threadId on success, raises on failure."""
    msg = MIMEMultipart("alternative")
    msg["to"] = to
    msg["subject"] = subject
    msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_text.replace("\n", "<br>"), "html"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return result.get("threadId", "")


def check_thread_for_reply(service, thread_id: str) -> str | None:
    """Return preview text of reply if thread has >1 message, else None."""
    try:
        thread = service.users().threads().get(
            userId="me", id=thread_id, format="metadata",
            metadataHeaders=["Subject", "From"]
        ).execute()
        messages = thread.get("messages", [])
        if len(messages) <= 1:
            return None
        # Get snippet of the latest reply (not the original sent message)
        latest = messages[-1]
        return latest.get("snippet", "")[:200]
    except Exception as e:
        log.warning(f"check_thread_for_reply error thread={thread_id}: {e}")
        return None


async def poll_gmail_replies(db) -> None:
    """Background task: polls Gmail threads for replies every hour.
    db is a Database instance passed in from api.py.
    """
    while True:
        try:
            await asyncio.sleep(3600)  # wait first, then poll
            await asyncio.to_thread(_run_poll, db)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"poll_gmail_replies error: {e}")


def _run_poll(db) -> None:
    """Synchronous poll — called via asyncio.to_thread."""
    pending = db.buscar_destinatarios_para_poll()
    if not pending:
        return

    log.info(f"Polling replies for {len(pending)} recipients")

    # Group by token to share one Gmail service per user
    by_token: dict[str, list[dict]] = {}
    for p in pending:
        by_token.setdefault(p["token"], []).append(p)

    for token, items in by_token.items():
        creds = db.buscar_gmail_tokens(token)
        if not creds:
            continue
        try:
            service, new_at, new_exp = build_gmail_service(
                creds["access_token"], creds["refresh_token"], creds["expiry"]
            )
            # Save refreshed token if it changed
            if new_at != creds["access_token"]:
                db.salvar_gmail_tokens(token, new_at, creds["refresh_token"], new_exp)
        except Exception as e:
            log.warning(f"Gmail auth failed for token {token[:8]}: {e}")
            db.limpar_gmail_tokens(token)
            continue

        for item in items:
            preview = check_thread_for_reply(service, item["gmail_thread_id"])
            if preview is not None:
                db.marcar_resposta(item["id"], preview)
                log.info(f"Reply detected for destinatario id={item['id']}")
