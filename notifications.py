import os, io, json, ssl
import smtplib
from datetime import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv, find_dotenv

# carga .env (busca automáticamente hacia arriba)
load_dotenv(find_dotenv(), override=True)

# SMTP_SERVER = os.getenv("SMTP_SERVER")
# SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER   = os.getenv("SMTP_USER")
# SMTP_PASS   = os.getenv("SMTP_PASS")
# EMAIL_TO    = [addr.strip() for addr in os.getenv("EMAIL_TO", "").split(",") if addr.strip()]

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
DELEGATED_USER       = os.getenv("GOOGLE_DELEGATED_USER")
SCOPES               = ["https://www.googleapis.com/auth/gmail.send"]
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]  # acceso completo a Drive


def get_drive_service_sa(service_account_json_path=None, service_account_json_str=None, scopes=None):
    """
    Devuelve cliente Drive autenticado con Service Account.
    - Pasá ruta a archivo (service_account_json_path) o el JSON en string (service_account_json_str).
    - No usa OAuth del usuario. No abre navegador. Ideal para Airflow.
    """
    scopes = scopes or DRIVE_SCOPES
    if service_account_json_str:
        info = json.loads(service_account_json_str)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif service_account_json_path:
        creds = service_account.Credentials.from_service_account_file(service_account_json_path, scopes=scopes)
    else:
        raise ValueError("Debes indicar service_account_json_path o service_account_json_str")
    return build("drive", "v3", credentials=creds)


def send_email(subject: str, body: str, to: list[str] | None = None) -> None:
    host = (os.getenv("SMTP_SERVER") or "smtp.gmail.com").strip().strip('"').strip("'")
    port = int(os.getenv("SMTP_PORT") or "587")
    user = (os.getenv("SMTP_USER") or "").strip()
    pw   = (os.getenv("SMTP_PASS") or "").strip().strip('"').strip("'").replace(" ", "")
    if to is None:
        to = [a.strip() for a in (os.getenv("EMAIL_TO") or user).split(",") if a.strip()]

    if not host or host.startswith("."):
        print(f"[email] SMTP_SERVER inválido: {host!r}"); return
    if not user or not pw:
        print("[email] Falta SMTP_USER o SMTP_PASS"); return

    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = user, ", ".join(to), subject
    msg.attach(MIMEText(body, "plain"))

    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=30, context=ssl.create_default_context()) as s:
                s.login(user, pw); s.sendmail(user, to, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=30) as s:
                s.ehlo(); s.starttls(context=ssl.create_default_context()); s.ehlo()
                s.login(user, pw); s.sendmail(user, to, msg.as_string())
        print(f"📧 Email enviado a {to} via {host}:{port}")
    except Exception as e:
        print(f"ADVERTENCIA: no se pudo enviar el email ({type(e).__name__}): {e}")

# # VERSION ERROR
# envio_mail(
# 	mensaje = "msj_error",
# 	tipo = "error"
# )

# # VERSION ALERTA/WARNING/INFO
# envio_mail(
# 	mensaje = "msj_alerta",
# 	tipo = "alerta"
# )