import smtplib
import os
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()


def send_email(subject: str, body: str):
    gmail_user = os.environ.get("GMAIL_ADDRESS")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    recipient = os.environ.get("ALERT_EMAIL_TO")

    if not all([gmail_user, gmail_password, recipient]):
        raise RuntimeError("Missing email environment variables")

    msg = EmailMessage()
    msg["From"] = gmail_user
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.send_message(msg)
