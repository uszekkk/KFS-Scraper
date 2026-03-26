"""Test wysyłki email przez Gmail SMTP."""
import smtplib
import os
from email.mime.text import MIMEText

smtp_user = os.environ.get("SMTP_USER", "")
smtp_pass = os.environ.get("SMTP_PASSWORD", "")

if not smtp_user or not smtp_pass:
    print("Brak SMTP_USER lub SMTP_PASSWORD w zmiennych srodowiskowych!")
    exit(1)

msg = MIMEText("Testowy email z GitHub Actions - dziala!")
msg["Subject"] = "Test SMTP z GitHub Actions"
msg["From"] = smtp_user
msg["To"] = smtp_user

try:
    print(f"Laczenie z smtp.gmail.com:587...")
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
        server.starttls()
        print("STARTTLS OK")
        server.login(smtp_user, smtp_pass)
        print("LOGIN OK")
        server.send_message(msg)
        print("Email wyslany pomyslnie!")
except Exception as e:
    print(f"Blad: {e}")
    exit(1)
