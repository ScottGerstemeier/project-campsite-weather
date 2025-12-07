# email_construction/sender.py

import smtplib
from email.mime.text import MIMEText

class EmailSender:
    def __init__(self, smtp_server, smtp_port, username, password, use_tls=True):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls

    def send(self, to_email, subject, body):
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = to_email

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)
            server.sendmail(self.username, [to_email], msg.as_string())
