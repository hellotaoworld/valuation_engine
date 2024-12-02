from dotenv import load_dotenv
load_dotenv()
import os

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(subject, body, to_email):
    try:
        # Create a MIMEText email object
        msg = MIMEMultipart()
        msg['From'] = os.getenv("GMAILAPP_USER")
        msg['To'] = to_email
        msg['Subject'] = subject

        # Add body text
        msg.attach(MIMEText(body, 'plain'))
        # Connect to the SMTP server
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(os.getenv("GMAILAPP_USER"), os.getenv("GMAILAPP_PS"))
            server.sendmail(os.getenv("GMAILAPP_USER"), to_email, msg.as_string())
        
        print("Email sent successfully.", flush=True)
    except Exception as e:
        print(f"Failed to send email: {e}", flush=True)