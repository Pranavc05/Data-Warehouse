"""
FREE Email Service using Gmail SMTP - No costs involved!
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, List
from datetime import datetime
import asyncio
import logging
from dataclasses import dataclass
from enum import Enum

from config import GMAIL_EMAIL, GMAIL_APP_PASSWORD, ENVIRONMENT

logger = logging.getLogger(__name__)

class EmailTemplate(Enum):
    WELCOME = "welcome"
    EMAIL_VERIFICATION = "email_verification"
    PASSWORD_RESET = "password_reset"
    SECURITY_ALERT = "security_alert"
    OPTIMIZATION_REPORT = "optimization_report"

@dataclass
class EmailMessage:
    to: str
    subject: str
    html_content: str
    text_content: str

class GmailEmailService:
    """
    FREE Email service using Gmail SMTP - No costs involved!
    Uses Gmail's free SMTP service (500 emails/day limit)
    """
    
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.gmail_email = GMAIL_EMAIL
        self.gmail_password = GMAIL_APP_PASSWORD
        
        if not self.gmail_email or not self.gmail_password:
            logger.warning("📧 Gmail SMTP not configured - using mock email service")
            self.mock_mode = True
        else:
            self.mock_mode = False
            logger.info("✅ Gmail SMTP configured successfully")
        
        # Beautiful email templates
        self.templates = {
            EmailTemplate.EMAIL_VERIFICATION: {
                "subject": "🚀 Verify Your AutoSQL Account - Free Data Engineering Platform!",
                "html": """
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <style>
                        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }
                        .container { max-width: 600px; margin: 0 auto; background: white; }
                        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 40px 20px; text-align: center; }
                        .header h1 { color: white; margin: 0; font-size: 28px; }
                        .header p { color: rgba(255,255,255,0.9); margin: 10px 0 0; font-size: 16px; }
                        .content { padding: 40px 30px; }
                        .welcome { font-size: 20px; color: #333; margin-bottom: 20px; }
                        .features { background: #f8f9fa; padding: 25px; border-radius: 8px; margin: 25px 0; }
                        .features h3 { color: #667eea; margin-top: 0; }
                        .features ul { margin: 15px 0; padding-left: 20px; }
                        .features li { margin: 8px 0; color: #555; }
                        .button { text-align: center; margin: 35px 0; }
                        .button a { 
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            color: white; 
                            padding: 16px 35px; 
                            text-decoration: none; 
                            border-radius: 8px; 
                            font-weight: 600;
                            font-size: 16px;
                            display: inline-block;
                            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
                        }
                        .footer { background: #f8f9fa; padding: 25px; text-align: center; border-top: 1px solid #e9ecef; }
                        .footer p { color: #666; margin: 5px 0; font-size: 14px; }
                        .highlight { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>🚀 Welcome to AutoSQL!</h1>
                            <p>Intelligent Data Warehouse Orchestrator</p>
                        </div>
                        
                        <div class="content">
                            <div class="welcome">Hello {username}! 👋</div>
                            
                            <p>Welcome to the most advanced <strong>FREE</strong> data engineering platform! You've just joined something incredible.</p>
                            
                            <div class="features">
                                <h3>🎯 What You Get (100% Free!):</h3>
                                <ul>
                                    <li>🤖 <strong>AI-Powered Query Optimization</strong> - Get 65%+ performance improvements</li>
                                    <li>📊 <strong>Advanced Analytics Dashboard</strong> - Real-time insights</li>
                                    <li>⚡ <strong>Performance Monitoring</strong> - Never miss a bottleneck</li>
                                    <li>💰 <strong>Cost Optimization</strong> - Save thousands on infrastructure</li>
                                    <li>🔧 <strong>Schema Evolution</strong> - Zero-downtime migrations</li>
                                </ul>
                            </div>
                            
                            <div class="highlight">
                                <strong>🎓 Built for Autodesk Data Engineer Internship</strong><br>
                                Showcasing enterprise-grade SQL + AI integration that Fortune 500 companies need!
                            </div>
                            
                            <div class="button">
                                <a href="{verification_url}">✅ Activate My Account</a>
                            </div>
                            
                            <p><small>🔒 This verification link expires in 24 hours for security.</small></p>
                            
                            <p>Ready to revolutionize your data engineering workflow? Let's get started! 🚀</p>
                        </div>
                        
                        <div class="footer">
                            <p><strong>AutoSQL - Intelligent Data Warehouse Orchestrator</strong></p>
                            <p>Built with ❤️ for the Autodesk Data Engineering Team</p>
                            <p>🎯 Demonstrating enterprise SQL + AI mastery</p>
                        </div>
                    </div>
                </body>
                </html>
                """,
                "text": """
                🚀 Welcome to AutoSQL - Intelligent Data Warehouse Orchestrator!
                
                Hello {username}!
                
                Welcome to the most advanced FREE data engineering platform!
                
                What you get (100% Free):
                • AI-Powered Query Optimization (65%+ performance gains)
                • Advanced Analytics Dashboard  
                • Real-time Performance Monitoring
                • Cost Optimization Recommendations
                • Schema Evolution with Zero Downtime
                
                Verify your account: {verification_url}
                
                Built for Autodesk Data Engineer Internship
                Showcasing enterprise-grade SQL + AI integration
                
                This verification link expires in 24 hours.
                """
            }
        }
    
    async def send_verification_email(self, email: str, username: str, verification_token: str) -> bool:
        """Send beautiful verification email via Gmail SMTP"""
        verification_url = f"http://localhost:8000/verify-email/{verification_token}"
        
        template = self.templates[EmailTemplate.EMAIL_VERIFICATION]
        
        message = EmailMessage(
            to=email,
            subject=template["subject"],
            html_content=template["html"].format(
                username=username,
                verification_url=verification_url
            ),
            text_content=template["text"].format(
                username=username,
                verification_url=verification_url
            )
        )
        
        return await self._send_email(message)
    
    async def send_security_alert(self, email: str, event_type: str, ip_address: str) -> bool:
        """Send security alert email"""
        message = EmailMessage(
            to=email,
            subject="🚨 AutoSQL Security Alert",
            html_content=f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 30px; text-align: center;">
                    <h1 style="color: white; margin: 0;">🚨 Security Alert</h1>
                </div>
                <div style="padding: 30px;">
                    <h2>Security Event Detected</h2>
                    <p><strong>Event:</strong> {event_type}</p>
                    <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
                    <p><strong>IP Address:</strong> {ip_address}</p>
                    <p>If this was you, no action needed. If not, contact support immediately.</p>
                </div>
            </div>
            """,
            text_content=f"Security Alert: {event_type} detected at {datetime.now()} from {ip_address}"
        )
        
        return await self._send_email(message)
    
    async def _send_email(self, message: EmailMessage) -> bool:
        """Send email via Gmail SMTP or mock for development"""
        if self.mock_mode or ENVIRONMENT == "development":
            # Mock mode for development/demo
            logger.info(f"""
            📧 [MOCK EMAIL SERVICE] Would send email:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            To: {message.to}
            Subject: {message.subject}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            {message.text_content[:200]}...
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            ✅ Email would be sent successfully!
            """)
            return True
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = message.subject
            msg['From'] = self.gmail_email
            msg['To'] = message.to
            
            # Add both text and HTML parts
            text_part = MIMEText(message.text_content, 'plain', 'utf-8')
            html_part = MIMEText(message.html_content, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send via Gmail SMTP
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.gmail_email, self.gmail_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to {message.to} via Gmail SMTP")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email via Gmail SMTP: {e}")
            logger.info(f"📧 [FALLBACK] Mock sending email to {message.to}: {message.subject}")
            return False

# Free email service instance
email_service = GmailEmailService()