"""
Messaging provider abstraction for WhatsApp dispatch.

Provides a Twilio WhatsApp client when credentials are configured; otherwise
falls back to a no-op logger for local development.
"""

from __future__ import annotations

import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class WhatsAppClient:
    """Abstract WhatsApp client interface."""

    def send_message(self, to_e164: str, body: str) -> Dict[str, Any]:
        raise NotImplementedError


class NoopWhatsAppClient(WhatsAppClient):
    """No-op client that logs messages for development/testing."""

    def send_message(self, to_e164: str, body: str) -> Dict[str, Any]:
        logger.info(f"[NOOP WA] To {to_e164}:\n{body}")
        # Return a fake provider id
        return {"sid": f"NOOP-{abs(hash((to_e164, body)))%1000000}"}


class TwilioWhatsAppClient(WhatsAppClient):
    """Twilio WhatsApp client using environment variables for credentials."""

    def __init__(self, account_sid: str, auth_token: str, from_whatsapp: str):
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_whatsapp = from_whatsapp if from_whatsapp.startswith("whatsapp:") else f"whatsapp:{from_whatsapp}"

        try:
            from twilio.rest import Client  # type: ignore
        except Exception:
            Client = None  # type: ignore
        self._twilio_client = None
        if 'Client' in locals() and Client is not None:
            try:
                self._twilio_client = Client(self.account_sid, self.auth_token)
            except Exception as e:
                logger.warning(f"Failed to initialize Twilio client: {e}")
                self._twilio_client = None

    def send_message(self, to_e164: str, body: str) -> Dict[str, Any]:
        to_addr = to_e164 if to_e164.startswith("whatsapp:") else f"whatsapp:{to_e164}"
        if not self._twilio_client:
            logger.warning("Twilio client not available; falling back to NOOP log")
            logger.info(f"[TWILIO-FAKE] To {to_addr}:\n{body}")
            return {"sid": f"FAKE-{abs(hash((to_e164, body)))%1000000}"}
        try:
            msg = self._twilio_client.messages.create(from_=self.from_whatsapp, to=to_addr, body=body)
            return {"sid": getattr(msg, 'sid', None)}
        except Exception as e:
            logger.error(f"Twilio send failed: {e}")
            raise


def get_whatsapp_client() -> WhatsAppClient:
    """Factory returning a WhatsApp client based on env configuration."""
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_WHATSAPP_FROM")
    if sid and token and from_number:
        return TwilioWhatsAppClient(sid, token, from_number)
    return NoopWhatsAppClient()
