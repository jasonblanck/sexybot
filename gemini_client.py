"""
gemini_client.py
Lightweight, zero-dependency HTTP client for the Google Gemini API.
"""

from __future__ import annotations

import logging
import os
import httpx
from typing import Optional

log = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DEFAULT_MODEL = "gemini-2.5-flash"


def generate_content(
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.2,
    max_output_tokens: Optional[int] = None,
) -> Optional[str]:
    """
    Generate content using Google's Gemini API via a direct high-speed HTTP POST request.
    This bypasses any need for external Google SDK packages and runs extremely fast.
    """
    api_key = os.getenv("GEMINI_API_KEY", GEMINI_API_KEY)
    if not api_key:
        log.warning("Gemini API call skipped: GEMINI_API_KEY not found in environment")
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    
    # Build payload structure matching Google's specifications
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ],
        "generationConfig": {
            "temperature": temperature,
        }
    }
    
    if max_output_tokens is not None:
        payload["generationConfig"]["maxOutputTokens"] = max_output_tokens

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=payload)
            if response.status_code != 200:
                log.warning(
                    "Gemini API request failed with status %d: %s",
                    response.status_code,
                    response.text,
                )
                return None
            
            data = response.json()
            candidates = data.get("candidates", [])
            if not candidates:
                log.warning("Gemini API returned no candidates: %s", data)
                return None
            
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                log.warning("Gemini API candidate has no parts: %s", candidates[0])
                return None
            
            text = parts[0].get("text")
            return text
            
    except httpx.RequestError as exc:
        log.error("Gemini request network error: %s", exc)
        return None
    except Exception as exc:
        log.error("Gemini generate_content unexpected error: %s", exc)
        return None
