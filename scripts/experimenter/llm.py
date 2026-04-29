"""
LLM client — OpenRouter API (OpenAI-compatible), defaults to MiniMax M2.

Usage:
    client = LLMClient(api_key=os.environ["OPENROUTER_API_KEY"])
    response, usage = await client.complete(messages, json_mode=True)

Prompt caching: OpenRouter passes cache-control headers through to providers
that support it. MiniMax M2 supports prompt caching; the client adds the
cache_control marker on the system message so the static context (RESULTS.md,
SPEC.md) is cached across calls within the same session.

Cost tracking is returned as a Usage object so budget.py can accumulate it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pricing table  (per 1M tokens, USD)
# ---------------------------------------------------------------------------
_PRICING: dict[str, dict[str, float]] = {
    "minimax/minimax-m2": {
        "input": 0.30,
        "output": 1.20,
        "cache_read": 0.03,
    },
    "minimax/minimax-m2.7": {
        "input": 0.30,
        "output": 1.20,
        "cache_read": 0.03,
    },
    # Fallback for any other model — conservative estimate
    "default": {
        "input": 3.00,
        "output": 15.00,
        "cache_read": 0.30,
    },
}


def _price(model: str) -> dict[str, float]:
    for key in _PRICING:
        if key in model:
            return _PRICING[key]
    return _PRICING["default"]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Usage:
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0

    @property
    def cost_usd(self) -> float:
        p = _price(self.model)
        billed_input = max(0, self.input_tokens - self.cache_read_tokens)
        return (
            billed_input * p["input"] / 1_000_000
            + self.cache_read_tokens * p["cache_read"] / 1_000_000
            + self.output_tokens * p["output"] / 1_000_000
        )

    def __add__(self, other: "Usage") -> "Usage":
        return Usage(
            model=self.model,
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class LLMClient:
    """Async OpenRouter client for MiniMax M2."""

    OPENROUTER_BASE = "https://openrouter.ai/api/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "minimax/minimax-m2.7",
        max_retries: int = 4,
        base_delay_s: float = 2.0,
        timeout_s: float = 120.0,
        http_referer: str = "https://github.com/robotaxi-sim",
    ) -> None:
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "OPENROUTER_API_KEY environment variable not set. "
                "Get a key at https://openrouter.ai/keys"
            )
        self.model = model
        self.max_retries = max_retries
        self.base_delay_s = base_delay_s
        self.timeout_s = timeout_s
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": http_referer,
            "X-Title": "robotaxi-sim-experimenter",
        }

    async def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        json_mode: bool = False,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        cache_system: bool = True,
    ) -> tuple[str, Usage]:
        """
        Send a chat completion request.

        Args:
            messages: OpenAI-format message list.
            json_mode: If True, request JSON response format and parse result.
            max_tokens: Max output tokens.
            temperature: Sampling temperature (lower = more deterministic).
            cache_system: If True, add cache_control to the first system message
                          so large static context is cached across calls.

        Returns:
            (content_str, Usage)
        """
        msgs = list(messages)

        # Add cache marker to system message for prompt caching
        if cache_system:
            for i, m in enumerate(msgs):
                if m.get("role") == "system":
                    msgs[i] = {
                        **m,
                        "cache_control": {"type": "ephemeral"},
                    }
                    break

        body: dict[str, Any] = {
            "model": self.model,
            "messages": msgs,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "provider": {
                "sort": "price",
                "ignore": ["MiniMax Highspeed"],
            },
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}

        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_s) as http:
                    resp = await http.post(
                        f"{self.OPENROUTER_BASE}/chat/completions",
                        headers=self._headers,
                        json=body,
                    )
                if resp.status_code == 429:
                    wait = self.base_delay_s * (2 ** attempt)
                    logger.warning("Rate limited — waiting %.1fs (attempt %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                choice0 = data["choices"][0]
                msg = choice0.get("message") or {}
                content = (msg.get("content") or "").strip()
                finish_reason = choice0.get("finish_reason", "")

                if not content:
                    u0 = data.get("usage") or {}
                    logger.warning(
                        "Empty completion (finish_reason=%s, completion_tokens=%s, attempt %d/%d) — retrying",
                        finish_reason,
                        u0.get("completion_tokens"),
                        attempt + 1,
                        self.max_retries,
                    )
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.base_delay_s * (2**attempt))
                        continue
                    raise ValueError(
                        f"Empty model content after {self.max_retries} attempts "
                        f"(finish_reason={finish_reason!r})"
                    )

                u = data.get("usage", {})
                usage = Usage(
                    model=self.model,
                    input_tokens=u.get("prompt_tokens", 0),
                    output_tokens=u.get("completion_tokens", 0),
                    cache_read_tokens=u.get("prompt_tokens_details", {}).get("cached_tokens", 0),
                )
                logger.debug(
                    "LLM call: %d in / %d cached / %d out → $%.4f",
                    usage.input_tokens,
                    usage.cache_read_tokens,
                    usage.output_tokens,
                    usage.cost_usd,
                )
                return content, usage

            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                wait = self.base_delay_s * (2 ** attempt)
                logger.warning(
                    "LLM request failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1, self.max_retries, exc, wait,
                )
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(wait)

        raise RuntimeError("LLM request failed after all retries")

    async def complete_json(
        self,
        messages: list[dict[str, Any]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.3,
        cache_system: bool = True,
    ) -> tuple[dict, Usage]:
        """
        Like complete() but parses and returns the JSON response as a dict.
        Retries if the response is not valid JSON (up to 2 extra attempts).
        """
        last_exc: Exception | None = None
        for parse_attempt in range(3):
            content, usage = await self.complete(
                messages,
                json_mode=True,
                max_tokens=max_tokens,
                temperature=temperature,
                cache_system=cache_system,
            )
            try:
                # Some models wrap JSON in markdown code fences — strip them
                text = content.strip()
                if text.startswith("```"):
                    text = text.split("```", 2)[1]
                    if text.startswith("json"):
                        text = text[4:]
                    text = text.rstrip("`").strip()
                return json.loads(text), usage
            except json.JSONDecodeError as exc:
                last_exc = exc
                logger.warning(
                    "JSON parse failed on attempt %d: %s\nRaw: %.200s",
                    parse_attempt + 1, exc, content,
                )
                # Ask the model to fix it
                messages = list(messages) + [
                    {"role": "assistant", "content": content},
                    {
                        "role": "user",
                        "content": "Your response was not valid JSON. Please output ONLY the JSON object, no markdown fences or extra text.",
                    },
                ]
        raise ValueError(f"LLM did not return valid JSON after 3 attempts: {last_exc}")
