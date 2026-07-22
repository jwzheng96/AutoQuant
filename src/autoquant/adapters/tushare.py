from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx

from autoquant.config import TushareCredentials
from autoquant.data.models import SourceEvidence
from autoquant.errors import (
    VendorAuthenticationError,
    VendorPermissionError,
    VendorRateLimitError,
    VendorResponseError,
)

_SOURCE = "tushare"
_PERMISSION_CODE = 2002
_DEFAULT_RETRY_DELAYS = (0.25, 1.0, 2.0)

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class TushareApiResult:
    rows: tuple[dict[str, object], ...]
    evidence: SourceEvidence


class TushareHttpClient:
    def __init__(
        self,
        *,
        credentials: TushareCredentials,
        api_url: str,
        timeout: float = 15.0,
        retry_delays: tuple[float, ...] = _DEFAULT_RETRY_DELAYS,
        sleep: Sleep = asyncio.sleep,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not isinstance(credentials, TushareCredentials):
            raise TypeError("credentials must be TushareCredentials")
        parsed = urlsplit(api_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("Tushare API URL must use HTTPS without embedded credentials")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if any(delay < 0 for delay in retry_delays):
            raise ValueError("retry delays cannot be negative")
        self._credentials = credentials
        self._api_url = api_url.rstrip("/")
        self._retry_delays = tuple(retry_delays)
        self._sleep = sleep
        self._client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)

    def __repr__(self) -> str:
        return f"TushareHttpClient(api_url={self._api_url!r})"

    async def post(
        self,
        api_name: str,
        *,
        params: Mapping[str, object],
        fields: tuple[str, ...],
    ) -> TushareApiResult:
        if not isinstance(api_name, str) or not api_name.strip():
            raise ValueError("api_name cannot be empty")
        if any(not isinstance(field, str) or not field.strip() for field in fields):
            raise ValueError("fields must contain nonempty strings")
        if len(set(fields)) != len(fields):
            raise ValueError("fields must be unique")
        requested_at = datetime.now(UTC)
        token = self._credentials.token.get_secret_value()
        payload: dict[str, object] = {
            "api_name": api_name,
            "token": token,
            "params": dict(params),
            "fields": ",".join(fields),
        }
        response = await self._post_with_retry(api_name, payload)
        parsed = self._parse_json(response, api_name=api_name)
        rows = self._parse_rows(parsed, api_name=api_name)
        evidence_body = self._evidence_body(api_name, parsed, token=token)
        response_hash = hashlib.sha256(evidence_body).hexdigest()
        return TushareApiResult(
            rows=rows,
            evidence=SourceEvidence(
                source=_SOURCE,
                method=api_name,
                requested_at=requested_at,
                response_body=evidence_body,
                response_hash=response_hash,
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _post_with_retry(
        self, api_name: str, payload: Mapping[str, object]
    ) -> httpx.Response:
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                response = await self._client.post(self._api_url, json=payload)
            except httpx.TransportError:
                if attempt == attempts - 1:
                    raise VendorResponseError(
                        f"Tushare {api_name} transport failed after retries"
                    ) from None
                await self._sleep(self._retry_delays[attempt])
                continue

            if response.status_code == 429:
                if attempt == attempts - 1:
                    raise VendorRateLimitError(
                        f"Tushare {api_name} rate limit remained exhausted"
                    )
                await self._sleep(self._retry_delays[attempt])
                continue
            if 500 <= response.status_code <= 599:
                if attempt == attempts - 1:
                    raise VendorResponseError(
                        f"Tushare {api_name} server failed after retries"
                    )
                await self._sleep(self._retry_delays[attempt])
                continue
            if response.status_code == 401:
                raise VendorAuthenticationError(
                    f"Tushare {api_name} authentication failed"
                )
            if response.status_code == 403:
                raise VendorPermissionError(f"Tushare {api_name} permission denied")
            if response.status_code < 200 or response.status_code >= 300:
                raise VendorResponseError(
                    f"Tushare {api_name} returned HTTP status {response.status_code}"
                )
            return response
        raise AssertionError("retry loop exhausted without a result")

    @staticmethod
    def _parse_json(response: httpx.Response, *, api_name: str) -> dict[str, object]:
        try:
            payload = response.json()
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise VendorResponseError(
                f"Tushare {api_name} returned invalid JSON"
            ) from None
        if not isinstance(payload, dict) or any(
            not isinstance(key, str) for key in payload
        ):
            raise VendorResponseError(f"Tushare {api_name} returned malformed JSON")
        return payload

    @staticmethod
    def _parse_rows(
        payload: Mapping[str, object], *, api_name: str
    ) -> tuple[dict[str, object], ...]:
        code = payload.get("code")
        if not isinstance(code, int) or isinstance(code, bool):
            raise VendorResponseError(
                f"Tushare {api_name} returned malformed business status"
            )
        if code == _PERMISSION_CODE:
            raise VendorPermissionError(f"Tushare {api_name} permission denied")
        if code != 0:
            raise VendorResponseError(f"Tushare {api_name} returned business code {code}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise VendorResponseError(f"Tushare {api_name} returned malformed data")
        field_values = data.get("fields")
        item_values = data.get("items")
        if (
            not isinstance(field_values, list)
            or any(not isinstance(field, str) or not field for field in field_values)
            or len(set(field_values)) != len(field_values)
            or not isinstance(item_values, list)
        ):
            raise VendorResponseError(f"Tushare {api_name} returned malformed data")

        rows: list[dict[str, object]] = []
        for item in item_values:
            if not isinstance(item, list) or len(item) != len(field_values):
                raise VendorResponseError(f"Tushare {api_name} returned malformed row")
            rows.append(dict(zip(field_values, item, strict=True)))
        return tuple(rows)

    @classmethod
    def _evidence_body(
        cls, api_name: str, payload: Mapping[str, object], *, token: str
    ) -> bytes:
        normalized = {
            "api_name": api_name,
            "response": cls._redact(payload, token=token),
        }
        try:
            return json.dumps(
                normalized,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            raise VendorResponseError(
                f"Tushare {api_name} returned malformed JSON values"
            ) from None

    @classmethod
    def _redact(cls, value: object, *, token: str) -> object:
        if isinstance(value, str):
            return value.replace(token, "***") if token else value
        if isinstance(value, list):
            return [cls._redact(item, token=token) for item in value]
        if isinstance(value, dict):
            return {
                str(key): cls._redact(item, token=token)
                for key, item in value.items()
            }
        return value
