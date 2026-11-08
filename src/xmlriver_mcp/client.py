"""Reusable async client. Credentials come only from arguments or environment."""

import asyncio
import json
import math
import os
import random
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from .errors import XmlRiverError
from .models import Engine, HistoryArgs, SerpArgs, WordstatArgs
from .parsers import extract_error, parse_serp, parse_wordstat

GOOGLE_URL = "https://xmlriver.com/search/xml"
YANDEX_URL = "https://xmlriver.com/search_yandex/xml"
WORDSTAT_URL = "https://xmlriver.com/wordstat/new/json"
BALANCE_URL = "https://xmlriver.com/api/get_balance/"
COST_URL = "https://xmlriver.com/api/get_cost/{system}/"
DEFAULT_TIMEOUT = 45.0
CREDS_HELP = "Set XMLRIVER_USER and XMLRIVER_KEY in the environment."
TEMP_CODES = {"110", "111", "115", "201", "202", "500"}
WS_TEMP_CODES = {"101", "110", "115", "500"}
NO_RESULTS_CODES = {"15"}


def classify_error_code(code: str) -> str:
    return {
        "31": "auth",
        "42": "auth",
        "45": "ip",
        "200": "balance",
        "2": "params",
        "121": "params",
        "400": "params",
    }.get(code, "temp" if code in TEMP_CODES else "other")


def resolve_xmlriver_creds() -> tuple[str, str]:
    return os.environ.get("XMLRIVER_USER", "").strip(), os.environ.get("XMLRIVER_KEY", "").strip()


class RateLimiter:
    def __init__(self, max_requests: int = 5, window_seconds: float = 1.5):
        if max_requests < 1 or window_seconds <= 0:
            raise ValueError("Invalid rate limit")
        self.maximum, self.window = max_requests, window_seconds
        self.times: deque[float] = deque()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                while self.times and self.times[0] <= now - self.window:
                    self.times.popleft()
                if len(self.times) < self.maximum:
                    self.times.append(now)
                    return
                await asyncio.sleep(max(0, self.window - (now - self.times[0])))


def retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            seconds = (date - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0, seconds) if math.isfinite(seconds) else None


class XmlRiverClient:
    def __init__(
        self,
        user: str = "",
        key: str = "",
        *,
        timeout: float = DEFAULT_TIMEOUT,
        rate_max: int = 5,
        rate_window: float = 1.5,
        max_concurrent: int = 5,
        max_attempts: int | None = None,
        transport=None,
    ):
        if timeout <= 0 or max_concurrent < 1 or (max_attempts is not None and max_attempts < 0):
            raise ValueError("Invalid client limits")
        self._user, self._key = user, key
        self._timeout, self._transport = timeout, transport
        self._limiter = RateLimiter(rate_max, rate_window)
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._http: httpx.AsyncClient | None = None
        self.max_attempts = max_attempts
        self.http_attempts = 0

    async def __aenter__(self):
        self._http = httpx.AsyncClient(timeout=self._timeout, transport=self._transport)
        return self

    async def __aexit__(self, *exc):
        if self._http:
            await self._http.aclose()
            self._http = None

    async def _request(self, url: str, params: dict, parser, *, max_retries: int = 3):
        if not self._user or not self._key:
            raise XmlRiverError(kind="auth")
        if self._http is None:
            raise RuntimeError("Use XmlRiverClient inside async with")
        if not 0 <= max_retries <= 10:
            raise ValueError("max_retries must be 0..10")
        for attempt in range(max_retries + 1):
            delay = None
            async with self._semaphore:
                await self._limiter.acquire()
                if self.max_attempts is not None and self.http_attempts >= self.max_attempts:
                    raise XmlRiverError(kind="budget")
                self.http_attempts += 1
                try:
                    response = await self._http.get(
                        url, params={**params, "user": self._user, "key": self._key}
                    )
                    status = response.status_code
                    if status == 429 or 500 <= status < 600:
                        delay = retry_after(response.headers.get("Retry-After"))
                        raise XmlRiverError(code=str(status), kind="temp")
                    if not 200 <= status < 300:
                        raise XmlRiverError(
                            code=str(status), kind="auth" if status in (401, 403) else "params"
                        )
                    return parser(response)
                except httpx.HTTPError:
                    error = XmlRiverError(kind="temp")
                except XmlRiverError as exc:
                    error = exc
            if error.kind != "temp" or attempt == max_retries:
                raise error from None
            await asyncio.sleep(
                delay if delay is not None else min(2**attempt, 15) + random.uniform(0, 0.25)
            )
        raise AssertionError("unreachable")

    async def fetch_serp(
        self,
        query: str,
        *,
        engine: Engine,
        region: int,
        device: str = "mobile",
        page_index: int = 0,
        max_retries: int = 3,
    ) -> str:
        args = SerpArgs.model_validate(
            dict(query=query, engine=engine, region=region, device=device)
        )
        if type(page_index) is not int or not 0 <= page_index < 10:
            raise ValueError("page_index must be 0..9")
        params = {
            "query": args.query,
            "device": device,
            "loc" if engine == "google" else "lr": region,
            "page": page_index + (1 if engine == "google" else 0),
        }

        def parse(response):
            text = response.text
            error = extract_error(text)
            if error and error[0] != "15":
                raise XmlRiverError(code=error[0], kind=classify_error_code(error[0]))
            parse_serp(text)
            return text

        return await self._request(
            GOOGLE_URL if engine == "google" else YANDEX_URL, params, parse, max_retries=max_retries
        )

    async def fetch_wordstat(
        self,
        query: str,
        *,
        regions: int | None = 213,
        device: str | None = None,
        pagetype: str = "words",
        period: str | None = None,
        start: str | None = None,
        end: str | None = None,
        max_retries: int = 3,
    ) -> dict:
        args = dict(query=query, regions=regions, device=device)
        validated: WordstatArgs
        if pagetype == "history":
            args.update(period=period or "month", start=start, end=end)
            validated = HistoryArgs.model_validate(args)
        elif pagetype == "words":
            validated = WordstatArgs.model_validate(args)
        else:
            raise ValueError("Unknown pagetype")

        def parse(response):
            try:
                # JSON is decoded as UTF-8. Do not replace undecodable bytes silently.
                data = json.loads(response.content.decode("utf-8-sig"))
            except (ValueError, UnicodeError):
                raise XmlRiverError(kind="format") from None
            if not isinstance(data, dict):
                raise XmlRiverError(kind="format")
            if "error" in data:
                code = str(data.get("code", ""))
                raise XmlRiverError(
                    code=code, kind="temp" if code in WS_TEMP_CODES else classify_error_code(code)
                )
            if pagetype == "words":
                parse_wordstat(data, include_associations=True)
            return data

        return await self._request(
            WORDSTAT_URL,
            {**validated.model_dump(exclude_none=True), "pagetype": pagetype},
            parse,
            max_retries=max_retries,
        )

    @staticmethod
    def _number(response) -> float:
        try:
            value = float(response.text.strip().replace(",", "."))
            if not math.isfinite(value) or value < 0:
                raise ValueError
            return value
        except ValueError:
            raise XmlRiverError(kind="format") from None

    async def get_balance(self, *, max_retries: int = 3) -> float:
        return await self._request(BALANCE_URL, {}, self._number, max_retries=max_retries)

    async def get_cost(self, engine: Engine, *, max_retries: int = 3) -> float | None:
        if engine not in ("google", "yandex"):
            raise ValueError("Unknown engine")
        try:
            return await self._request(
                COST_URL.format(system=engine), {}, self._number, max_retries=max_retries
            )
        except XmlRiverError as exc:
            if exc.kind in {"format", "temp"}:
                return None
            raise
