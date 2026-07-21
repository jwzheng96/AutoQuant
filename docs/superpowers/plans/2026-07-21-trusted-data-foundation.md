# Trusted A-Share Data Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build phase 1 of the approved system: safe configuration, point-in-time minute-bar contracts, a real RQData HTTP adapter, quality gates, ClickHouse/PostgreSQL persistence, and an auditable ingestion CLI.

**Architecture:** A Python 3.11 package separates domain contracts from vendor and database adapters. RQData responses are mapped into immutable revision records, validated before persistence, stored in ClickHouse, and accompanied by PostgreSQL checkpoints, quality reports, manifests, and hash-chained audit events. Missing credentials or unavailable databases fail closed and never produce a successful production manifest.

**Tech Stack:** Python 3.11, uv 0.11.30, Pydantic 2, pydantic-settings, HTTPX, SQLAlchemy 2, asyncpg, clickhouse-connect, Typer, structlog, pytest, pytest-asyncio, Hypothesis, respx, Ruff, mypy.

## Global Constraints

- Run every install, test, formatter, linter, type-checker, migration, project command, and Git mutation on `rlocal` in `/Users/zjw/Documents/github-project/quant/open-quant`.
- Production market data comes only from RQData and QMT; generated records are permitted only in safety and contract tests and cannot support performance claims.
- Every research query requires `as_of`; it may not return a record whose `available_at` is later than `as_of`.
- Raw records are append-only. Vendor corrections create new revisions and retain old values.
- Real-time `available_at` is actual receipt time. Historical `available_at` is produced by a versioned conservative `AvailabilityPolicy`.
- Credentials, tokens, certificates, and actual brokerage account identifiers never enter Git or logs.
- A missing credential, failed quality gate, incomplete dataset, or unavailable audit store fails closed.
- Phase 1 does not implement backtest performance, Alpha models, QMT order placement, or live trading.

## File Structure

```text
pyproject.toml                         dependency, tool, and packaging configuration
uv.lock                               locked dependency graph generated on rlocal
.python-version                       Python 3.11 runtime pin
.env.example                          non-secret configuration contract
README.md                             remote setup and phase-1 commands
src/open_quant/__init__.py            package version
src/open_quant/config.py              fail-closed typed settings
src/open_quant/errors.py              stable application exception hierarchy
src/open_quant/clock.py               UTC/Shanghai time normalization
src/open_quant/data/models.py         immutable minute-bar and manifest domain types
src/open_quant/data/availability.py   versioned real-time and historical availability rules
src/open_quant/data/ports.py          source and repository protocols
src/open_quant/data/quality.py        deterministic data-quality checks
src/open_quant/data/ingestion.py      orchestration and checkpoint rules
src/open_quant/adapters/rqdata.py     real RQData HTTP authentication and get_price adapter
src/open_quant/adapters/clickhouse.py ClickHouse append/query repository
src/open_quant/adapters/postgres.py   PostgreSQL manifests, checkpoints, reports, and audit
src/open_quant/cli.py                 operator-facing connection and ingestion commands
migrations/postgres/001_phase1.sql    PostgreSQL phase-1 schema
migrations/clickhouse/001_phase1.sql  ClickHouse raw revision schema
tests/unit/                            pure domain and configuration tests
tests/contract/                        RQData HTTP request/response contract tests
tests/integration/                     real PostgreSQL/ClickHouse adapter tests
tests/live/                            opt-in RQData read-only smoke test
```

---

### Task 1: Reproducible Python Foundation and Fail-Closed Settings

**Files:**
- Create: `.python-version`
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `src/open_quant/__init__.py`
- Create: `src/open_quant/errors.py`
- Create: `src/open_quant/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: environment variables with prefix `OQ_`.
- Produces: `AppSettings`, `RuntimeEnvironment`, `MissingCapabilityError`, and `AppSettings.require_rqdata()`.

- [ ] **Step 1: Write the failing configuration tests**

```python
# tests/unit/test_config.py
from pydantic import SecretStr
import pytest

from open_quant.config import AppSettings, RuntimeEnvironment
from open_quant.errors import MissingCapabilityError


def test_defaults_are_non_live_and_fail_closed() -> None:
    settings = AppSettings(_env_file=None)
    assert settings.environment is RuntimeEnvironment.BACKTEST
    assert settings.live_trading_enabled is False
    with pytest.raises(MissingCapabilityError, match="RQData credentials"):
        settings.require_rqdata()


def test_rqdata_credentials_are_secret_values() -> None:
    settings = AppSettings(
        _env_file=None,
        rqdata_username="user",
        rqdata_password="password",
    )
    credentials = settings.require_rqdata()
    assert isinstance(credentials.password, SecretStr)
    assert "password" not in repr(credentials)


def test_live_flag_is_rejected_outside_live_environment() -> None:
    with pytest.raises(ValueError, match="live environment"):
        AppSettings(_env_file=None, live_trading_enabled=True)
```

- [ ] **Step 2: Install the pinned tool and verify the tests fail on rlocal**

Run:

```bash
ssh rlocal 'curl -LsSf https://astral.sh/uv/0.11.30/install.sh -o /tmp/open-quant-uv-install.sh && sh /tmp/open-quant-uv-install.sh'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv python install 3.11 && /Users/zjw/.local/bin/uv sync --all-groups'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/test_config.py -q'
```

Expected: collection fails because `open_quant.config` does not exist.

- [ ] **Step 3: Add the project configuration and minimal implementation**

```toml
# pyproject.toml
[build-system]
requires = ["hatchling>=1.27,<2"]
build-backend = "hatchling.build"

[project]
name = "open-quant"
version = "0.1.0"
requires-python = ">=3.11,<3.12"
dependencies = [
  "clickhouse-connect>=0.8,<1",
  "httpx>=0.28,<1",
  "pandas>=2.2,<3",
  "pydantic>=2.10,<3",
  "pydantic-settings>=2.7,<3",
  "sqlalchemy>=2.0,<3",
  "asyncpg>=0.30,<1",
  "structlog>=25,<26",
  "tenacity>=9,<10",
  "typer>=0.15,<1",
]

[dependency-groups]
dev = [
  "hypothesis>=6.120,<7",
  "mypy>=1.14,<2",
  "pytest>=8.3,<9",
  "pytest-asyncio>=0.25,<2",
  "pytest-cov>=6,<8",
  "respx>=0.22,<1",
  "ruff>=0.9,<1",
]

[project.scripts]
open-quant = "open_quant.cli:app"

[tool.pytest.ini_options]
addopts = "--strict-markers --strict-config"
testpaths = ["tests"]
markers = [
  "integration: requires configured PostgreSQL and ClickHouse",
  "live: performs a read-only call to a real vendor API",
]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "RUF"]

[tool.mypy]
python_version = "3.11"
strict = true
packages = ["open_quant"]
```

```text
# .python-version
3.11
```

```dotenv
# .env.example
OQ_ENVIRONMENT=backtest
OQ_LIVE_TRADING_ENABLED=false
OQ_RQDATA_USERNAME=
OQ_RQDATA_PASSWORD=
OQ_RQDATA_AUTH_URL=https://rqdata.ricequant.com/auth
OQ_RQDATA_API_URL=https://rqdata.ricequant.com/api
OQ_POSTGRES_DSN=
OQ_CLICKHOUSE_DSN=
```

```python
# src/open_quant/errors.py
class OpenQuantError(Exception):
    """Base class for stable application failures."""


class MissingCapabilityError(OpenQuantError):
    """A required external capability is not configured."""


class VendorAuthenticationError(OpenQuantError):
    """A vendor rejected authentication without exposing credentials."""


class VendorResponseError(OpenQuantError):
    """A vendor response violated the declared contract."""


class PersistenceUnavailableError(OpenQuantError):
    """A required durable store is unavailable."""
```

```python
# src/open_quant/__init__.py
__version__ = "0.1.0"
```

```python
# src/open_quant/config.py
from enum import StrEnum

from pydantic import BaseModel, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from open_quant.errors import MissingCapabilityError


class RuntimeEnvironment(StrEnum):
    BACKTEST = "backtest"
    PAPER = "paper"
    CANARY = "canary"
    LIVE = "live"


class RqdataCredentials(BaseModel):
    username: str
    password: SecretStr


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OQ_", env_file=".env", extra="forbid")

    environment: RuntimeEnvironment = RuntimeEnvironment.BACKTEST
    live_trading_enabled: bool = False
    rqdata_username: str | None = None
    rqdata_password: SecretStr | None = None
    rqdata_auth_url: str = "https://rqdata.ricequant.com/auth"
    rqdata_api_url: str = "https://rqdata.ricequant.com/api"
    postgres_dsn: SecretStr | None = None
    clickhouse_dsn: SecretStr | None = None

    @model_validator(mode="after")
    def reject_unsafe_live_flag(self) -> "AppSettings":
        if self.live_trading_enabled and self.environment is not RuntimeEnvironment.LIVE:
            raise ValueError("live_trading_enabled requires the live environment")
        return self

    def require_rqdata(self) -> RqdataCredentials:
        if not self.rqdata_username or self.rqdata_password is None:
            raise MissingCapabilityError("RQData credentials are not configured")
        return RqdataCredentials(username=self.rqdata_username, password=self.rqdata_password)
```

- [ ] **Step 4: Run focused and static checks**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv lock && /Users/zjw/.local/bin/uv sync --all-groups && /Users/zjw/.local/bin/uv run pytest tests/unit/test_config.py -q && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src'
```

Expected: 3 tests pass; Ruff and mypy report success.

- [ ] **Step 5: Commit the foundation**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add .python-version pyproject.toml uv.lock .env.example src tests/unit/test_config.py && git commit -m "build: add safe Python project foundation"'
```

### Task 2: Immutable Point-in-Time Domain Contracts

**Files:**
- Create: `src/open_quant/clock.py`
- Create: `src/open_quant/data/__init__.py`
- Create: `src/open_quant/data/models.py`
- Create: `src/open_quant/data/availability.py`
- Create: `src/open_quant/data/ports.py`
- Test: `tests/unit/data/test_availability.py`
- Test: `tests/unit/data/test_models.py`

**Interfaces:**
- Consumes: timezone-aware `datetime` values and raw unadjusted minute prices.
- Produces: `MinuteBarRevision`, `TradingPeriod`, `SuspensionStatus`, `MarketCoverageEvidence`, `SourceEvidence`, `MinuteBarBatch`, `CoverageBatch`, `DatasetManifest`, `AvailabilityPolicy`, `HistoricalMinutePolicy.assign()`, `LiveArrivalPolicy.assign()`, `visible_as_of()`, `MarketDataSource`, `MinuteBarRepository`, and `ManifestRepository`.

- [ ] **Step 1: Write failing temporal and immutability tests**

```python
# tests/unit/data/test_availability.py
from datetime import UTC, datetime, timedelta

from open_quant.data.availability import HistoricalMinutePolicy, visible_as_of
from open_quant.data.models import MinuteBarRevision


def make_bar(available_at: datetime) -> MinuteBarRevision:
    return MinuteBarRevision.from_values(
        source="rqdata",
        instrument="000001.XSHE",
        event_time=datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        published_at=None,
        available_at=available_at,
        ingested_at=datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        source_revision="initial",
        availability_policy="rqdata-minute-v1",
        open_price="10.00",
        high_price="10.10",
        low_price="9.99",
        close_price="10.05",
        volume=1000,
        turnover="10050.00",
    )


def test_historical_policy_never_makes_bar_visible_at_bar_end() -> None:
    policy = HistoricalMinutePolicy(version="rqdata-minute-v1", delay=timedelta(seconds=5))
    bar_end = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    assert policy.assign(bar_end=bar_end) == bar_end + timedelta(seconds=5)


def test_visible_as_of_excludes_future_available_revision() -> None:
    cutoff = datetime(2026, 7, 20, 1, 31, 4, tzinfo=UTC)
    assert visible_as_of([make_bar(cutoff + timedelta(seconds=1))], cutoff) == ()
```

```python
# tests/unit/data/test_models.py
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
import pytest

from open_quant.data.models import MinuteBarRevision


def test_revision_is_immutable_and_hash_is_deterministic() -> None:
    values = dict(
        source="rqdata", instrument="000001.XSHE",
        event_time=datetime(2026, 7, 20, 1, 31, tzinfo=UTC), published_at=None,
        available_at=datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 21, 8, 0, tzinfo=UTC), source_revision="initial",
        availability_policy="rqdata-minute-v1",
        open_price="10", high_price="10.1", low_price="9.9", close_price="10.05",
        volume=1000, turnover="10050",
    )
    first = MinuteBarRevision.from_values(**values)
    second = MinuteBarRevision.from_values(**values)
    assert first.content_hash == second.content_hash
    with pytest.raises(FrozenInstanceError):
        first.volume = 1  # type: ignore[misc]
```

- [ ] **Step 2: Run the tests and confirm missing domain modules**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/data/test_availability.py tests/unit/data/test_models.py -q'
```

Expected: collection fails because the point-in-time modules do not exist.

- [ ] **Step 3: Implement the immutable contracts and policies**

Use frozen dataclasses, `Decimal` for prices and turnover, UTC storage, explicit Asia/Shanghai conversion helpers, and SHA-256 over canonical JSON. `MinuteBarRevision.__post_init__` must reject naive datetimes, an empty `availability_policy`, non-positive prices, `high < max(open, close)`, `low > min(open, close)`, negative volume/turnover, `available_at < event_time`, and `ingested_at < available_at` only for live records. Historical backfills may have `ingested_at` after the historical `available_at`.

Define coverage types with these exact fields:

```python
@dataclass(frozen=True, slots=True)
class TradingPeriod:
    source: str
    instrument: str
    session_date: date
    minute_ends: tuple[datetime, ...]
    available_at: datetime
    response_hash: str


@dataclass(frozen=True, slots=True)
class SuspensionStatus:
    source: str
    instrument: str
    session_date: date
    suspended: bool
    available_at: datetime
    response_hash: str


@dataclass(frozen=True, slots=True)
class MarketCoverageEvidence:
    periods: tuple[TradingPeriod, ...]
    suspensions: tuple[SuspensionStatus, ...]


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    source: str
    method: str
    requested_at: datetime
    response_body: bytes
    response_hash: str


@dataclass(frozen=True, slots=True)
class MinuteBarBatch:
    records: tuple[MinuteBarRevision, ...]
    source_evidence: tuple[SourceEvidence, ...]


@dataclass(frozen=True, slots=True)
class CoverageBatch:
    coverage: MarketCoverageEvidence
    source_evidence: tuple[SourceEvidence, ...]
```

`MarketCoverageEvidence.__post_init__` rejects conflicting evidence for the same `(source, instrument, session_date)`.

```python
# src/open_quant/data/availability.py
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, Sequence

from open_quant.data.models import MinuteBarRevision


class AvailabilityPolicy(Protocol):
    version: str
    def assign(self, *, bar_end: datetime) -> datetime: ...


@dataclass(frozen=True, slots=True)
class HistoricalMinutePolicy:
    version: str
    delay: timedelta

    def assign(self, *, bar_end: datetime) -> datetime:
        return bar_end + self.delay


@dataclass(frozen=True, slots=True)
class LiveArrivalPolicy:
    version: str = "live-arrival-v1"

    def assign(self, *, bar_end: datetime, received_at: datetime) -> datetime:
        if received_at < bar_end:
            raise ValueError("received_at cannot precede bar_end")
        return received_at


def visible_as_of(
    revisions: Sequence[MinuteBarRevision], as_of: datetime
) -> tuple[MinuteBarRevision, ...]:
    return tuple(record for record in revisions if record.available_at <= as_of)
```

Define protocol methods exactly as follows in `ports.py`:

```python
class MarketDataSource(Protocol):
    async def fetch_minute_bars(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> MinuteBarBatch: ...
    async def fetch_coverage_evidence(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> CoverageBatch: ...


class MinuteBarRepository(Protocol):
    async def append(self, records: tuple[MinuteBarRevision, ...]) -> int: ...
    async def query_as_of(
        self, instruments: tuple[str, ...], start: datetime, end: datetime, as_of: datetime
    ) -> tuple[MinuteBarRevision, ...]: ...


class ManifestRepository(Protocol):
    async def save_manifest(self, manifest: DatasetManifest) -> None: ...
```

- [ ] **Step 4: Run domain tests and property checks**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/data/test_availability.py tests/unit/data/test_models.py -q && /Users/zjw/.local/bin/uv run ruff check src/open_quant/data src/open_quant/clock.py tests/unit/data && /Users/zjw/.local/bin/uv run mypy src'
```

Expected: temporal and immutability tests pass; static checks succeed.

- [ ] **Step 5: Commit the domain contracts**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add src/open_quant/clock.py src/open_quant/data tests/unit/data && git commit -m "feat: add point-in-time market data contracts"'
```

### Task 3: Real RQData HTTP Adapter

**Files:**
- Create: `src/open_quant/adapters/__init__.py`
- Create: `src/open_quant/adapters/rqdata.py`
- Test: `tests/contract/test_rqdata_http.py`
- Test: `tests/live/test_rqdata_readonly.py`

**Interfaces:**
- Consumes: `RqdataCredentials`, official `/auth` and `/api` endpoints, and `HistoricalMinutePolicy`.
- Produces: `RqdataHttpSource.authenticate()`, `fetch_minute_bars()`, and `fetch_coverage_evidence()` implementing `MarketDataSource`.

- [ ] **Step 1: Write failing HTTP contract tests**

```python
# tests/contract/test_rqdata_http.py
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from open_quant.adapters.rqdata import RqdataHttpSource
from open_quant.config import RqdataCredentials
from open_quant.data.availability import HistoricalMinutePolicy


@pytest.mark.asyncio
@respx.mock
async def test_fetch_uses_unadjusted_one_minute_contract_and_maps_csv() -> None:
    respx.post("https://rqdata.example/auth").mock(
        return_value=httpx.Response(200, json={"token": "secret-token"})
    )
    api = respx.post("https://rqdata.example/api").mock(
        return_value=httpx.Response(
            200,
            text=(
                "order_book_id,datetime,open,high,low,close,volume,total_turnover\n"
                "000001.XSHE,2026-07-20 09:31:00,10,10.1,9.9,10.05,1000,10050\n"
            ),
        )
    )
    source = RqdataHttpSource(
        credentials=RqdataCredentials(username="u", password="p"),
        auth_url="https://rqdata.example/auth",
        api_url="https://rqdata.example/api",
        availability=HistoricalMinutePolicy("rqdata-minute-v1", timedelta(seconds=5)),
    )
    batch = await source.fetch_minute_bars(
        ("000001.XSHE",),
        datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
        datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
    )
    request = api.calls.last.request
    assert request.headers["token"] == "secret-token"
    assert request.read().decode().find('"frequency":"1m"') >= 0
    assert request.read().decode().find('"adjust_type":"none"') >= 0
    assert batch.records[0].instrument == "000001.XSHE"
    assert batch.records[0].available_at > batch.records[0].event_time
    assert batch.source_evidence[0].response_hash


@pytest.mark.asyncio
@respx.mock
async def test_coverage_uses_trading_periods_and_suspension_contracts() -> None:
    respx.post("https://rqdata.example/auth").mock(
        return_value=httpx.Response(200, json={"token": "secret-token"})
    )
    api = respx.post("https://rqdata.example/api").mock(
        side_effect=[
            httpx.Response(
                200,
                text='order_book_id,date,trading_hours\n000001.XSHE,2026-07-20,"09:31-11:30,13:01-15:00"\n',
            ),
            httpx.Response(200, text="date,000001.XSHE\n2026-07-20,False\n"),
        ]
    )
    source = RqdataHttpSource(
        credentials=RqdataCredentials(username="u", password="p"),
        auth_url="https://rqdata.example/auth",
        api_url="https://rqdata.example/api",
        availability=HistoricalMinutePolicy("rqdata-minute-v1", timedelta(seconds=5)),
    )
    batch = await source.fetch_coverage_evidence(
        ("000001.XSHE",),
        datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
        datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
    )
    methods = [call.request.read().decode() for call in api.calls]
    assert any('"method":"get_trading_periods"' in body for body in methods)
    assert any('"method":"is_suspended"' in body for body in methods)
    assert batch.coverage.suspensions[0].suspended is False
    assert len(batch.source_evidence) == 2
```

- [ ] **Step 2: Run the contract test and confirm the adapter is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/contract/test_rqdata_http.py -q'
```

Expected: collection fails because `open_quant.adapters.rqdata` does not exist.

- [ ] **Step 3: Implement authentication, exact request mapping, CSV parsing, and safe errors**

`RqdataHttpSource` must:

- POST `{"user_name": username, "password": secret}` to `/auth`;
- retain the token only in memory and exclude it from `repr` and logs;
- create `SourceEvidence` only for market-data API responses; authentication responses and tokens are never persisted;
- POST `/api` once per instrument with method `get_price`, `frequency="1m"`, fields `open/high/low/close/volume/total_turnover`, `adjust_type="none"`, `skip_suspended=True`, and `market="cn"`; per-instrument calls avoid vendor ambiguity when suspended rows are skipped;
- fetch `get_trading_periods(..., frequency="1m", market="cn")` and `is_suspended(...)` for the same instruments and dates, retain response hashes, and convert them into `MarketCoverageEvidence`;
- convert UTC query bounds to Asia/Shanghai strings;
- parse CSV with explicit column checks and reject HTML, empty bodies, duplicate columns, naive timestamps, non-numeric values, and unexpected instruments;
- use exponential retry only for connection errors, 429, and 5xx; never retry 400/401/403;
- generate `source_revision` from response headers plus a response SHA-256 when no vendor revision is supplied.

The adapter must use `httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))` and expose `async close()`.

- [ ] **Step 4: Add an opt-in real read-only smoke test**

```python
# tests/live/test_rqdata_readonly.py
from datetime import UTC, datetime
import os

import pytest

from open_quant.adapters.rqdata import RqdataHttpSource
from open_quant.config import AppSettings
from open_quant.data.availability import HistoricalMinutePolicy
from datetime import timedelta


@pytest.mark.live
@pytest.mark.asyncio
async def test_real_rqdata_can_read_one_known_minute() -> None:
    if os.getenv("OQ_RUN_RQDATA_LIVE") != "1":
        pytest.skip("set OQ_RUN_RQDATA_LIVE=1 to perform a real read-only API call")
    settings = AppSettings()
    source = RqdataHttpSource(
        credentials=settings.require_rqdata(),
        auth_url=settings.rqdata_auth_url,
        api_url=settings.rqdata_api_url,
        availability=HistoricalMinutePolicy("rqdata-minute-v1", timedelta(seconds=5)),
    )
    try:
        bars = await source.fetch_minute_bars(
            ("000001.XSHE",),
            datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        )
        evidence = await source.fetch_coverage_evidence(
            ("000001.XSHE",),
            datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
            datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        )
        assert bars.records and all(bar.source == "rqdata" for bar in bars.records)
        assert evidence.coverage.periods and evidence.coverage.suspensions
    finally:
        await source.close()
```

- [ ] **Step 5: Run contract tests, leaving the real test explicitly skipped without credentials**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/contract/test_rqdata_http.py tests/live/test_rqdata_readonly.py -q -rs'
```

Expected: contract tests pass; the real read-only test reports one explicit skip.

- [ ] **Step 6: Commit the RQData adapter**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add src/open_quant/adapters tests/contract tests/live && git commit -m "feat: add real RQData HTTP adapter"'
```

### Task 4: Deterministic Minute-Bar Quality Gate

**Files:**
- Create: `src/open_quant/data/quality.py`
- Test: `tests/unit/data/test_quality.py`

**Interfaces:**
- Consumes: `tuple[MinuteBarRevision, ...]`, `MarketCoverageEvidence`, and requested instruments/time bounds.
- Produces: `QualityIssue`, `QualityReport`, `QualitySeverity`, and `MinuteBarQualityGate.evaluate()`.

- [ ] **Step 1: Write failing property and example tests**

Tests must prove rejection of duplicate `(source, instrument, event_time, source_revision)`, non-monotonic event times, bars outside the requested interval, unexpected instruments, missing requested instruments, and conflicting OHLC values for the same source revision. They must also prove that a complete ordered batch yields `QualityReport.passed is True` and a stable report hash.

```python
from datetime import date

from open_quant.data.models import MarketCoverageEvidence, SuspensionStatus, TradingPeriod


def coverage_for(bar: MinuteBarRevision, *, suspended: bool) -> MarketCoverageEvidence:
    return MarketCoverageEvidence(
        periods=(TradingPeriod(
            source=bar.source,
            instrument=bar.instrument,
            session_date=date(2026, 7, 20),
            minute_ends=(bar.event_time,),
            available_at=bar.available_at,
            response_hash="a" * 64,
        ),),
        suspensions=(SuspensionStatus(
            source=bar.source,
            instrument=bar.instrument,
            session_date=date(2026, 7, 20),
            suspended=suspended,
            available_at=bar.available_at,
            response_hash="b" * 64,
        ),),
    )


def test_duplicate_revision_fails_quality_gate(bar: MinuteBarRevision) -> None:
    report = MinuteBarQualityGate().evaluate(
        records=(bar, bar),
        requested_instruments=(bar.instrument,),
        start=bar.event_time,
        end=bar.event_time,
        coverage=coverage_for(bar, suspended=False),
    )
    assert report.passed is False
    assert {issue.code for issue in report.issues} == {"duplicate_revision"}
```

- [ ] **Step 2: Run the test and confirm `quality.py` is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/data/test_quality.py -q'
```

Expected: collection fails because the quality module does not exist.

- [ ] **Step 3: Implement deterministic issue ordering and fail-closed severity**

`QualityReport.passed` is true only when there are no `ERROR` issues. Issue ordering is `(instrument, event_time, code)` so repeated runs produce the same SHA-256. Missing instruments, schema conflicts, invalid intervals, unexpected instruments, and duplicate revisions are errors. For each non-suspended instrument/date, compare bar endpoints with the exact endpoints expanded from `TradingPeriod`; missing and off-session bars are errors. A suspended date must contain no traded bars. Missing, conflicting, or future-visible coverage evidence produces an error and sets `production_complete=False`. A complete ordered batch with full period and suspension evidence sets both `passed=True` and `production_complete=True`.

- [ ] **Step 4: Run quality and full unit tests**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit -q && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src'
```

Expected: all unit tests and static checks pass.

- [ ] **Step 5: Commit the quality gate**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add src/open_quant/data/quality.py tests/unit/data/test_quality.py && git commit -m "feat: add fail-closed minute data quality gate"'
```

### Task 5: Append-Only ClickHouse Minute Revision Repository

**Files:**
- Create: `migrations/clickhouse/001_phase1.sql`
- Create: `src/open_quant/adapters/clickhouse.py`
- Test: `tests/unit/adapters/test_clickhouse_mapping.py`
- Test: `tests/integration/test_clickhouse_repository.py`

**Interfaces:**
- Consumes: `MinuteBarRevision` records and `OQ_CLICKHOUSE_DSN`.
- Produces: `ClickHouseMinuteBarRepository.append()` and `query_as_of()` implementing `MinuteBarRepository`.

- [ ] **Step 1: Write failing SQL-mapping and as-of query tests**

The unit test asserts that append rows preserve all four timestamps, decimals, revision, policy version, and content hash. The integration test inserts two revisions for one minute and proves that a cutoff before the correction returns the original while a later cutoff returns the correction.

- [ ] **Step 2: Run the unit test and confirm the adapter is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/adapters/test_clickhouse_mapping.py -q'
```

Expected: collection fails because the ClickHouse adapter does not exist.

- [ ] **Step 3: Add the append-only schema**

```sql
CREATE TABLE IF NOT EXISTS minute_bar_revisions
(
    record_id UUID,
    source LowCardinality(String),
    instrument String,
    event_time DateTime64(6, 'UTC'),
    published_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC'),
    source_revision String,
    availability_policy String,
    open_price Decimal(20, 6),
    high_price Decimal(20, 6),
    low_price Decimal(20, 6),
    close_price Decimal(20, 6),
    volume UInt64,
    turnover Decimal(24, 4),
    content_hash FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (instrument, event_time, source, available_at, ingested_at, record_id);
```

No mutation or `ReplacingMergeTree` is permitted. `query_as_of()` filters `available_at <= %(as_of)s` and uses `argMax(..., tuple(available_at, ingested_at))` per `(source, instrument, event_time)`.

- [ ] **Step 4: Implement strict row mapping and explicit connection failure**

The adapter accepts an injected `clickhouse_connect.driver.asyncclient.AsyncClient` in tests, batches inserts without string interpolation, validates inserted row count, and raises `PersistenceUnavailableError` on connection or write failure. It does not log the DSN.

- [ ] **Step 5: Run unit tests and conditionally run real integration tests**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/adapters/test_clickhouse_mapping.py -q'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/integration/test_clickhouse_repository.py -q -rs'
```

Expected: unit tests pass. Integration tests pass when `OQ_CLICKHOUSE_DSN` points to a real test service; otherwise they report an explicit infrastructure skip and phase 1 remains unverified.

- [ ] **Step 6: Commit the ClickHouse repository**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add migrations/clickhouse src/open_quant/adapters/clickhouse.py tests/unit/adapters tests/integration/test_clickhouse_repository.py && git commit -m "feat: persist append-only minute revisions"'
```

### Task 6: PostgreSQL Checkpoints, Manifests, Quality Reports, and Audit Chain

**Files:**
- Create: `migrations/postgres/001_phase1.sql`
- Create: `src/open_quant/adapters/postgres.py`
- Test: `tests/unit/adapters/test_postgres_mapping.py`
- Test: `tests/integration/test_postgres_repository.py`

**Interfaces:**
- Consumes: `DatasetManifest`, `QualityReport`, checkpoint values, audit payloads, and `OQ_POSTGRES_DSN`.
- Produces: `PostgresControlRepository.save_source_evidence()`, `save_quality_report()`, `save_manifest()`, `advance_checkpoint()`, `append_audit_event()`, and read methods used for verification.

- [ ] **Step 1: Write failing hash-chain and checkpoint monotonicity tests**

Unit tests prove that an audit event hash includes the prior hash, canonical payload, event type, and UTC timestamp. Integration tests prove a checkpoint cannot move backward, a production manifest cannot reference a failing quality report, and duplicate manifest hashes are idempotent.
They also prove raw RQData coverage response bodies are append-only, addressable by SHA-256, and contain no authentication headers.

- [ ] **Step 2: Run the tests and confirm the adapter is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/adapters/test_postgres_mapping.py -q'
```

Expected: collection fails because the PostgreSQL adapter does not exist.

- [ ] **Step 3: Add the transactional control schema**

The SQL creates:

- `ingestion_checkpoints(source, stream, instrument, event_time, content_hash, updated_at)` with a composite primary key;
- `source_evidence(evidence_hash primary key, source, method, requested_at, response_body bytea, created_at)`; response bodies exclude HTTP authentication headers and are inserted idempotently by hash;
- `quality_reports(report_hash primary key, passed, production_complete, payload jsonb, created_at)`;
- `dataset_manifests(manifest_hash primary key, source, start_time, end_time, as_of, quality_report_hash references quality_reports, row_count, payload jsonb, created_at)`;
- `audit_events(sequence bigserial primary key, event_type, occurred_at, payload jsonb, previous_hash, event_hash unique)`.

A `BEFORE INSERT OR UPDATE` trigger on `dataset_manifests` rejects a production manifest whose linked report is not both `passed` and `production_complete`; a plain `CHECK` is insufficient because PostgreSQL checks cannot safely enforce this cross-table rule. All PostgreSQL writes for one ingestion result occur in one SQLAlchemy transaction after the ClickHouse raw append succeeds.

- [ ] **Step 4: Implement repositories with parameterized SQL and secret-safe errors**

Use SQLAlchemy async engine with `pool_pre_ping=True`. `advance_checkpoint()` locks the row and rejects a timestamp lower than its current value. `append_audit_event()` first takes `pg_advisory_xact_lock(hashtext('open_quant.audit_events'))`, reads the last hash, calculates SHA-256 over canonical JSON plus the previous hash, and inserts the next event atomically; the advisory lock also serializes the empty-table case.

- [ ] **Step 5: Run unit and conditional real integration tests**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/adapters/test_postgres_mapping.py -q'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/integration/test_postgres_repository.py -q -rs'
```

Expected: unit tests pass; real integration tests either pass against a configured test database or explicitly skip and keep phase 1 unverified.

- [ ] **Step 6: Commit the PostgreSQL control repository**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add migrations/postgres src/open_quant/adapters/postgres.py tests/unit/adapters/test_postgres_mapping.py tests/integration/test_postgres_repository.py && git commit -m "feat: add auditable ingestion control store"'
```

### Task 7: Atomic Ingestion Orchestration

**Files:**
- Create: `src/open_quant/data/ingestion.py`
- Test: `tests/unit/data/test_ingestion.py`

**Interfaces:**
- Consumes: `MarketDataSource`, `MinuteBarQualityGate`, `MinuteBarRepository`, `PostgresControlRepository`, instrument tuple, time range, and `as_of`.
- Produces: `IngestionService.run(request: IngestionRequest) -> IngestionResult` and `ValidatedDatasetReader.query(manifest_hash: str, as_of: datetime)`.

- [ ] **Step 1: Write failing orchestration tests**

Tests use recording protocol implementations to prove this order:

1. fetch source;
2. fetch trading-period and suspension evidence;
3. persist raw source evidence and parsable revisions without mutating prior rows;
4. evaluate quality;
5. save the quality report;
6. stop without a production manifest or checkpoint when quality fails, while retaining quarantined raw evidence and revisions;
7. save manifest, checkpoint, and audit only after quality passes;
8. never advance the checkpoint when any prior operation raises.

```python
@pytest.mark.asyncio
async def test_failed_quality_never_persists_bars_or_checkpoint() -> None:
    recorder = RecordingDependencies(failing_quality=True)
    result = await recorder.service.run(recorder.request)
    assert result.status == "quality_rejected"
    assert recorder.calls == [
        "fetch_bars", "fetch_coverage", "save_source_evidence", "append_raw_bars",
        "quality", "save_quality_report", "audit_rejection",
    ]
```

- [ ] **Step 2: Run the test and confirm the orchestration module is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/data/test_ingestion.py -q'
```

Expected: collection fails because `open_quant.data.ingestion` does not exist.

- [ ] **Step 3: Implement fail-closed orchestration**

`IngestionRequest` is a frozen dataclass containing a non-empty instrument tuple, aware UTC `start/end/as_of`, and `production_complete_requested`. It rejects `start > end` and `as_of < end`. `IngestionResult` contains status, fetched count, persisted count, quality hash, and optional manifest hash; it never contains credentials.

If raw ClickHouse append succeeds but PostgreSQL finalization fails, emit a failure result, do not advance the checkpoint, and allow an idempotent rerun based on content hashes. A manifest can set `production_complete=True` only when the quality report has complete RQData trading-period and suspension evidence for every requested instrument/date. `MinuteBarRepository.query_as_of()` is adapter-internal. Research callers use `ValidatedDatasetReader`, which first requires a passing, production-complete PostgreSQL manifest and then constrains the ClickHouse query to that manifest's instruments, bounds, hashes, and `as_of`. Failed-quality revisions are available only to an explicitly named quarantine inspection command.

- [ ] **Step 4: Run orchestration and full non-live tests**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest -m "not live and not integration" -q && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src'
```

Expected: all non-live unit and contract tests pass; static checks succeed.

- [ ] **Step 5: Commit the ingestion service**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add src/open_quant/data/ingestion.py tests/unit/data/test_ingestion.py && git commit -m "feat: orchestrate fail-closed data ingestion"'
```

### Task 8: Operator CLI, Connection Checks, and Phase-1 Evidence

**Files:**
- Create: `src/open_quant/cli.py`
- Create: `docs/runbooks/phase1-data-foundation.md`
- Create: `tests/unit/test_cli.py`
- Create: `README.md`

**Interfaces:**
- Consumes: `AppSettings` and adapters from Tasks 3, 5, and 6.
- Produces: `open-quant config-check`, `open-quant rqdata-check`, `open-quant db-check`, and `open-quant ingest-minute` commands with JSON output and non-zero exit codes on incomplete evidence.

- [ ] **Step 1: Write failing CLI safety tests**

```python
# tests/unit/test_cli.py
from typer.testing import CliRunner
from open_quant.cli import app


runner = CliRunner()


def test_config_check_reports_missing_capability_without_secret() -> None:
    result = runner.invoke(app, ["config-check"], env={})
    assert result.exit_code == 2
    assert '"rqdata":"missing"' in result.stdout
    assert "password" not in result.stdout.lower()


def test_ingestion_refuses_live_environment_flag() -> None:
    result = runner.invoke(
        app,
        ["ingest-minute", "--instrument", "000001.XSHE", "--start", "2026-07-20T09:30:00+08:00", "--end", "2026-07-20T09:31:00+08:00"],
        env={"OQ_ENVIRONMENT": "live", "OQ_LIVE_TRADING_ENABLED": "true"},
    )
    assert result.exit_code != 0
    assert "phase-1 ingestion does not enable trading" in result.stdout
```

- [ ] **Step 2: Run CLI tests and confirm the module is missing**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest tests/unit/test_cli.py -q'
```

Expected: collection fails because `open_quant.cli` does not exist.

- [ ] **Step 3: Implement JSON-only operational commands**

- `config-check` reports configured/missing capabilities without values.
- `rqdata-check` performs authentication and one caller-specified read-only minute query; it never runs by default during tests.
- `db-check` runs `SELECT 1` on both real databases and verifies the expected schema versions.
- `ingest-minute` requires explicit instruments and UTC-convertible bounds, rejects the live-trading flag, and exits non-zero unless the quality report and manifest persist successfully.
- All commands log structured events and redact secrets and DSNs.

- [ ] **Step 4: Document exact remote setup and evidence commands**

The runbook contains these commands:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv sync --frozen --all-groups'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run open-quant config-check'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest -m "not live" -q'
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src'
```

It separately lists the opt-in `OQ_RUN_RQDATA_LIVE=1` command and states that the real API, PostgreSQL, and ClickHouse checks are required before phase 1 can be declared complete.

- [ ] **Step 5: Run the complete available verification suite**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && /Users/zjw/.local/bin/uv run pytest -q -rs && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src && /Users/zjw/.local/bin/uv lock --check'
```

Expected: all available tests pass; credential/service-dependent tests are explicit skips, and no completion claim is made for skipped evidence.

- [ ] **Step 6: Inspect tracked files for secrets**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git grep -nE "(password|token|secret|account)[[:space:]]*=[[:space:]]*[^[:space:]]+" -- . ":(exclude)docs/superpowers" || true'
```

Expected: no credential assignments with non-empty values.

- [ ] **Step 7: Commit phase-1 operator surface**

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant && git add README.md docs/runbooks src/open_quant/cli.py tests/unit/test_cli.py && git commit -m "feat: add auditable data ingestion CLI"'
```

## Phase-1 Completion Evidence

Phase 1 is complete only when all of the following are present in current-state evidence:

1. `uv sync --frozen --all-groups` succeeds on rlocal using Python 3.11.
2. Unit, contract, integration, Ruff, and mypy checks pass with no unexplained skips.
3. A real RQData read-only call returns a valid unadjusted one-minute bar.
4. Real ClickHouse integration proves append-only revisions and `as_of` correction selection.
5. Real PostgreSQL integration proves monotonic checkpoints, manifest constraints, and the audit hash chain.
6. An end-to-end real ingestion produces a passing quality report, immutable manifest, ClickHouse rows, PostgreSQL checkpoint, and audit event.
7. The Git tree contains no credentials, tokens, certificates, or actual brokerage account identifiers.

Until credentials and database services exist, Tasks 1–8 can be implemented and locally verified, but items 3–6 remain explicitly incomplete.
