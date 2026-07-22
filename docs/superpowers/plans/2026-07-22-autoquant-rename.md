# AutoQuant Full Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every supported Open Quant identity with AutoQuant, verify the renamed package, and publish it through a pull request to `jwzheng96/AutoQuant`.

**Architecture:** Perform one atomic namespace migration from `open_quant` to `autoquant`, including distribution metadata, CLI, environment prefix, tests, and project-owned SQL identifiers. Keep generic domain table names stable, preserve historical design documents with supersession notices, and publish the verified feature branch against a baseline `main` in the currently empty GitHub repository.

**Tech Stack:** Python 3.11, uv 0.11.30, Hatchling, Typer, Pydantic Settings, pytest, Ruff, mypy, Git, GitHub.

## Global Constraints

- Run every project tool, test, linter, type-checker, lock operation, migration check, Git mutation, push, and PR command on `rlocal` under `/Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation`.
- Keep the active checkout and linked-worktree physical paths unchanged during this session.
- AutoQuant is the only supported identity: distribution/import/CLI `autoquant`, environment prefix `AQ_`, PostgreSQL prefix `autoquant_`, and advisory-lock key `autoquant.audit_events`.
- Do not retain `open_quant`, `open-quant`, or `OQ_` compatibility aliases in active code, configuration, tests, migrations, README, or runbook.
- Keep generic domain table names unchanged.
- Treat the baseline SQL as a fresh-deployment migration; do not invent an online database migration for an externally unverified deployment.
- Historical design/plan records may retain legacy names only when clearly marked as superseded naming records.
- Never force-push, rewrite history, delete branches, remove the worktree, or expose credentials.
- External RQData/PostgreSQL/ClickHouse skips remain incomplete evidence.

---

### Task 1: Rename the Python Distribution, Package, CLI, Configuration, and SQL Identity

**Files:**
- Create: `tests/unit/test_project_identity.py`
- Rename: `src/open_quant/` to `src/autoquant/`
- Modify: every Python file under `src/autoquant/` and `tests/`
- Modify: `pyproject.toml`
- Modify: `.env.example`
- Modify: `migrations/postgres/001_phase1.sql`
- Modify: `tests/live/test_rqdata_readonly.py`
- Modify: `tests/integration/test_clickhouse_repository.py`
- Modify: `tests/integration/test_postgres_repository.py`
- Regenerate: `uv.lock`

**Interfaces:**
- Consumes: the existing `open_quant` package, `open-quant` console script, `OQ_*` settings contract, and `open_quant_*` PostgreSQL identifiers.
- Produces: importable `autoquant`, console script `autoquant`, `AppSettings` with `AQ_`, `AQ_RUN_RQDATA_LIVE`, `AQ_CLICKHOUSE_DSN`, `AQ_POSTGRES_DSN`, `autoquant_*` PostgreSQL functions, and `autoquant.audit_events`.

- [ ] **Step 1: Add the failing identity contract**

Create `tests/unit/test_project_identity.py`:

```python
from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path

import pytest

from autoquant.config import AppSettings, RuntimeEnvironment


ROOT = Path(__file__).parents[2]


def test_autoquant_is_the_only_runtime_package_and_console_script() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["name"] == "autoquant"
    assert project["project"]["scripts"] == {"autoquant": "autoquant.cli:app"}
    assert project["tool"]["mypy"]["packages"] == ["autoquant"]
    assert importlib.util.find_spec("autoquant") is not None
    assert importlib.util.find_spec("open_quant") is None


def test_autoquant_uses_only_the_aq_environment_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AQ_ENVIRONMENT", "paper")
    monkeypatch.setenv("OQ_ENVIRONMENT", "live")

    settings = AppSettings(_env_file=None)

    assert settings.environment is RuntimeEnvironment.PAPER
    assert settings.model_config["env_prefix"] == "AQ_"


def test_active_configuration_and_sql_use_autoquant_identity() -> None:
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    postgres = (ROOT / "migrations/postgres/001_phase1.sql").read_text(
        encoding="utf-8"
    )

    assert "AQ_ENVIRONMENT=" in env_example
    assert "OQ_" not in env_example
    assert "autoquant_validate_checkpoint" in postgres
    assert "autoquant.audit_events" in postgres
    assert "open_quant" not in postgres

    active_python = "\n".join(
        path.read_text(encoding="utf-8")
        for tree in (ROOT / "src", ROOT / "tests")
        for path in tree.rglob("*.py")
        if path.name != "test_project_identity.py"
    )
    assert "OpenQuant" not in active_python
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv run pytest tests/unit/test_project_identity.py -q'
```

Expected: collection fails with `ModuleNotFoundError: No module named 'autoquant'`.

- [ ] **Step 3: Rename the package directory and mechanically replace active identifiers**

Run the Git rename on `rlocal`:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git mv src/open_quant src/autoquant'
```

Apply these exact identity changes to active source, tests, configuration, and migrations:

```text
open_quant        -> autoquant
open-quant        -> autoquant
Open Quant        -> AutoQuant
OpenQuant         -> AutoQuant
OQ_               -> AQ_
```

Preserve the deliberate legacy strings in `tests/unit/test_project_identity.py`; exclude
that new contract file from the mechanical replacement.

The resulting `pyproject.toml` identity blocks must be:

```toml
[project]
name = "autoquant"

[project.scripts]
autoquant = "autoquant.cli:app"

[tool.mypy]
packages = ["autoquant"]
```

The resulting settings prefix must be:

```python
class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AQ_", env_file=".env", extra="forbid")
```

The resulting `.env.example` keys must be:

```dotenv
AQ_ENVIRONMENT=backtest
AQ_LIVE_TRADING_ENABLED=false
AQ_RQDATA_USERNAME=
AQ_RQDATA_PASSWORD=
AQ_RQDATA_AUTH_URL=https://rqdata.ricequant.com/auth
AQ_RQDATA_API_URL=https://rqdata.ricequant.com/api
AQ_POSTGRES_DSN=
AQ_CLICKHOUSE_DSN=
```

Rename the PostgreSQL functions and lock call to these exact values:

```sql
autoquant_validate_checkpoint
autoquant_validate_manifest
autoquant_reject_immutable_change
SELECT pg_advisory_xact_lock(hashtext('autoquant.audit_events'))
```

Update opt-in and database environment reads to:

```python
AQ_RUN_RQDATA_LIVE
AQ_CLICKHOUSE_DSN
AQ_POSTGRES_DSN
```

- [ ] **Step 4: Regenerate the lock file on the project host**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv lock && /Users/zjw/.local/bin/uv sync --all-groups'
```

Expected: `uv.lock` names the root package `autoquant`, and sync succeeds with Python 3.11.

- [ ] **Step 5: Run the focused and full package verification**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv run pytest tests/unit/test_project_identity.py -q && /Users/zjw/.local/bin/uv run pytest -q -rs && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src && /Users/zjw/.local/bin/uv lock --check && git diff --check'
```

Expected: identity tests and all available tests pass; the same six external checks remain explicit skips unless credentials/services are newly configured; Ruff, mypy, lock, and diff checks succeed.

- [ ] **Step 6: Commit the runtime identity migration**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git add .env.example pyproject.toml uv.lock src/autoquant tests migrations/postgres/001_phase1.sql && git diff --cached --check && git commit -m "refactor: rename runtime package to AutoQuant"'
```

---

### Task 2: Rename Current Documentation and Mark Historical Naming Records

**Files:**
- Modify: `README.md`
- Modify: `docs/runbooks/phase1-data-foundation.md`
- Modify: `docs/superpowers/specs/2026-07-21-a-share-quant-system-design.md`
- Modify: `docs/superpowers/plans/2026-07-21-trusted-data-foundation.md`

**Interfaces:**
- Consumes: the Task 1 `autoquant` CLI and `AQ_*` configuration contract.
- Produces: current AutoQuant clone/setup/run commands and explicit supersession markers on historical Open Quant records.

- [ ] **Step 1: Extend the identity test with active-document assertions**

Append to `tests/unit/test_project_identity.py`:

```python
def test_current_documentation_uses_autoquant_commands_and_repository() -> None:
    current_docs = (
        (ROOT / "README.md").read_text(encoding="utf-8"),
        (ROOT / "docs/runbooks/phase1-data-foundation.md").read_text(encoding="utf-8"),
    )
    combined = "\n".join(current_docs)

    assert "# AutoQuant" in current_docs[0]
    assert "https://github.com/jwzheng96/AutoQuant" in combined
    assert "/AutoQuant" in combined
    assert "uv run autoquant config-check" in combined
    assert "AQ_RUN_RQDATA_LIVE=1" in combined
    assert "open-quant" not in combined
    assert "OQ_" not in combined


def test_historical_documents_are_marked_as_superseded() -> None:
    historical = (
        ROOT / "docs/superpowers/specs/2026-07-21-a-share-quant-system-design.md",
        ROOT / "docs/superpowers/plans/2026-07-21-trusted-data-foundation.md",
    )
    for path in historical:
        first_lines = path.read_text(encoding="utf-8").splitlines()[:6]
        assert any(
            "AutoQuant" in line and "historical" in line.lower()
            for line in first_lines
        )
```

- [ ] **Step 2: Run the documentation contract and verify RED**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv run pytest tests/unit/test_project_identity.py -q'
```

Expected: the runtime identity cases pass, while current-documentation and historical-marker cases fail against the old README/runbook text and missing supersession notices.

- [ ] **Step 3: Update current documentation to AutoQuant**

The README heading and opening must be:

```markdown
# AutoQuant

AutoQuant is an A-share research data foundation.
```

README and runbook commands must use the future clean-clone path and new CLI:

```bash
git clone https://github.com/jwzheng96/AutoQuant.git
cd AutoQuant
/Users/zjw/.local/bin/uv sync --frozen --all-groups
/Users/zjw/.local/bin/uv run autoquant config-check
```

The live-test command must use:

```bash
AQ_RUN_RQDATA_LIVE=1 /Users/zjw/.local/bin/uv run pytest tests/live/test_rqdata_readonly.py -q -rs
```

Do not put the obsolete physical checkout path into current user-facing instructions.

- [ ] **Step 4: Mark the two historical records without rewriting their content**

Insert this block immediately below each historical document's title:

```markdown
> **Historical naming record:** This document uses the former Open Quant identifiers.
> The current project name and supported interfaces are AutoQuant; see
> `docs/superpowers/specs/2026-07-22-autoquant-rename-design.md`.
```

- [ ] **Step 5: Run documentation and legacy-identity checks**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv run pytest tests/unit/test_project_identity.py -q && ! rg -n "open_quant|open-quant|Open Quant|OpenQuant|OQ_" README.md docs/runbooks src tests migrations pyproject.toml .env.example --glob "!tests/unit/test_project_identity.py" && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src && git diff --check'
```

Expected: identity tests pass, the active-interface legacy scan returns no matches, and static/diff checks succeed.

- [ ] **Step 6: Commit the documentation migration**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git add README.md docs/runbooks docs/superpowers/specs/2026-07-21-a-share-quant-system-design.md docs/superpowers/plans/2026-07-21-trusted-data-foundation.md tests/unit/test_project_identity.py && git diff --cached --check && git commit -m "docs: adopt AutoQuant project identity"'
```

---

### Task 3: Final Verification and GitHub Pull Request Publication

**Files:**
- No repository file changes expected.
- Configure Git remote `origin` as `https://github.com/jwzheng96/AutoQuant.git`.

**Interfaces:**
- Consumes: clean local `main` at `9dd29be`, verified `feature/trusted-data-foundation`, and the empty GitHub repository `jwzheng96/AutoQuant`.
- Produces: remote `main`, remote `feature/trusted-data-foundation`, and a pull request from the feature branch into `main`.

- [ ] **Step 1: Run fresh completion verification**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && /Users/zjw/.local/bin/uv sync --frozen --all-groups && /Users/zjw/.local/bin/uv run pytest -q -rs && /Users/zjw/.local/bin/uv run ruff check . && /Users/zjw/.local/bin/uv run mypy src && /Users/zjw/.local/bin/uv lock --check && git diff --check && git status --short --branch'
```

Expected: the branch is clean; all available tests pass; external dependencies are only explicit skips; Ruff, mypy, lock, and diff checks succeed.

- [ ] **Step 2: Scan tracked files for real credentials**

Run the broad project scan:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git grep -nE "(password|token|secret|account)[[:space:]]*=[[:space:]]*[^[:space:]]+" -- . ":(exclude)docs/superpowers" || true'
```

Inspect every match. Only variable assignments and explicitly synthetic test values are permitted. Also run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && ! git grep -nE "BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY|github_pat_|ghp_[A-Za-z0-9]{20,}|postgres(ql)?(\+asyncpg)?://[^[:space:]@]+:[^[:space:]@]+@" -- .'
```

Expected: no private key, GitHub token, or credential-bearing PostgreSQL URL exists in tracked content.

- [ ] **Step 3: Configure and verify the GitHub remote non-destructively**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && if git remote get-url origin >/dev/null 2>&1; then test "$(git remote get-url origin)" = "https://github.com/jwzheng96/AutoQuant.git"; else git remote add origin https://github.com/jwzheng96/AutoQuant.git; fi && git remote -v && git ls-remote --heads origin'
```

Expected: `origin` is exactly the approved URL. Before first push, the remote head list is empty. If it is no longer empty, stop and compare the remote branches before pushing.

- [ ] **Step 4: Push the baseline main branch**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git push origin main:main'
```

Expected: remote `main` is created at local commit `9dd29be` without force.

- [ ] **Step 5: Push the verified feature branch**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && git push -u origin feature/trusted-data-foundation'
```

Expected: the feature branch is uploaded and configured to track `origin/feature/trusted-data-foundation`.

- [ ] **Step 6: Create the pull request when authenticated GitHub tooling is available**

First check without printing credentials:

```bash
ssh rlocal 'if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then printf "gh-ready\n"; else printf "gh-auth-required\n"; fi'
```

When output is `gh-ready`, run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && gh pr create --repo jwzheng96/AutoQuant --base main --head feature/trusted-data-foundation --title "Build trusted AutoQuant data foundation" --body "## Summary
- build the point-in-time A-share data foundation
- rename the distribution, package, CLI, configuration, and SQL identity to AutoQuant
- add RQData, ClickHouse, PostgreSQL, quality, ingestion, and operator interfaces

## Verification
- full pytest suite passes for available checks
- Ruff and strict mypy pass
- RQData and database integration checks remain explicit until credentials and services are configured"'
```

If output is `gh-auth-required`, do not install tooling, request credentials, or alter branches. Report the already-pushed compare URL exactly:

```text
https://github.com/jwzheng96/AutoQuant/compare/main...feature/trusted-data-foundation?expand=1
```

and ask the user to authenticate `gh` on `rlocal` or open that URL to complete PR creation.

- [ ] **Step 7: Verify remote branch identities**

Run:

```bash
ssh rlocal 'cd /Users/zjw/Documents/github-project/quant/open-quant/.worktrees/trusted-data-foundation && test "$(git rev-parse main)" = "$(git ls-remote origin refs/heads/main | cut -f1)" && test "$(git rev-parse feature/trusted-data-foundation)" = "$(git ls-remote origin refs/heads/feature/trusted-data-foundation | cut -f1)" && git status --short --branch'
```

Expected: both remote hashes exactly match the intended local branches and the worktree remains clean and preserved for PR feedback.
