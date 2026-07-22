# AutoQuant Full Rename Design

**Date:** 2026-07-22

## Objective

Rename the project from Open Quant to AutoQuant as a single coherent identity, verify the
renamed package on the trusted `rlocal` host, and publish it through a pull request to the
empty GitHub repository at `https://github.com/jwzheng96/AutoQuant`.

## Identity Boundary

AutoQuant becomes the only supported identity:

- Product and documentation title: `AutoQuant`
- Python distribution: `autoquant`
- Python import package: `autoquant`
- Installed CLI command: `autoquant`
- Environment prefix: `AQ_`
- Live-test opt-in flag: `AQ_RUN_RQDATA_LIVE`
- PostgreSQL function prefix: `autoquant_`
- PostgreSQL audit advisory-lock key: `autoquant.audit_events`

Generic domain table names such as `minute_bar_revisions`, `quality_reports`, and
`dataset_manifests` remain unchanged. No compatibility aliases for `open_quant`,
`open-quant`, or `OQ_` will be retained.

The active SSHFS checkout and linked worktree keep their existing physical paths during
this session because Git worktree metadata stores absolute paths. A future clean clone
from GitHub will naturally use an `AutoQuant` directory.

## Code and Configuration Migration

The source directory moves from `src/open_quant` to `src/autoquant`. All internal imports,
tests, mock patch targets, mypy package configuration, entry points, and documentation are
updated together. `pyproject.toml` exposes `autoquant = "autoquant.cli:app"`, and the lock
file is regenerated on `rlocal`.

`.env.example` and configuration tests move every supported environment setting from the
`OQ_` prefix to `AQ_`. The settings model loads only the new prefix. Database and live
test configuration use `AQ_POSTGRES_DSN`, `AQ_CLICKHOUSE_DSN`, and
`AQ_RUN_RQDATA_LIVE`.

The PostgreSQL baseline migration renames project-owned functions and the audit lock key.
The existing database tables remain generic. Because no real database deployment has
been verified or declared operational, the rename targets fresh phase-1 deployments; it
does not add an online migration for an existing Open Quant database.

## Documentation and Historical Records

Current user-facing documentation, examples, commands, and clone instructions use
AutoQuant and the GitHub repository URL. Historical approved design and implementation
plan documents remain historical records rather than being mechanically rewritten. A
clear supersession note in those documents or their containing documentation identifies
AutoQuant as the current name, so searches do not mistake historical names for supported
interfaces.

## Test Strategy

The implementation follows strict TDD:

1. Add a naming-contract test that requires the AutoQuant distribution, package, CLI,
   environment prefix, and database identifiers and rejects active legacy interfaces.
2. Run the focused test and observe failure against the existing Open Quant identity.
3. Apply the source and configuration rename.
4. Regenerate `uv.lock` on `rlocal`.
5. Run the focused naming test, full pytest suite, Ruff, strict mypy, lock verification,
   Git diff checks, and tracked-file credential scan.
6. Search active source, tests, configuration, migrations, README, and runbooks for legacy
   identifiers. Only explicitly marked historical design records may retain them.

External RQData, PostgreSQL, and ClickHouse checks remain explicit skips unless their
credentials and DSNs are configured. The rename does not convert a skipped external check
into completion evidence.

## GitHub Publication

The target GitHub repository currently has no branches. Publication preserves the chosen
pull-request workflow:

1. Add `https://github.com/jwzheng96/AutoQuant.git` as `origin`.
2. Push the existing local `main` baseline to remote `main` without rewriting history.
3. Commit the verified rename on `feature/trusted-data-foundation`.
4. Push `feature/trusted-data-foundation` and set its upstream.
5. Create a pull request targeting `main` with a concise summary and verification results.

The GitHub CLI is not currently installed or authenticated on `rlocal`. Git push may use
the existing Git credential helper if it is configured. If PR creation lacks an
authenticated client after both branches are safely pushed, stop without altering the
remote branches and request the smallest required authentication step; do not expose or
persist credentials in the repository.

## Failure Handling

- Any rename test, full test, static check, lock check, or secret scan failure blocks the
  upload until corrected and reverified.
- A push authentication failure leaves all local commits and branches intact.
- No force push, history rewrite, branch deletion, or worktree cleanup is part of this
  change.
- The remote repository is empty, but publication still uses non-destructive ordinary
  pushes and a PR rather than directly replacing `main`.
