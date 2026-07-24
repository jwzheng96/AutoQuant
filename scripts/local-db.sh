#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infra/compose.yaml"
compose_env="$project_root/infra/.env"

if command -v docker >/dev/null 2>&1; then
  docker_cli="$(command -v docker)"
elif [[ -x /Applications/Docker.app/Contents/Resources/bin/docker ]]; then
  docker_cli=/Applications/Docker.app/Contents/Resources/bin/docker
else
  echo "Docker CLI is unavailable" >&2
  exit 2
fi

if [[ ! -f "$compose_env" ]]; then
  echo "infra/.env is required" >&2
  exit 2
fi

compose=("$docker_cli" compose --env-file "$compose_env" -f "$compose_file")

wait_healthy() {
  local container="$1"
  local attempt
  local state
  for attempt in {1..60}; do
    state="$($docker_cli inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$state" == "healthy" ]]; then
      return 0
    fi
    if [[ "$state" == "unhealthy" || "$state" == "exited" ]]; then
      echo "$container failed health validation" >&2
      return 1
    fi
    sleep 2
  done
  echo "$container did not become healthy in time" >&2
  return 1
}

case "${1:-}" in
  up)
    "${compose[@]}" up -d
    wait_healthy autoquant-postgres
    wait_healthy autoquant-clickhouse
    "${compose[@]}" ps
    ;;
  migrate)
    wait_healthy autoquant-postgres
    wait_healthy autoquant-clickhouse
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/001_phase1.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/002_operator_console.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/003_revision_checkpoints.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/004_backtest_runs.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/005_walk_forward_validation.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/006_validation_benchmark.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/007_risk_decisions.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/008_paper_execution.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/009_execution_controls.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/010_simulated_broker.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/011_paper_session_risk.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/012_qmt_session_leases.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/013_paper_scheduler_events.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/014_paper_scheduler_leases.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/015_paper_strategy_registry.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/016_paper_runtime_unlock.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/017_qmt_readonly_acceptance.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/018_qmt_recovery_drills.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/019_paper_portfolio_registry.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/020_portfolio_oos_assessment.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/021_validation_campaigns.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/022_portfolio_validation.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/023_research_universes.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/024_research_data_campaigns.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/025_dynamic_research_specs.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/026_dynamic_validation_evidence.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/027_dynamic_regime_research.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/028_fundamental_research.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/029_fundamental_dataset.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/030_fundamental_panels.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/031_fundamental_validation.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/032_low_volatility_research.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/033_low_volatility_validation.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/034_low_volatility_forward_evidence.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/035_low_volatility_forward_sessions.sql"
    "${compose[@]}" exec -T postgres sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
      < "$project_root/migrations/postgres/036_paper_compliance_approvals.sql"
    "${compose[@]}" exec -T clickhouse sh -c \
      'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --multiquery' \
      < "$project_root/migrations/clickhouse/001_phase1.sql"
    "${compose[@]}" exec -T clickhouse sh -c \
      'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --multiquery' \
      < "$project_root/migrations/clickhouse/002_tushare_daily.sql"
    "${compose[@]}" exec -T clickhouse sh -c \
      'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --multiquery' \
      < "$project_root/migrations/clickhouse/003_daily_coverage.sql"
    "${compose[@]}" exec -T clickhouse sh -c \
      'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --multiquery' \
      < "$project_root/migrations/clickhouse/004_fundamental_revisions.sql"
    echo "Database migrations completed"
    ;;
  status)
    "${compose[@]}" ps
    ;;
  stop)
    "${compose[@]}" stop
    ;;
  *)
    echo "Usage: scripts/local-db.sh {up|migrate|status|stop}" >&2
    exit 2
    ;;
esac
