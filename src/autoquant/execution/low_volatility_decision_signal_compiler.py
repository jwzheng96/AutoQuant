from __future__ import annotations

from datetime import datetime, timedelta

from autoquant.backtest.low_volatility_execution_compatibility import (
    LowVolatilityExecutionCompatibilityRun,
)
from autoquant.clock import to_utc
from autoquant.execution.control import KillSwitchControl
from autoquant.execution.low_volatility_decision_signal import (
    LowVolatilityDecisionTimePaperSignal,
)
from autoquant.execution.low_volatility_paper_approval import (
    LowVolatilityPaperCandidateApproval,
)
from autoquant.execution.low_volatility_paper_deployment_contract import (
    LowVolatilityPaperDeploymentContract,
)
from autoquant.execution.low_volatility_paper_signal import (
    LowVolatilityPaperDailySignal,
)
from autoquant.execution.reconciliation import (
    ExecutionAccountSnapshot,
    ReconciliationReport,
)
from autoquant.risk.models import RiskPolicy

_RECONCILIATION_SNAPSHOT_MAX_AGE = timedelta(seconds=5)


class LowVolatilityDecisionTimeSignalCompiler:
    """Bind pre-open research, account and control evidence without authority."""

    def compile(
        self,
        *,
        contract: LowVolatilityPaperDeploymentContract,
        candidate: LowVolatilityPaperCandidateApproval,
        compatibility: LowVolatilityExecutionCompatibilityRun,
        observation: LowVolatilityPaperDailySignal,
        internal_account: ExecutionAccountSnapshot,
        broker_account: ExecutionAccountSnapshot,
        reconciliation: ReconciliationReport,
        risk_policy: RiskPolicy,
        kill_switch: KillSwitchControl,
        prepared_by: str,
        prepared_at: datetime,
    ) -> LowVolatilityDecisionTimePaperSignal:
        prepared_at = to_utc(
            prepared_at,
            name="decision-time signal preparation time",
        )
        if (
            contract.source_spec_hash != candidate.source_spec_hash
            or contract.forward_spec_hash != candidate.forward_spec_hash
            or contract.compatibility_spec_hash
            != compatibility.compatibility_spec_hash
            or contract.required_order_policy_version
            != compatibility.decision_order_policy_version
            or contract.frozen_at > compatibility.completed_at
            or compatibility.original_evaluation_result_hash
            != candidate.evaluation_result_hash
            or compatibility.forward_spec_hash != candidate.forward_spec_hash
            or compatibility.source_spec_hash != candidate.source_spec_hash
            or not compatibility.execution_timing_compatible
            or compatibility.completed_at > candidate.approved_at
            or candidate.runtime_activation_allowed
            or not candidate.live_trading_locked
        ):
            raise ValueError(
                "decision-time signal deployment evidence is inconsistent"
            )
        if (
            observation.candidate_approval_hash != candidate.approval_hash
            or observation.account_id != candidate.account_id
            or observation.strategy_id != candidate.strategy_id
            or observation.source_spec_hash != candidate.source_spec_hash
            or observation.risk_policy_hash != candidate.risk_policy_hash
            or observation.prepared_at < candidate.approved_at
            or observation.execution_timing_compatible
            or observation.runtime_activation_allowed
            or not observation.live_trading_locked
        ):
            raise ValueError(
                "decision-time signal observation evidence is inconsistent"
            )
        if (
            risk_policy.policy_hash != candidate.risk_policy_hash
            or risk_policy.allowed_instruments != candidate.instruments
        ):
            raise ValueError(
                "decision-time signal risk policy is inconsistent"
            )
        if (
            reconciliation.account_id != candidate.account_id
            or reconciliation.internal_snapshot_hash
            != internal_account.snapshot_hash
            or reconciliation.broker_snapshot_hash
            != broker_account.snapshot_hash
            or not reconciliation.reconciled
            or internal_account.account_id != candidate.account_id
            or broker_account.account_id != candidate.account_id
            or internal_account.as_of > reconciliation.evaluated_at
            or broker_account.as_of > reconciliation.evaluated_at
            or reconciliation.evaluated_at - internal_account.as_of
            > _RECONCILIATION_SNAPSHOT_MAX_AGE
            or reconciliation.evaluated_at - broker_account.as_of
            > _RECONCILIATION_SNAPSHOT_MAX_AGE
            or internal_account.open_client_order_ids
            or broker_account.open_client_order_ids
        ):
            raise ValueError(
                "decision-time signal account evidence is inconsistent"
            )
        if (
            kill_switch.account_id != candidate.account_id
            or not kill_switch.active
            or kill_switch.changed_at > prepared_at
        ):
            raise ValueError(
                "decision-time signal requires an active kill switch"
            )
        internal_held = {
            value.instrument
            for value in internal_account.positions
            if value.total_quantity > 0
        }
        broker_held = {
            value.instrument
            for value in broker_account.positions
            if value.total_quantity > 0
        }
        if internal_held != broker_held:
            raise ValueError(
                "decision-time signal reconciled holdings are inconsistent"
            )
        held = tuple(sorted(internal_held | broker_held))
        valuations = tuple(
            sorted(value.instrument for value in observation.valuations)
        )
        if not set(held) <= set(valuations):
            raise ValueError(
                "decision-time signal does not value every held position"
            )
        return LowVolatilityDecisionTimePaperSignal(
            deployment_contract_hash=contract.contract_hash,
            candidate_approval_hash=candidate.approval_hash,
            compatibility_run_hash=compatibility.run_hash,
            observation_signal_hash=observation.signal_hash,
            reconciliation_report_hash=reconciliation.report_hash,
            internal_account_snapshot_hash=internal_account.snapshot_hash,
            broker_account_snapshot_hash=broker_account.snapshot_hash,
            kill_switch_event_hash=kill_switch.last_event_hash,
            account_id=candidate.account_id,
            strategy_id=candidate.strategy_id,
            source_spec_hash=candidate.source_spec_hash,
            forward_spec_hash=candidate.forward_spec_hash,
            compatibility_spec_hash=compatibility.compatibility_spec_hash,
            risk_policy_hash=candidate.risk_policy_hash,
            snapshot_hash=observation.snapshot_hash,
            dataset_manifest_hash=observation.dataset_manifest_hash,
            rule_set_hash=observation.rule_set_hash,
            session_sequence=observation.session_sequence,
            session_date=observation.session_date,
            signal_date=observation.signal_date,
            account_evidence_at=reconciliation.evaluated_at,
            kill_switch_changed_at=kill_switch.changed_at,
            selected_instruments=observation.selected_instruments,
            held_instruments=held,
            valuation_instruments=valuations,
            prepared_by=prepared_by,
            prepared_at=prepared_at,
        )
