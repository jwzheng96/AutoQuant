from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, localcontext

from autoquant.backtest.low_volatility_portfolio import (
    LOW_VOLATILITY_STRATEGY_ID,
)
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

LOW_VOLATILITY_FORWARD_SPEC_VERSION = "low-volatility-forward-evidence-spec-v1"
LOW_VOLATILITY_STABILITY_METHOD_VERSION = "annualized-geometric-return-gap-v1"
LOW_VOLATILITY_FORWARD_BLOCK_VERSION = "nonoverlapping-21-session-blocks-v1"
LOW_VOLATILITY_FORWARD_SESSION_VERSION = "low-volatility-forward-session-binding-v1"
LOW_VOLATILITY_FORWARD_SOURCE_URLS = (
    "https://doi.org/10.1057/s41260-021-00218-0",
    "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253",
)
_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:XSHG|XSHE)\Z")


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardEvidenceSpec:
    predecessor_result_hash: str
    predecessor_assessment_hash: str
    source_spec_hash: str
    source_dataset_manifest_hash: str
    forward_start_date: date
    maximum_annualized_stability_gap: Decimal
    minimum_forward_sessions: int = 126
    minimum_forward_blocks: int = 6
    minimum_profitable_block_rate: Decimal = Decimal("0.55")
    maximum_forward_drawdown: Decimal = Decimal("0.18")
    minimum_forward_compounded_return: Decimal = Decimal("0")
    minimum_forward_excess_return: Decimal = Decimal("0")
    maximum_rejected_orders: int = 0
    minimum_paper_sessions: int = 60
    annualization_sessions: int = 252
    formal_hypothesis_count: int = 4
    outcome_observed_at_design: bool = True
    strategy_parameters_unchanged: bool = True
    retrospective_reclassification_allowed: bool = False
    historical_result_eligible_for_promotion: bool = False
    strategy_id: str = LOW_VOLATILITY_STRATEGY_ID
    stability_method_version: str = LOW_VOLATILITY_STABILITY_METHOD_VERSION
    block_version: str = LOW_VOLATILITY_FORWARD_BLOCK_VERSION
    methodology_sources: tuple[str, ...] = LOW_VOLATILITY_FORWARD_SOURCE_URLS
    version: str = LOW_VOLATILITY_FORWARD_SPEC_VERSION
    spec_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (
                self.predecessor_result_hash,
                "forward predecessor result hash",
            ),
            (
                self.predecessor_assessment_hash,
                "forward predecessor assessment hash",
            ),
            (self.source_spec_hash, "forward source spec hash"),
            (
                self.source_dataset_manifest_hash,
                "forward source dataset manifest hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        bounded = (
            self.maximum_annualized_stability_gap,
            self.minimum_profitable_block_rate,
            self.maximum_forward_drawdown,
        )
        thresholds = (
            self.minimum_forward_compounded_return,
            self.minimum_forward_excess_return,
        )
        if (
            any(
                not isinstance(value, Decimal) or not value.is_finite() or value < 0 or value > 1
                for value in bounded
            )
            or any(not isinstance(value, Decimal) or not value.is_finite() for value in thresholds)
            or self.minimum_forward_sessions != 126
            or self.minimum_forward_blocks != 6
            or self.minimum_paper_sessions != 60
            or self.annualization_sessions != 252
            or self.formal_hypothesis_count != 4
            or self.maximum_rejected_orders != 0
        ):
            raise ValueError("low-volatility forward evidence gates are unsupported")
        if (
            not self.outcome_observed_at_design
            or not self.strategy_parameters_unchanged
            or self.retrospective_reclassification_allowed
            or self.historical_result_eligible_for_promotion
            or self.strategy_id != LOW_VOLATILITY_STRATEGY_ID
            or self.stability_method_version != LOW_VOLATILITY_STABILITY_METHOD_VERSION
            or self.block_version != LOW_VOLATILITY_FORWARD_BLOCK_VERSION
            or self.methodology_sources != LOW_VOLATILITY_FORWARD_SOURCE_URLS
            or self.version != LOW_VOLATILITY_FORWARD_SPEC_VERSION
        ):
            raise ValueError("low-volatility forward governance is unsupported")
        object.__setattr__(
            self,
            "spec_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "annualization_sessions": self.annualization_sessions,
            "block_version": self.block_version,
            "formal_hypothesis_count": self.formal_hypothesis_count,
            "forward_start_date": self.forward_start_date.isoformat(),
            "historical_result_eligible_for_promotion": (
                self.historical_result_eligible_for_promotion
            ),
            "maximum_annualized_stability_gap": _decimal_text(
                self.maximum_annualized_stability_gap
            ),
            "maximum_forward_drawdown": _decimal_text(self.maximum_forward_drawdown),
            "maximum_rejected_orders": self.maximum_rejected_orders,
            "methodology_sources": list(self.methodology_sources),
            "minimum_forward_blocks": self.minimum_forward_blocks,
            "minimum_forward_compounded_return": _decimal_text(
                self.minimum_forward_compounded_return
            ),
            "minimum_forward_excess_return": _decimal_text(self.minimum_forward_excess_return),
            "minimum_forward_sessions": self.minimum_forward_sessions,
            "minimum_paper_sessions": self.minimum_paper_sessions,
            "minimum_profitable_block_rate": _decimal_text(self.minimum_profitable_block_rate),
            "outcome_observed_at_design": (self.outcome_observed_at_design),
            "predecessor_assessment_hash": (self.predecessor_assessment_hash),
            "predecessor_result_hash": self.predecessor_result_hash,
            "retrospective_reclassification_allowed": (self.retrospective_reclassification_allowed),
            "source_dataset_manifest_hash": (self.source_dataset_manifest_hash),
            "source_spec_hash": self.source_spec_hash,
            "stability_method_version": (self.stability_method_version),
            "strategy_id": self.strategy_id,
            "strategy_parameters_unchanged": (self.strategy_parameters_unchanged),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityForwardEvidenceSpec:
        raw_sources = payload["methodology_sources"]
        if not isinstance(raw_sources, list) or any(
            not isinstance(value, str) for value in raw_sources
        ):
            raise TypeError("forward methodology sources are invalid")
        value = cls(
            predecessor_result_hash=str(payload["predecessor_result_hash"]),
            predecessor_assessment_hash=str(payload["predecessor_assessment_hash"]),
            source_spec_hash=str(payload["source_spec_hash"]),
            source_dataset_manifest_hash=str(payload["source_dataset_manifest_hash"]),
            forward_start_date=date.fromisoformat(str(payload["forward_start_date"])),
            maximum_annualized_stability_gap=Decimal(
                str(payload["maximum_annualized_stability_gap"])
            ),
            minimum_forward_sessions=int(str(payload["minimum_forward_sessions"])),
            minimum_forward_blocks=int(str(payload["minimum_forward_blocks"])),
            minimum_profitable_block_rate=Decimal(str(payload["minimum_profitable_block_rate"])),
            maximum_forward_drawdown=Decimal(str(payload["maximum_forward_drawdown"])),
            minimum_forward_compounded_return=Decimal(
                str(payload["minimum_forward_compounded_return"])
            ),
            minimum_forward_excess_return=Decimal(str(payload["minimum_forward_excess_return"])),
            maximum_rejected_orders=int(str(payload["maximum_rejected_orders"])),
            minimum_paper_sessions=int(str(payload["minimum_paper_sessions"])),
            annualization_sessions=int(str(payload["annualization_sessions"])),
            formal_hypothesis_count=int(str(payload["formal_hypothesis_count"])),
            outcome_observed_at_design=_boolean(payload["outcome_observed_at_design"]),
            strategy_parameters_unchanged=_boolean(payload["strategy_parameters_unchanged"]),
            retrospective_reclassification_allowed=_boolean(
                payload["retrospective_reclassification_allowed"]
            ),
            historical_result_eligible_for_promotion=_boolean(
                payload["historical_result_eligible_for_promotion"]
            ),
            strategy_id=str(payload["strategy_id"]),
            stability_method_version=str(payload["stability_method_version"]),
            block_version=str(payload["block_version"]),
            methodology_sources=tuple(raw_sources),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("forward evidence spec payload is not canonical")
        return value


@dataclass(frozen=True, slots=True)
class LowVolatilityForwardSessionBinding:
    forward_spec_hash: str
    dataset_manifest_hash: str
    policy_hash: str
    session_date: date
    snapshot_hash: str
    snapshot_reference_date: date
    calendar_content_hash: str
    instruments: tuple[str, ...]
    version: str = LOW_VOLATILITY_FORWARD_SESSION_VERSION
    binding_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.forward_spec_hash, "forward session spec hash"),
            (
                self.dataset_manifest_hash,
                "forward session dataset manifest hash",
            ),
            (self.policy_hash, "forward session policy hash"),
            (self.snapshot_hash, "forward session snapshot hash"),
            (
                self.calendar_content_hash,
                "forward session calendar content hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        instruments = tuple(self.instruments)
        if (
            self.snapshot_reference_date >= self.session_date
            or not instruments
            or instruments != tuple(sorted(instruments))
            or len(set(instruments)) != len(instruments)
            or any(_INSTRUMENT.fullmatch(value) is None for value in instruments)
            or self.version != LOW_VOLATILITY_FORWARD_SESSION_VERSION
        ):
            raise ValueError("low-volatility forward session binding is invalid")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(
            self,
            "binding_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        return {
            "calendar_content_hash": self.calendar_content_hash,
            "dataset_manifest_hash": self.dataset_manifest_hash,
            "forward_spec_hash": self.forward_spec_hash,
            "instruments": list(self.instruments),
            "policy_hash": self.policy_hash,
            "session_date": self.session_date.isoformat(),
            "snapshot_hash": self.snapshot_hash,
            "snapshot_reference_date": (self.snapshot_reference_date.isoformat()),
            "version": self.version,
        }

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, object],
    ) -> LowVolatilityForwardSessionBinding:
        raw_instruments = payload["instruments"]
        if not isinstance(raw_instruments, list) or any(
            not isinstance(value, str) for value in raw_instruments
        ):
            raise TypeError("forward session instruments are invalid")
        value = cls(
            forward_spec_hash=str(payload["forward_spec_hash"]),
            dataset_manifest_hash=str(payload["dataset_manifest_hash"]),
            policy_hash=str(payload["policy_hash"]),
            session_date=date.fromisoformat(str(payload["session_date"])),
            snapshot_hash=str(payload["snapshot_hash"]),
            snapshot_reference_date=date.fromisoformat(str(payload["snapshot_reference_date"])),
            calendar_content_hash=str(payload["calendar_content_hash"]),
            instruments=tuple(raw_instruments),
            version=str(payload["version"]),
        )
        if value.payload() != payload:
            raise ValueError("forward session payload is not canonical")
        return value


def annualized_geometric_return(
    total_return: Decimal,
    *,
    sessions: int,
    annualization_sessions: int = 252,
) -> Decimal:
    if (
        not isinstance(total_return, Decimal)
        or not total_return.is_finite()
        or total_return <= Decimal("-1")
        or sessions < 1
        or annualization_sessions < 1
    ):
        raise ValueError("annualized geometric return input is invalid")
    with localcontext() as context:
        context.prec = 34
        exponent = Decimal(annualization_sessions) / Decimal(sessions)
        return +(((Decimal("1") + total_return).ln() * exponent).exp() - Decimal("1"))


def annualized_stability_gap(
    *,
    training_return: Decimal,
    training_sessions: int,
    evaluation_return: Decimal,
    evaluation_sessions: int,
    annualization_sessions: int = 252,
) -> Decimal:
    return annualized_geometric_return(
        training_return,
        sessions=training_sessions,
        annualization_sessions=annualization_sessions,
    ) - annualized_geometric_return(
        evaluation_return,
        sessions=evaluation_sessions,
        annualization_sessions=annualization_sessions,
    )


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("forward evidence boolean is invalid")
    return value
