from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_FLOOR, Decimal
from itertools import pairwise

from autoquant.backtest.dynamic_panel import DynamicMarketPanel
from autoquant.backtest.dynamic_strategy import (
    _can_trade,
    _liquidity_quantity,
)
from autoquant.backtest.fundamental_panel import (
    FundamentalFeatureObservation,
    FundamentalFeatureSession,
    FundamentalResearchPanel,
)
from autoquant.backtest.fundamental_portfolio import (
    FundamentalPortfolioResearchSpec,
)
from autoquant.backtest.models import (
    AccountSnapshot,
    MarketState,
    OrderIntent,
    OrderSide,
)
from autoquant.backtest.runner import _baseline_quantity, _order
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
)

FUNDAMENTAL_EXECUTABLE_PANEL_VERSION = "fundamental-executable-market-panel-v1"
FUNDAMENTAL_ORDER_POLICY_VERSION = "quality-value-equal-weight-orders-v1"


@dataclass(frozen=True, slots=True)
class FundamentalExecutableSession:
    session_date: date
    snapshot_hash: str
    active_members: tuple[str, ...]
    features: FundamentalFeatureSession
    markets: tuple[MarketState, ...]
    session_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_lowercase_sha256(
            self.snapshot_hash,
            name="fundamental executable snapshot hash",
        )
        members = tuple(self.active_members)
        markets = tuple(
            sorted(
                self.markets,
                key=lambda value: value.bar.instrument,
            )
        )
        if (
            self.features.execution_date != self.session_date
            or self.features.snapshot_hash != self.snapshot_hash
            or self.features.active_member_count != len(members)
            or not members
            or members != tuple(sorted(members))
            or len(set(members)) != len(members)
            or not markets
            or any(value.bar.session_date != self.session_date for value in markets)
            or len({value.bar.instrument for value in markets}) != len(markets)
            or any(value.instrument not in members for value in self.features.observations)
        ):
            raise ValueError("fundamental executable session is inconsistent")
        object.__setattr__(self, "active_members", members)
        object.__setattr__(self, "markets", markets)
        object.__setattr__(
            self,
            "session_hash",
            _canonical_hash(
                {
                    "feature_session_hash": (self.features.session_hash),
                    "market_rows": [
                        {
                            "bar_hash": value.bar.content_hash,
                            "limit_hash": (
                                None
                                if value.daily_price_limit is None
                                else value.daily_price_limit.content_hash
                            ),
                            "price_limit_rule": (value.rules.price_limit.rule_version),
                            "rule_version": value.rules.rule_version,
                            "suspended": value.suspended,
                        }
                        for value in markets
                    ],
                    "session_date": self.session_date.isoformat(),
                    "snapshot_hash": self.snapshot_hash,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class FundamentalExecutablePanel:
    spec_hash: str
    feature_panel_hash: str
    market_panel_hash: str
    as_of: datetime
    sessions: tuple[FundamentalExecutableSession, ...]
    version: str = FUNDAMENTAL_EXECUTABLE_PANEL_VERSION
    panel_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.spec_hash, "fundamental executable spec hash"),
            (
                self.feature_panel_hash,
                "fundamental feature panel hash",
            ),
            (
                self.market_panel_hash,
                "fundamental market panel hash",
            ),
        ):
            _require_lowercase_sha256(value, name=name)
        sessions = tuple(self.sessions)
        if (
            not sessions
            or any(
                current.session_date >= following.session_date
                for current, following in pairwise(sessions)
            )
            or self.version != FUNDAMENTAL_EXECUTABLE_PANEL_VERSION
        ):
            raise ValueError("fundamental executable panel is inconsistent")
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(
            self,
            "as_of",
            to_utc(
                self.as_of,
                name="fundamental executable panel as_of",
            ),
        )
        object.__setattr__(
            self,
            "panel_hash",
            _canonical_hash(
                {
                    "as_of": self.as_of.isoformat(),
                    "feature_panel_hash": self.feature_panel_hash,
                    "market_panel_hash": self.market_panel_hash,
                    "session_hashes": [value.session_hash for value in sessions],
                    "spec_hash": self.spec_hash,
                    "version": self.version,
                }
            ),
        )


def compile_fundamental_executable_panel(
    *,
    spec: FundamentalPortfolioResearchSpec,
    features: FundamentalResearchPanel,
    markets: DynamicMarketPanel,
) -> FundamentalExecutablePanel:
    if (
        features.spec_hash != spec.spec_hash
        or markets.spec_hash != spec.spec_hash
        or markets.dataset_manifest_hash != spec.daily_dataset_manifest_hash
        or markets.plan_hash != spec.plan_hash
    ):
        raise ValueError("fundamental executable inputs do not match the spec")
    market_by_date = {value.session_date: value for value in markets.sessions}
    sessions: list[FundamentalExecutableSession] = []
    for feature in features.sessions:
        try:
            market = market_by_date[feature.execution_date]
        except KeyError:
            raise ValueError("fundamental feature session has no executable market") from None
        sessions.append(
            FundamentalExecutableSession(
                session_date=feature.execution_date,
                snapshot_hash=market.snapshot_hash,
                active_members=market.active_members,
                features=feature,
                markets=market.markets,
            )
        )
    return FundamentalExecutablePanel(
        spec_hash=spec.spec_hash,
        feature_panel_hash=features.panel_hash,
        market_panel_hash=markets.panel_hash,
        as_of=max(features.as_of, markets.as_of),
        sessions=tuple(sessions),
    )


class FundamentalQualityValueOrderPolicy:
    """Fixed quality/value ranks translated into conservative A-share orders."""

    def __init__(
        self,
        *,
        sessions: tuple[FundamentalExecutableSession, ...],
        start_index: int,
        trade_session_count: int,
        spec: FundamentalPortfolioResearchSpec,
    ) -> None:
        sessions = tuple(sessions)
        if (
            not sessions
            or start_index < 0
            or trade_session_count < 2
            or start_index + trade_session_count > len(sessions)
        ):
            raise ValueError("fundamental order policy interval is invalid")
        self._sessions = sessions
        self._start_index = start_index
        self._trade_session_count = trade_session_count
        self._spec = spec
        self._last_rebalance: int | None = None
        self._order_sequence = 0

    def __call__(
        self,
        trade_index: int,
        markets: tuple[MarketState, ...],
        previous: AccountSnapshot | None,
    ) -> tuple[OrderIntent, ...]:
        if not 0 <= trade_index < self._trade_session_count:
            raise ValueError("fundamental strategy trade index is invalid")
        session = self._sessions[self._start_index + trade_index]
        if not markets or markets != session.markets:
            raise ValueError("fundamental strategy markets do not match the panel")
        holdings = (
            {}
            if previous is None
            else {value.instrument: value.total_quantity for value in previous.positions}
        )
        current = {value.bar.instrument: value for value in markets}
        if trade_index == self._trade_session_count - 1:
            return self._orders_to_targets(
                holdings=holdings,
                targets={},
                markets=current,
                session_date=session.session_date,
                suffix="forced-exit",
            )
        rebalance = (
            self._last_rebalance is None
            or trade_index - self._last_rebalance >= self._spec.rebalance_sessions
        )
        if not rebalance:
            return ()
        self._last_rebalance = trade_index
        observations = session.features.observations
        selected = (
            _ranked_instruments(observations)[: self._spec.selection_count]
            if len(observations) >= self._spec.minimum_eligible_members
            else ()
        )
        targets = {
            instrument: quantity
            for instrument in selected
            if (market := current.get(instrument)) is not None
            and _can_trade(OrderSide.BUY, market)
            and (
                quantity := _target_quantity(
                    market=market,
                    spec=self._spec,
                )
            )
            >= market.rules.buy_minimum
        }
        return self._orders_to_targets(
            holdings=holdings,
            targets=targets,
            markets=current,
            session_date=session.session_date,
            suffix="rebalance",
        )

    def _orders_to_targets(
        self,
        *,
        holdings: dict[str, int],
        targets: dict[str, int],
        markets: dict[str, MarketState],
        session_date: date,
        suffix: str,
    ) -> tuple[OrderIntent, ...]:
        orders: list[OrderIntent] = []
        for side in (OrderSide.SELL, OrderSide.BUY):
            for instrument in sorted(set(holdings) | set(targets)):
                held = holdings.get(instrument, 0)
                target = targets.get(instrument, 0)
                delta = target - held
                if (side is OrderSide.SELL and delta >= 0) or (
                    side is OrderSide.BUY and delta <= 0
                ):
                    continue
                market = markets.get(instrument)
                if market is None or not _can_trade(side, market):
                    continue
                step = market.rules.sell_step if side is OrderSide.SELL else market.rules.buy_step
                desired = abs(delta)
                if side is OrderSide.BUY:
                    desired = desired // step * step
                elif desired != held:
                    desired = desired // step * step
                quantity = min(
                    desired,
                    _liquidity_quantity(
                        market,
                        participation=(self._spec.maximum_volume_participation),
                        step=step,
                    ),
                    _notional_quantity_limit(
                        market=market,
                        maximum_notional=(self._spec.maximum_order_notional),
                        step=step,
                    ),
                )
                if quantity <= 0 or (side is OrderSide.BUY and quantity < market.rules.buy_minimum):
                    continue
                self._order_sequence += 1
                orders.append(
                    _order(
                        order_id=(f"fundamental-{self._order_sequence:06d}-{suffix}"),
                        instrument=instrument,
                        side=side,
                        quantity=quantity,
                        session_date=session_date,
                    )
                )
        return tuple(orders)


def _ranked_instruments(
    observations: tuple[FundamentalFeatureObservation, ...],
) -> tuple[str, ...]:
    if not observations:
        return ()
    factors = (
        "earnings_yield",
        "book_to_price",
        "roe_diluted_percent",
        "roa_percent",
        "operating_cashflow_to_revenue_percent",
    )
    scores = {value.instrument: Decimal("0") for value in observations}
    for factor in factors:
        values = tuple((getattr(value, factor), value.instrument) for value in observations)
        for instrument, percentile in _percentiles(values).items():
            scores[instrument] += percentile / Decimal(len(factors))
    return tuple(
        sorted(
            scores,
            key=lambda instrument: (
                -scores[instrument],
                instrument,
            ),
        )
    )


def _percentiles(
    values: tuple[tuple[Decimal, str], ...],
) -> dict[str, Decimal]:
    ordered = tuple(sorted(values, key=lambda value: (value[0], value[1])))
    if len(ordered) == 1:
        return {ordered[0][1]: Decimal("1")}
    result: dict[str, Decimal] = {}
    start = 0
    denominator = Decimal(len(ordered) - 1)
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][0] == ordered[start][0]:
            end += 1
        average_rank = Decimal(start + end - 1) / Decimal("2")
        percentile = average_rank / denominator
        for _, instrument in ordered[start:end]:
            result[instrument] = percentile
        start = end
    return result


def _target_quantity(
    *,
    market: MarketState,
    spec: FundamentalPortfolioResearchSpec,
) -> int:
    allocation = min(
        spec.maximum_position_weight,
        spec.gross_allocation / Decimal(spec.selection_count),
    )
    quantity = _baseline_quantity(
        initial_cash=spec.initial_cash,
        allocation=allocation,
        reference_price=market.bar.pre_close,
        slippage_bps=spec.slippage_bps,
        buy_minimum=market.rules.buy_minimum,
        buy_step=market.rules.buy_step,
        maximum=market.rules.max_order_quantity,
    )
    return min(
        quantity,
        _notional_quantity_limit(
            market=market,
            maximum_notional=spec.maximum_order_notional,
            step=market.rules.buy_step,
        ),
    )


def _notional_quantity_limit(
    *,
    market: MarketState,
    maximum_notional: Decimal,
    step: int,
) -> int:
    raw = (maximum_notional / market.bar.pre_close).to_integral_value(rounding=ROUND_FLOOR)
    return int(raw) // step * step


def fundamental_ranking_payload(
    observations: tuple[FundamentalFeatureObservation, ...],
) -> dict[str, object]:
    """Expose deterministic scores only for tests and audit diagnostics."""

    ranked = _ranked_instruments(observations)
    return {
        "eligible_count": len(observations),
        "ranked_instruments": list(ranked),
        "ranking_version": "cross-sectional-percentile-equal-factor-v1",
        "score_evidence_hash": _canonical_hash(
            {
                "observation_hashes": [value.observation_hash for value in observations],
                "ranked_instruments": list(ranked),
            }
        ),
        "weight": _decimal_text(Decimal("0.20")),
    }
