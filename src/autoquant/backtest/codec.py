from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal

from autoquant.backtest.models import (
    AccountSnapshot,
    BacktestResult,
    ExecutionReport,
    ExecutionState,
    FeeBreakdown,
    LedgerEvent,
    OrderSide,
    PositionSnapshot,
    RejectionCode,
    backtest_artifact_hash,
)


def encode_backtest_result(result: BacktestResult) -> dict[str, object]:
    return {
        "artifact_hash": backtest_artifact_hash(result),
        "as_of": result.as_of.isoformat(timespec="microseconds"),
        "ending_equity": str(result.ending_equity),
        "events": [
            {
                "client_order_id": event.client_order_id,
                "event_hash": event.event_hash,
                "event_type": event.event_type,
                "payload": dict(event.payload),
                "previous_hash": event.previous_hash,
                "sequence": event.sequence,
                "session_date": event.session_date.isoformat(),
            }
            for event in result.events
        ],
        "execution_version": result.execution_version,
        "fee_version": result.fee_version,
        "initial_cash": str(result.initial_cash),
        "ledger_hash": result.ledger_hash,
        "manifest_hash": result.manifest_hash,
        "max_drawdown": str(result.max_drawdown),
        "reports": [
            {
                "client_order_id": report.client_order_id,
                "commission": str(report.fees.commission),
                "fill_price": (
                    None if report.fill_price is None else str(report.fill_price)
                ),
                "filled_quantity": report.filled_quantity,
                "gross_amount": str(report.gross_amount),
                "instrument": report.instrument,
                "ledger_hash": report.ledger_hash,
                "rejection_code": (
                    None
                    if report.rejection_code is None
                    else report.rejection_code.value
                ),
                "requested_quantity": report.requested_quantity,
                "session_date": report.session_date.isoformat(),
                "side": report.side.value,
                "stamp_duty": str(report.fees.stamp_duty),
                "state": report.state.value,
                "transfer_fee": str(report.fees.transfer_fee),
            }
            for report in result.reports
        ],
        "result_hash": result.result_hash,
        "rule_versions": list(result.rule_versions),
        "snapshots": [
            {
                "cash": str(snapshot.cash),
                "equity": str(snapshot.equity),
                "ledger_hash": snapshot.ledger_hash,
                "market_value": str(snapshot.market_value),
                "positions": [
                    {
                        "average_cost": str(position.average_cost),
                        "instrument": position.instrument,
                        "market_price": str(position.market_price),
                        "market_value": str(position.market_value),
                        "sellable_quantity": position.sellable_quantity,
                        "total_quantity": position.total_quantity,
                        "unrealized_pnl": str(position.unrealized_pnl),
                    }
                    for position in snapshot.positions
                ],
                "session_date": snapshot.session_date.isoformat(),
            }
            for snapshot in result.snapshots
        ],
        "strategy_id": result.strategy_id,
        "total_fees": str(result.total_fees),
        "total_return": str(result.total_return),
        "turnover": str(result.turnover),
    }


def decode_backtest_result(value: object) -> BacktestResult:
    payload = _mapping(value, name="backtest result")
    reports_raw = _sequence(payload.get("reports"), name="reports")
    snapshots_raw = _sequence(payload.get("snapshots"), name="snapshots")
    events_raw = _sequence(payload.get("events"), name="events")
    reports = tuple(_decode_report(item) for item in reports_raw)
    snapshots = tuple(_decode_snapshot(item) for item in snapshots_raw)
    events = tuple(_decode_event(item) for item in events_raw)
    result = BacktestResult(
        strategy_id=str(payload["strategy_id"]),
        manifest_hash=str(payload["manifest_hash"]),
        as_of=datetime.fromisoformat(str(payload["as_of"])),
        initial_cash=Decimal(str(payload["initial_cash"])),
        ending_equity=Decimal(str(payload["ending_equity"])),
        total_return=Decimal(str(payload["total_return"])),
        max_drawdown=Decimal(str(payload["max_drawdown"])),
        turnover=Decimal(str(payload["turnover"])),
        total_fees=Decimal(str(payload["total_fees"])),
        reports=reports,
        snapshots=snapshots,
        events=events,
        rule_versions=tuple(
            str(item)
            for item in _sequence(payload.get("rule_versions"), name="rule_versions")
        ),
        fee_version=str(payload["fee_version"]),
        execution_version=str(payload["execution_version"]),
        ledger_hash=str(payload["ledger_hash"]),
    )
    if result.result_hash != str(payload["result_hash"]):
        raise ValueError("backtest result hash mismatch")
    if backtest_artifact_hash(result) != str(payload["artifact_hash"]):
        raise ValueError("backtest artifact hash mismatch")
    return result


def _decode_report(value: object) -> ExecutionReport:
    item = _mapping(value, name="report")
    rejection = item.get("rejection_code")
    fill_price = item.get("fill_price")
    return ExecutionReport(
        client_order_id=str(item["client_order_id"]),
        instrument=str(item["instrument"]),
        side=OrderSide(str(item["side"])),
        requested_quantity=int(str(item["requested_quantity"])),
        state=ExecutionState(str(item["state"])),
        session_date=date.fromisoformat(str(item["session_date"])),
        filled_quantity=int(str(item["filled_quantity"])),
        fill_price=None if fill_price is None else Decimal(str(fill_price)),
        gross_amount=Decimal(str(item["gross_amount"])),
        fees=FeeBreakdown(
            commission=Decimal(str(item["commission"])),
            stamp_duty=Decimal(str(item["stamp_duty"])),
            transfer_fee=Decimal(str(item["transfer_fee"])),
        ),
        rejection_code=(None if rejection is None else RejectionCode(str(rejection))),
        ledger_hash=str(item["ledger_hash"]),
    )


def _decode_snapshot(value: object) -> AccountSnapshot:
    item = _mapping(value, name="snapshot")
    raw_positions = _sequence(item.get("positions"), name="positions")
    positions = tuple(_decode_position(position) for position in raw_positions)
    return AccountSnapshot(
        session_date=date.fromisoformat(str(item["session_date"])),
        cash=Decimal(str(item["cash"])),
        market_value=Decimal(str(item["market_value"])),
        equity=Decimal(str(item["equity"])),
        positions=positions,
        ledger_hash=str(item["ledger_hash"]),
    )


def _decode_position(value: object) -> PositionSnapshot:
    item = _mapping(value, name="position")
    return PositionSnapshot(
        instrument=str(item["instrument"]),
        total_quantity=int(str(item["total_quantity"])),
        sellable_quantity=int(str(item["sellable_quantity"])),
        average_cost=Decimal(str(item["average_cost"])),
        market_price=Decimal(str(item["market_price"])),
        market_value=Decimal(str(item["market_value"])),
        unrealized_pnl=Decimal(str(item["unrealized_pnl"])),
    )


def _decode_event(value: object) -> LedgerEvent:
    item = _mapping(value, name="event")
    raw_payload = _mapping(item.get("payload"), name="event payload")
    event = LedgerEvent(
        sequence=int(str(item["sequence"])),
        event_type=str(item["event_type"]),
        session_date=date.fromisoformat(str(item["session_date"])),
        client_order_id=str(item["client_order_id"]),
        payload=tuple(sorted((str(key), str(value)) for key, value in raw_payload.items())),
        previous_hash=str(item["previous_hash"]),
    )
    if event.event_hash != str(item["event_hash"]):
        raise ValueError("backtest event hash mismatch")
    return event


def _mapping(value: object, *, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return value


def _sequence(value: object, *, name: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{name} must be a sequence")
    return tuple(value)
