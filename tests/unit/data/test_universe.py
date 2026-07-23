from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from autoquant.data.universe import (
    DailyLiquidityMetric,
    IndexConstituent,
    PointInTimeUniversePolicy,
    build_point_in_time_universe,
)

AS_OF = datetime(2026, 7, 23, tzinfo=UTC)


def _constituent(index: int, trade_date: date) -> IndexConstituent:
    return IndexConstituent(
        source="tushare",
        index_code="399300.SZ",
        instrument=f"{index:06d}.XSHE",
        trade_date=trade_date,
        weight=Decimal("1"),
        available_at=AS_OF,
        response_hash="a" * 64,
    )


def _liquidity(index: int, trade_date: date) -> DailyLiquidityMetric:
    return DailyLiquidityMetric(
        source="tushare",
        instrument=f"{index:06d}.XSHE",
        trade_date=trade_date,
        turnover_rate_f=Decimal("1"),
        volume_ratio=Decimal("1.2"),
        circulating_market_value=Decimal("100000"),
        available_at=AS_OF,
        response_hash="b" * 64,
    )


def test_point_in_time_universe_uses_latest_cross_sections_at_cutoff() -> None:
    policy = PointInTimeUniversePolicy(
        index_code="399300.SZ",
        minimum_members=20,
        maximum_members=30,
    )
    old = date(2026, 6, 3)
    current = date(2026, 7, 1)
    future = date(2026, 8, 1)
    constituents = tuple(
        [
            *(_constituent(index, old) for index in range(20)),
            *(
                _constituent(index, current)
                for index in range(20, 40)
            ),
            *(
                _constituent(index, future)
                for index in range(40, 60)
            ),
        ]
    )
    liquidity = tuple(
        _liquidity(index, date(2026, 7, 22))
        for index in range(20, 40)
    )

    snapshot = build_point_in_time_universe(
        policy=policy,
        reference_date=date(2026, 7, 22),
        constituents=constituents,
        liquidity=liquidity,
    )
    repeated = build_point_in_time_universe(
        policy=policy,
        reference_date=date(2026, 7, 22),
        constituents=tuple(reversed(constituents)),
        liquidity=tuple(reversed(liquidity)),
    )

    assert snapshot.index_constituent_date == current
    assert snapshot.liquidity_date == date(2026, 7, 22)
    assert snapshot.snapshot_hash == repeated.snapshot_hash
    assert len(snapshot.members) == 20
    assert snapshot.members[0].instrument == "000020.XSHE"


def test_point_in_time_universe_fails_on_incomplete_liquidity() -> None:
    policy = PointInTimeUniversePolicy(
        index_code="399300.SZ",
        minimum_members=20,
    )

    with pytest.raises(ValueError, match="membership"):
        build_point_in_time_universe(
            policy=policy,
            reference_date=date(2026, 7, 22),
            constituents=tuple(
                _constituent(index, date(2026, 7, 1))
                for index in range(20)
            ),
            liquidity=tuple(
                _liquidity(index, date(2026, 7, 22))
                for index in range(19)
            ),
        )
