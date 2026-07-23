from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from autoquant.backtest.validation import SmaParameters
from autoquant.web.validation_campaign_store import (
    ValidationCampaignSpec,
)

INSTRUMENTS = (
    "000001.XSHE",
    "600000.XSHG",
    "600519.XSHG",
)


def _spec() -> ValidationCampaignSpec:
    return ValidationCampaignSpec(
        campaign_key="portfolio-research-20260723-0001",
        manifest_hash="a" * 64,
        instruments=INSTRUMENTS,
        initial_cash=Decimal("1000000"),
        allocation=Decimal("0.20"),
        slippage_bps=Decimal("5"),
        train_sessions=120,
        test_sessions=20,
        embargo_sessions=1,
        candidates=(
            SmaParameters(5, 20),
            SmaParameters(10, 30),
            SmaParameters(20, 60),
        ),
        requested_by="operator",
    )


def test_campaign_spec_is_canonical_and_builds_aligned_requests() -> None:
    spec = _spec()

    assert ValidationCampaignSpec.from_payload(spec.payload()) == spec
    assert len(spec.campaign_hash) == 64
    requests = tuple(
        spec.request_for(instrument)
        for instrument in spec.instruments
    )
    assert {
        value.manifest_hash for value in requests
    } == {spec.manifest_hash}
    assert {
        value.initial_cash for value in requests
    } == {spec.initial_cash}
    assert {
        (
            value.train_sessions,
            value.test_sessions,
            value.embargo_sessions,
        )
        for value in requests
    } == {(120, 20, 1)}
    assert len(
        {value.idempotency_key for value in requests}
    ) == len(requests)


def test_campaign_hash_changes_with_any_research_control() -> None:
    spec = _spec()

    changed = replace(
        spec,
        slippage_bps=Decimal("6"),
    )

    assert changed.campaign_hash != spec.campaign_hash


def test_campaign_rejects_concentrated_or_small_universe() -> None:
    spec = _spec()

    with pytest.raises(ValueError, match="3-20"):
        replace(spec, instruments=INSTRUMENTS[:2])
    with pytest.raises(ValueError, match="gross allocation"):
        replace(spec, allocation=Decimal("0.40"))
