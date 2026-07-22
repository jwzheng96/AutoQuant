from __future__ import annotations

import os
from datetime import UTC, date, datetime

import pytest

from autoquant.adapters.tushare import TushareDailySource, TushareHttpClient
from autoquant.config import AppSettings

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AQ_RUN_TUSHARE_LIVE") != "1",
        reason="set AQ_RUN_TUSHARE_LIVE=1 for an explicit read-only Tushare check",
    ),
]


@pytest.mark.asyncio
async def test_real_tushare_can_build_one_daily_dataset() -> None:
    settings = AppSettings()
    source = TushareDailySource(
        client=TushareHttpClient(
            credentials=settings.require_tushare(),
            api_url=settings.tushare_api_url,
        ),
        now=lambda: datetime.now(UTC),
    )
    try:
        dataset = await source.fetch_daily_dataset(
            ("000001.XSHE",), date(2020, 1, 2), date(2020, 1, 2)
        )
    finally:
        await source.close()

    assert dataset.bars and dataset.bars[0].instrument == "000001.XSHE"
    assert dataset.factors and dataset.factors[0].instrument == "000001.XSHE"
    assert dataset.coverage.sessions
    assert dataset.coverage.lifecycles
    assert dataset.coverage.suspensions
