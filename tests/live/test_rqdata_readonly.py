import os
from datetime import UTC, datetime, timedelta

import pytest

from autoquant.adapters.rqdata import RqdataHttpSource
from autoquant.config import AppSettings
from autoquant.data.availability import HistoricalMinutePolicy


@pytest.mark.live
@pytest.mark.asyncio
async def test_real_rqdata_can_read_one_known_minute() -> None:
    if os.getenv("AQ_RUN_RQDATA_LIVE") != "1":
        pytest.skip("set AQ_RUN_RQDATA_LIVE=1 to perform a real read-only API call")
    settings = AppSettings()
    source = RqdataHttpSource(
        credentials=settings.require_rqdata(),
        auth_url=settings.rqdata_auth_url,
        api_url=settings.rqdata_api_url,
        availability=HistoricalMinutePolicy("rqdata-minute-v1", timedelta(seconds=5)),
    )
    try:
        bars = await source.fetch_minute_bars(
            ("000001.XSHE",),
            datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        )
        evidence = await source.fetch_coverage_evidence(
            ("000001.XSHE",),
            datetime(2026, 7, 20, 1, 30, tzinfo=UTC),
            datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        )
        assert bars.records and all(bar.source == "rqdata" for bar in bars.records)
        assert evidence.coverage.periods and evidence.coverage.suspensions
    finally:
        await source.close()
