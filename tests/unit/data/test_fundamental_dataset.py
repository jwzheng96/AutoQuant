from datetime import UTC, date, datetime

import pytest

from autoquant.data.fundamental_dataset import (
    FundamentalDatasetShard,
    FundamentalResearchDatasetManifest,
    ValidatedFundamentalDatasetReader,
)
from autoquant.data.fundamental_models import DailyValuationRevision
from autoquant.data.models import DatasetManifest
from autoquant.errors import ManifestIntegrityError


def manifest() -> FundamentalResearchDatasetManifest:
    return FundamentalResearchDatasetManifest(
        spec_hash="a" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
        shards=(
            FundamentalDatasetShard(
                1,
                "000001.XSHE",
                "b" * 64,
            ),
            FundamentalDatasetShard(
                2,
                "600000.XSHG",
                "c" * 64,
            ),
        ),
    )


def test_fundamental_dataset_manifest_round_trips() -> None:
    value = manifest()

    restored = FundamentalResearchDatasetManifest.from_payload(
        value.payload()
    )

    assert restored == value
    assert restored.manifest_hash == value.manifest_hash
    assert restored.instruments == (
        "000001.XSHE",
        "600000.XSHG",
    )


def test_fundamental_dataset_requires_sorted_contiguous_shards() -> None:
    with pytest.raises(ValueError, match="shards"):
        FundamentalResearchDatasetManifest(
            spec_hash="a" * 64,
            start_date=date(2020, 1, 1),
            end_date=date(2026, 7, 22),
            shards=(
                FundamentalDatasetShard(
                    1,
                    "600000.XSHG",
                    "b" * 64,
                ),
                FundamentalDatasetShard(
                    2,
                    "000001.XSHE",
                    "c" * 64,
                ),
            ),
        )


class _ManifestReader:
    def __init__(self, value: DatasetManifest) -> None:
        self.value = value

    async def read_manifest(self, _manifest_hash: str) -> DatasetManifest:
        return self.value


class _DataReader:
    def __init__(
        self,
        valuations: tuple[DailyValuationRevision, ...],
    ) -> None:
        self.valuations = valuations

    async def query_valuations_as_of(
        self,
        _instruments,
        _start,
        _end,
        _as_of,
    ):
        return self.valuations

    async def query_indicator_revisions_as_of(
        self,
        _instruments,
        _announced_start,
        _announced_end,
        _as_of,
    ):
        return ()


def _valuation() -> DailyValuationRevision:
    return DailyValuationRevision.from_values(
        source="tushare",
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        event_time=datetime(2026, 7, 20, 7, tzinfo=UTC),
        available_at=datetime(2026, 7, 21, 1, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 23, 8, tzinfo=UTC),
        source_revision="tushare:daily_basic:test",
        availability_policy="next-open-v1",
        evidence_hash="d" * 64,
        close_price="10",
        free_float_turnover_rate_percent="1",
        pe_ttm="10",
        pb="1",
        ps_ttm="2",
        dividend_yield_ttm_percent="3",
        total_market_value_cny="100",
        circulating_market_value_cny="90",
    )


@pytest.mark.asyncio
async def test_validated_reader_checks_exact_fundamental_hash_sequence() -> None:
    valuation = _valuation()
    shard_manifest = DatasetManifest(
        source="tushare-fundamental",
        instruments=(valuation.instrument,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=datetime(2026, 7, 23, 8, tzinfo=UTC),
        record_hashes=(valuation.content_hash,),
        quality_report_hash="quality",
        production_complete=True,
        row_count=1,
    )
    aggregate = FundamentalResearchDatasetManifest(
        spec_hash="a" * 64,
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
        shards=(
            FundamentalDatasetShard(
                1,
                valuation.instrument,
                shard_manifest.manifest_hash,
            ),
        ),
    )
    reader = ValidatedFundamentalDatasetReader(
        aggregate=aggregate,
        manifest_reader=_ManifestReader(shard_manifest),
        data_reader=_DataReader((valuation,)),
        retry_delay_seconds=0,
    )

    shard = await reader.query_instrument(valuation.instrument)

    assert shard.manifest == shard_manifest
    assert shard.valuations == (valuation,)

    corrupt = ValidatedFundamentalDatasetReader(
        aggregate=aggregate,
        manifest_reader=_ManifestReader(shard_manifest),
        data_reader=_DataReader(()),
        retry_delay_seconds=0,
    )
    with pytest.raises(ManifestIntegrityError, match="row-hash"):
        await corrupt.query_instrument(valuation.instrument)
