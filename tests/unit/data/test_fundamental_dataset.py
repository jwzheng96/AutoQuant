from datetime import date

import pytest

from autoquant.data.fundamental_dataset import (
    FundamentalDatasetShard,
    FundamentalResearchDatasetManifest,
)


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
