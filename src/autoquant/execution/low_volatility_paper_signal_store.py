from __future__ import annotations

import json
import re
from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from autoquant.data.models import _require_lowercase_sha256
from autoquant.errors import PersistenceUnavailableError
from autoquant.execution.low_volatility_paper_signal import (
    LowVolatilityPaperDailySignal,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PostgresLowVolatilityPaperSignalRepository:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        schema: str = "public",
    ) -> None:
        if _IDENTIFIER.fullmatch(schema) is None:
            raise ValueError("schema must be a safe PostgreSQL identifier")
        self._engine = engine
        self._schema = schema

    @classmethod
    def connect(
        cls,
        *,
        dsn: str,
        schema: str = "public",
    ) -> PostgresLowVolatilityPaperSignalRepository:
        if not dsn.strip():
            raise ValueError("dsn cannot be empty")
        try:
            engine = create_async_engine(
                dsn,
                pool_pre_ping=True,
            )
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper signal connection failed"
            ) from None
        return cls(engine=engine, schema=schema)

    async def close(self) -> None:
        await self._engine.dispose()

    async def save(
        self,
        signal: LowVolatilityPaperDailySignal,
    ) -> LowVolatilityPaperDailySignal:
        if not isinstance(
            signal,
            LowVolatilityPaperDailySignal,
        ):
            raise TypeError("signal must be low-volatility paper daily evidence")
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                    {
                        "identity": (
                            f"low-volatility-paper-signals:{signal.candidate_approval_hash}"
                        )
                    },
                )
                latest = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT signal_hash, session_sequence,
                                       session_date
                                FROM {self._schema}.
                                    low_volatility_paper_daily_signals
                                WHERE candidate_approval_hash =
                                    :candidate_approval_hash
                                ORDER BY session_sequence DESC
                                LIMIT 1
                                """
                            ),
                            {"candidate_approval_hash": (signal.candidate_approval_hash)},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                idempotent = False
                if latest is not None:
                    if (
                        latest["session_date"] == signal.session_date
                        and str(latest["signal_hash"]) == signal.signal_hash
                    ):
                        idempotent = True
                    elif (
                        int(latest["session_sequence"]) + 1 != signal.session_sequence
                        or latest["session_date"] >= signal.session_date
                        or str(latest["signal_hash"]) != signal.previous_signal_hash
                    ):
                        raise ValueError("paper daily signal chain is inconsistent")
                elif signal.session_sequence != 1:
                    raise ValueError("paper daily signal chain must start at one")
                if not idempotent:
                    await connection.execute(
                        text(
                            f"""
                        INSERT INTO {self._schema}.
                            low_volatility_paper_daily_signals
                            (signal_hash, candidate_approval_hash,
                             account_id, strategy_id,
                             source_spec_hash, risk_policy_hash,
                             session_sequence, session_date,
                             signal_date, window_start_date,
                             previous_signal_hash, snapshot_hash,
                             dataset_manifest_hash, rule_set_hash,
                             universe_member_count,
                             evidence_instrument_count,
                             observation_count, selected_count,
                             rebalance_due, prepared_by, prepared_at,
                             decision_policy,
                             execution_timing_compatible,
                             runtime_activation_allowed,
                             live_trading_locked,
                             signal_version, payload)
                        VALUES
                            (:signal_hash, :candidate_approval_hash,
                             :account_id, :strategy_id,
                             :source_spec_hash, :risk_policy_hash,
                             :session_sequence, :session_date,
                             :signal_date, :window_start_date,
                             :previous_signal_hash, :snapshot_hash,
                             :dataset_manifest_hash, :rule_set_hash,
                             :universe_member_count,
                             :evidence_instrument_count,
                             :observation_count, :selected_count,
                             :rebalance_due, :prepared_by, :prepared_at,
                             :decision_policy, false, false, true,
                             :signal_version, CAST(:payload AS jsonb))
                        ON CONFLICT (signal_hash) DO NOTHING
                        """
                        ),
                        _parameters(signal),
                    )
        except (TypeError, ValueError):
            raise
        except Exception:
            raise PersistenceUnavailableError(
                "low-volatility paper signal persistence failed"
            ) from None
        stored = await self.read(signal.signal_hash)
        if stored != signal:
            raise ValueError("a different low-volatility paper signal is stored")
        return stored

    async def read(
        self,
        signal_hash: str,
    ) -> LowVolatilityPaperDailySignal:
        _require_lowercase_sha256(
            signal_hash,
            name="low-volatility paper signal hash",
        )
        try:
            async with self._engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                SELECT *
                                FROM {self._schema}.
                                    low_volatility_paper_daily_signals
                                WHERE signal_hash = :signal_hash
                                """
                            ),
                            {"signal_hash": signal_hash},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise LookupError("low-volatility paper signal does not exist")
            return _signal(row)
        except (LookupError, PersistenceUnavailableError):
            raise
        except Exception:
            raise PersistenceUnavailableError("low-volatility paper signal lookup failed") from None

    async def latest(
        self,
        *,
        candidate_approval_hash: str,
        before: date | None = None,
    ) -> LowVolatilityPaperDailySignal | None:
        _require_lowercase_sha256(
            candidate_approval_hash,
            name="low-volatility paper approval hash",
        )
        try:
            async with self._engine.connect() as connection:
                value = (
                    await connection.execute(
                        text(
                            f"""
                            SELECT signal_hash
                            FROM {self._schema}.
                                low_volatility_paper_daily_signals
                            WHERE candidate_approval_hash =
                                :candidate_approval_hash
                              AND (
                                  CAST(:before AS date) IS NULL
                                  OR session_date < CAST(:before AS date)
                              )
                            ORDER BY session_sequence DESC
                            LIMIT 1
                            """
                        ),
                        {
                            "candidate_approval_hash": (candidate_approval_hash),
                            "before": before,
                        },
                    )
                ).scalar_one_or_none()
        except Exception:
            raise PersistenceUnavailableError("low-volatility paper signal lookup failed") from None
        return None if value is None else await self.read(str(value))

    async def for_session(
        self,
        *,
        candidate_approval_hash: str,
        session_date: date,
    ) -> LowVolatilityPaperDailySignal | None:
        _require_lowercase_sha256(
            candidate_approval_hash,
            name="low-volatility paper approval hash",
        )
        try:
            async with self._engine.connect() as connection:
                value = (
                    await connection.execute(
                        text(
                            f"""
                            SELECT signal_hash
                            FROM {self._schema}.
                                low_volatility_paper_daily_signals
                            WHERE candidate_approval_hash =
                                :candidate_approval_hash
                              AND session_date = :session_date
                            """
                        ),
                        {
                            "candidate_approval_hash": (candidate_approval_hash),
                            "session_date": session_date,
                        },
                    )
                ).scalar_one_or_none()
        except Exception:
            raise PersistenceUnavailableError("low-volatility paper signal lookup failed") from None
        return None if value is None else await self.read(str(value))


def _parameters(
    signal: LowVolatilityPaperDailySignal,
) -> dict[str, object]:
    return {
        **signal.payload(),
        "evidence_instrument_count": len(signal.evidence_instruments),
        "observation_count": len(signal.observations),
        "payload": json.dumps(
            signal.payload(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "prepared_at": signal.prepared_at,
        "selected_count": len(signal.selected_instruments),
        "signal_hash": signal.signal_hash,
        "signal_version": signal.version,
        "universe_member_count": len(signal.universe_members),
    }


def _signal(row: RowMapping) -> LowVolatilityPaperDailySignal:
    try:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise TypeError("paper signal payload is not an object")
        signal = LowVolatilityPaperDailySignal.from_payload(
            {str(key): value for key, value in payload.items()}
        )
        if (
            signal.signal_hash != str(row["signal_hash"])
            or signal.candidate_approval_hash != str(row["candidate_approval_hash"])
            or signal.account_id != str(row["account_id"])
            or signal.strategy_id != str(row["strategy_id"])
            or signal.source_spec_hash != str(row["source_spec_hash"])
            or signal.risk_policy_hash != str(row["risk_policy_hash"])
            or signal.session_sequence != int(row["session_sequence"])
            or signal.session_date != row["session_date"]
            or signal.signal_date != row["signal_date"]
            or signal.window_start_date != row["window_start_date"]
            or signal.previous_signal_hash != str(row["previous_signal_hash"])
            or signal.snapshot_hash != str(row["snapshot_hash"])
            or signal.dataset_manifest_hash != str(row["dataset_manifest_hash"])
            or signal.rule_set_hash != str(row["rule_set_hash"])
            or len(signal.universe_members) != int(row["universe_member_count"])
            or len(signal.evidence_instruments) != int(row["evidence_instrument_count"])
            or len(signal.observations) != int(row["observation_count"])
            or len(signal.selected_instruments) != int(row["selected_count"])
            or signal.rebalance_due is not row["rebalance_due"]
            or signal.prepared_by != str(row["prepared_by"])
            or signal.prepared_at != row["prepared_at"]
            or signal.decision_policy != str(row["decision_policy"])
            or row["execution_timing_compatible"] is not False
            or row["runtime_activation_allowed"] is not False
            or row["live_trading_locked"] is not True
            or signal.version != str(row["signal_version"])
        ):
            raise ValueError("stored low-volatility paper signal mismatch")
        return signal
    except (KeyError, TypeError, ValueError):
        raise PersistenceUnavailableError(
            "stored low-volatility paper signal failed integrity"
        ) from None
