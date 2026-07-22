# AutoQuant Walk-Forward Validation Design

## Objective

Add a reproducible out-of-sample gate between a working backtest and any paper-trading
candidate. The gate measures a fixed SMA-cross research baseline; it does not claim the
strategy is suitable or profitable and cannot unlock live execution.

## Leakage controls

- A signal for session `D` uses closes through `D-1` only and submits a pre-open order for
  `D`.
- Each rolling fold selects parameters only inside its training window.
- At least one trading session separates training and test windows. The embargo is visible
  in the persisted request and bounded to 1-20 sessions.
- Test windows do not overlap. A completed earlier test may enter a later rolling training
  window, matching sequential walk-forward operation.
- The parameter grid is explicit, unique, and limited to 25 server-validated SMA pairs. No
  uploaded code or expression evaluation exists.
- A manifest with a changing adjustment factor remains ineligible until exact corporate-
  action position and cash accounting is implemented.

## Execution and benchmark

Training, test, and benchmark runs use the same manifest cutoff, point-in-time suspension
and exact price-limit records, board lots, T+1, fee history, slippage, liquidity limits, and
daily-open fill model. Every test fold also runs a same-period buy-and-hold benchmark. The
experiment reports both absolute and excess out-of-sample return.

## Persistence and integrity

PostgreSQL schema v5 stores a persistent experiment queue and fold metadata. Each selected
training and test result is stored as a complete JSON artifact containing executions,
snapshots, positions, events, semantic result hash, and artifact hash. Schema v6 adds the
equivalent benchmark artifact. Completion of all folds and the experiment state transition
is atomic. Reads reconstruct all domain objects and recompute event, artifact, fold, and
experiment hashes.

## Preliminary evidence assessment

The console applies a transparent pre-screen and persists stable failure codes. A research
candidate needs at least six folds, 120 out-of-sample sessions, positive absolute and excess
return, at least half of folds profitable, and no test-fold drawdown above 15%. Passing this
pre-screen still does not authorize paper or live trading; benchmark breadth, multiple-
testing controls, corporate actions, portfolio validation, and statistical confidence remain
separate gates.
