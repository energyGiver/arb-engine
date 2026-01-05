"""Database schema definitions for DuckDB tables."""

from typing import List

# DuckDB table schemas using SQL DDL
# These will be used to create tables in the database


PRICE_TICKS_TABLE = """
CREATE TABLE IF NOT EXISTS price_ticks (
    id INTEGER PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    volume_24h DECIMAL(18, 8),
    exchange_timestamp TIMESTAMP NOT NULL,
    received_timestamp TIMESTAMP NOT NULL,
    date DATE GENERATED ALWAYS AS (CAST(received_timestamp AS DATE)) VIRTUAL
);

CREATE INDEX IF NOT EXISTS idx_price_ticks_date
    ON price_ticks(date, exchange, symbol);
CREATE INDEX IF NOT EXISTS idx_price_ticks_timestamp
    ON price_ticks(received_timestamp);
"""


ORDERBOOKS_TABLE = """
CREATE TABLE IF NOT EXISTS orderbooks (
    id INTEGER PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    -- Bids: stored as JSON array for L5
    bids JSON NOT NULL,
    -- Asks: stored as JSON array for L5
    asks JSON NOT NULL,
    best_bid DECIMAL(18, 8),
    best_ask DECIMAL(18, 8),
    mid_price DECIMAL(18, 8),
    spread DECIMAL(18, 8),
    spread_bps DECIMAL(10, 4),
    exchange_timestamp TIMESTAMP NOT NULL,
    received_timestamp TIMESTAMP NOT NULL,
    date DATE GENERATED ALWAYS AS (CAST(received_timestamp AS DATE)) VIRTUAL
);

CREATE INDEX IF NOT EXISTS idx_orderbooks_date
    ON orderbooks(date, exchange, symbol);
CREATE INDEX IF NOT EXISTS idx_orderbooks_timestamp
    ON orderbooks(received_timestamp);
"""


FUNDING_RATES_TABLE = """
CREATE TABLE IF NOT EXISTS funding_rates (
    id INTEGER PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    funding_rate DECIMAL(10, 8) NOT NULL,
    next_funding_time TIMESTAMP,
    predicted_rate DECIMAL(10, 8),
    exchange_timestamp TIMESTAMP NOT NULL,
    received_timestamp TIMESTAMP NOT NULL,
    date DATE GENERATED ALWAYS AS (CAST(received_timestamp AS DATE)) VIRTUAL
);

CREATE INDEX IF NOT EXISTS idx_funding_rates_date
    ON funding_rates(date, exchange, symbol);
CREATE INDEX IF NOT EXISTS idx_funding_rates_timestamp
    ON funding_rates(received_timestamp);
"""


ARB_SIGNALS_TABLE = """
CREATE TABLE IF NOT EXISTS arb_signals (
    signal_id VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    buy_exchange VARCHAR NOT NULL,
    sell_exchange VARCHAR NOT NULL,
    buy_price DECIMAL(18, 8) NOT NULL,
    sell_price DECIMAL(18, 8) NOT NULL,
    gross_spread DECIMAL(18, 8) NOT NULL,
    gross_spread_bps DECIMAL(10, 4) NOT NULL,
    net_spread DECIMAL(18, 8) NOT NULL,
    net_spread_bps DECIMAL(10, 4) NOT NULL,
    estimated_size DECIMAL(18, 8) NOT NULL,
    estimated_slippage DECIMAL(18, 8) NOT NULL,
    buy_fee DECIMAL(18, 8) NOT NULL,
    sell_fee DECIMAL(18, 8) NOT NULL,
    funding_cost DECIMAL(18, 8),
    status VARCHAR NOT NULL,
    converged_at TIMESTAMP,
    convergence_time_seconds DOUBLE,
    final_spread_bps DECIMAL(10, 4),
    date DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE)) VIRTUAL
);

CREATE INDEX IF NOT EXISTS idx_arb_signals_date
    ON arb_signals(date, symbol);
CREATE INDEX IF NOT EXISTS idx_arb_signals_status
    ON arb_signals(status, timestamp);
CREATE INDEX IF NOT EXISTS idx_arb_signals_timestamp
    ON arb_signals(timestamp);
"""


# Sequence for auto-increment IDs
SEQUENCES = """
CREATE SEQUENCE IF NOT EXISTS price_ticks_seq START 1;
CREATE SEQUENCE IF NOT EXISTS orderbooks_seq START 1;
CREATE SEQUENCE IF NOT EXISTS funding_rates_seq START 1;
"""


def get_all_table_schemas() -> List[str]:
    """
    Get all table creation DDL statements.

    Returns:
        List of DDL statements
    """
    return [
        SEQUENCES,
        PRICE_TICKS_TABLE,
        ORDERBOOKS_TABLE,
        FUNDING_RATES_TABLE,
        ARB_SIGNALS_TABLE,
    ]


# Parquet partitioning strategy
PARQUET_PARTITION_CONFIG = {
    "price_ticks": {
        "partition_by": ["date"],
        "filename_pattern": "price_ticks_{date}.parquet",
    },
    "orderbooks": {
        "partition_by": ["date"],
        "filename_pattern": "orderbooks_{date}.parquet",
    },
    "funding_rates": {
        "partition_by": ["date"],
        "filename_pattern": "funding_rates_{date}.parquet",
    },
    "arb_signals": {
        "partition_by": ["date"],
        "filename_pattern": "arb_signals_{date}.parquet",
    },
}
