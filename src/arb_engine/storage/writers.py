"""Data writers for storing market data."""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from arb_engine.core.logging import get_logger
from arb_engine.domain.models import ArbSignal, FundingRate, OrderbookL5, PriceTick
from arb_engine.storage.duckdb import DuckDBConnection

logger = get_logger(__name__)


class DataWriter:
    """
    Write market data to DuckDB and optionally to Parquet.

    Handles conversion from domain models to database records.
    """

    def __init__(self, db: DuckDBConnection, parquet_path: Optional[Path] = None):
        """
        Initialize data writer.

        Args:
            db: DuckDB connection
            parquet_path: Optional path for Parquet exports
        """
        self.db = db
        self.parquet_path = parquet_path
        if parquet_path:
            parquet_path.mkdir(parents=True, exist_ok=True)

    def write_price_tick(self, tick: PriceTick) -> None:
        """Write single price tick."""
        self.write_price_ticks([tick])

    def write_price_ticks(self, ticks: List[PriceTick]) -> None:
        """
        Write multiple price ticks to database.

        Args:
            ticks: List of price ticks
        """
        if not ticks:
            return

        records = []
        for tick in ticks:
            records.append(
                {
                    "id": None,  # Auto-increment
                    "exchange": tick.exchange,
                    "symbol": tick.symbol,
                    "price": float(tick.price),
                    "volume_24h": float(tick.volume_24h) if tick.volume_24h else None,
                    "exchange_timestamp": tick.exchange_timestamp,
                    "received_timestamp": tick.received_timestamp,
                }
            )

        try:
            self.db.insert_batch("price_ticks", records)
            logger.debug("price_ticks_written", count=len(records))
        except Exception as e:
            logger.error("price_ticks_write_failed", error=str(e))
            raise

    def write_orderbook(self, orderbook: OrderbookL5) -> None:
        """Write single orderbook."""
        self.write_orderbooks([orderbook])

    def write_orderbooks(self, orderbooks: List[OrderbookL5]) -> None:
        """
        Write multiple orderbooks to database.

        Args:
            orderbooks: List of orderbooks
        """
        if not orderbooks:
            return

        records = []
        for ob in orderbooks:
            # Convert bids/asks to JSON
            bids_json = json.dumps([{"price": float(lvl.price), "size": float(lvl.size)} for lvl in ob.bids])
            asks_json = json.dumps([{"price": float(lvl.price), "size": float(lvl.size)} for lvl in ob.asks])

            records.append(
                {
                    "id": None,  # Auto-increment
                    "exchange": ob.exchange,
                    "symbol": ob.symbol,
                    "bids": bids_json,
                    "asks": asks_json,
                    "best_bid": float(ob.best_bid) if ob.best_bid else None,
                    "best_ask": float(ob.best_ask) if ob.best_ask else None,
                    "mid_price": float(ob.mid_price) if ob.mid_price else None,
                    "spread": float(ob.spread) if ob.spread else None,
                    "spread_bps": float(ob.spread_bps) if ob.spread_bps else None,
                    "exchange_timestamp": ob.exchange_timestamp,
                    "received_timestamp": ob.received_timestamp,
                }
            )

        try:
            self.db.insert_batch("orderbooks", records)
            logger.debug("orderbooks_written", count=len(records))
        except Exception as e:
            logger.error("orderbooks_write_failed", error=str(e))
            raise

    def write_funding_rate(self, funding: FundingRate) -> None:
        """Write single funding rate."""
        self.write_funding_rates([funding])

    def write_funding_rates(self, fundings: List[FundingRate]) -> None:
        """
        Write multiple funding rates to database.

        Args:
            fundings: List of funding rates
        """
        if not fundings:
            return

        records = []
        for funding in fundings:
            records.append(
                {
                    "id": None,  # Auto-increment
                    "exchange": funding.exchange,
                    "symbol": funding.symbol,
                    "funding_rate": float(funding.funding_rate),
                    "next_funding_time": funding.next_funding_time,
                    "predicted_rate": float(funding.predicted_rate) if funding.predicted_rate else None,
                    "exchange_timestamp": funding.exchange_timestamp,
                    "received_timestamp": funding.received_timestamp,
                }
            )

        try:
            self.db.insert_batch("funding_rates", records)
            logger.debug("funding_rates_written", count=len(records))
        except Exception as e:
            logger.error("funding_rates_write_failed", error=str(e))
            raise

    def write_arb_signal(self, signal: ArbSignal) -> None:
        """Write single arbitrage signal."""
        self.write_arb_signals([signal])

    def write_arb_signals(self, signals: List[ArbSignal]) -> None:
        """
        Write multiple arbitrage signals to database.

        Args:
            signals: List of arbitrage signals
        """
        if not signals:
            return

        records = []
        for signal in signals:
            records.append(
                {
                    "signal_id": signal.signal_id,
                    "timestamp": signal.timestamp,
                    "symbol": signal.symbol,
                    "buy_exchange": signal.buy_exchange,
                    "sell_exchange": signal.sell_exchange,
                    "buy_price": float(signal.buy_price),
                    "sell_price": float(signal.sell_price),
                    "gross_spread": float(signal.gross_spread),
                    "gross_spread_bps": float(signal.gross_spread_bps),
                    "net_spread": float(signal.net_spread),
                    "net_spread_bps": float(signal.net_spread_bps),
                    "estimated_size": float(signal.estimated_size),
                    "estimated_slippage": float(signal.estimated_slippage),
                    "buy_fee": float(signal.buy_fee),
                    "sell_fee": float(signal.sell_fee),
                    "funding_cost": float(signal.funding_cost) if signal.funding_cost else None,
                    "status": signal.status.value,
                    "converged_at": signal.converged_at,
                    "convergence_time_seconds": signal.convergence_time_seconds,
                    "final_spread_bps": float(signal.final_spread_bps) if signal.final_spread_bps else None,
                }
            )

        try:
            # Use replace for signals since they can be updated
            self.db.insert_batch("arb_signals", records, replace=True)
            logger.debug("arb_signals_written", count=len(records))
        except Exception as e:
            logger.error("arb_signals_write_failed", error=str(e))
            raise

    def export_daily_to_parquet(self, date: str) -> None:
        """
        Export a day's worth of data to Parquet files.

        Args:
            date: Date in YYYY-MM-DD format
        """
        if not self.parquet_path:
            logger.warning("parquet_export_skipped", reason="no_parquet_path")
            return

        try:
            # Export each table
            for table in ["price_ticks", "orderbooks", "funding_rates", "arb_signals"]:
                query = f"SELECT * FROM {table} WHERE date = '{date}'"
                output_file = self.parquet_path / f"{table}_{date}.parquet"
                self.db.export_to_parquet(query, output_file)

            logger.info("daily_parquet_export_complete", date=date)
        except Exception as e:
            logger.error("daily_parquet_export_failed", date=date, error=str(e))
            raise
