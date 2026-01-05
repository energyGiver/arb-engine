"""Time synchronization and clock utilities."""

import time
from datetime import datetime, timezone
from typing import Optional

from arb_engine.core.logging import get_logger

logger = get_logger(__name__)


class Clock:
    """
    Clock utility for time synchronization.

    In production, this could integrate with NTP for precise time sync.
    For MVP, we use system time with UTC normalization.
    """

    def __init__(self, ntp_server: Optional[str] = None):
        """
        Initialize clock.

        Args:
            ntp_server: NTP server address (not used in MVP, reserved for future)
        """
        self.ntp_server = ntp_server
        self._offset_seconds = 0.0  # Time offset from system clock

    def now(self) -> datetime:
        """
        Get current UTC time.

        Returns:
            Current datetime in UTC
        """
        return datetime.now(timezone.utc)

    def now_timestamp(self) -> float:
        """
        Get current Unix timestamp in seconds.

        Returns:
            Current Unix timestamp
        """
        return time.time() + self._offset_seconds

    def now_ms(self) -> int:
        """
        Get current Unix timestamp in milliseconds.

        Returns:
            Current Unix timestamp in milliseconds
        """
        return int(self.now_timestamp() * 1000)

    def from_timestamp(self, ts: float) -> datetime:
        """
        Convert Unix timestamp to datetime.

        Args:
            ts: Unix timestamp in seconds

        Returns:
            Datetime in UTC
        """
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    def from_timestamp_ms(self, ts_ms: int) -> datetime:
        """
        Convert Unix timestamp in milliseconds to datetime.

        Args:
            ts_ms: Unix timestamp in milliseconds

        Returns:
            Datetime in UTC
        """
        return self.from_timestamp(ts_ms / 1000.0)

    def is_stale(self, timestamp: datetime, max_age_seconds: float = 2.0) -> bool:
        """
        Check if timestamp is stale (too old).

        Args:
            timestamp: Timestamp to check
            max_age_seconds: Maximum age in seconds before considering stale

        Returns:
            True if timestamp is older than max_age_seconds
        """
        age = (self.now() - timestamp).total_seconds()
        return age > max_age_seconds

    def seconds_since(self, timestamp: datetime) -> float:
        """
        Calculate seconds elapsed since timestamp.

        Args:
            timestamp: Past timestamp

        Returns:
            Seconds elapsed
        """
        return (self.now() - timestamp).total_seconds()


# Global clock instance
_clock_instance: Optional[Clock] = None


def get_clock(ntp_server: Optional[str] = None) -> Clock:
    """
    Get or create global clock instance.

    Args:
        ntp_server: NTP server address (optional)

    Returns:
        Global clock instance
    """
    global _clock_instance
    if _clock_instance is None:
        _clock_instance = Clock(ntp_server=ntp_server)
        logger.info("clock_initialized", ntp_server=ntp_server)
    return _clock_instance
