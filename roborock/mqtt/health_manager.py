"""A health manager for monitoring MQTT connections to Roborock devices.

We observe a problem where sometimes the MQTT connection appears to be alive but
no messages are being received. To mitigate this, we track consecutive timeouts
and restart the connection if too many timeouts occur in succession.

The broker can also stop delivering messages for a single device's topic while
the rest of the session keeps working, so timeouts are tracked both for the
session as a whole and for each device.
"""

import datetime
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from roborock.diagnostics import redact_device_uid

_LOGGER = logging.getLogger(__name__)

# Number of consecutive timeouts before considering the connection unhealthy.
TIMEOUT_THRESHOLD = 3

# We won't restart the session more often than this interval.
RESTART_COOLDOWN = datetime.timedelta(minutes=30)

# A device that fails while the rest of the session works may simply be offline,
# which a restart can't fix. Until it responds again, that device won't restart
# the session more often than this interval.
DEVICE_RESTART_COOLDOWN = datetime.timedelta(hours=6)


@dataclass
class _DeviceHealth:
    """The MQTT request history of a single device."""

    consecutive_timeouts: int = 0
    restart_cooldown_start: datetime.datetime | None = None
    timeouts: int = 0
    successes: int = 0
    restarts_triggered: int = 0
    last_success: datetime.datetime | None = None
    last_timeout: datetime.datetime | None = None


def _isoformat(value: datetime.datetime | None) -> str | None:
    """Format an optional timestamp for diagnostics."""
    return value.isoformat() if value else None


class HealthManager:
    """Manager for monitoring the health of MQTT connections.

    This tracks communication timeouts and can trigger restarts of the MQTT
    session if too many timeouts occur in succession, either across the whole
    session or for a single device.
    """

    def __init__(self, restart: Callable[[], Awaitable[None]]) -> None:
        """Initialize the health manager.

        Args:
            restart: A callable to restart the MQTT session.
        """
        self._consecutive_timeouts = 0
        self._timed_out_devices: set[str] = set()
        self._devices: dict[str, _DeviceHealth] = {}
        self._restart = restart
        self._last_restart: datetime.datetime | None = None
        self._restarts = 0

    def _device(self, device_id: str) -> _DeviceHealth:
        """Return the history of a device, creating it on first use."""
        return self._devices.setdefault(device_id, _DeviceHealth())

    async def on_success(self, device_id: str) -> None:
        """Record a successful communication event for a device."""
        self._consecutive_timeouts = 0
        self._timed_out_devices.clear()
        device = self._device(device_id)
        device.consecutive_timeouts = 0
        device.restart_cooldown_start = None
        device.successes += 1
        device.last_success = datetime.datetime.now(datetime.UTC)

    async def on_timeout(self, device_id: str) -> None:
        """Record a timeout event for a device.

        This may trigger a restart of the MQTT session if too many timeouts
        have occurred in succession.
        """
        now = datetime.datetime.now(datetime.UTC)
        self._consecutive_timeouts += 1
        self._timed_out_devices.add(device_id)
        device = self._device(device_id)
        device.consecutive_timeouts += 1
        device.timeouts += 1
        device.last_timeout = now

        # Timeouts from one device only implicate the session if it is the only device we know about.
        session_unhealthy = self._consecutive_timeouts >= TIMEOUT_THRESHOLD and (
            len(self._timed_out_devices) > 1 or len(self._devices) == 1
        )
        if not session_unhealthy and device.consecutive_timeouts < TIMEOUT_THRESHOLD:
            return

        since_last = (now - self._last_restart) if self._last_restart else None
        if since_last is not None and since_last < RESTART_COOLDOWN:
            return

        if not session_unhealthy:
            cooldown_start = device.restart_cooldown_start
            if cooldown_start is not None and now - cooldown_start < DEVICE_RESTART_COOLDOWN:
                return
            device.restart_cooldown_start = now

        _LOGGER.debug(
            "Restarting MQTT session after %d consecutive timeouts, %d for device %s (duration since last restart %s)",
            self._consecutive_timeouts,
            device.consecutive_timeouts,
            redact_device_uid(device_id),
            since_last,
        )
        await self._restart()
        self._last_restart = now
        self._restarts += 1
        device.restarts_triggered += 1
        self._consecutive_timeouts = 0
        self._timed_out_devices.clear()
        for tracked_device in self._devices.values():
            tracked_device.consecutive_timeouts = 0

    def device_diagnostic_data(self, device_id: str) -> dict[str, Any]:
        """Return diagnostics about a device's MQTT requests and the session restarts."""
        device = self._devices.get(device_id, _DeviceHealth())
        return {
            "timeouts": device.timeouts,
            "successes": device.successes,
            "consecutive_timeouts": device.consecutive_timeouts,
            "restarts_triggered": device.restarts_triggered,
            "last_success": _isoformat(device.last_success),
            "last_timeout": _isoformat(device.last_timeout),
            "session_restarts": self._restarts,
            "session_last_restart": _isoformat(self._last_restart),
        }


class DeviceHealthManager:
    """Reports the outcome of a single device's MQTT requests to the session's health manager."""

    def __init__(self, health_manager: HealthManager, device_id: str) -> None:
        """Initialize the device health manager."""
        self._health_manager = health_manager
        self._device_id = device_id

    async def on_success(self) -> None:
        """Record a successful communication event."""
        await self._health_manager.on_success(self._device_id)

    async def on_timeout(self) -> None:
        """Record a timeout event."""
        await self._health_manager.on_timeout(self._device_id)

    def diagnostic_data(self) -> dict[str, Any]:
        """Return diagnostics about this device's MQTT requests."""
        return self._health_manager.device_diagnostic_data(self._device_id)
