"""A health manager for monitoring MQTT connections to Roborock devices.

We observe a problem where sometimes the MQTT connection appears to be alive but
no messages are being received. To mitigate this, we track consecutive timeouts
and restart the connection if too many timeouts occur in succession.

The broker can also stop delivering messages for a single device's topic while
the rest of the session keeps working, so timeouts are tracked both for the
session as a whole and for each device. Subscribing to the device's topic again
on the same connection does not bring the messages back; only a new connection
does, so the session is restarted.
"""

import datetime
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from roborock.diagnostics import redact_device_uid

_LOGGER = logging.getLogger(__name__)

# Number of consecutive timeouts before considering the connection unhealthy.
TIMEOUT_THRESHOLD = 3

# We won't restart the session more often than this interval.
RESTART_COOLDOWN = datetime.timedelta(minutes=30)

# A device that fails while the rest of the session works may simply be offline,
# which a restart can't fix. Until it responds again, the interval between the
# session restarts it triggers doubles after each restart, up to this maximum.
MAX_DEVICE_RESTART_COOLDOWN = datetime.timedelta(hours=6)

# Number of health changes kept per device for diagnostics.
MAX_DEVICE_EVENTS = 20


@dataclass
class _DeviceHealth:
    """The MQTT request history of a single device."""

    consecutive_timeouts: int = 0
    failing_since: datetime.datetime | None = None
    restart_cooldown_start: datetime.datetime | None = None
    restart_cooldown: datetime.timedelta | None = None
    timeouts: int = 0
    successes: int = 0
    last_success: datetime.datetime | None = None
    last_timeout: datetime.datetime | None = None
    events: deque[dict[str, str]] = field(default_factory=lambda: deque(maxlen=MAX_DEVICE_EVENTS))

    def add_event(self, time: datetime.datetime, event: str, **details: str) -> None:
        """Record a change of the device's health for diagnostics."""
        self.events.append({"time": time.isoformat(), "event": event, **details})


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

    def _device(self, device_id: str) -> _DeviceHealth:
        """Return the history of a device, creating it on first use."""
        return self._devices.setdefault(device_id, _DeviceHealth())

    async def on_success(self, device_id: str) -> None:
        """Record a successful communication event for a device."""
        self._consecutive_timeouts = 0
        self._timed_out_devices.clear()
        device = self._device(device_id)
        now = datetime.datetime.now(datetime.UTC)
        if device.failing_since is not None:
            _LOGGER.info(
                "Device %s responds over MQTT again after timing out since %s",
                redact_device_uid(device_id),
                device.failing_since.isoformat(timespec="seconds"),
            )
            device.add_event(now, "recovered")
        device.consecutive_timeouts = 0
        device.failing_since = None
        device.restart_cooldown_start = None
        device.restart_cooldown = None
        device.successes += 1
        device.last_success = now

    async def on_timeout(self, device_id: str) -> None:
        """Record a timeout event for a device.

        This may trigger a restart of the MQTT session if too many timeouts
        have occurred in succession.
        """
        now = datetime.datetime.now(datetime.UTC)
        self._consecutive_timeouts += 1
        self._timed_out_devices.add(device_id)
        device = self._device(device_id)
        device_name = redact_device_uid(device_id)
        if device.failing_since is None:
            device.failing_since = now
            _LOGGER.info("Device %s started timing out over MQTT", device_name)
            device.add_event(now, "timeouts_started")
        device.consecutive_timeouts += 1
        device.timeouts += 1
        device.last_timeout = now

        # Only timeouts from several devices implicate the session itself. A single failing device
        # may just be offline, even when it is the only device reporting to this session.
        session_unhealthy = self._consecutive_timeouts >= TIMEOUT_THRESHOLD and len(self._timed_out_devices) > 1
        if not session_unhealthy and device.consecutive_timeouts < TIMEOUT_THRESHOLD:
            return

        since_last = (now - self._last_restart) if self._last_restart else None
        if since_last is not None and since_last < RESTART_COOLDOWN:
            _LOGGER.debug("Not restarting MQTT session for device %s, last restart was %s ago", device_name, since_last)
            return

        reason = f"{self._consecutive_timeouts} timeouts in a row across {len(self._timed_out_devices)} devices"
        if not session_unhealthy:
            cooldown_start, cooldown = device.restart_cooldown_start, device.restart_cooldown
            if cooldown_start is not None and cooldown is not None and now - cooldown_start < cooldown:
                _LOGGER.debug(
                    "Not restarting MQTT session for device %s, it is still failing and may restart it again after %s",
                    device_name,
                    (cooldown_start + cooldown).isoformat(timespec="seconds"),
                )
                return
            device.restart_cooldown_start = now
            device.restart_cooldown = (
                min(cooldown * 2, MAX_DEVICE_RESTART_COOLDOWN) if cooldown is not None else RESTART_COOLDOWN
            )
            reason = (
                f"device {device_name} timed out {device.consecutive_timeouts} times in a row; "
                f"it may restart the session again in {device.restart_cooldown} if it keeps failing"
            )

        _LOGGER.info(
            "Restarting MQTT session: %s (last restart: %s)",
            reason,
            f"{since_last} ago" if since_last is not None else "never",
        )
        # Update the state before awaiting, so timeouts arriving meanwhile don't restart again.
        self._last_restart = now
        # The restart affects every device, so each one records it along with what caused it.
        several_devices = len(self._timed_out_devices) > 1
        for tracked_id, tracked_device in self._devices.items():
            if several_devices:
                cause = "several_devices"
            else:
                cause = "this_device" if tracked_id == device_id else "other_device"
            tracked_device.add_event(now, "session_restart", cause=cause)
            tracked_device.consecutive_timeouts = 0
        self._consecutive_timeouts = 0
        self._timed_out_devices.clear()
        await self._restart()

    def device_diagnostic_data(self, device_id: str) -> dict[str, Any]:
        """Return diagnostics about a device's MQTT requests."""
        device = self._devices.get(device_id, _DeviceHealth())
        return {
            "failing_since": _isoformat(device.failing_since),
            "last_success": _isoformat(device.last_success),
            "last_timeout": _isoformat(device.last_timeout),
            "successes": device.successes,
            "timeouts": device.timeouts,
            # Newest first, so the current state of the device is at the top.
            "events": [dict(event) for event in reversed(device.events)],
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
