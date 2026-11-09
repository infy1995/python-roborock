"""Tests for the health manager."""

import datetime
from unittest.mock import AsyncMock, patch

from roborock.mqtt.health_manager import DeviceHealthManager, HealthManager

DEVICE_ID = "device-1"


async def test_health_manager_restart_called_after_timeouts() -> None:
    """Test that the health manager calls restart after consecutive timeouts."""
    restart = AsyncMock()
    health_manager = DeviceHealthManager(HealthManager(restart=restart), DEVICE_ID)

    await health_manager.on_timeout()
    await health_manager.on_timeout()
    restart.assert_not_called()

    await health_manager.on_timeout()
    restart.assert_called_once()


async def test_health_manager_success_resets_counter() -> None:
    """Test that a successful message resets the timeout counter."""
    restart = AsyncMock()
    health_manager = DeviceHealthManager(HealthManager(restart=restart), DEVICE_ID)

    await health_manager.on_timeout()
    await health_manager.on_timeout()
    restart.assert_not_called()

    await health_manager.on_success()

    await health_manager.on_timeout()
    await health_manager.on_timeout()
    restart.assert_not_called()

    await health_manager.on_timeout()
    restart.assert_called_once()


async def test_cooldown() -> None:
    """Test that the health manager respects the restart cooldown."""
    restart = AsyncMock()
    health_manager = DeviceHealthManager(HealthManager(restart=restart), DEVICE_ID)

    with patch("roborock.mqtt.health_manager.datetime") as mock_datetime:
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = now

        # Trigger first restart
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        restart.assert_called_once()
        restart.reset_mock()

        # Advance time but stay within cooldown (30 mins)
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(minutes=10)

        # Trigger timeouts again
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        restart.assert_not_called()

        # Advance time past cooldown
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(minutes=31)

        # Trigger timeouts again
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        await health_manager.on_timeout()
        restart.assert_called_once()


async def test_timeouts_across_devices_restart_session() -> None:
    """Test that timeouts spread across devices still count towards the session."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)

    await DeviceHealthManager(health_manager, "device-1").on_timeout()
    await DeviceHealthManager(health_manager, "device-2").on_timeout()
    restart.assert_not_called()

    await DeviceHealthManager(health_manager, "device-3").on_timeout()
    restart.assert_called_once()


async def test_other_device_success_does_not_hide_failing_device() -> None:
    """Test that one device's timeouts restart the session while another device works."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    failing = DeviceHealthManager(health_manager, "failing")
    working = DeviceHealthManager(health_manager, "working")

    await failing.on_timeout()
    await failing.on_timeout()
    await working.on_success()
    restart.assert_not_called()

    await failing.on_timeout()
    restart.assert_called_once()


async def test_device_success_resets_only_its_own_counter() -> None:
    """Test that a device needs consecutive timeouts of its own to restart the session."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    device = DeviceHealthManager(health_manager, DEVICE_ID)
    other = DeviceHealthManager(health_manager, "other")

    await device.on_timeout()
    await device.on_timeout()
    await device.on_success()
    await other.on_success()

    await device.on_timeout()
    await device.on_timeout()
    restart.assert_not_called()


async def test_restart_clears_counters_of_all_devices() -> None:
    """Test that every device starts counting again after a session restart."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    first = DeviceHealthManager(health_manager, "first")
    second = DeviceHealthManager(health_manager, "second")
    working = DeviceHealthManager(health_manager, "working")

    with patch("roborock.mqtt.health_manager.datetime") as mock_datetime:
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = now

        await second.on_timeout()
        await second.on_timeout()
        await working.on_success()
        await first.on_timeout()
        await first.on_timeout()
        await working.on_success()
        await first.on_timeout()
        restart.assert_called_once()
        restart.reset_mock()

        # The restart gave "second" a new subscription, so its earlier timeouts no longer count.
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(minutes=31)
        await working.on_success()
        await second.on_timeout()
        restart.assert_not_called()


async def test_single_failing_device_has_longer_cooldown() -> None:
    """Test that a device that stays unreachable does not keep restarting a working session."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    failing = DeviceHealthManager(health_manager, "failing")
    working = DeviceHealthManager(health_manager, "working")

    async def time_out_while_session_works() -> None:
        for _ in range(3):
            await working.on_success()
            await failing.on_timeout()

    with patch("roborock.mqtt.health_manager.datetime") as mock_datetime:
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = now

        await time_out_while_session_works()
        restart.assert_called_once()
        restart.reset_mock()

        # Past the session cooldown, but this device already had its restart.
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(hours=1)
        await time_out_while_session_works()
        restart.assert_not_called()

        # Past the device cooldown the session is restarted again.
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(hours=6, minutes=1)
        await time_out_while_session_works()
        restart.assert_called_once()


async def test_device_success_clears_device_cooldown() -> None:
    """Test that a device that recovered can restart the session again if it fails later."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    failing = DeviceHealthManager(health_manager, "failing")
    working = DeviceHealthManager(health_manager, "working")

    async def time_out_while_session_works() -> None:
        for _ in range(3):
            await working.on_success()
            await failing.on_timeout()

    with patch("roborock.mqtt.health_manager.datetime") as mock_datetime:
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = now

        await time_out_while_session_works()
        restart.assert_called_once()
        restart.reset_mock()

        await failing.on_success()

        # Only the session cooldown applies now.
        mock_datetime.datetime.now.return_value = now + datetime.timedelta(minutes=31)
        await time_out_while_session_works()
        restart.assert_called_once()


async def test_device_diagnostics_are_separate_per_device() -> None:
    """Test that diagnostics keep each device's history apart and survive counter resets."""
    restart = AsyncMock()
    health_manager = HealthManager(restart=restart)
    failing = DeviceHealthManager(health_manager, "failing")
    working = DeviceHealthManager(health_manager, "working")

    with patch("roborock.mqtt.health_manager.datetime") as mock_datetime:
        now = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        mock_datetime.datetime.now.return_value = now
        await failing.on_success()

        mock_datetime.datetime.now.return_value = now + datetime.timedelta(hours=1)
        for _ in range(4):
            await working.on_success()
            await failing.on_timeout()

    last_timeout = "2023-01-01T13:00:00+00:00"
    assert failing.diagnostic_data() == {
        "timeouts": 4,
        "successes": 1,
        "consecutive_timeouts": 1,
        "restarts_triggered": 1,
        "last_success": "2023-01-01T12:00:00+00:00",
        "last_timeout": last_timeout,
        "session_restarts": 1,
        "session_last_restart": last_timeout,
    }
    assert working.diagnostic_data() == {
        "timeouts": 0,
        "successes": 4,
        "consecutive_timeouts": 0,
        "restarts_triggered": 0,
        "last_success": last_timeout,
        "last_timeout": None,
        "session_restarts": 1,
        "session_last_restart": last_timeout,
    }


async def test_diagnostics_for_device_without_requests() -> None:
    """Test the diagnostics of a device that has not used MQTT yet."""
    device = DeviceHealthManager(HealthManager(restart=AsyncMock()), DEVICE_ID)

    assert device.diagnostic_data() == {
        "timeouts": 0,
        "successes": 0,
        "consecutive_timeouts": 0,
        "restarts_triggered": 0,
        "last_success": None,
        "last_timeout": None,
        "session_restarts": 0,
        "session_last_restart": None,
    }
