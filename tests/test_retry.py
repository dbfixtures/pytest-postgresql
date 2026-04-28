"""Unit tests for retry and retry_async."""

import datetime
from unittest.mock import AsyncMock, patch

import pytest

from pytest_postgresql.retry import retry_async


@pytest.mark.asyncio
async def test_retry_async_immediate_success() -> None:
    """Test that retry_async returns immediately when function succeeds on first call."""

    async def ok() -> int:
        return 42

    assert await retry_async(ok, timeout=5) == 42


@pytest.mark.asyncio
async def test_retry_async_succeeds_after_failures() -> None:
    """Test that retry_async retries on the expected exception and returns on success."""
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("transient")
        return "ok"

    sleep_mock = AsyncMock()
    with patch("pytest_postgresql.retry.asyncio.sleep", sleep_mock):
        result = await retry_async(flaky, timeout=10, possible_exception=ConnectionError)

    assert result == "ok"
    assert attempts == 3
    assert sleep_mock.call_count == 2


@pytest.mark.asyncio
async def test_retry_async_timeout() -> None:
    """Test that retry_async raises TimeoutError after the timeout elapses."""

    async def always_fail() -> None:
        raise ValueError("boom")

    sleep_mock = AsyncMock()
    base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    call_count = 0

    def advancing_clock() -> datetime.datetime:
        nonlocal call_count
        call_count += 1
        # First call captures starting time; all subsequent calls report past the timeout.
        return base if call_count == 1 else base + datetime.timedelta(seconds=10)

    with (
        patch("pytest_postgresql.retry.asyncio.sleep", sleep_mock),
        patch("pytest_postgresql.retry.get_current_datetime", advancing_clock),
    ):
        with pytest.raises(TimeoutError, match="Failed after"):
            await retry_async(always_fail, timeout=1, possible_exception=ValueError)


@pytest.mark.asyncio
async def test_retry_async_unmatched_exception_propagates() -> None:
    """Test that an exception not matching possible_exception propagates immediately."""

    async def wrong_exc() -> None:
        raise TypeError("unexpected")

    with pytest.raises(TypeError, match="unexpected"):
        await retry_async(wrong_exc, timeout=5, possible_exception=ValueError)
