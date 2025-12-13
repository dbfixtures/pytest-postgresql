"""Small retry callable in case of specific error occurred."""

import asyncio
import datetime
import sys
from time import sleep
from typing import Awaitable, Callable, Type, TypeVar

T = TypeVar("T")


def retry(
    func: Callable[[], T],
    timeout: int = 60,
    possible_exception: Type[Exception] = Exception,
) -> T:
    """Attempt to retry the function for timeout time.

    Most often used for connecting to postgresql database as,
    especially on macos on github-actions, first few tries fails
    with this message:

    ... ::
        FATAL:  the database system is starting up
    """
    time: datetime.datetime = get_current_datetime()
    timeout_diff: datetime.timedelta = datetime.timedelta(seconds=timeout)
    i = 0
    while True:
        i += 1
        try:
            res = func()
            return res
        except possible_exception as e:
            if time + timeout_diff < get_current_datetime():
                raise TimeoutError(f"Failed after {i} attempts") from e
            sleep(1)


async def retry_async(
    func: Callable[[], Awaitable[T]],
    timeout: int = 60,
    possible_exception: Type[Exception] = Exception,
) -> T:
    """Attempt to retry the async function for timeout time.

    Most often used for connecting to postgresql database as,
    especially on macos on github-actions, first few tries fails
    with this message:

    ... ::
        FATAL:  the database system is starting up
    """
    time: datetime.datetime = get_current_datetime()
    timeout_diff: datetime.timedelta = datetime.timedelta(seconds=timeout)
    i = 0
    while True:
        i += 1
        try:
            res = await func()
            return res
        except possible_exception as e:
            if time + timeout_diff < get_current_datetime():
                raise TimeoutError(f"Failed after {i} attempts") from e
            await asyncio.sleep(1)


def get_current_datetime() -> datetime.datetime:
    """Get the current datetime."""
    # To ensure the current datetime retrieval is adjusted with the latest
    # versions of Python while ensuring retro-compatibility with
    # Python 3.8, 3.9 and 3.10, we check what version of Python is
    # being used before deciding how to operate
    if sys.version_info.major == 3 and sys.version_info.minor > 10:
        return datetime.datetime.now(datetime.UTC)

    return datetime.datetime.utcnow()
