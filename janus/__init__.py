import asyncio
import sys
import threading
from asyncio import QueueEmpty as AsyncQueueEmpty
from asyncio import QueueFull as AsyncQueueFull
from collections import deque
from heapq import heappop, heappush
from queue import Empty as SyncQueueEmpty
from queue import Full as SyncQueueFull
from time import monotonic
from typing import Callable, Generic, Optional, Protocol, TypeVar

if sys.version_info >= (3, 13):
    from asyncio import QueueShutDown as AsyncQueueShutDown
    from queue import ShutDown as SyncQueueShutDown
else:
    class QueueShutDown(Exception):
        pass

    AsyncQueueShutDown = QueueShutDown

    class ShutDown(Exception):
        pass

    SyncQueueShutDown = ShutDown


__version__ = "2.0.0"
__all__ = (
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    "SyncQueue",
    "SyncQueueEmpty",
    "SyncQueueFull",
    "SyncQueueShutDown",
    "AsyncQueue",
    "AsyncQueueEmpty",
    "AsyncQueueFull",
    "AsyncQueueShutDown",
    "BaseQueue",
)


T = TypeVar("T")
OptFloat = Optional[float]


class BaseQueue(Protocol[T]):
    @property
    def maxsize(self) -> int: ...

    @property
    def closed(self) -> bool: ...

    def task_done(self) -> None: ...

    def qsize(self) -> int: ...

    @property
    def unfinished_tasks(self) -> int: ...

    def empty(self) -> bool: ...

    def full(self) -> bool: ...

    def put_nowait(self, item: T) -> None: ...

    def get_nowait(self) -> T: ...

    def shutdown(self, immediate: bool = False) -> None: ...


class SyncQueue(BaseQueue[T], Protocol[T]):

    def put(self, item: T, block: bool = True, timeout: OptFloat = None) -> None: ...

    def get(self, block: bool = True, timeout: OptFloat = None) -> T: ...

    def join(self) -> None: ...


class AsyncQueue(BaseQueue[T], Protocol[T]):
    async def put(self, item: T) -> None: ...

    async def get(self) -> T: ...

    async def join(self) -> None: ...


class Queue(Generic[T]):
    _loop: Optional[asyncio.AbstractEventLoop] = None

    def __init__(self, maxsize: int = 0) -> None:
        if sys.version_info < (3, 10):
            self._loop = asyncio.get_running_loop()

        self._maxsize = maxsize
        self._is_shutdown = False

        self._init(maxsize)

        self._unfinished_tasks = 0

        self._sync_mutex = threading.Lock()
        self._sync_not_empty = threading.Condition(self._sync_mutex)
        self._sync_not_empty_waiting = 0
        self._sync_not_full = threading.Condition(self._sync_mutex)
        self._sync_not_full_waiting = 0
        self._sync_tasks_done = threading.Condition(self._sync_mutex)
        self._sync_tasks_done_waiting = 0

        self._async_mutex = asyncio.Lock()
        if sys.version_info[:3] == (3, 10, 0):
            # Workaround for Python 3.10 bug, see #358:
            getattr(self._async_mutex, "_get_loop", lambda: None)()
        self._async_not_empty = asyncio.Condition(self._async_mutex)
        self._async_not_empty_waiting = 0
        self._async_not_full = asyncio.Condition(self._async_mutex)
        self._async_not_full_waiting = 0
        self._async_tasks_done = asyncio.Condition(self._async_mutex)
        self._async_tasks_done_waiting = 0

        self._pending: deque[asyncio.Future[None]] = deque()

        self._sync_queue = _SyncQueueProxy(self)
        self._async_queue = _AsyncQueueProxy(self)

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        # Warning!
        # The function should be called when self._sync_mutex is locked,
        # otherwise the code is not thread-safe
        pass

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        pass

    def close(self) -> None:
        """Close the queue.

        The method is a shortcut for .shutdown(immediate=True)
        """
        pass

    async def wait_closed(self) -> None:
        """Wait for finishing all pending activities"""
        pass

    async def aclose(self) -> None:
        """Shutdown the queue and wait for actual shutting down"""
        pass

    @property
    def closed(self) -> bool:
        pass

    @property
    def maxsize(self) -> int:
        pass

    @property
    def sync_q(self) -> "_SyncQueueProxy[T]":
        pass

    @property
    def async_q(self) -> "_AsyncQueueProxy[T]":
        pass

    # Override these methods to implement other queue organizations
    # (e.g. stack or priority queue).
    # These will only be called with appropriate locks held

    def _init(self, maxsize: int) -> None:
        pass

    def _qsize(self) -> int:
        pass

    # Put a new item in the queue
    def _put(self, item: T) -> None:
        pass

    # Get an item from the queue
    def _get(self) -> T:
        pass

    def _put_internal(self, item: T) -> None:
        pass

    async def _do_async_notifier(self, method: Callable[[], None]) -> None:
        pass

    def _setup_async_notifier(
        self, loop: asyncio.AbstractEventLoop, method: Callable[[], None]
    ) -> None:
        pass

    def _notify_async(self, method: Callable[[], None]) -> None:
        # Warning!
        # The function should be called when self._sync_mutex is locked,
        # otherwise the code is not thread-safe
        pass


class _SyncQueueProxy(SyncQueue[T]):
    """Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    """

    def __init__(self, parent: Queue[T]):
        self._parent = parent

    @property
    def maxsize(self) -> int:
        pass

    @property
    def closed(self) -> bool:
        pass

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items
        placed in the queue.
        """
        pass

    def join(self) -> None:
        """Blocks until all items in the Queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        pass

    def qsize(self) -> int:
        """Return the approximate size of the queue (not reliable!)."""
        pass

    @property
    def unfinished_tasks(self) -> int:
        """Return the number of unfinished tasks."""
        pass

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() == 0
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can grow before the result of empty() or
        qsize() can be used.

        To create code that needs to wait for all queued tasks to be
        completed, the preferred technique is to use the join() method.
        """
        pass

    def full(self) -> bool:
        """Return True if the queue is full, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() >= n
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can shrink before the result of full() or
        qsize() can be used.
        """
        pass

    def put(self, item: T, block: bool = True, timeout: OptFloat = None) -> None:
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        pass

    def get(self, block: bool = True, timeout: OptFloat = None) -> T:
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        pass

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        pass

    def get_nowait(self) -> T:
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        pass

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        pass


class _AsyncQueueProxy(AsyncQueue[T]):
    """Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    """

    def __init__(self, parent: Queue[T]):
        self._parent = parent

    @property
    def closed(self) -> bool:
        pass

    def qsize(self) -> int:
        """Number of items in the queue."""
        pass

    @property
    def unfinished_tasks(self) -> int:
        """Return the number of unfinished tasks."""
        pass

    @property
    def maxsize(self) -> int:
        """Number of items allowed in the queue."""
        pass

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        pass

    def full(self) -> bool:
        """Return True if there are maxsize items in the queue.

        Note: if the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        pass

    async def put(self, item: T) -> None:
        """Put an item into the queue.

        Put an item into the queue. If the queue is full, wait until a free
        slot is available before adding item.

        This method is a coroutine.
        """
        pass

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise QueueFull.
        """
        pass

    async def get(self) -> T:
        """Remove and return an item from the queue.

        If queue is empty, wait until an item is available.

        This method is a coroutine.
        """
        pass

    def get_nowait(self) -> T:
        """Remove and return an item from the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        pass

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises ValueError if called more times than there were items placed in
        the queue.
        """
        pass

    async def join(self) -> None:
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer calls task_done() to
        indicate that the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        pass

    def shutdown(self, immediate: bool = False) -> None:
        """Shut-down the queue, making queue gets and puts raise an exception.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().

        The raise exception is SyncQueueShutDown for sync api and AsyncQueueShutDown
        for async one.
        """
        pass


class PriorityQueue(Queue[T]):
    """Variant of Queue that retrieves open entries in priority order
    (lowest first).

    Entries are typically tuples of the form:  (priority number, data).

    """

    def _init(self, maxsize: int) -> None:
        pass

    def _qsize(self) -> int:
        pass

    def _put(self, item: T) -> None:
        pass

    def _get(self) -> T:
        pass


class LifoQueue(Queue[T]):
    """Variant of Queue that retrieves most recently added entries first."""

    def _qsize(self) -> int:
        pass

    def _put(self, item: T) -> None:
        pass

    def _get(self) -> T:
        pass
