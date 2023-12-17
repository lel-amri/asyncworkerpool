import asyncio
import queue as python_queue
from typing import TypeVar, Generic, Any, Type, Optional, Tuple, Set, Dict, Union, Coroutine, Iterable
from typing_extensions import Protocol, Final


_Tco = TypeVar("_Tco", covariant=True)
_CoroReturn_cont = TypeVar("_CoroReturn_cont", contravariant=True)
_CoroReturn = TypeVar("_CoroReturn")


class Queue(Protocol, Generic[_Tco]):
    async def get(self) -> _Tco: ...

    def get_nowait(self) -> _Tco: ...

    def task_done(self) -> Any: ...


class AsyncWorkerPoolDoneCallback(Protocol, Generic[_CoroReturn_cont]):
    def __call__(self, coro: Coroutine[Any, Any, _CoroReturn_cont], ret: Any, *, exc: Optional[Exception]) -> Any: ...


class Error(Exception):
    pass


class AlreadyRunning(Error):
    pass


class NotRunning(Error):
    pass


class AsyncWorkerPool(Generic[_CoroReturn], object):
    def __init__(
            self,
            size: int,
            queue: Queue[Coroutine[Any, Any, _CoroReturn]],
            done_callback: AsyncWorkerPoolDoneCallback[_CoroReturn],
            *,
            loop: Optional[asyncio.AbstractEventLoop] = None,
            queue_empty_exception: Optional[Union[Type[Exception], Iterable[Type[Exception]]]] = None,
        ) -> None:
        if size <= 0:
            raise ValueError("Size must be greater than zero")
        self._size: int = size
        self._queue: Final[Queue[Coroutine[Any, Any, _CoroReturn]]] = queue
        self._done_callback: Final[AsyncWorkerPoolDoneCallback[_CoroReturn]] = done_callback
        self._loop: Final[asyncio.AbstractEventLoop]
        self._loop = asyncio.get_running_loop() if loop is None else loop
        self._queue_empty_exception: Final[Tuple[Type[Exception], ...]]
        if queue_empty_exception is None:
            self._queue_empty_exception = (asyncio.QueueEmpty, python_queue.Empty)
        elif isinstance(queue_empty_exception, Iterable):
            self._queue_empty_exception = tuple(queue_empty_exception)
        else:
            self._queue_empty_exception = (queue_empty_exception, )
        self._pending_coroutines: Final[Set[Tuple[Coroutine[Any, Any, _CoroReturn], asyncio.Task[_CoroReturn]]]] = set()
        self._stop_event: Final[asyncio.Event] = asyncio.Event()
        self._worker_task: Optional[asyncio.Task[Any]] = None

    @property
    def size(self) -> int:
        return self._size

    def set_size(self, size: int) -> None:
        if size <= 0:
            raise ValueError("Size must be greater than zero")
        self._size = size

    async def _worker(self) -> None:
        task_to_coroutine: Final[Dict[asyncio.Task[_CoroReturn], Coroutine[Any, Any, _CoroReturn]]] = {task: coro for coro, task in self._pending_coroutines}
        self._pending_coroutines.clear()
        pending_set: Set[asyncio.Task[_CoroReturn]] = set(task_to_coroutine.keys())
        candidates: Final[Set[Coroutine[Any, Any, _CoroReturn]]] = set()
        while not self._stop_event.is_set():
            assert len(candidates) == 0, "The candidates set should be empty"
            # If the pending set is empty, do a blocking get on the queue.
            if len(pending_set) == 0:
                queue_get_task = asyncio.create_task(self._queue.get())
                stop_event_wait_task = asyncio.create_task(self._stop_event.wait())
                try:
                    done, pending = await asyncio.wait({queue_get_task, stop_event_wait_task}, return_when=asyncio.FIRST_COMPLETED)
                except asyncio.CancelledError:
                    assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"
                    self._pending_coroutines.update(((task_to_coroutine[e], e) for e in pending_set))
                    raise
                for task in pending:
                    task.cancel()
                if queue_get_task in done:
                    candidates.add(queue_get_task.result())
                if stop_event_wait_task in done:
                    break
            candidates_needed = self._size - len(pending_set) - len(candidates)
            # Try to fill the pending set with tasks that are on the queue
            for _ in range(candidates_needed, 0, -1):
                try:
                    coroutine = self._queue.get_nowait()
                except self._queue_empty_exception:
                    break
                candidates.add(coroutine)
            # Schedule tasks
            for coroutine in candidates:
                task = asyncio.create_task(coroutine)
                task_to_coroutine[task] = coroutine
                pending_set.add(task)
            candidates.clear()
            # Wait for at least one tasks to complete
            try:
                done_set, pending_set = await asyncio.wait(
                    pending_set,
                    return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"
                self._pending_coroutines.update(((task_to_coroutine[e], e) for e in pending_set))
                raise
            # Call the done callback for each complete task
            for task_done in done_set:
                coroutine = task_to_coroutine.pop(task_done)
                self._queue.task_done()
                if task_done.cancelled():
                    # TODO: Raise a warning here (tasks shouldn't be cancelled)
                    continue
                exc = task_done.exception()
                if isinstance(exc, BaseException) and not isinstance(exc, Exception):
                    raise
                try:
                    if exc is None:
                        self._done_callback(coroutine, task_done.result(), exc=None)
                    else:
                        self._done_callback(coroutine, None, exc=exc)
                except:
                    pass
        if len(pending_set) > 0:
            # Wait for the remaining tasks to complete
            try:
                done_set, pending_set = await asyncio.wait(
                    pending_set,
                    return_when=asyncio.ALL_COMPLETED
                )
            except asyncio.CancelledError:
                assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"
                self._pending_coroutines.update(((task_to_coroutine[e], e) for e in pending_set))
                raise
            assert len(pending_set) == 0, "The pending tasks set should be empty"
            # Call the done callback for each complete task
            for task_done in done_set:
                coroutine = task_to_coroutine.pop(task_done)
                self._queue.task_done()
                if task_done.cancelled():
                    # TODO: Raise a warning here (tasks shouldn't be cancelled)
                    continue
                exc = task_done.exception()
                if isinstance(exc, BaseException) and not isinstance(exc, Exception):
                    raise
                try:
                    if exc is None:
                        self._done_callback(coroutine, task_done.result(), exc=None)
                    else:
                        self._done_callback(coroutine, None, exc=exc)
                except:
                    pass
        assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"

    def start(self) -> None:
        if self._worker_task is not None:
            raise AlreadyRunning()
        self._stop_event.clear()
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self, *, graceful_timeout: Optional[float] = None) -> Set[Tuple[Coroutine[Any, Any, _CoroReturn], "asyncio.Task[_CoroReturn]"]]:
        if self._worker_task is None:
            raise NotRunning()
        self._stop_event.set()
        await asyncio.wait_for(asyncio.shield(self._worker_task), graceful_timeout)
        if not self._worker_task.done():
            self._worker_task.cancel()
            await self._worker_task
        return set(self._pending_coroutines)
