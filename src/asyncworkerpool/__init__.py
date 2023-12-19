import asyncio
from typing import TypeVar, Generic, Any, Optional, Tuple, Set, Dict, Coroutine
from typing_extensions import Protocol, Final


_Tco = TypeVar("_Tco", covariant=True)
_CoroReturn_cont = TypeVar("_CoroReturn_cont", contravariant=True)
_CoroReturn = TypeVar("_CoroReturn")


class Queue(Protocol, Generic[_Tco]):
    async def get(self) -> _Tco: ...

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
        ) -> None:
        if size <= 0:
            raise ValueError("Size must be greater than zero")
        self._size: int = size
        self._queue: Final[Queue[Coroutine[Any, Any, _CoroReturn]]] = queue
        self._done_callback: Final[AsyncWorkerPoolDoneCallback[_CoroReturn]] = done_callback
        self._loop: Final[asyncio.AbstractEventLoop]
        self._loop = asyncio.get_running_loop() if loop is None else loop
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
        user_tasks_set: Final[Set[asyncio.Task[_CoroReturn]]] = set(task_to_coroutine.keys())
        pending_set: Set[asyncio.Task[Any]] = set()
        queue_get_task: Optional[asyncio.Task[Coroutine[Any, Any, _CoroReturn]]] = None
        stop_event_wait_task: Optional[asyncio.Task[Any]] = None
        while not self._stop_event.is_set():
            # Schedule self._queue.get()
            if len(user_tasks_set) < self._size and queue_get_task is None:
                queue_get_task = asyncio.create_task(self._queue.get())
                pending_set.add(queue_get_task)
            assert (
                (len(user_tasks_set) < self._size and queue_get_task is not None)
                or (len(user_tasks_set) >= self._size and queue_get_task is None)
            )
            # Schedule self._stop_event.wait()
            if stop_event_wait_task is None:
                stop_event_wait_task = asyncio.create_task(self._stop_event.wait())
                pending_set.add(stop_event_wait_task)
            # Wait for at least one tasks to complete
            try:
                done_set, pending_set = await asyncio.wait(
                    pending_set,
                    return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"
                self._pending_coroutines.update(((task_to_coroutine[e], e) for e in user_tasks_set))
                if queue_get_task is not None:
                    queue_get_task.cancel()
                if stop_event_wait_task is not None:
                    stop_event_wait_task.cancel()
                if queue_get_task is not None:
                    await queue_get_task
                if stop_event_wait_task is not None:
                    await stop_event_wait_task
                raise
            # For each completed task
            for task_done in done_set:
                # If the task is self._stop_event.wait()
                # Acknowledge the stop event
                if task_done is stop_event_wait_task:
                    # This is a simple interrupt
                    stop_event_wait_task = None
                # Else, if the task is self._queue.get()
                elif task_done is queue_get_task:
                    coroutine = task_done.result()
                    task = asyncio.create_task(coroutine)
                    task_to_coroutine[task] = coroutine
                    user_tasks_set.add(task)
                    pending_set.add(task)
                    queue_get_task = None
                # Else, if this is a coroutine passed by the user
                # Call the done callback
                elif task_done in user_tasks_set:
                    user_tasks_set.remove(task_done)
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
                # DEAD CODE
                else:
                    assert False, "DEAD CODE"
        if queue_get_task is not None:
            queue_get_task.cancel()
        if stop_event_wait_task is not None:
            stop_event_wait_task.cancel()
        if len(user_tasks_set) > 0:
            # Wait for the remaining tasks to complete
            try:
                done_set, pending_set = await asyncio.wait(
                    user_tasks_set,
                    return_when=asyncio.ALL_COMPLETED
                )
            except asyncio.CancelledError:
                assert len(self._pending_coroutines) == 0, "The AsyncPool pending tasks set should be empty"
                self._pending_coroutines.update(((task_to_coroutine[e], e) for e in user_tasks_set))
                if queue_get_task is not None:
                    await queue_get_task
                    queue_get_task = None
                if stop_event_wait_task is not None:
                    await stop_event_wait_task
                    stop_event_wait_task = None
                raise
            assert len(user_tasks_set) == 0, "The pending tasks set should be empty"
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
        if queue_get_task is not None:
            await queue_get_task
            queue_get_task = None
        if stop_event_wait_task is not None:
            await stop_event_wait_task
            stop_event_wait_task = None
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
