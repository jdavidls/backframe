from __future__ import annotations
from inspect import iscoroutinefunction
from typing import Awaitable, Callable, Generic, Optional, TypeAlias, TypeVar
from anyio import create_task_group
from backframe.logging import get_logger

E = TypeVar("E")

EventHandler: TypeAlias = Callable[[E], Awaitable[None] | None]
AsyncEventHandler: TypeAlias = Callable[[E], Awaitable[None]]


class EventEmitter(Generic[E]):
    """Representa una fuente de eventos

    >>> from pydantic import BaseModel
    >>> class UserSigninEvent(BaseModel)
    >>> user_signin_event = EventEmitter()

    >>> user_signin_event.emit(UserSigninEvent())

    """

    _name: str
    _subscriptions: set[EventSubscription]

    def __init__(self, name: str):
        self._name = name
        self._log = get_logger(f"{name} event emitter")
        self._subscriptions = set()
        self._exception_count: int = 0

    def __repr__(self):
        return (
            f"EventEmitter({self._name}, "
            f"{len(self._subscriptions)} enabled subscriptions, "
            f"{len(self._exception_count)} exceptions counted)"
        )

    @property
    def name(self):
        return self._name

    @property
    def log(self):
        return self._log

    @property
    def subscriptions(self) -> list[EventSubscription]:
        return list(self._subscriptions)

    async def emit(self, event: E):
        """Emite un evento"""
        async with create_task_group() as tg:
            for sub in self.subscriptions:
                tg.start_soon(sub._event_task, event)

    __call__ = emit

    @property
    def exception_count(self):
        return self._exception_count

    def subscribe(
        self, handler: EventHandler, name: Optional[str] = None
    ) -> EventSubscription:
        subscription = EventSubscription(self, handler, name=name)
        subscription.subscribe()
        return subscription

    def handler(
        self, /, name: Optional[str] = None
    ) -> Callable[[EventHandler], EventHandler]:
        def decorator(handler: EventHandler) -> EventHandler:
            self.subscribe(handler, name=name)
            return handler

        return decorator


class EventSubscription(Generic[E]):
    _name: str
    _emitter: EventEmitter[E]
    _event_handler: AsyncEventHandler
    _exception_handler: Callable[[ExceptionGroup]]

    def __init__(
        self,
        emitter: EventEmitter[E],
        handler: EventHandler,
        name: Optional[str] = None,
    ):
        self._name = name or handler.__name__
        self._emitter = emitter
        self._event_handler = ensure_async(handler)
        self._exception_handler = default_exception_handler

    def __repr__(self):
        return (
            f"EventSubscription({self._emitter._name}, "
            f"{self._name} "
            f'{"enabled" if self.is_enabled else "disabled"})'
        )

    @property
    def emitter(self):
        return self._emitter

    @property
    def name(self):
        return self._name

    @property
    def is_enabled(self) -> bool:
        return self in self._emitter._subscriptions

    def subscribe(self):
        self._emitter._subscriptions.add(self)

    def unsubscribe(self):
        self._emitter._subscriptions.remove(self)

    async def _event_task(self, event: E):
        try:
            await self._event_handler(event)
        except* Exception as exc:
            self._emitter._exception_count += 1
            try:
                self._exception_handler(exc, self)
            except* Exception as nested_exc:
                self.emitter.log.fatal("Exception handling exception")
                self.emitter.log.exception(nested_exc)


def default_exception_handler(exc: Exception, subscription: EventSubscription):
    subscription.emitter.log.exception(exc)


def ensure_async(fn: Callable[[E], Awaitable[None]]):
    if not iscoroutinefunction(fn):

        async def async_wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return async_wrapper
    return fn
