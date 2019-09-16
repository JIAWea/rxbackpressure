import sys
import traceback
from typing import Callable

from rx.internal import SequenceContainsNoElementsError
from rxbp.ack.ackimpl import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo


class FirstObservable(Observable):
    def __init__(
            self,
            source: Observable,
            raise_exception: Callable[[Callable[[], None]], None] = None,
    ):
        super().__init__()

        self.source = source
        self.raise_exception = raise_exception

        self.is_first = True

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        source = self

        class FirstObserver(Observer):
            def on_next(self, v):
                source.is_first = False
                observer.on_next(v)
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                if source.is_first:
                    def func():
                        raise SequenceContainsNoElementsError()

                    try:
                        if source.raise_exception is None:
                            func()
                        else:
                            source.raise_exception(func)
                    except:
                        exc = sys.exc_info()
                        observer.on_error(exc)
                else:
                    observer.on_completed()

        first_observer = FirstObserver()
        map_subscription = ObserverInfo(first_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(map_subscription)
