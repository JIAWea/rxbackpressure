import sys

from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.single import Single
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class RepeatFirstObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler, batch_size: int):
        self.source = source
        self.scheduler = scheduler
        self.batch_size = batch_size

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        batch_size = self.batch_size
        source = self

        class RepeatFirstObserver(Observer):
            def on_next(self, elem: ElementType):
                if isinstance(elem, list):
                    first_elem = elem[0]
                else:
                    try:
                        first_elem = next(elem)
                    except StopIteration:
                        # empty element, wait for next
                        return continue_ack
                    except:
                        exc = sys.exc_info()
                        observer.on_error(exc)
                        return stop_ack

                batch = [first_elem for _ in range(batch_size)]

                def action(_, __):
                    while True:
                        ack = observer.on_next(batch)

                        if isinstance(ack, ContinueAck):
                            pass
                        elif isinstance(ack, StopAck):
                            break
                        else:
                            class RepeatFirstSingle(Single):
                                def on_next(self, elem):
                                    if isinstance(elem, ContinueAck):
                                        source.scheduler.schedule(action)

                                def on_error(self, exc: Exception):
                                    pass

                            ack.subscribe(RepeatFirstSingle())
                            break

                source.scheduler.schedule(action)
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                # return observer.on_completed()
                pass

        repeat_first_observer = RepeatFirstObserver()
        repeat_first_subscription = observer_info.copy(repeat_first_observer)
        return self.source.observe(repeat_first_subscription)
