import math
from typing import List

import rx
from rx import config
from rx.core import Disposable
from rx.core.notification import OnNext, OnCompleted, OnError, Notification
from rx.disposables import BooleanDisposable
from rx.internal.concurrency import RLock

from rxbackpressure.ack import Continue, Stop, Ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.scheduler import SchedulerBase, ExecutionModel, Scheduler


class CachedServeFirstSubject(Observable, Observer):

    def __init__(self, name=None, scheduler=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler
        self.is_disposed = False
        self.is_stopped = False

        # track current index of each inner subscription
        self.current_index = {}

        # a inner subscription is inactive if all elements in the buffer are sent
        self.inactive_subsriptions: List[CachedServeFirstSubject.InnerSubscription] = []

        self.buffer = self.DequeuableBuffer()
        self.exception = None

        self.current_ack = None

        self.lock = config["concurrency"].RLock()

    class DequeuableBuffer:
        def __init__(self):
            self.first_idx = 0
            self.queue = []

            self.lock = config["concurrency"].RLock()

        @property
        def last_idx(self):
            return self.first_idx + len(self.queue)

        def __len__(self):
            return len(self.queue)

        def has_element_at(self, idx):
            return idx <= self.last_idx

        def append(self, value):
            self.queue.append(value)

        def get(self, idx):
            if idx < self.first_idx:
                raise Exception('index {} is smaller than first index {}'.format(idx, self.first_idx))
            elif idx - self.first_idx >= len(self.queue):
                raise Exception(
                    'index {} is bigger or equal than length of queue {}'.format(idx - self.first_idx, len(self.queue)))
            return self.queue[idx - self.first_idx]

        def dequeue(self, idx):
            # empty buffer up until some index
            with self.lock:
                while self.first_idx <= idx and len(self.queue) > 0:
                    self.first_idx += 1
                    self.queue.pop(0)

    class InnerSubscription:
        def __init__(self, source: 'CachedServeFirstSubject', observer: Observer,
                     scheduler: Scheduler, em: ExecutionModel):
            self.source = source
            self.observer = observer
            self.scheduler = scheduler
            self.em = em

        def notify_on_next(self, value) -> Ack:
            # inner subscription gets only notified if all items from buffer are sent and ack received

            with self.source.lock:
                # increase current index
                self.source.current_index[self] += 1
                current_index = self.source.current_index[self]

            ack = self.observer.on_next(value)

            if isinstance(ack, Continue):
                self.source.inactive_subsriptions.append(self)
                return ack
            elif isinstance(ack, Stop):
                del self.source.current_index[self]
                return ack
            else:
                def _(v):
                    if isinstance(v, Continue):
                        with self.source.lock:
                            if current_index < self.source.buffer.last_idx:
                                has_elem = True
                            else:
                                has_elem = False

                        if has_elem:
                            disposable = BooleanDisposable()
                            self.fast_loop(current_index, 0, disposable)
                        else:
                            self.source.inactive_subsriptions.append(self)
                    else:
                        raise NotImplementedError

                inner_ack = Ack()
                async_ack = ack.observe_on(scheduler=self.scheduler).share()
                async_ack.subscribe(_)
                async_ack.subscribe(inner_ack)
                return inner_ack

        def notify_on_completed(self):
            self.observer.on_completed()

        def fast_loop(self, current_idx: int, sync_index: int, disposable: BooleanDisposable):
            while True:
                # buffer has an element at current_idx
                notification = self.source.buffer.get(current_idx)
                current_idx += 1

                is_last = False
                with self.source.lock:
                    # is this subscription last?
                    self.source.current_index[self] = current_idx
                    if min(self.source.current_index.values()) == current_idx:
                        # dequeing is required
                        self.source.buffer.dequeue(current_idx - 1)

                try:
                    # if is_last:
                    #     self.source.buffer.dequeue(current_idx - 1)

                    if isinstance(notification, OnCompleted):
                        self.observer.on_completed()
                        break
                    elif isinstance(notification, OnError):
                        self.observer.on_error(notification.exception)
                        break
                    else:
                        ack = self.observer.on_next(notification.value)

                    has_next = False
                    with self.source.lock:
                        # does it has element in the buffer?
                        if current_idx < self.source.buffer.last_idx:
                            has_next = True
                        else:
                            if isinstance(ack, Continue):
                                self.source.inactive_subsriptions.append(self)
                                self.source.current_ack.on_next(ack)
                                self.source.current_ack.on_completed()
                            elif isinstance(ack, Stop):
                                del self.source.current_index[self]
                                break
                            else:
                                def _(v):
                                    with self.source.lock:
                                        if current_idx < self.source.buffer.last_idx:
                                            has_elem = True
                                        else:
                                            has_elem = False
                                            self.source.inactive_subsriptions.append(self)
                                            self.source.current_ack.on_next(v)
                                            self.source.current_ack.on_completed()

                                    if has_elem:
                                        disposable = BooleanDisposable()
                                        self.fast_loop(current_idx, 0, disposable)
                                    else:
                                        pass

                                ack.observe_on(self.scheduler).subscribe(_)

                    if not has_next:
                        break
                    else:
                        if isinstance(ack, Continue):
                            next_index = self.em.next_frame_index(sync_index)
                        elif isinstance(ack, Stop):
                            next_index = -1
                        else:
                            next_index = 0

                        if next_index > 0:
                            sync_index = next_index
                        elif next_index == 0 and not disposable.is_disposed:
                            def on_next(next):
                                if isinstance(next, Continue):
                                    try:
                                        self.fast_loop(current_idx, sync_index=0, disposable=disposable)
                                    except Exception as e:
                                        raise NotImplementedError
                                else:
                                    raise NotImplementedError

                            def on_error(err):
                                raise NotImplementedError

                            ack.observe_on(self.scheduler).subscribe(on_next=on_next, on_error=on_error)
                            break
                        else:
                            raise NotImplementedError
                except:
                    raise Exception('fatal error')

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        # self.scheduler = self.scheduler or scheduler
        source = self
        em = scheduler.get_execution_model()

        inner_subscription = self.InnerSubscription(source=self, observer=observer, scheduler=scheduler, em=em)

        with self.lock:
            if not self.is_stopped:
                # get current buffer index
                current_idx = self.buffer.first_idx
                self.current_index[inner_subscription] = current_idx
                # todo: is that ok?
                self.inactive_subsriptions.append(inner_subscription)
                return Disposable.empty()

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

        observer.on_completed()
        return Disposable.empty()

    def on_next(self, value):

        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # empty inactive subscriptions; they are added to the list once they reach the top of the buffer again
            inactive_subsriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

            # add item to buffer
            self.buffer.append(OnNext(value))

            # current ack is used by subscriptions that weren't inactive, but reached the top of the buffer
            current_ack = Ack()
            self.current_ack = current_ack

            last_index = self.buffer.last_idx

        def gen_inner_ack():
            # send notification to inactive subscriptions
            for inner_subscription in inactive_subsriptions:
                inner_ack = inner_subscription.notify_on_next(value)
                yield inner_ack

        inner_ack_list = list(gen_inner_ack())

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, Continue)]
        # stop_ack = [ack for ack in inner_ack_list if isinstance(ack, Stop)]

        # with self.lock:
        #     # dequeue buffer if all inner subscriptions returned Continue
        #     dequeue_buffer = len(continue_ack) == len(inactive_subsriptions)
        #
        #     if dequeue_buffer:
        #         # dequeue single item
        #         self.buffer.dequeue(last_index - 1)

        # if 0 < len(stop_ack):
        #     # return any Stop ack
        #     return stop_ack[0]
        if 0 < len(continue_ack):
            # return any Continue ack
            return continue_ack[0]
        else:
            # return merged acknowledgments from inner subscriptions

            ack_list = [current_ack] + inner_ack_list

            upper_ack = Ack()
            rx.Observable.merge(*ack_list).first().subscribe(upper_ack)
            return upper_ack

    def on_completed(self):
        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # inner subscriptions that return Continue or Stop need to be added to inactive subscriptions again
            inactive_subsriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

            # add item to buffer
            self.buffer.append(OnCompleted())

        # send notification to inactive subscriptions
        for inner_subscription in inactive_subsriptions:
            inner_subscription.notify_on_completed()

    def on_error(self, exception):
        raise NotImplementedError

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.current_index = None