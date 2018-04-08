from rx import config
from rx.concurrency import current_thread_scheduler
from rx.core.notification import OnNext
from rx.internal import DisposedException
from rx.subjects import AsyncSubject, Subject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest


class BufferBackpressure():
    def __init__(self, buffer, last_idx, observer, update_source, dispose, scheduler=None):
        """

        :param buffer:
        :param last_idx:
        :param observer:
        :param update_source: function that is called, if items from buffer is consumed
        :param dispose:
        :param scheduler:
        """
        super().__init__()

        self.observer = observer
        self.buffer = buffer
        self.current_idx = last_idx
        self.requests = []
        self.is_stopped = False
        self._lock = config["concurrency"].RLock()
        self.dispose_func = dispose
        self.scheduler = scheduler or current_thread_scheduler
        self.is_disposed = False
        self.update_source = update_source

        self.child_disposable = observer.subscribe_backpressure(self, scheduler)

        # print(observer.observer)

        assert self.child_disposable is not None

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def request(self, number_of_items):
        # print('request {}'.format(number_of_items))
        future = Subject()

        def action(a, s):
            if not self.is_stopped:
                with self._lock:
                    self.requests.append((future, number_of_items, 0))
                self.update()
            else:
                future.on_next(0)
                future.on_completed()

        self.scheduler.schedule(action)
        return future

    def update(self) -> int:
        """ Sends buffered items to the observer

        :return: current buffer index
        """

        def take_requests_gen():
            """Updates the request list by checking new items in the buffer.

            Returns:
            A tuple3:
            - updated request list
            - items from buffer
            - fullfilled (, deleted) requests

            :return:
            """
            check_stop_request = False

            for future, number_of_items, counter in self.requests:

                if check_stop_request:
                    if isinstance(number_of_items, StopRequest):
                        yield None, None, (future, number_of_items)
                        break
                    check_stop_request = False

                if self.current_idx < self.buffer.last_idx:
                    # there are still new items in buffer

                    if isinstance(number_of_items, StopRequest):
                        yield None, None, (future, number_of_items)
                        break

                    def get_value_from_buffer(num):
                        for _ in range(num):
                            value = self.buffer.get(self.current_idx)
                            self.current_idx += 1
                            yield value

                    if self.current_idx + number_of_items - counter <= self.buffer.last_idx:
                        # request fully fullfilled
                        d_number_of_items = number_of_items - counter
                        values = list(get_value_from_buffer(d_number_of_items))
                        num_of_items = number_of_items - len(values) + sum(1 for v in values if isinstance(v, OnNext))
                        yield None, values, (future, num_of_items)
                    else:
                        # request not fully fullfilled
                        d_number_of_items = self.buffer.last_idx - self.current_idx
                        values = list(get_value_from_buffer(d_number_of_items))
                        yield (future, number_of_items, counter + d_number_of_items), values, None

                    if self.current_idx == self.buffer.last_idx:
                        check_stop_request = True
                else:
                    # there are no new items in buffer
                    yield (future, number_of_items, counter), None, None

        if self.observer:
            # take as many requests as possible from self.requests
            has_elements = False
            with self._lock:
                if len(self.requests):
                    has_elements = True
                    request_list, buffer_value_list, future_tuple_list = zip(*take_requests_gen())
                    self.requests = [request for request in request_list if request is not None]

            # send values at some later time
            if has_elements is True:
                # inform source about update
                self.update_source(self, self.current_idx)

                def action(a, s):

                    # send items taken from buffer
                    value_to_send = [e for value_list in buffer_value_list if value_list is not None for e in
                                     value_list]

                    for value in value_to_send:
                        # print(value)
                        if isinstance(value, OnNext):
                            self.observer.on_next(value.value)
                        else:
                            self.is_stopped = True
                            self.observer.on_completed()

                            with self._lock:
                                requests = self.requests
                                self.requests = []

                            def action(a, s):
                                if requests:
                                    for future, _, __ in requests:
                                        future.on_next(0)
                                        future.on_completed()

                            self.scheduler.schedule(action)

                    # set future from request
                    future_tuple_list_ = [e for e in future_tuple_list if e is not None]
                    for future, number_of_items in future_tuple_list_:
                        # print(future)
                        future.on_next(number_of_items)
                        future.on_completed()
                        if isinstance(number_of_items, StopRequest):
                            if not self.is_disposed:
                                self.observer.on_completed()
                                # self.check_disposed()
                                # print('disposed')
                                # self.dispose()

                self.scheduler.schedule(action)

        # return current index in shared buffer
        return self.current_idx

    def dispose(self):
        with self._lock:
            self.is_disposed = True
            self.is_stopped = True
            self.requests = None
            self.observer = None
            self.dispose_func(self)
            self.child_disposable.dispose()
