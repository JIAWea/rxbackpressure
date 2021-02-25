import threading
import time
from threading import Thread

import rxbp
from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.testing.tobserver import TObserver
from rxbp.typing import ElementType


def demo1():
    def work(o, skd):
        for i in range(10):
            o.on_next([i])
        o.on_completed()

    source = rxbp.create(work)
    source = source.pipe(
        rxbp.op.observe_on(scheduler=AsyncIOScheduler()),
        # rxbp.op.observe_on(scheduler=EventLoopScheduler()),
        # rxbp.op.observe_on(scheduler=ThreadPoolScheduler("receiver")),
        rxbp.op.subscribe_on(scheduler=ThreadPoolScheduler("publisher")),
    )

    sink = TObserver(immediate_continue=4)
    # source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))
    source.subscribe(observer=sink)

    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4]

    print("-" * 80)
    sink.immediate_continue += 2
    sink.ack.on_next(continue_ack)
    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7]

    print("-" * 80)
    sink.immediate_continue += 1
    sink.ack.on_next(continue_ack)
    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def demo2():
    def counter(sink):
        while True:
            time.sleep(5)
            print(f"[**client**] received: ", sink.received)

    def work(o, skd):
        for i in range(100_000):
            o.on_next([i])
        o.on_completed()

    source = rxbp.create(work)
    # no back-pressure until the buffer is full
    source = source.pipe(
        rxbp.op.buffer(10)
    )

    sink = TObserver(immediate_continue=4)
    source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))

    t1 = Thread(target=counter, args=(sink,))
    t1.start()
    t1.join()


def demo3():
    class Subscriber(Observer):
        def __init__(self, stop_num: int):
            self.received = []
            self.stop_num = stop_num

            # counts the number of times `on_next` is called
            self.on_next_counter = 0

        def on_next(self, elem: ElementType):
            try:
                values = list(elem)
            except Exception:
                return stop_ack

            self.received += values

            self.on_next_counter += 1

            if self.on_next_counter >= self.stop_num:
                return stop_ack

            return continue_ack

        def on_error(self, err):
            print("Exception: ", err)

        def on_completed(self):
            print("Completed")

    def counter(sink):
        while True:
            time.sleep(3)
            print(f"[**client**] received: ", sink.received)

    def work(o, skd):
        for i in range(8):
            ack = o.on_next([i])
            if isinstance(ack, StopAck):
                # break
                print("got stopped!")

        o.on_completed()

    source = rxbp.create(work)
    source = source.pipe(
        rxbp.op.subscribe_on(ThreadPoolScheduler("")),
    )

    sink = Subscriber(stop_num=2)
    source.subscribe(observer=sink)

    t1 = Thread(target=counter, args=(sink, ))
    t1.start()
    t1.join()


if __name__ == '__main__':
    # demo1()

    # demo2()

    demo3()
