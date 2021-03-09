import time
from rx.scheduler.threadpoolscheduler import ThreadPoolScheduler
from rxbp.scheduler import SchedulerBase


class RXThreadPoolScheduler(SchedulerBase, ThreadPoolScheduler):
    @property
    def idle(self) -> bool:
        return True

    @property
    def is_order_guaranteed(self) -> bool:
        return True

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)
