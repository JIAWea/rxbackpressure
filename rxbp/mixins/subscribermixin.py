from abc import ABC, abstractmethod

from rxbp.mixins.copymixin import CopyMixin
from rxbp.scheduler import Scheduler


class SubscriberMixin(CopyMixin, ABC):
    @property
    @abstractmethod
    def scheduler(self) -> Scheduler:
        ...

    @property
    @abstractmethod
    def subscribe_scheduler(self) -> Scheduler:
        ...