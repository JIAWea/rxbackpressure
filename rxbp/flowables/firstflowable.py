from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.firstobservable import FirstObservable
from rxbp.observables.tolistobservable import ToListObservable
from rxbp.selectors.bases import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class FirstFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, raise_exception: Callable[[Callable[[], None]], None] = None):
        super().__init__()

        self._source = source
        self.raise_exception = raise_exception

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = FirstObservable(source=subscription.observable, raise_exception=self.raise_exception)

        # first emits exactly one element
        base = NumericalBase(1)

        return Subscription(SubscriptionInfo(base=base), observable=observable)