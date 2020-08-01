from typing import Callable, Any

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.observables.flatmergenobackpressureobservable import FlatMergeNoBackpressureObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlatMergeNoBackpressureFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            selector: Callable[[Any], FlowableMixin],
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self._source = source
        self._selector = selector
        self._subscribe_scheduler = subscribe_scheduler

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(val: Any):
            flowable = self._selector(val)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatMergeNoBackpressureObservable(
            source=subscription.observable,
            selector=observable_selector,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=self._subscribe_scheduler,
        )

        # base becomes undefined after flat mapping
        base = None

        return init_subscription(BaseAndSelectors(base=base), observable=observable)