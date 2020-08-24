from dataclasses import dataclass
from typing import Any, Callable

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.observables.flatconcatnobackpressureobservable import FlatConcatNoBackpressureObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FlatConcatNoBackpressureFlowable(FlowableMixin):
    source: FlowableMixin
    selector: Callable[[Any], FlowableMixin]
    subscribe_scheduler: Scheduler

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self.selector(elem)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        return subscription.copy(observable=FlatConcatNoBackpressureObservable(
            source=subscription.observable,
            selector=observable_selector,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
        ))