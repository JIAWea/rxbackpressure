from dataclasses import dataclass
from traceback import FrameSummary
from typing import Any, Callable, List

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.ishotflowablemixin import IsHotFlowableMixin
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FlatMapFlowable(FlowableMixin):
    source: FlowableMixin
    func: Callable[[Any], IsHotFlowableMixin]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self.func(elem)

            if flowable.is_hot:
                raise Exception(to_operator_exception(
                    message=f'a hot Flowable cannot be flattened, use MultiCast instead',
                    stack=self.stack,
                ))

            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatMapObservable(source=subscription.observable, func=observable_selector,
                                       scheduler=subscriber.scheduler, subscribe_scheduler=subscriber.subscribe_scheduler)

        return init_subscription(observable=observable)