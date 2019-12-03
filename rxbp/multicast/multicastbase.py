from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import Generic, Union

import rx

import rxbp

from rx import operators as rxop

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.singleflowablemixin import SingleFlowableMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType
from rxbp.multicast.typing import MultiCastValue


class MultiCastBase(Generic[MultiCastValue], ABC):
    @abstractmethod
    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        ...

    def to_flowable(self) -> Flowable[ValueType]:
        source = self

        class MultiCastToFlowable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                def flat_map_func(v: MultiCastValue):
                    if isinstance(v, SingleFlowableMixin):
                        return v.get_single_flowable()
                    else:
                        return v

                scheduler = TrampolineScheduler()

                info = MultiCastInfo(
                    source_scheduler=subscriber.subscribe_scheduler,
                    multicast_scheduler=scheduler,
                )

                source_flowable = rxbp.from_rx(source.get_source(info=info).pipe(
                    rxop.filter(lambda v: isinstance(v, SingleFlowableMixin) or isinstance(v, FlowableBase)),
                    rxop.first(),
                ))
                return Flowable(SubscribeOnFlowable(source_flowable, scheduler=info.multicast_scheduler)).pipe(
                    rxbp.op.flat_map(flat_map_func),
                ).unsafe_subscribe(subscriber=subscriber)

        return Flowable(MultiCastToFlowable())


MultiCastFlowable = Union[SingleFlowableMixin, Flowable]
