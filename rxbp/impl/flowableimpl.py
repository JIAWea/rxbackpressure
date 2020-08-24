from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class FlowableImpl(Flowable[ValueType]):
    underlying: FlowableMixin
    is_hot: bool

    def _copy(self, underlying: FlowableMixin, *args, **kwargs):
        return replace(self, underlying=underlying, *args, **kwargs)
