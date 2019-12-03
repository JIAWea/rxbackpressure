from typing import List, Callable, Tuple

import rx

from rx import operators as rxop

from rxbp.flowable import Flowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.rxextensions.liftobservable import LiftObservable
from rxbp.multicast.singleflowablemixin import SingleFlowableMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase, MultiCastFlowable
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicasts.defermulticast import DeferMultiCast
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.typing import DeferType, MultiCastValue
from rxbp.typing import ValueType
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.rxextensions.debug_ import debug as rx_debug


def split(
        left_ops: List[MultiCastOperator],
        filter_left: Callable[[MultiCastValue], bool] = None,
        filter_right: Callable[[MultiCastValue], bool] = None,
        right_ops: List[MultiCastOperator] = None,
):
    """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
    two streams together again.

            left
            +----> op1 --> op2 -------------+
           /                                 \
    ------+------> op1 --> op2 --> op3 ------>+------>
       share  right                         merge

    :param left_ops: `MultiCast` operators applied to the left
    :param filter_left: (optional) a function that returns True, if the current element is passed left,
    :param filter_right: (optional) a function that returns True, if the current element is passed right,
    :param right_ops: (optional) `MultiCast` operators applied to the right
    """

    if filter_left is None:
        filter_left = (lambda v: True)

    if filter_right is None:
        filter_right = (lambda v: True)

    def op_func(source: MultiCast):
        class SplitMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                shared_source = source.get_source(info=info).pipe(
                    rxop.share(),
                )

                left_source = shared_source.pipe(
                    rxop.filter(filter_left),
                )
                right_source = shared_source.pipe(
                    rxop.filter(filter_right),
                )

                class LeftOpsStream(MultiCastBase):
                    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                        return left_source

                class RightOpsStream(MultiCastBase):
                    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                        return right_source

                left = MultiCast(LeftOpsStream()).pipe(*left_ops)

                if right_ops is None:
                    right = RightOpsStream()
                else:
                    right = MultiCast(RightOpsStream()).pipe(*right_ops)

                return left.get_source(info=info).pipe(
                    rxop.merge(right.get_source(info=info))
                )

        multicast = MultiCast(SplitMultiCast())
        return multicast
    return MultiCastOperator(op_func)


# def zip(
#     *predicates: Callable[[MultiCastFlowable], bool],
# ):
#     """ Zips a set of `Flowables` together, which were selected by a `predicate`.
#
#     :param predicates: a list of functions that return True, if the current element is used for the zip operation
#     :param selector: a function that maps the selected `Flowables` to some `MultiCast` value
#     """
#
#     def op_func(multicast: MultiCast):
#         class ZipMultiCast(MultiCastBase):
#             def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#                 shared_source = multicast.get_source(info=info).pipe(
#                     rxop.share(),
#                 )
#
#                 def flat_map_func(v: MultiCastValue):
#                     if isinstance(v, SingleFlowableMixin):
#                         return v.get_single_flowable()
#                     if isinstance(v, Flowable):
#                         return v
#                     else:
#                         raise Exception(f'illegal case {v}')
#
#                 def gen_flowables():
#                     for predicate in predicates:
#                         yield shared_source.pipe(
#                             # rxop.filter(
#                             #     lambda v: isinstance(v, MultiCastBase.LiftedFlowable) or isinstance(v, Flowable)),
#                             rxop.filter(predicate),
#                             rxop.map(flat_map_func),
#                         )
#
#                 return rx.zip(*gen_flowables()).pipe(
#                     rxop.map(lambda t: FlowableDict({idx: v for idx, v in enumerate(t)})),
#                 )
#
#         return MultiCast(ZipMultiCast())
#
#     return MultiCastOperator(func=op_func)


def filter(
        func: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCast` values for which the given predicate hold.
    """

    def op_func(multicast: MultiCast):


        class FilterMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> Flowable:
                source = multicast.get_source(info=info).pipe(
                    rxop.filter(func)
                )
                return source

        return MultiCast(FilterMultiCast())
    return MultiCastOperator(op_func)


def reduce():
    """ Lift the current `MultiCast[ReducableMixin[T]]` to a `MultiCast[ReducableMixin[T]]`.
    """

    def op_func(source: MultiCastBase):
        return MultiCast(ReduceMultiCast(source=source))

    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCast, MultiCastValue], MultiCastValue],
):
    """ Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(multi_cast: MultiCastBase):
        class LiftMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                # class InnerLiftMultiCast(MultiCastBase):
                #     def __init__(self, source: Flowable[MultiCastValue]):
                #         self._source = source
                #
                #     def get_source(self, info: MultiCastInfo) -> Flowable:
                #         return self._source
                #
                # inner_multicast = InnerLiftMultiCast(source=multi_cast.get_source(info=info))
                # multicast_val = func(MultiCast(inner_multicast))
                # return rxbp.return_value(multicast_val)

                def lift_func(first: MultiCastValue, obs: rx.typing.Observable):

                    class InnerLiftMultiCast(MultiCastBase):
                        def __init__(self, source: Flowable[MultiCastValue]):
                            self._source = source

                        def get_source(self, info: MultiCastInfo) -> Flowable:
                            return self._source

                    inner_multicast = InnerLiftMultiCast(source=obs)
                    multicast_val = func(MultiCast(inner_multicast), first)
                    return multicast_val

                return LiftObservable(
                    source=multi_cast.get_source(info=info),
                    func=lift_func,
                    subscribe_scheduler=info.multicast_scheduler,
                )

        return MultiCast(LiftMultiCast())

    return MultiCastOperator(op_func)


def share(
    func: Callable[[MultiCastValue], Flowable],
    selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
):
    """ Shares a `Flowable` defined by a function `share` that maps `MultiCast` value to a `Flowable`.
    A `selector` function is then used used extend the `MultiCast` value with the shared `Flowable`.

    :param func: a function that maps `MultiCast` value to a `Flowable`
    :param selector: a function that maps `MultiCast` value and the shared `Flowable` to some new `MultiCast` value
    """

    def op_func(multicast: MultiCast):
        class ShareAndMapMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> Flowable:
                if selector is None:
                    selector_ = lambda _, v: v
                else:
                    selector_ = selector

                def map_func(base: MultiCastValue):
                    flowable = Flowable(RefCountFlowable(func(base)))

                    return selector_(base, flowable)

                source = multicast.get_source(info=info).pipe(
                    rxop.map(map_func),
                )

                return source

        return MultiCast(ShareAndMapMultiCast())
    return MultiCastOperator(func=op_func)


def defer(
        func: Callable[[MultiCastValue], MultiCastValue],
        initial: ValueType,
):
    def stream_op_func(multi_cast: MultiCast):
        def lifted_func(multicast: MultiCastBase):
            return func(MultiCast(multicast))

        return MultiCast(DeferMultiCast(source=multi_cast, func=lifted_func, initial=initial))

    return MultiCastOperator(func=stream_op_func)


def merge(*others: MultiCast):
    """ Merges two or more `MultiCast` streams together
    """

    def op_func(multicast: MultiCast):
        class MergeMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                multicasts = reversed([multicast] + list(others))
                return rx.merge(*[e.get_source(info=info) for e in multicasts])

        return MultiCast(MergeMultiCast())

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    """ Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(multi_cast: MultiCast):
        return MultiCast(MapMultiCast(source=multi_cast, func=func))

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCast[MultiCastValue]]):
    """ Maps each `MultiCast` value by applying the given function `func` and flattens the result.
    """

    def op_func(multicast: MultiCast):
        class FlatMapMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> Flowable:
                return multicast.get_source(info=info).pipe(
                    rxop.flat_map(lambda v: func(v).get_source(info=info)),
                )

        return MultiCast(FlatMapMultiCast())

    return MultiCastOperator(op_func)


def debug(name: str):
    def func(multicast: MultiCast):
        class DebugMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> Flowable:
                return multicast.get_source(info=info).pipe(
                    rx_debug(name),
                )

        return MultiCast(DebugMultiCast())

    return MultiCastOperator(func)