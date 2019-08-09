from typing import Any, Callable

from rx.disposable import SingleAssignmentDisposable, CompositeDisposable
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.anonymousflowable import AnonymousFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.observable import Observable
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observesubscription import ObserveSubscription
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber



class DeferFlowable(FlowableBase):
    def __init__(self, base: Base, func: Callable[[FlowableBase], FlowableBase], initial: Any,
                 defer_selector: Callable[[FlowableBase], FlowableBase]):
        super().__init__()

        self._base = base
        self._func = func
        self._initial = initial
        self._defer_selector = defer_selector or (lambda f: f)

    def unsafe_subscribe(self, subscriber: Subscriber):
        initial = self._initial

        class StartWithInitialObservable(Observable):
            def observe(self, subscription: ObserveSubscription):
                buffer_observer.underlying = subscription.observer
                d1 = SingleAssignmentDisposable()

                def action(_, __):
                    def gen_initial():
                        yield initial

                    _ = buffer_observer.on_next(gen_initial)

                    # let any elements flow to buffer_observer
                    _, d3 = conn_observer.connect()

                    d1.disposable = d3

                d2 = subscriber.subscribe_scheduler.schedule(action)

                return CompositeDisposable(d1, d2)

        source = AnonymousFlowable(lambda subscriber: (StartWithInitialObservable(), {}))

        scheduled_source = source.observe_on(scheduler=subscriber.scheduler)

        result_flowable = scheduled_source.share(lambda flowable: self._func(flowable))

        ref_count_flowable = RefCountFlowable(result_flowable)

        defer_flowable = self._defer_selector(Flowable(ref_count_flowable))
        defer_obs, selector = defer_flowable.unsafe_subscribe(subscriber)

        obs, selector = ref_count_flowable.unsafe_subscribe(subscriber)

        buffer_observer = BackpressureBufferedObserver(underlying=None,
                                                       scheduler=subscriber.scheduler,
                                                       subscribe_scheduler=subscriber.subscribe_scheduler,
                                                       buffer_size=1)

        conn_observer = ConnectableObserver(underlying=buffer_observer,
                                            scheduler=subscriber.scheduler,
                                            subscribe_scheduler=subscriber.subscribe_scheduler)

        class DeferObservable(Observable):
            def observe(self, subscription: ObserveSubscription):
                d1 = obs.observe(subscription)

                # once Defer operator is observe from outside,
                volatile_subscription = ObserveSubscription(conn_observer, is_volatile=True)
                d2 = defer_obs.observe(volatile_subscription)

                return CompositeDisposable(d1, d2)

        return DeferObservable(), selector