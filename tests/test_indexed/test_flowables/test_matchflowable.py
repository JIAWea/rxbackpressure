from rxbp.indexed.flowables.matchindexedflowable import MatchIndexedFlowable
from rxbp.indexed.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestMatchFlowable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TScheduler()
        self.sink = TObserver()

    def test_matching_base_with_automatching(self):
        b1 = NumericalBase(1)
        b2 = NumericalBase(1)
        b3 = NumericalBase(3)
        b4 = NumericalBase(4)
        s1 = TestFlowable(base=b1, selectors={b3: None})
        s2 = TestFlowable(base=b2, selectors={b4: None})

        flowable = MatchIndexedFlowable(
            left=s1,
            right=s2,
        )

        subscription = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        self.assertIn(b3, subscription.info.selectors)
        self.assertIn(b4, subscription.info.selectors)

    def test_matching_selector_with_automatching(self):
        b1 = NumericalBase(1)
        b2 = NumericalBase(2)
        b3 = NumericalBase(3)
        b4 = NumericalBase(4)
        s1 = TestFlowable(base=b1, selectors={b3: 'sel3'})
        s2 = TestFlowable(base=b2, selectors={b1: 'sel1', b4: 'sel4'})

        flowable = MatchIndexedFlowable(
            left=s1,
            right=s2,
        )

        subscription = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        self.assertIn(b1, subscription.info.selectors)
        self.assertIn(b3, subscription.info.selectors)
        self.assertIn(b4, subscription.info.selectors)