from rxbp.ack.ackimpl import Continue, Stop
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestConnectableObserver(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.exception = Exception('dummy')

    def test_initialize(self):
        sink = TestObserver()
        ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

    def test_connect_empty(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        observer.connect()

        self.assertEqual(0, len(sink.received))

    def test_on_next(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        ack = observer.on_next([1])

        self.assertEqual(0, len(sink.received))

    def test_on_next_then_connect(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])

        observer.connect()

        self.assertEqual([1], sink.received)
        self.assertIsInstance(ack.value, Continue)

    def test_on_error(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        observer.on_error(self.exception)

    def test_on_error_then_continue(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        observer.on_error(self.exception)

        observer.connect()

        self.assertEqual(self.exception, sink.exception)

    def test_on_next_on_error_then_connect(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])
        observer.on_error(self.exception)

        observer.connect()

        self.assertEqual([1], sink.received)
        self.assertEqual(self.exception, sink.exception)
        self.assertIsInstance(ack.value, Continue)

    def test_on_next_on_error_then_connect_on_next(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])
        observer.on_error(self.exception)

        observer.connect()

        ack = observer.on_next([2])

        self.assertEqual([1], sink.received)
        self.assertEqual(self.exception, sink.exception)
        self.assertIsInstance(ack, Stop)

    # def test_should_block_onnext_until_connected(self):
    #     s: TestScheduler = self.scheduler
    #
    #     sink = TestObserver()
    #     conn_obs = ConnectableObserver(sink, scheduler=s, subscribe_scheduler=TrampolineScheduler())
    #     s1 = TestObservable(observer=conn_obs)
    #
    #     f = s1.on_next_single(10)
    #     s1.on_completed()
    #     s.advance_by(1)
    #
    #     self.assertFalse(sink.is_completed, 'f should not be completed')
    #     for i in range(9):
    #         conn_obs.push_first([i+1])
    #
    #     s.advance_by(1)
    #     self.assertFalse(sink.is_completed, 'f should not be completed')
    #     self.assertEqual(len(sink.received), 0)
    #
    #     conn_obs.connect()
    #     s.advance_by(1)
    #
    #     self.assertEqual(len(sink.received), 10)
    #     self.assertLessEqual(sink.received, [e+1 for e in range(10)])
    #     self.assertTrue(sink.is_completed, 'f should be completed')
    #
    # def test_should_emit_pushed_items_immediately_after_connect(self):
    #     o1 = TestObserver()
    #     conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
    #                                       subscribe_scheduler=self.scheduler)
    #
    #     conn_obs.push_first([1])
    #     conn_obs.push_first([2])
    #
    #     conn_obs.connect()
    #     self.scheduler.advance_by(1)
    #
    #     self.assertEqual(len(o1.received), 2)
    #     self.assertLessEqual(o1.received, [e+1 for e in range(2)])
    #     self.assertFalse(o1.is_completed)
    #
    # def test_should_not_allow_push_first_after_connect(self):
    #     o1 = TestObserver()
    #     conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
    #                                       subscribe_scheduler=self.scheduler)
    #
    #     conn_obs.connect()
    #     self.scheduler.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         conn_obs.push_first([1])
    #
    # def test_should_not_allow_push_first_all_after_connect(self):
    #     o1 = TestObserver()
    #     conn_obs = ConnectableObserver(o1, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
    #
    #     conn_obs.connect()
    #     self.scheduler.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         conn_obs.push_first_all([1])
    #
    # def test_should_schedule_push_complete(self):
    #     o1 = TestObserver()
    #     conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
    #                                       subscribe_scheduler=self.scheduler)
    #     s1 = TestObservable(observer=conn_obs)
    #
    #     s1.on_next_iter([10])
    #     conn_obs.push_first([1])
    #     conn_obs.push_first([2])
    #     conn_obs.push_complete()
    #     conn_obs.connect()
    #
    #     self.scheduler.advance_by(1)
    #
    #     self.assertEqual(len(o1.received), 2)
    #     self.assertLessEqual(o1.received, [e+1 for e in range(2)])
    #     self.assertTrue(o1.is_completed)
    #
    # def test_should_not_allow_push_complete_after_connect(self):
    #     s: TestScheduler = self.scheduler
    #
    #     o1 = TestObserver()
    #     down_stream = ConnectableObserver(o1, scheduler=s, subscribe_scheduler=self.scheduler)
    #
    #     down_stream.connect()
    #     s.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         down_stream._on_completed_or_error()
    #
    # def test_should_schedule_push_error(self):
    #     s: TestScheduler = self.scheduler
    #
    #     received = []
    #     was_completed = [False]
    #
    #     o1 = TestObserver()
    #     conn_obs = ConnectableObserver(TestObserver(), scheduler=s, subscribe_scheduler=self.scheduler)
    #     s1 = TestObservable(observer=conn_obs)
    #
    #     s1.on_next_iter([10])
    #     conn_obs.push_first(1)
    #     conn_obs.push_first(2)
    #     conn_obs.push_error(Exception('dummy exception'))
    #     conn_obs.connect()
    #
    #     # s.advance_by(1)
    #     #
    #     # self.assertEqual(len(received), 2)
    #     # self.assertLessEqual(received, [e+1 for e in range(2)])
    #     # self.assertTrue(was_completed[0])