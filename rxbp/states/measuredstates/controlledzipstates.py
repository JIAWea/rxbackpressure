from abc import ABC
from collections import Iterator
from typing import Optional, Any

from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject


class ControlledZipStates:
    class ZipState(ABC):
        pass

    class WaitOnLeft(ZipState):
        """ Zip observable actor has or will back-pressure the left source, but no element
        has yet been received.

        In this state, the left buffer is empty.
        """

        def __init__(
                self,
                right_val: Any,
                right_ack: AckSubject,
                right_iter: Iterator,
                right_sel_ack: Optional[AckBase],
        ):
            self.right_val = right_val
            self.right_ack = right_ack
            self.right_iter = right_iter
            self.right_sel_ack = right_sel_ack

    class WaitOnRight(ZipState):
        """ Equivalent of WaitOnLeft """

        def __init__(
                self,
                left_val: Any,
                left_ack: AckSubject,
                left_iter: Iterator,
                left_sel_ack: Optional[AckBase],
        ):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_ack = left_ack
            self.left_sel_ack = left_sel_ack

    class WaitOnLeftRight(ZipState):
        """ Zip observable actor has or will back-pressure the left and right source, but
        no element has yet been received.

        In this state, the left and right buffer are empty.
        """

        pass

    class ZipElements(ZipState):
        """ Zip observable actor is zipping the values just received by a source and
         from the buffer.

        In this state the actual termination state is ignored in the `get_measured_state`
        method.
        """

        def __init__(
                self,
                val: Any,
                is_left: bool,
                ack: AckSubject,
                iter: Iterator,
        ):
            self.val = val
            self.is_left = is_left
            self.ack = ack
            self.iter = iter

    class Stopped(ZipState):
        pass