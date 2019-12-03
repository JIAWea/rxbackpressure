"""
This example demonstrates a use-case of the reduce operator.
The reduce operator reduces series of Flowables emitted by
the multi-cast object by merging the elements emitted by each
Flowable. The multicast created by the reduce operator emits
a single element.

Besides a single Flowable, the reduce operator
can also be applied to a dictionary of Flowables or an object
of type FlowableStateMixin.
"""
import rxbp

# reduce the following two dictionaries, such that the new multicast
# emits a single element: {'val1': flowable1, 'val2': flowable2} where
# flowable1 emits all elements associated to 'val1' and flowable2 emits
# all elements associated to 'val2'
base1 = {'val1': rxbp.range(5), 'val2': rxbp.range(5)}
base2 = {'val1': rxbp.range(3), 'val2': rxbp.range(3)}

rxbp.multicast.from_flowable(base1).pipe(
    rxbp.multicast.op.merge(
        rxbp.multicast.from_flowable(base2)
    ),
    rxbp.multicast.op.reduce(),
    rxbp.multicast.op.map(lambda v: v['val1'].zip(v['val2'])),
).to_flowable().subscribe(print)

# reduce single Flowable
# ----------------------

# the sample example, but with just one Flowable
rxbp.multicast.from_flowable(rxbp.range(5)).pipe(
    rxbp.multicast.op.merge(
        rxbp.multicast.from_flowable(rxbp.range(3))
    ),
    rxbp.multicast.op.reduce(),
).to_flowable().subscribe(print)