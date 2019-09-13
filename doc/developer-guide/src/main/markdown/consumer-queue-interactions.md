# Consumer-Queue Interactions

This article overviews implementation behind interactions between consumers and queue.

<!-- toc -->

- [Overview](#overview)
- [Responsibilities](#responsibilities)
  * [Simple Example](#simple-example)
  * [Threading Model](#threading-model)
  * [Interfaces Between Consumers and Queues](#interfaces-between-consumers-and-queues)
  * [QueueConsumerManager internals](#queueconsumermanager-internals)
    + [QueueConsumerNodeList](#queueconsumernodelist)
    + [The "All" List](#the-all-list)
    + [The "NonAcquiring" List](#the-nonacquiring-list)
    + [The "NotInterested" List](#the-notinterested-list)
    + [The "Interested" List](#the-interested-list)
    + [The "Notified" List](#the-notified-list)
    + [Handling Consumer Priorities](#handling-consumer-priorities)

<!-- tocstop -->

## Overview

The main players are

 * Queue - model object providing the messaging queue functionality.
    * QueueConsumerManager - queue entity responsible for managing queue consumers
 * Consumers - queue consumers

The `ConsumerTarget` is the broker-side representation of a consuming client. Due to multi-queue consumers
a `ConsumerTarget` has one or more `Consumers` associated with one `Queue` each. It is this `Consumer` that
interacts with the `Queue`.

## Responsibilities

A `Queue` is responsible for notification of at least one interested `Consumer` when there is work to be done
(message to consume).

A `Consumer` is responsible for notification of its `Queue` when it is ready to do some work (for example, consume messages).
When notified by a `Queue` of available work, a `Consumer` MUST try to pull messages of said `Queue` until either
it notifies the `Queue` that it is no longer interested OR there are no more messages available on the `Queue`
(i.e., the Queue does not return a message).

### Simple Example

 1. `Message` arrives on the `Queue`
 2. The `Queue` notifies some of interested `Consumers` that there is work to be done
 3. The `Consumers` notify their `ConsumerTarget` that they would like to do work
 4. The `ConsumerTargets` notify their `Sessions` that they would like to do work
 5. The `Sessions` notify their `Connections` that they would like to do work
 6. The `Connections` schedule themselves. This is the switch from the incoming `Thread` to the `IO-Thread`.
 7. The `Scheduler` kicks off a IO-Thread to process the work of a `Connection`
 8. The `Connection` iterates over its `Sessions` that want to do work
 9. The `Sessions` iterate over its `ConsumerTargets` that want to do work
 10. The `ConsumerTargets` iterate over its `Consumers` that want to do work
 11. The Consumer tries to pulls a message from the `Queue`
 12. If successful, the message is put on the IO-buffer to be sent down the wire

### Threading Model

The consuming part is always invoked from the consuming connection's IO-Thread
whereas the publishing part might be invoked from different threads (producing connection's IO-Thread,
Housekeeping thread for held or TTLed messages, a consuming connection's IO-Thread in case for message reject).

Therefore, the interfaces between `Consumers` and the `Queue` MUST be thread-safe and SHOULD be lock free.

### Interfaces Between Consumers and Queues

These are the interfaces between `Consumers` and `Queues` and the scenarios when they are called.

  * `AbstractQueue#setNotifyWorkDesired`
    Called by the `Consumer` to notify the `Queue` whether it is interested in doing work or not.
    * FlowControl
    * Credit
    * TCP backpressure
  * `QueueConsumer#notifyWork`
    Called by the `Queue` to notify the `Consumer` that there is potentially work available.
    * Consumer becomes Interested
    * A new message arrives
    * A previously unavailable (acquired, held, blocked by message grouping) message becomes available
    * A notified consumer did not do the work we expected it to do we need to notify someone else
    * A high priority consumer becomes uninterested and thus allows a low priority consumer to consume messages
  * `AbstractQueue#deliverSingleMessage`
    Called by the `Consumer` to get a message from the Queue.
    * A consumer was notified and now tries to pull a message of a queue

### QueueConsumerManager internals

The `QueueConsumerManager` (QCM for short) keeps track of the state of `Consumers` from the perspective of the `Queue`.
It shares and decides which `Consumer` to notify of work with the `Queue`. To do this in a performant way it maintains
a number of lists and moves `Consumers` between those lists to indicate state change. The lists it maintains are:

  *  All (all queue consumers)
  *  NonAcquiring
  *  NotInterested
  *  Interested
  *  Notified

Typically we want these lists to be thread-safe and give us O(1) access/deletion if we know the element and O(1) size information.
Unfortunately there is no data structure in the Java standard library with those characteristics
which is why they are based on our own data structure `QueueConsumerNodeList`.

#### QueueConsumerNodeList

The `QueueConsumerNodeList` is the underlying data structure of all of QCM's lists.
It is thread-safe and allows O(1) appending and given you have a pointer to an entry O(1) deletion.
It is essentially a singly linked list. To achieve O(1) deletion entries are marked for deletion
but only actually removed upon the next iteration.  The rationale being that, to delete an entry you would need
to update the previous entry's "next" pointer but to get to the previous element you would need a doubly linked list
which it impossible to maintain in a thread-safe way without locking. Special care must be taken when removing elements
from the tail since we keep an explicit reference to it in the `QueueConsumerNodeList` to achieve O(1) appending.
The data structure in the `QueueConsumerNodeList` are called `QueueConsumerNodeListEntries` which themselves have
a reference to a `QueueConsumerNode` which is the persistent entity that moves between QCM's lists and has a reference
to the `QueueConsumer`. The `QueueConsumer` itself also has a reference to the `QueueConsumerNode` to enable O(1)
deletion given a Consumer. This tightly couples the `QueueConsumer` and QCM classes.

#### The "All" List

The `All` list contains all `Consumers` registered with the `Queue`. `Consumers` are added to this list when they are
created and only removed when the `Consumer` is closed. This list is necessary to be able to iterate over all consumers
in a thread-safe way without locking. The danger of using several lists instead of a single `All` list is that you might
miss a `Consumer` if it moves between lists during iteration.

#### The "NonAcquiring" List

This is a list of `Consumers` that do not acquire messages for example `Queue Browsers`. These need to be handled
separately because they should always be notified about new messages. Where they kept in the same list
as the acquiring consumers we would have to iterate of the entire list to make sure we did not miss
a non-acquiring consumer. Non-acquiring consumers can only move between the "NonAcquiring" and "NotInterested" lists.

#### The "NotInterested" List

This list contains all `Consumer`s that indicated to the `Queue` that they currently are not interested in doing any
work (i.e., taking messages). This typically happens when a Consumer/Connection is suspended due to
FlowControl/TCP backpressure. The main purpose of this list is to avoid spurious wake-ups of `Consumers` which we know
are not going to do any work.

#### The "Interested" List

This is the default list for acquiring `Consumers`. It signifies that they are ready to process messages.
When a message becomes available, the `Queue` will notify `Consumers` from this list and move them to the
"Notified" list. It will only notify a single interested `Consumer` to avoid spurious wake-ups.

#### The "Notified" List

Once an acquiring `Consumer` is notified that there is work to do it is moved from the "Interested" list to the
"Notified" list. The QCM expects such a `Consumer` to either indicate that it is no longer interested
(e.g., it became suspended in the meantime and therefore will not do the work we expected it to) or call
`AbstractQueue#deliverSingleMessage`. The `Consumer` should remain in the "Notified" list and continue to call
`deliverSingleMessage` until `deliverSingleMessage` cannot deliver a message to it any more, in which case it is moved
to the back of the "Interested" list. This is to decrease latency due to wake-ups when there continues to be work
available (i.e., there is a steady stream of messages). Appending it to the end of the "Interested" list ensures
some level of fairness. Note that this is not perfect. It is possible that a consumer is notified but at the time
it tries to pull a message of the Queue there no longer is a message available and the Consumer is returned to the end
of the "Interested" list without having done work. The assumption is that while this may happen it is unlikely to always
happen to the same consumer leading to a kind of "asymptotic fairness".

#### Handling Consumer Priorities

When deciding which `Consumer` to notify the QCM should take consumer priorities into account.
To do this in a performant way it maintains a `QueueConsumerNodeList` per consumer priority in a list of
`PriorityConsumerListPairs`. This ensures that iteration of the Interested list happens in the right order and lookup
of consumers with higher priority can be performed efficiently.
