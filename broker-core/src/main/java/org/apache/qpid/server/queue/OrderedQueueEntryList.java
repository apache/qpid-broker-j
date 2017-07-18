/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/
package org.apache.qpid.server.queue;

import static org.apache.qpid.server.model.Queue.QUEUE_SCAVANGE_COUNT;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageEnqueueRecord;

public abstract class OrderedQueueEntryList extends AbstractQueueEntryList
{

    private final OrderedQueueEntry _head;

    private volatile OrderedQueueEntry _tail;

    static final AtomicReferenceFieldUpdater<OrderedQueueEntryList, OrderedQueueEntry>
            _tailUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (OrderedQueueEntryList.class, OrderedQueueEntry.class, "_tail");


    private final Queue<?> _queue;

    static final AtomicReferenceFieldUpdater<OrderedQueueEntry, OrderedQueueEntry>
                _nextUpdater = OrderedQueueEntry._nextUpdater;

    private AtomicLong _scavenges = new AtomicLong(0L);
    private final long _scavengeCount;
    private final AtomicReference<QueueEntry> _unscavengedHWM = new AtomicReference<QueueEntry>();


    public OrderedQueueEntryList(Queue<?> queue,
                                 final QueueStatistics queueStatistics,
                                 HeadCreator headCreator)
    {
        super(queue, queueStatistics);
        _queue = queue;
        _scavengeCount = _queue.getContextValue(Integer.class, QUEUE_SCAVANGE_COUNT);
        _head = headCreator.createHead(this);
        _tail = _head;
    }

    void scavenge()
    {
        QueueEntry hwm = _unscavengedHWM.getAndSet(null);
        QueueEntry next = _head.getNextValidEntry();

        if(hwm != null)
        {
            while (next != null && hwm.compareTo(next)>0)
            {
                next = next.getNextValidEntry();
            }
        }
    }


    @Override
    public Queue<?> getQueue()
    {
        return _queue;
    }


    @Override
    public QueueEntry add(ServerMessage message, final MessageEnqueueRecord enqueueRecord)
    {
        final OrderedQueueEntry node = createQueueEntry(message, enqueueRecord);
        updateStatsOnEnqueue(node);
        for (;;)
        {
            OrderedQueueEntry tail = _tail;
            OrderedQueueEntry next = tail.getNextNode();
            if (tail == _tail)
            {
                if (next == null)
                {
                    node.setEntryId(tail.getEntryId()+1);
                    if (_nextUpdater.compareAndSet(tail, null, node))
                    {
                        _tailUpdater.compareAndSet(this, tail, node);

                        return node;
                    }
                }
                else
                {
                    _tailUpdater.compareAndSet(this,tail, next);
                }
            }
        }
    }

    abstract protected OrderedQueueEntry createQueueEntry(ServerMessage<?> message,
                                                          final MessageEnqueueRecord enqueueRecord);

    @Override
    public QueueEntry next(QueueEntry node)
    {
        return node.getNextValidEntry();
    }

    public static interface HeadCreator
    {
        OrderedQueueEntry createHead(QueueEntryList list);
    }

    public static class QueueEntryIteratorImpl implements QueueEntryIterator
    {
        private QueueEntry _lastNode;

        QueueEntryIteratorImpl(QueueEntry startNode)
        {
            _lastNode = startNode;
        }

        @Override
        public boolean atTail()
        {
            return _lastNode.getNextValidEntry() == null;
        }

        @Override
        public QueueEntry getNode()
        {
            return _lastNode;
        }

        @Override
        public boolean advance()
        {
            QueueEntry nextValidNode = _lastNode.getNextValidEntry();

            if(nextValidNode != null)
            {
                _lastNode = nextValidNode;
            }

            return nextValidNode != null;
        }
    }

    @Override
    public QueueEntryIterator iterator()
    {
        return new QueueEntryIteratorImpl(_head);
    }


    @Override
    public QueueEntry getHead()
    {
        return _head;
    }

    @Override
    public QueueEntry getTail()
    {
        return _tail;
    }

    @Override
    public void entryDeleted(QueueEntry queueEntry)
    {
        QueueEntry next = _head.getNextNode();
        QueueEntry newNext = _head.getNextValidEntry();

        // the head of the queue has not been deleted, hence the deletion must have been mid queue.
        if (next == newNext)
        {
            QueueEntry unscavengedHWM = _unscavengedHWM.get();
            while(unscavengedHWM == null || unscavengedHWM.compareTo(queueEntry)<0)
            {
                _unscavengedHWM.compareAndSet(unscavengedHWM, queueEntry);
                unscavengedHWM = _unscavengedHWM.get();
            }
            if (_scavenges.incrementAndGet() > _scavengeCount)
            {
                _scavenges.set(0L);
                scavenge();
            }
        }
        else
        {
            QueueEntry unscavengedHWM = _unscavengedHWM.get();
            if(unscavengedHWM != null && (next == null || unscavengedHWM.compareTo(next) < 0))
            {
                _unscavengedHWM.compareAndSet(unscavengedHWM, null);
            }
        }
    }

    @Override
    public int getPriorities()
    {
        return 0;
    }

    @Override
    public QueueEntry getOldestEntry()
    {
        return next(getHead());
    }

}
