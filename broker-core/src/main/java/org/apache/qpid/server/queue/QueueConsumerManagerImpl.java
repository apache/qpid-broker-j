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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueueConsumerManagerImpl implements QueueConsumerManager
{
    private static final EnumSet<NodeState> REMOVED = EnumSet.of(NodeState.REMOVED);
    private static final EnumSet<NodeState> STATES_OTHER_THAN_REMOVED =
            EnumSet.complementOf(REMOVED);
    private static final EnumSet<NodeState> NOT_INTERESTED = EnumSet.of(NodeState.NOT_INTERESTED);
    private static final EnumSet<NodeState>
            EITHER_INTERESTED_OR_NOTIFIED = EnumSet.of(NodeState.INTERESTED, NodeState.NOTIFIED);
    private static final EnumSet<NodeState> NON_ACQUIRING = EnumSet.of(NodeState.NON_ACQUIRING);
    private static final EnumSet<NodeState> INTERESTED = EnumSet.of(NodeState.INTERESTED);
    private static final EnumSet<NodeState> NOTIFIED = EnumSet.of(NodeState.NOTIFIED);

    private final AbstractQueue<?> _queue;

    private final List<PriorityConsumerListPair> _interested;
    private final QueueConsumerNodeList _notInterested;
    private final List<PriorityConsumerListPair> _notified;
    private final QueueConsumerNodeList _nonAcquiring;

    private final List<PriorityConsumerListPair> _allConsumers;

    private volatile int _count;

    enum NodeState
    {
        REMOVED,
        INTERESTED,
        NOT_INTERESTED,
        NOTIFIED,
        NON_ACQUIRING;
    }

    public QueueConsumerManagerImpl(final AbstractQueue<?> queue)
    {
        _queue = queue;
        _notInterested = new QueueConsumerNodeList(queue);
        _interested = new CopyOnWriteArrayList<>();
        _notified = new CopyOnWriteArrayList<>();
        _nonAcquiring = new QueueConsumerNodeList(queue);
        _allConsumers = new CopyOnWriteArrayList<>();
    }

    // Always in the config thread
    @Override
    public void addConsumer(final QueueConsumer<?,?> consumer)
    {
        QueueConsumerNode node = new QueueConsumerNode(this, consumer);
        consumer.setQueueConsumerNode(node);
        addToAll(node);
        if (consumer.isNotifyWorkDesired())
        {
            if (consumer.acquires())
            {
                node.moveFromTo(REMOVED, NodeState.INTERESTED);
            }
            else
            {
                node.moveFromTo(REMOVED, NodeState.NON_ACQUIRING);
            }
        }
        else
        {
            node.moveFromTo(REMOVED, NodeState.NOT_INTERESTED);
        }
        _count++;
    }

    // Always in the config thread
    @Override
    public boolean removeConsumer(final QueueConsumer<?,?> consumer)
    {
        removeFromAll(consumer);
        QueueConsumerNode node = consumer.getQueueConsumerNode();

        if (node.moveFromTo(STATES_OTHER_THAN_REMOVED, NodeState.REMOVED))
        {
            _count--;
            return true;
        }
        return false;
    }

    // Set by the consumer always in the IO thread
    @Override
    public boolean setInterest(final QueueConsumer<?,?> consumer, final boolean interested)
    {
        QueueConsumerNode node = consumer.getQueueConsumerNode();
        if (interested)
        {
            if (consumer.acquires())
            {
                return node.moveFromTo(NOT_INTERESTED, NodeState.INTERESTED);
            }
            else
            {
                return node.moveFromTo(NOT_INTERESTED, NodeState.NON_ACQUIRING);
            }
        }
        else
        {
            if (consumer.acquires())
            {
                return node.moveFromTo(EITHER_INTERESTED_OR_NOTIFIED, NodeState.NOT_INTERESTED);
            }
            else
            {
                return node.moveFromTo(NON_ACQUIRING, NodeState.NOT_INTERESTED);
            }
        }
    }

    // Set by the Queue any IO thread
    @Override
    public boolean setNotified(final QueueConsumer<?,?> consumer, final boolean notified)
    {
        QueueConsumerNode node = consumer.getQueueConsumerNode();
        if (consumer.acquires())
        {
            if (notified)
            {
                return node.moveFromTo(INTERESTED, NodeState.NOTIFIED);
            }
            else
            {
                return node.moveFromTo(NOTIFIED, NodeState.INTERESTED);
            }
        }
        else
        {
            return true;
        }
    }

    @Override
    public Iterator<QueueConsumer<?,?>> getInterestedIterator()
    {
        return new QueueConsumerIterator(new PrioritisedQueueConsumerNodeIterator(_interested));
    }

    @Override
    public Iterator<QueueConsumer<?,?>> getAllIterator()
    {
        return new QueueConsumerIterator(new PrioritisedQueueConsumerNodeIterator(_allConsumers));
    }

    @Override
    public Iterator<QueueConsumer<?,?>> getNonAcquiringIterator()
    {
        return new QueueConsumerIterator(_nonAcquiring.iterator());
    }

    @Override
    public int getAllSize()
    {
        return _count;
    }

    @Override
    public int getHighestNotifiedPriority()
    {
        final Iterator<QueueConsumerNode> notifiedIterator =
                new PrioritisedQueueConsumerNodeIterator(_notified);
        if(notifiedIterator.hasNext())
        {
            final QueueConsumerNode queueConsumerNode = notifiedIterator.next();
            return queueConsumerNode.getQueueConsumer().getPriority();
        }
        else
        {
            return Integer.MIN_VALUE;
        }
    }

    QueueConsumerNodeListEntry addNodeToInterestList(final QueueConsumerNode queueConsumerNode)
    {
        QueueConsumerNodeListEntry newListEntry;
        switch (queueConsumerNode.getState())
        {
            case INTERESTED:
                newListEntry = null;
                for (PriorityConsumerListPair pair : _interested)
                {
                    if (pair._priority == queueConsumerNode.getQueueConsumer().getPriority())
                    {
                        newListEntry = pair._consumers.add(queueConsumerNode);
                        break;
                    }
                }
                break;
            case NOT_INTERESTED:
                newListEntry = _notInterested.add(queueConsumerNode);
                break;
            case NOTIFIED:
                newListEntry = null;
                for (PriorityConsumerListPair pair : _notified)
                {
                    if (pair._priority == queueConsumerNode.getQueueConsumer().getPriority())
                    {
                        newListEntry = pair._consumers.add(queueConsumerNode);
                        break;
                    }
                }
                break;
            case NON_ACQUIRING:
                newListEntry = _nonAcquiring.add(queueConsumerNode);
                break;
            default:
                newListEntry = null;
                break;
        }
        return newListEntry;
    }

    private static class QueueConsumerIterator implements Iterator<QueueConsumer<?,?>>
    {
        private final Iterator<QueueConsumerNode> _underlying;

        private QueueConsumerIterator(final Iterator<QueueConsumerNode> underlying)
        {
            _underlying = underlying;
        }

        @Override
        public boolean hasNext()
        {
            return _underlying.hasNext();
        }

        @Override
        public QueueConsumer<?,?> next()
        {
            return _underlying.next().getQueueConsumer();
        }

        @Override
        public void remove()
        {
            _underlying.remove();
        }
    }

    private void addToAll(final QueueConsumerNode consumerNode)
    {
        int consumerPriority = consumerNode.getQueueConsumer().getPriority();
        int i;
        for (i = 0; i < _allConsumers.size(); ++i)
        {
            final PriorityConsumerListPair priorityConsumerListPair = _allConsumers.get(i);
            if (priorityConsumerListPair._priority == consumerPriority)
            {
                final QueueConsumerNodeListEntry entry = priorityConsumerListPair._consumers.add(consumerNode);
                consumerNode.setAllEntry(entry);
                return;
            }
            else if (priorityConsumerListPair._priority < consumerPriority)
            {
                break;
            }
        }

        PriorityConsumerListPair newPriorityConsumerListPair = new PriorityConsumerListPair(consumerPriority);
        final QueueConsumerNodeListEntry entry = newPriorityConsumerListPair._consumers.add(consumerNode);
        consumerNode.setAllEntry(entry);
        _allConsumers.add(i, newPriorityConsumerListPair);
        _notified.add(i, new PriorityConsumerListPair(consumerPriority));
        _interested.add(i, new PriorityConsumerListPair(consumerPriority));
    }

    private void removeFromAll(final QueueConsumer<?,?> consumer)
    {
        final QueueConsumerNode node = consumer.getQueueConsumerNode();
        int consumerPriority = consumer.getPriority();
        for (int i = 0; i < _allConsumers.size(); ++i)
        {
            final PriorityConsumerListPair priorityConsumerListPair = _allConsumers.get(i);
            if (priorityConsumerListPair._priority == consumerPriority)
            {
                priorityConsumerListPair._consumers.removeEntry(node.getAllEntry());
                if (priorityConsumerListPair._consumers.isEmpty())
                {
                    _allConsumers.remove(i);
                    _notified.remove(i);
                    _interested.remove(i);
                }
                return;
            }
            else if (priorityConsumerListPair._priority < consumerPriority)
            {
                break;
            }
        }
    }


    private class PriorityConsumerListPair
    {
        final int _priority;
        final QueueConsumerNodeList _consumers;

        private PriorityConsumerListPair(final int priority)
        {
            _priority = priority;
            _consumers = new QueueConsumerNodeList(_queue);
        }
    }

    private class PrioritisedQueueConsumerNodeIterator implements Iterator<QueueConsumerNode>
    {
        final Iterator<PriorityConsumerListPair> _outerIterator;
        Iterator<QueueConsumerNode> _innerIterator;

        private PrioritisedQueueConsumerNodeIterator(List<PriorityConsumerListPair> list)
        {
            _outerIterator = list.iterator();
            _innerIterator = Collections.emptyIterator();
        }

        @Override
        public boolean hasNext()
        {
            while (true)
            {
                if (_innerIterator.hasNext())
                {
                    return true;
                }
                else if (_outerIterator.hasNext())
                {
                    final PriorityConsumerListPair priorityConsumersPair = _outerIterator.next();
                    _innerIterator = priorityConsumersPair._consumers.iterator();
                }
                else
                {
                    return false;
                }
            }
        }

        @Override
        public QueueConsumerNode next()
        {
            try
            {
                return _innerIterator.next();
            }
            catch (NoSuchElementException e)
            {
                hasNext();
                return _innerIterator.next();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
