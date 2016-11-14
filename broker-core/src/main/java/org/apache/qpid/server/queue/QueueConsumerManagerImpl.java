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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class QueueConsumerManagerImpl implements QueueConsumerManager
{
    private final AbstractQueue<?> _queue;

    private final ConcurrentLinkedDeque<QueueConsumerNode> _interested = new ConcurrentLinkedDeque<QueueConsumerNode>();
    private final ConcurrentLinkedDeque<QueueConsumerNode> _notInterested = new ConcurrentLinkedDeque<QueueConsumerNode>();
    private final ConcurrentLinkedDeque<QueueConsumerNode> _notified = new ConcurrentLinkedDeque<QueueConsumerNode>();
    private final ConcurrentLinkedDeque<QueueConsumerNode> _nonAcquiring = new ConcurrentLinkedDeque<QueueConsumerNode>();

    private final Set<QueueConsumer<?>> _allConsumers = Collections.newSetFromMap(new ConcurrentHashMap<QueueConsumer<?>, Boolean>());

    private volatile int _count;

    public QueueConsumerManagerImpl(final AbstractQueue<?> queue)
    {
        _queue = queue;
    }

    // Always in the config thread
    @Override
    public void addConsumer(final QueueConsumer<?> consumer)
    {
        _allConsumers.add(consumer);
        QueueConsumerNode node = new QueueConsumerNode(consumer);
        consumer.setQueueConsumerNode(node);
        if(consumer.isNotifyWorkDesired())
        {
            if (consumer.acquires())
            {
                _interested.add(node);
            }
            else
            {
                _nonAcquiring.add(node);
            }
        }
        else
        {
            _notInterested.add(node);
        }
        _count++;
    }

    // Always in the config thread
    @Override
    public boolean removeConsumer(final QueueConsumer<?> consumer)
    {
        _allConsumers.remove(consumer);
        QueueConsumerNode node = consumer.getQueueConsumerNode();
        while(!node.isRemoved())
        {
            boolean removed = _notInterested.remove(node);
            if(!removed)
            {
                if(consumer.acquires())
                {
                    removed = _interested.remove(node);
                    removed = removed || _notified.remove(node);
                }
                else
                {
                    removed = _nonAcquiring.remove(node);
                }
            }

            if(removed)
            {
                node.setRemoved();
                _count--;
                return true;
            }
        }
        return false;
    }

    // Set by the consumer always in the IO thread
    @Override
    public void setInterest(final QueueConsumer consumer, final boolean interested)
    {
        QueueConsumerNode node = consumer.getQueueConsumerNode();
        if(interested)
        {
            if(_notInterested.remove(node))
            {
                if(consumer.acquires())
                {
                    _interested.add(node);
                }
                else
                {
                    _nonAcquiring.add(node);
                }
            }
        }
        else
        {
            if(consumer.acquires())
            {
                while(!node.isRemoved())
                {
                    if(_interested.remove(node) || _notified.remove(node))
                    {
                        _notInterested.add(node);
                        break;
                    }
                }
            }
            else
            {
                if(_nonAcquiring.remove(node))
                {
                    _notInterested.add(node);
                }
            }
        }
    }

    // Set by the Queue any IO thread
    @Override
    public boolean setNotified(final QueueConsumer consumer, final boolean notified)
    {
        QueueConsumerNode node = consumer.getQueueConsumerNode();
        if(consumer.acquires())
        {
            if(notified)
            {
                // TODO - Fix responsibility
                QueueEntry queueEntry;
                if((queueEntry = _queue.getNextAvailableEntry(consumer)) != null
                   && _queue.noHigherPriorityWithCredit(consumer, queueEntry)
                   && _interested.remove(node))
                {
                    _notified.add(node);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                if(_notified.remove(node))
                {
                    _interested.add(node);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            return true;
        }
    }

    @Override
    public Iterator<QueueConsumer<?>> getInterestedIterator()
    {
        return new QueueConsumerIterator(_interested.iterator());
    }

    @Override
    public Iterator<QueueConsumer<?>> getAllIterator()
    {
        return _allConsumers.iterator();
    }

    @Override
    public Iterator<QueueConsumer<?>> getNonAcquiringIterator()
    {
        return new QueueConsumerIterator(_nonAcquiring.iterator());
    }

    @Override
    public Iterator<QueueConsumer<?>> getPrioritySortedNotifiedOrInterestedIterator()
    {
        return null;
    }

    @Override
    public int getAllSize()
    {
        return _count;
    }

    @Override
    public int getNotifiedAcquiringSize()
    {
        return _notified.size();
    }

    private static class QueueConsumerIterator implements Iterator<QueueConsumer<?>>
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
        public QueueConsumer<?> next()
        {
            return _underlying.next().getQueueConsumer();
        }

        @Override
        public void remove()
        {
            _underlying.remove();
        }
    }
}
