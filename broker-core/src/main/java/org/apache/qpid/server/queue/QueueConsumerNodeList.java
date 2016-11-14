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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.server.model.Queue;

class QueueConsumerNodeList
{
    private final QueueConsumerNodeListEntry _head;

    private final AtomicReference<QueueConsumerNodeListEntry> _tail;
    private final AtomicInteger _size = new AtomicInteger();
    private final AtomicInteger _scavengeCount = new AtomicInteger();
    private final int _scavengeCountThreshold;

    QueueConsumerNodeList(final Queue<?> queue)
    {
        _head = new QueueConsumerNodeListEntry(this);
        _tail = new AtomicReference<>(_head);
        _scavengeCountThreshold = queue.getContextValue(Integer.class, QUEUE_SCAVANGE_COUNT);
    }

    private void insert(final QueueConsumerNodeListEntry node, final boolean count)
    {
        for (;;)
        {
            QueueConsumerNodeListEntry tail = _tail.get();
            QueueConsumerNodeListEntry next = tail.nextNode();
            if (tail == _tail.get())
            {
                if (next == null)
                {
                    if (tail.setNext(node))
                    {
                        _tail.compareAndSet(tail, node);
                        if(count)
                        {
                            _size.incrementAndGet();
                        }
                        return;
                    }
                }
                else
                {
                    _tail.compareAndSet(tail, next);
                }
            }
        }
    }

    public QueueConsumerNodeListEntry add(final QueueConsumerNode node)
    {
        QueueConsumerNodeListEntry entry = new QueueConsumerNodeListEntry(this, node);
        insert(entry, true);
        return entry;
    }

    boolean removeEntry(final QueueConsumerNodeListEntry entry)
    {
        if (entry.setDeleted())
        {
            _size.decrementAndGet();
            if (_scavengeCount.incrementAndGet() > _scavengeCountThreshold)
            {
                scavenge();
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    private void scavenge()
    {
        _scavengeCount.set(0);
        QueueConsumerNodeListEntry node = _head;
        while (node != null)
        {
            node = node.findNext();
        }
    }

    public QueueConsumerNodeIterator iterator()
    {
        return new QueueConsumerNodeIterator(this);
    }

    public QueueConsumerNodeListEntry getHead()
    {
        return _head;
    }

    public int size()
    {
        return _size.get();
    }

    public boolean isEmpty()
    {
        return _size.get() == 0;
    }
}
