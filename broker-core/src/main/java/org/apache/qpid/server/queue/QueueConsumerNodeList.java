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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class QueueConsumerNodeList
{
    private final QueueConsumerNodeListEntry _head = new QueueConsumerNodeListEntry();

    private final AtomicReference<QueueConsumerNodeListEntry> _tail = new AtomicReference<>(_head);
    private final AtomicInteger _size = new AtomicInteger();

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

    public void add(final QueueConsumerNode node)
    {
        QueueConsumerNodeListEntry entry = new QueueConsumerNodeListEntry(node);
        insert(entry, true);
    }

    public boolean remove(final QueueConsumerNode consumerNode)
    {
        return removeEntry(consumerNode.getListEntry());
    }

    boolean removeEntry(final QueueConsumerNodeListEntry entry)
    {
        if (entry.delete())
        {
            _size.decrementAndGet();
            return true;
        }
        else
        {
            return false;
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
}



