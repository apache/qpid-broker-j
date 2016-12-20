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

import java.util.Iterator;
import java.util.NoSuchElementException;

public class QueueConsumerNodeIterator implements Iterator<QueueConsumerNode>
{
    private QueueConsumerNodeListEntry _previous;
    private QueueConsumerNodeListEntry _next;
    private QueueConsumerNode _nextQueueConsumerNode;

    QueueConsumerNodeIterator(QueueConsumerNodeList list)
    {
        _previous = list.getHead();
    }

    @Override
    public boolean hasNext()
    {
        do
        {
            _next = _previous.findNext();
            _nextQueueConsumerNode = _next == null ? null : _next.getQueueConsumerNode();
        }
        while(_next != null && _nextQueueConsumerNode == null);

        return _next != null;
    }

    @Override
    public QueueConsumerNode next()
    {
        if(_next == null)
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
        }
        _previous = _next;
        _next = null;

        QueueConsumerNode node = _nextQueueConsumerNode;
        _nextQueueConsumerNode = null;

        return node;
    }

    @Override
    public void remove()
    {
        // code should use QueueConsumerNodeListEntry#remove instead
        throw new UnsupportedOperationException();
    }
}
