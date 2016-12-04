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

import java.util.Collection;

final class QueueConsumerNode
{
    private final QueueConsumerManagerImpl _queueConsumerManager;
    private final QueueConsumer<?,?> _queueConsumer;
    private QueueConsumerNodeListEntry _listEntry;
    private QueueConsumerManagerImpl.NodeState _state = QueueConsumerManagerImpl.NodeState.REMOVED;
    private QueueConsumerNodeListEntry _allEntry;

    QueueConsumerNode(final QueueConsumerManagerImpl queueConsumerManager, final QueueConsumer<?,?> queueConsumer)
    {
        _queueConsumerManager = queueConsumerManager;
        _queueConsumer = queueConsumer;
    }

    public QueueConsumer<?,?> getQueueConsumer()
    {
        return _queueConsumer;
    }

    public QueueConsumerManagerImpl.NodeState getState()
    {
        return _state;
    }

    public synchronized boolean moveFromTo(Collection<QueueConsumerManagerImpl.NodeState> fromStates,
                                           QueueConsumerManagerImpl.NodeState toState)
    {
        if (fromStates.contains(_state))
        {
            if (_listEntry != null)
            {
                _listEntry.remove();
            }
            _state = toState;
            _listEntry = _queueConsumerManager.addNodeToInterestList(this);
            return true;
        }
        else
        {
            return false;
        }
    }

    public QueueConsumerNodeListEntry getAllEntry()
    {
        return _allEntry;
    }

    public void setAllEntry(final QueueConsumerNodeListEntry allEntry)
    {
        _allEntry = allEntry;
    }
}
