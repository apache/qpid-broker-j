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

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageDurability;

abstract class AbstractQueueEntryList implements QueueEntryList
{

    private final boolean _forcePersistent;
    private final boolean _respectPersistent;
    private final Queue<?> _queue;
    private final QueueStatistics _queueStatistics;

    protected AbstractQueueEntryList(final Queue<?> queue, final QueueStatistics queueStatistics)
    {

        final MessageDurability messageDurability = queue.getMessageDurability();
        _queue = queue;
        _queueStatistics = queueStatistics;
        _forcePersistent = messageDurability == MessageDurability.ALWAYS;
        _respectPersistent = messageDurability == MessageDurability.DEFAULT;
    }


    void updateStatsOnEnqueue(QueueEntry entry)
    {
        final long size = entry.getSize();
        final QueueStatistics queueStatistics = _queueStatistics;
        queueStatistics.addToAvailable(size);
        queueStatistics.addToQueue(size);
        queueStatistics.addToEnqueued(size);
        if(_forcePersistent || (_respectPersistent && entry.getMessage().isPersistent()))
        {
            queueStatistics.addToPersistentEnqueued(size);
        }
    }

    public void updateStatsOnStateChange(QueueEntry entry, QueueEntry.EntryState fromState, QueueEntry.EntryState toState)
    {
        final QueueStatistics queueStatistics = _queueStatistics;
        final long size = entry.getSize();

        final boolean isConsumerAcquired = toState instanceof MessageInstance.ConsumerAcquiredState;
        final boolean wasConsumerAcquired = fromState instanceof MessageInstance.ConsumerAcquiredState;

        switch(fromState.getState())
        {
            case AVAILABLE:
                queueStatistics.removeFromAvailable(size);
                break;
            case ACQUIRED:
                if(wasConsumerAcquired && !isConsumerAcquired)
                {
                    queueStatistics.removeFromUnacknowledged(size);
                }
                break;
        }
        switch(toState.getState())
        {
            case AVAILABLE:
                queueStatistics.addToAvailable(size);
                break;
            case ACQUIRED:
                if(isConsumerAcquired && !wasConsumerAcquired)
                {
                    queueStatistics.addToUnacknowledged(size);
                }
                break;
            case DELETED:
                queueStatistics.removeFromQueue(size);
                queueStatistics.addToDequeued(size);
                if(_forcePersistent || (_respectPersistent && entry.getMessage().isPersistent()))
                {
                    queueStatistics.addToPersistentDequeued(size);
                }
                _queue.checkCapacity();

        }
    }

}
