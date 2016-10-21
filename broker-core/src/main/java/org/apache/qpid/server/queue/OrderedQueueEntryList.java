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

import org.apache.qpid.server.message.BaseMessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;

public abstract class OrderedQueueEntryList extends OrderedBaseQueueEntryList<AbstractQueue<?>>
{
    public OrderedQueueEntryList(final AbstractQueue<?> queue,
                                 final HeadCreator headCreator)
    {
        super(queue, headCreator);
    }

    @Override
    public void onAcquiredByConsumer(final QueueEntry queueEntry, final MessageInstanceConsumer consumer)
    {
        getQueue().incrementUnackedMsgCount(queueEntry);
    }

    @Override
    public void onNoLongerAcquiredByConsumer(final QueueEntry queueEntry)
    {
        getQueue().decrementUnackedMsgCount(queueEntry);
    }

    @Override
    public void requeue(final QueueEntry queueEntry)
    {
        getQueue().requeue(queueEntry);
    }

    @Override
    public boolean isHeld(final QueueEntry queueEntry, final long evaluationTime)
    {
        return getQueue().isHeld(queueEntry, evaluationTime);
    }

    @Override
    public void dequeue(final QueueEntry queueEntry)
    {
        getQueue().dequeue(queueEntry);
    }

    @Override
    public int routeToAlternate(final QueueEntry queueEntry, final Action<? super BaseMessageInstance> action, ServerTransaction txn)
    {
        return getQueue().routeToAlternate(queueEntry, action, txn);
    }

    @Override
    public int getMaximumDeliveryAttempts()
    {
        return getQueue().getMaximumDeliveryAttempts();
    }
}
