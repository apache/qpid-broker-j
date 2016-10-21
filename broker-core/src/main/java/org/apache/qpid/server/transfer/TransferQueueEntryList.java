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
package org.apache.qpid.server.transfer;

import org.apache.qpid.server.message.BaseMessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.OrderedBaseQueueEntryList;
import org.apache.qpid.server.queue.OrderedQueueEntry;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryList;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;

public class TransferQueueEntryList extends OrderedBaseQueueEntryList<TransferQueue>
{

    private static final HeadCreator HEAD_CREATOR = new HeadCreator()
    {
        @Override
        public TransferQueueEntry createHead(final QueueEntryList list)
        {
            return new TransferQueueEntry((TransferQueueEntryList) list);
        }
    };

    public TransferQueueEntryList(final TransferQueue queue)
    {
        super(queue, HEAD_CREATOR);
    }


    @Override
    protected OrderedQueueEntry createQueueEntry(final ServerMessage<?> message,
                                                 final MessageEnqueueRecord enqueueRecord)
    {
        return new TransferQueueEntry(this, message, enqueueRecord);
    }

    @Override
    public void onAcquiredByConsumer(final QueueEntry queueEntry, final MessageInstanceConsumer consumer)
    {

    }

    @Override
    public void onNoLongerAcquiredByConsumer(final QueueEntry queueEntry)
    {

    }

    @Override
    public void requeue(final QueueEntry queueEntry)
    {

    }

    @Override
    public boolean isHeld(final QueueEntry queueEntry, final long evaluationTime)
    {
        return false;
    }

    @Override
    public void dequeue(final QueueEntry queueEntry)
    {

    }

    @Override
    public int routeToAlternate(final QueueEntry queueEntry,
                                final Action<? super BaseMessageInstance> action,
                                final ServerTransaction txn)
    {
        return 0;
    }

    @Override
    public int getMaximumDeliveryAttempts()
    {
        return 0;
    }
}
