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

import java.util.List;

import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

abstract class QueueSizeLimitRespectingTransaction extends QueueEntryTransaction
{
    private final Queue _destinationQueue;
    private long _pendingQueueDepthBytes;
    private long _pendingQueueDepthMessages;

    QueueSizeLimitRespectingTransaction(Queue sourceQueue,
                                        List<Long> messageIds,
                                        Queue destinationQueue,
                                        final MessageFilter filter,
                                        final int limit)
    {
        super(sourceQueue, messageIds, filter, limit);
        _destinationQueue = destinationQueue;
    }

    @Override
    protected boolean updateEntry(QueueEntry entry, QueueManagingVirtualHost.Transaction txn)
    {
        ServerMessage message = entry.getMessage();
        _pendingQueueDepthMessages++;
        _pendingQueueDepthBytes += message == null ? 0 : message.getSizeIncludingHeader();
        boolean underfull = isUnderfull();
        if (message != null && !message.isReferenced(_destinationQueue) && underfull)
        {
            performOperation(entry, txn, _destinationQueue);
        }

        return !underfull;
    }

    abstract void performOperation(final QueueEntry entry,
                                   final QueueManagingVirtualHost.Transaction txn,
                                   final Queue destinationQueue);

    private boolean isUnderfull()
    {
        return _destinationQueue.getOverflowPolicy() == OverflowPolicy.NONE ||
               ((_destinationQueue.getMaximumQueueDepthBytes() < 0
                 || _destinationQueue.getQueueDepthBytes() + _pendingQueueDepthBytes
                    <= _destinationQueue.getMaximumQueueDepthBytes())
                && (_destinationQueue.getMaximumQueueDepthMessages() < 0
                    || _destinationQueue.getQueueDepthMessages() + _pendingQueueDepthMessages
                       <= _destinationQueue.getMaximumQueueDepthMessages()));
    }
}
