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

import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;

abstract class QueueEntryTransaction implements VirtualHost.TransactionalOperation
{
    private final Queue _sourceQueue;
    private final List _messageIds;

    protected QueueEntryTransaction(Queue sourceQueue, List messageIds)
    {
        _sourceQueue = sourceQueue;
        _messageIds = messageIds;
    }

    @Override
    public void withinTransaction(final VirtualHost.Transaction txn)
    {

        _sourceQueue.visit(new QueueEntryVisitor()
        {

            public boolean visit(final QueueEntry entry)
            {
                final ServerMessage message = entry.getMessage();
                if(message != null)
                {
                    final long messageId = message.getMessageNumber();
                    if (_messageIds.remove(messageId) || (messageId <= (long) Integer.MAX_VALUE
                                                          && _messageIds.remove(Integer.valueOf((int)messageId))))
                    {
                        updateEntry(entry, txn);
                    }
                }
                return _messageIds.isEmpty();
            }
        });
    }


    protected abstract void updateEntry(QueueEntry entry, VirtualHost.Transaction txn);
}
