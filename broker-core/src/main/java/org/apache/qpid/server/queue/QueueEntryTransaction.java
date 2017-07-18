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

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

abstract class QueueEntryTransaction implements QueueManagingVirtualHost.TransactionalOperation
{
    private final Queue _sourceQueue;
    private final List<Long> _messageIds;
    private final MessageFilter _filter;
    private final List<Long> _modifiedMessageIds = new ArrayList<>();
    private int _limit;

    QueueEntryTransaction(Queue sourceQueue, List<Long> messageIds, final MessageFilter filter, final int limit)
    {
        _sourceQueue = sourceQueue;
        _messageIds = messageIds == null ? null : new ArrayList<>(messageIds);
        _filter = filter;
        _limit = limit;
    }

    @Override
    public final void withinTransaction(final QueueManagingVirtualHost.Transaction txn)
    {
        if(_limit != 0)
        {
            _sourceQueue.visit(new QueueEntryVisitor()
            {

                @Override
                public boolean visit(final QueueEntry entry)
                {
                    final ServerMessage message = entry.getMessage();
                    boolean stop = false;
                    if (message != null)
                    {
                        final long messageId = message.getMessageNumber();
                        if ((_messageIds == null || _messageIds.remove(messageId))
                            && (_filter == null || _filter.matches(entry.asFilterable())))
                        {
                            stop = updateEntry(entry, txn);
                            _modifiedMessageIds.add(messageId);
                            if (_limit > 0)
                            {
                                _limit--;
                            }
                        }
                    }
                    return stop || _limit == 0 || (_messageIds != null && _messageIds.isEmpty());
                }
            });
        }

    }

    protected abstract boolean updateEntry(QueueEntry entry, QueueManagingVirtualHost.Transaction txn);

    @Override
    public final List<Long> getModifiedMessageIds()
    {
        return _modifiedMessageIds;
    }
}
