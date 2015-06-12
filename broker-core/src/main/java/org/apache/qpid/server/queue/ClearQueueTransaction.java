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

import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;

public class ClearQueueTransaction implements VirtualHost.TransactionalOperation
{
    private final Queue _queue;

    public ClearQueueTransaction(Queue queue)
    {
        _queue = queue;
    }

    @Override
    public void withinTransaction(final VirtualHost.Transaction txn)
    {
        _queue.visit(new QueueEntryVisitor()
        {

            public boolean visit(final QueueEntry entry)
            {
                final ServerMessage message = entry.getMessage();
                if(message != null)
                {
                    txn.dequeue(entry);
                }
                return false;
            }
        });

    }
}
