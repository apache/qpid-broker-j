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
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class OutboundTransferDestination implements MessageDestination
{
    private final VirtualHost<?> _virtualHost;
    private final String _address;

    public OutboundTransferDestination(final VirtualHost<?> virtualHost, final String address)
    {
        _virtualHost = virtualHost;
        _address = address;
    }

    @Override
    public String getName()
    {
        return "$transfer";
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> int send(final M message,
                                                                                 final String routingAddress,
                                                                                 final InstanceProperties instanceProperties,
                                                                                 final ServerTransaction txn,
                                                                                 final Action<? super BaseMessageInstance> postEnqueueAction)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }


        final TransferQueue transferQueue = _virtualHost.getTransferQueue();
        txn.enqueue(transferQueue, message, new ServerTransaction.EnqueueAction()
        {
            MessageReference _reference = message.newReference();

            public void postCommit(MessageEnqueueRecord... records)
            {
                try
                {
                    for (final MessageEnqueueRecord record : records)
                    {
                        transferQueue.enqueue(message, postEnqueueAction, record);
                    }
                }
                finally
                {
                    _reference.release();
                }
            }

            public void onRollback()
            {
                _reference.release();
            }
        });
        return 1;

    }
}
