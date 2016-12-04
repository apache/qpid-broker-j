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
package org.apache.qpid.server.protocol.v1_0;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.txn.ServerTransaction;

public class QueueDestination extends MessageSourceDestination implements SendingDestination, ReceivingDestination
{
    private static final Accepted ACCEPTED = new Accepted();
    private static final Outcome[] OUTCOMES = new Outcome[] { ACCEPTED };
    private final String _address;
    private final Queue<?> _queue;


    public QueueDestination(Queue<?> queue, final String address)
    {
        super(queue);
        _queue = queue;
        _address = address;
    }

    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    public Outcome send(final Message_1_0 message, ServerTransaction txn)
    {

        txn.enqueue(getQueue(),message, new ServerTransaction.EnqueueAction()
        {
            MessageReference _reference = message.newReference();


            public void postCommit(MessageEnqueueRecord... records)
            {
                try
                {
                    getQueue().enqueue(message, null, records[0]);
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


        return ACCEPTED;
    }

    public int getCredit()
    {
        // TODO - fix
        return 100;
    }

    public Queue<?> getQueue()
    {
        return (Queue<?>) super.getQueue();
    }

    @Override
    public String getRoutingAddress(Message_1_0 message)
    {
        return "";
    }

    @Override
    public void authorizePublish(final SecurityToken securityToken, final Message_1_0 message)
    {

        _queue.authorisePublish(securityToken,
                                Collections.<String,Object>singletonMap("routingKey", getRoutingAddress(message)));


    }

    @Override
    public String getAddress()
    {
        return _address;
    }

    @Override
    public Symbol[] getCapabilities()
    {
        Set<Symbol> capabilities = new HashSet<>(Arrays.asList(super.getCapabilities()));
        if(_queue.isHoldOnPublishEnabled())
        {
            capabilities.add(DELAYED_DELIVERY);
        }
        return capabilities.toArray(new Symbol[capabilities.size()]);
    }
}
