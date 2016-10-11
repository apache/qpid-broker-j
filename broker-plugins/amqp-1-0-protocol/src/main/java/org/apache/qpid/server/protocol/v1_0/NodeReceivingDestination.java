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

import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.ServerTransaction;

public class NodeReceivingDestination implements ReceivingDestination
{
    private static final Accepted ACCEPTED = new Accepted();
    public static final Rejected REJECTED = new Rejected();
    private static final Outcome[] OUTCOMES = { ACCEPTED, REJECTED};
    private final boolean _discardUnroutable;

    private MessageDestination _destination;
    private TerminusDurability _durability;
    private TerminusExpiryPolicy _expiryPolicy;
    private final String _address;

    public NodeReceivingDestination(MessageDestination destination,
                                    TerminusDurability durable,
                                    TerminusExpiryPolicy expiryPolicy,
                                    final String address, final Symbol[] capabilities)
    {
        _destination = destination;
        _durability = durable;
        _expiryPolicy = expiryPolicy;
        _address = address;
        _discardUnroutable = destination instanceof Exchange
                             && ((capabilities != null && Arrays.asList(capabilities).contains(DISCARD_UNROUTABLE))
                                 || ((Exchange)destination).getUnroutableMessageBehaviour() == Exchange.UnroutableMessageBehaviour.DISCARD);

    }

    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    public Outcome send(final Message_1_0 message, ServerTransaction txn)
    {
        final InstanceProperties instanceProperties =
            new InstanceProperties()
            {

                @Override
                public Object getProperty(final Property prop)
                {
                    switch(prop)
                    {
                        case MANDATORY:
                            return false;
                        case REDELIVERED:
                            return false;
                        case PERSISTENT:
                            return message.isPersistent();
                        case IMMEDIATE:
                            return false;
                        case EXPIRATION:
                            return message.getExpiration();
                    }
                    return null;
                }};

        String routingAddress;
        routingAddress = getRoutingAddress(message);

        int enqueues = _destination.send(message, routingAddress, instanceProperties, txn, null);


        return enqueues == 0 && !_discardUnroutable ? REJECTED : ACCEPTED;
    }

    @Override
    public String getAddress()
    {
        return _address;
    }

    @Override
    public void authorizePublish(final SecurityToken securityToken, final Message_1_0 message)
    {
            _destination.authorisePublish(securityToken,
                                          Collections.<String, Object>singletonMap("routingKey", getRoutingAddress(message)));

    }

    @Override
    public String getRoutingAddress(final Message_1_0 message)
    {
        MessageMetaData_1_0.MessageHeader_1_0 messageHeader = message.getMessageHeader();
        String routingAddress = messageHeader.getSubject();
        if(routingAddress == null)
        {
            if (messageHeader.getHeader("routing-key") instanceof String)
            {
                routingAddress = (String) messageHeader.getHeader("routing-key");
            }
            else if (messageHeader.getHeader("routing_key") instanceof String)
            {
                routingAddress = (String) messageHeader.getHeader("routing_key");
            }
            else if (messageHeader.getTo() != null
                     && messageHeader.getTo().startsWith(_destination.getName() + "/"))
            {
                routingAddress = messageHeader.getTo().substring(1+_destination.getName().length());
            }
            else
            {
                routingAddress = "";
            }
        }
        return routingAddress;
    }

    TerminusDurability getDurability()
    {
        return _durability;
    }

    TerminusExpiryPolicy getExpiryPolicy()
    {
        return _expiryPolicy;
    }

    public int getCredit()
    {
        // TODO - fix
        return 20000;
    }

    public MessageDestination getDestination()
    {
        return _destination;
    }

    @Override
    public Symbol[] getCapabilities()
    {
        Symbol[] capabilities = new Symbol[1];
        capabilities[0] = _discardUnroutable ? DISCARD_UNROUTABLE : REJECT_UNROUTABLE;
        return capabilities;
    }
}
