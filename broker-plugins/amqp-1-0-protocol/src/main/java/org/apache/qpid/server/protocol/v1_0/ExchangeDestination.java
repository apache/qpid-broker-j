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

import static org.apache.qpid.server.protocol.v1_0.Session_1_0.DELAYED_DELIVERY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.ServerTransaction;

public class ExchangeDestination extends QueueDestination
{
    private static final Accepted ACCEPTED = new Accepted();
    private static final Rejected REJECTED = new Rejected();
    private static final Outcome[] OUTCOMES = { ACCEPTED, REJECTED};
    public static final Symbol TOPIC_CAPABILITY = Symbol.getSymbol("topic");
    public static final Symbol SHARED_CAPABILITY = Symbol.getSymbol("shared");
    public static final Symbol GLOBAL_CAPABILITY = Symbol.getSymbol("global");

    private final Exchange<?> _exchange;
    private final TerminusDurability _durability;
    private final TerminusExpiryPolicy _expiryPolicy;
    private final String _initialRoutingAddress;
    private final boolean _discardUnroutable;
    private final Symbol[] _capabilities;

    public ExchangeDestination(Exchange<?> exchange,
                               final Queue<?> queue,
                               TerminusDurability durable,
                               TerminusExpiryPolicy expiryPolicy,
                               String address,
                               final String initialRoutingAddress,
                               final List<Symbol> capabilities)
    {
        super(queue, address);
        _exchange = exchange;
        _durability = durable;
        _expiryPolicy = expiryPolicy;
        _discardUnroutable = (capabilities != null && capabilities.contains(DISCARD_UNROUTABLE)) || exchange.getUnroutableMessageBehaviour() == Exchange.UnroutableMessageBehaviour.DISCARD;
        _initialRoutingAddress = initialRoutingAddress;

        List<Symbol> destinationCapabilities = new ArrayList<>(capabilities);
        if (_discardUnroutable)
        {
            destinationCapabilities.add(DISCARD_UNROUTABLE);
        }
        else
        {
            destinationCapabilities.add(REJECT_UNROUTABLE);
        }
        destinationCapabilities.add(TOPIC_CAPABILITY);
        destinationCapabilities.add(DELAYED_DELIVERY);

        _capabilities = destinationCapabilities.toArray(new Symbol[destinationCapabilities.size()]);
    }

    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    public Outcome send(final ServerMessage<?> message, final ServerTransaction txn, final SecurityToken securityToken)
    {
        final String routingAddress = getRoutingAddress(message);
        _exchange.authorisePublish(securityToken, Collections.singletonMap("routingKey", routingAddress));

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

        final RoutingResult result = _exchange.route(message,
                                                     routingAddress,
                                                     instanceProperties);
        int enqueues = result.send(txn, null);

        if(enqueues == 0)
        {
            _exchange.getEventLogger().message(ExchangeMessages.DISCARDMSG(_exchange.getName(), routingAddress));
        }

        return enqueues == 0 && !_discardUnroutable ? createdRejectedOutcome(routingAddress) : ACCEPTED;
    }

    private Outcome createdRejectedOutcome(final String routingAddress)
    {
        Rejected rejected = new Rejected();
        final Error notFoundError = new Error(AmqpError.NOT_FOUND, "Unknown destination '"+routingAddress+'"');
        rejected.setError(notFoundError);
        return rejected;
    }

    @Override
    public MessageDestination getMessageDestination()
    {
        return _exchange;
    }

    private String getRoutingAddress(final ServerMessage<?> message)
    {
        String routingAddress;
        if (_initialRoutingAddress == null)
        {
            return ReceivingDestination.getRoutingAddress(message, _exchange.getName());
        }
        else
        {
            String initialRoutingAddress = message.getInitialRoutingAddress();
            if (initialRoutingAddress.startsWith(_exchange.getName() + "/" + _initialRoutingAddress + "/"))
            {
                routingAddress = initialRoutingAddress.substring(2
                                                                 + _exchange.getName().length()
                                                                 + _initialRoutingAddress.length());
            }
            else
            {
                routingAddress = _initialRoutingAddress;
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

    public Exchange<?> getExchange()
    {
        return _exchange;
    }

    @Override
    public Symbol[] getCapabilities()
    {
        return _capabilities;
    }
}
