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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Exchange;
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
import org.apache.qpid.server.util.Action;

public class ExchangeDestination implements ReceivingDestination, SendingDestination
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeDestination.class);
    private static final Accepted ACCEPTED = new Accepted();
    public static final Rejected REJECTED = new Rejected();
    private static final Outcome[] OUTCOMES = { ACCEPTED, REJECTED};
    private final String _address;

    private Exchange<?> _exchange;
    private TerminusDurability _durability;
    private TerminusExpiryPolicy _expiryPolicy;
    private String _initialRoutingAddress;
    private final boolean _discardUnroutable;

    public ExchangeDestination(Exchange<?> exchange,
                               TerminusDurability durable,
                               TerminusExpiryPolicy expiryPolicy,
                               String address,
                               final Symbol[] capabilities)
    {
        _exchange = exchange;
        _durability = durable;
        _expiryPolicy = expiryPolicy;
        _address = address;
        _discardUnroutable = (capabilities != null && Arrays.asList(capabilities).contains(DISCARD_UNROUTABLE)) || exchange.getUnroutableMessageBehaviour() == Exchange.UnroutableMessageBehaviour.DISCARD;


    }

    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    public Outcome send(final ServerMessage<?> message,
                        final String routingAddress,
                        ServerTransaction txn,
                        final Action<MessageInstance> action)
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

        final RoutingResult result = _exchange.route(message,
                                                     routingAddress,
                                                     instanceProperties);
        int enqueues = result.send(txn, action);

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
    public String getAddress()
    {
        return _address;
    }

    @Override
    public void authorizePublish(final SecurityToken securityToken,
                                 final String routingAddress)
    {
        _exchange.authorisePublish(securityToken,
                            Collections.<String,Object>singletonMap("routingKey", routingAddress));


    }

    @Override
    public String getRoutingAddress(final Message_1_0 message)
    {
        String routingAddress;
        MessageMetaData_1_0.MessageHeader_1_0 messageHeader = message.getMessageHeader();
        if(_initialRoutingAddress == null)
        {
            final String to = messageHeader.getTo();
            if (to != null
                && (_exchange.getName() == null || _exchange.getName().trim().equals("")))
            {
                routingAddress = to;
            }
            else if (to != null
                     && to.startsWith(_exchange.getName() + "/"))
            {
                routingAddress = to.substring(1 + _exchange.getName().length());
            }
            else if (to != null && !to.equals(_exchange.getName()))
            {
                routingAddress = to;
            }
            else if (messageHeader.getHeader("routing-key") instanceof String)
            {
                routingAddress = (String) messageHeader.getHeader("routing-key");
            }
            else if (messageHeader.getHeader("routing_key") instanceof String)
            {
                routingAddress = (String) messageHeader.getHeader("routing_key");
            }
            else if (messageHeader.getSubject() != null)
            {
                routingAddress = messageHeader.getSubject();
            }
            else
            {
                routingAddress = "";
            }

        }
        else
        {
            if (messageHeader.getTo() != null
                && messageHeader.getTo().startsWith(_exchange.getName() + "/" + _initialRoutingAddress + "/"))
            {
                routingAddress = messageHeader.getTo().substring(2+_exchange.getName().length()+_initialRoutingAddress.length());
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

    public void setInitialRoutingAddress(final String initialRoutingAddress)
    {
        _initialRoutingAddress = initialRoutingAddress;
    }

    public String getInitialRoutingAddress()
    {
        return _initialRoutingAddress;
    }

    @Override
    public Symbol[] getCapabilities()
    {
        Symbol[] capabilities = new Symbol[2];
        capabilities[0] = _discardUnroutable ? DISCARD_UNROUTABLE : REJECT_UNROUTABLE;
        capabilities[1] = DELAYED_DELIVERY;
        return capabilities;
    }
}
