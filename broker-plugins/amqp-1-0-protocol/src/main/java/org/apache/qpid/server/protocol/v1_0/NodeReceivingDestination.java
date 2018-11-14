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

import java.util.Arrays;
import java.util.Collections;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RejectType;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.TransactionMonitor;

public class NodeReceivingDestination implements ReceivingDestination
{
    private final boolean _discardUnroutable;
    private final EventLogger _eventLogger;

    private final MessageDestination _destination;
    private final TerminusDurability _durability;
    private final TerminusExpiryPolicy _expiryPolicy;
    private final String _address;
    private final String _routingAddress;

    public NodeReceivingDestination(final DestinationAddress destinationAddress,
                                    final TerminusDurability durable,
                                    final TerminusExpiryPolicy expiryPolicy,
                                    final Symbol[] capabilities,
                                    final EventLogger eventLogger)
    {
        _destination = destinationAddress.getMessageDestination();
        _durability = durable;
        _expiryPolicy = expiryPolicy;

        _eventLogger = eventLogger;

        if (_destination instanceof Exchange)
        {
            _discardUnroutable = ((capabilities != null && Arrays.asList(capabilities).contains(DISCARD_UNROUTABLE))
                                     || ((Exchange)_destination).getUnroutableMessageBehaviour() == Exchange.UnroutableMessageBehaviour.DISCARD);
            _routingAddress = destinationAddress.getRoutingKey();
            _address = _destination.getName();
        }
        else
        {
            _discardUnroutable = false;
            _routingAddress = "";
            _address = destinationAddress.getRoutingAddress();
        }
    }

    @Override
    public void send(final ServerMessage<?> message,
                     final ServerTransaction txn,
                     final SecurityToken securityToken) throws UnroutableMessageException
    {
        final String routingAddress = "".equals(_routingAddress) ? getRoutingAddress(message) : _routingAddress;
        _destination.authorisePublish(securityToken, Collections.singletonMap("routingKey", routingAddress));

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

        final RoutingResult<? extends ServerMessage<? extends StorableMessageMetaData>> result =
                _destination.route(message, routingAddress, instanceProperties);
        final int enqueues = result.send(txn, null);

        if (enqueues == 0)
        {
            if (!_discardUnroutable)
            {
                final String errorMessage;
                final AmqpError errorCode;
                if (result.isRejected())
                {
                    if (result.containsReject(RejectType.LIMIT_EXCEEDED))
                    {
                        errorCode = AmqpError.RESOURCE_LIMIT_EXCEEDED;
                    }
                    else if (result.containsReject(RejectType.PRECONDITION_FAILED))
                    {
                        errorCode = AmqpError.PRECONDITION_FAILED;
                    }
                    else
                    {
                        errorCode = AmqpError.ILLEGAL_STATE;
                    }
                    errorMessage = result.getRejectReason();
                }
                else
                {
                    errorCode = AmqpError.NOT_FOUND;
                    errorMessage = String.format("Unknown destination '%s'", routingAddress);
                }
                throw new UnroutableMessageException(errorCode, errorMessage);
            }
            else
            {
                _eventLogger.message(ExchangeMessages.DISCARDMSG(_destination.getName(), routingAddress));
            }
        }
        else
        {
            result.getRoutes()
                  .stream()
                  .filter(q -> q instanceof TransactionMonitor)
                  .map(TransactionMonitor.class::cast)
                  .forEach(tm -> tm.registerTransaction(txn));
        }
    }

    private String getRoutingAddress(final ServerMessage<?> message)
    {
        String initialRoutingAddress = message.getInitialRoutingAddress();
        if (initialRoutingAddress == null || "".equals(initialRoutingAddress))
        {
            initialRoutingAddress = message.getTo() == null ? "" : message.getTo();
        }
        if (_address != null && initialRoutingAddress.startsWith(_address + "/"))
        {
            initialRoutingAddress = initialRoutingAddress.substring(_address.length() + 1);
        }
        return initialRoutingAddress;
    }

    @Override
    public String getAddress()
    {
        return _address;
    }

    @Override
    public MessageDestination getMessageDestination()
    {
        return _destination;
    }

    TerminusDurability getDurability()
    {
        return _durability;
    }

    TerminusExpiryPolicy getExpiryPolicy()
    {
        return _expiryPolicy;
    }

    @Override
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
        Symbol[] capabilities = new Symbol[2];
        capabilities[0] = _discardUnroutable ? DISCARD_UNROUTABLE : REJECT_UNROUTABLE;
        capabilities[1] = DELAYED_DELIVERY;
        return capabilities;
    }
}
