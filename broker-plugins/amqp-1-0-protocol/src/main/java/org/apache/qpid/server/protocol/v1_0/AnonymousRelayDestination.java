package org.apache.qpid.server.protocol.v1_0;/*
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

import static org.apache.qpid.server.protocol.v1_0.Session_1_0.DELAYED_DELIVERY;

import java.util.Arrays;
import java.util.Collections;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;

public class AnonymousRelayDestination implements ReceivingDestination
{
    private final Target _target;
    private final NamedAddressSpace _addressSpace;
    private final EventLogger _eventLogger;
    private final boolean _discardUnroutable;

    AnonymousRelayDestination(final NamedAddressSpace addressSpace,
                                     final Target target,
                                     final EventLogger eventLogger)
    {
        _addressSpace = addressSpace;
        _target = target;
        _eventLogger = eventLogger;
        _discardUnroutable = target.getCapabilities() != null && Arrays.asList(target.getCapabilities())
                                                                       .contains(DISCARD_UNROUTABLE);
    }

    @Override
    public Symbol[] getCapabilities()
    {
        return new Symbol[]{DELAYED_DELIVERY};
    }

    @Override
    public Outcome[] getOutcomes()
    {
        return new Outcome[0];
    }

    @Override
    public Outcome send(final ServerMessage<?> message,
                        final ServerTransaction txn,
                        final SecurityToken securityToken,
                        final boolean rejectedOutcomeSupportedBySource,
                        final boolean deliverySettled,
                        final Binary deliveryTag) throws AmqpErrorException
    {
        final ReceivingDestination destination;
        final String routingAddress = message.getTo();
        DestinationAddress destinationAddress = new DestinationAddress(_addressSpace, routingAddress);
        MessageDestination messageDestination = destinationAddress.getMessageDestination();
        if (messageDestination != null)
        {
            destination = new NodeReceivingDestination(destinationAddress,
                                                       _target.getDurable(),
                                                       _target.getExpiryPolicy(),
                                                       _target.getCapabilities(),
                                                       _eventLogger);
        }
        else
        {
            destination = null;
        }

        final Outcome outcome;
        if (destination == null)
        {
            if (_discardUnroutable)
            {
                _eventLogger.message(ExchangeMessages.DISCARDMSG("", routingAddress));
                outcome = new Accepted();
            }
            else
            {
                final Error notFoundError = new Error(AmqpError.NOT_FOUND,
                                                      String.format("Unknown destination '%s'", routingAddress));
                notFoundError.setInfo(Collections.singletonMap(DELIVERY_TAG, deliveryTag));

                // If the source of the link does not support the rejected outcome,
                // or the message has already been settled by the sender,
                // then the routing node MUST detach the link with an error.
                // AMQP-140: When pre-settled messages are being sent within a transaction,
                // then the behaviour defined for transactions should take precedence
                // (essentially marking the transaction as rollback only).
                if (!rejectedOutcomeSupportedBySource || (deliverySettled && !(txn instanceof LocalTransaction)))
                {
                    throw new AmqpErrorException(notFoundError);
                }
                else
                {
                    if (deliverySettled && txn instanceof LocalTransaction)
                    {
                        ((LocalTransaction) txn).setRollbackOnly();
                    }

                    Rejected rejected = new Rejected();
                    rejected.setError(notFoundError);
                    outcome = rejected;
                }
            }
        }
        else
        {
            outcome = destination.send(message,
                                       txn,
                                       securityToken,
                                       rejectedOutcomeSupportedBySource,
                                       deliverySettled,
                                       deliveryTag);
        }
        return outcome;
    }

    @Override
    public int getCredit()
    {
        // TODO - fix
        return 20000;
    }

    @Override
    public String getAddress()
    {
        return "";
    }

    @Override
    public MessageDestination getMessageDestination()
    {
        return null;
    }
}
