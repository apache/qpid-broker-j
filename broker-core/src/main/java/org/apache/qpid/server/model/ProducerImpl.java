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
package org.apache.qpid.server.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.session.AbstractAMQPSession;

// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ProducerImpl<X extends Producer<X>>
        extends AbstractConfiguredObject<ProducerImpl<X>>
        implements Producer<ProducerImpl<X>>
{
    private final String _sessionId;

    private final String _sessionName;

    private final String _principal;

    private final String _remoteAddress;

    private final DeliveryType _deliveryType;

    private final AtomicInteger _messagesOut = new AtomicInteger();

    private final AtomicLong _bytesOut = new AtomicLong();

    private DestinationType _destinationType;

    private String _destination;

    private UUID _destinationId;

    public ProducerImpl(final AbstractAMQPSession<?, ?> session,
                        final PublishingLink publishingLink,
                        final MessageDestination messageDestination)
    {
        super(session, createAttributeMap(publishingLink));
        _sessionId = String.valueOf(session.getId());
        _sessionName = session.getName();
        _principal = session.getAMQPConnection().getPrincipal();
        _remoteAddress = session.getAMQPConnection().getRemoteAddress();
        if (messageDestination == null)
        {
            _deliveryType = DeliveryType.DELAYED_DELIVERY;
        }
        else
        {
            _deliveryType = DeliveryType.STANDARD_DELIVERY;
            _destination = messageDestination.getName();
            _destinationType = DestinationType.from(messageDestination);
            _destinationId = DestinationType.getId(messageDestination);
        }

        registerWithParents();
        open();
    }

    private static Map<String, Object> createAttributeMap(final PublishingLink publishingLink)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ID, UUID.randomUUID());
        attributes.put(NAME, publishingLink.getName());
        attributes.put(DURABLE, false);
        attributes.put(LIFETIME_POLICY, LifetimePolicy.DELETE_ON_SESSION_END);
        attributes.put(STATE, State.ACTIVE);
        return attributes;
    }

    @SuppressWarnings("unused")
    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    private CompletableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteNoChecks()
    {
        return super.deleteNoChecks();
    }

    @Override
    public void registerMessageDelivered(long messageSize)
    {
        _messagesOut.incrementAndGet();
        _bytesOut.addAndGet(messageSize);
    }

    @Override
    public String getSessionId()
    {
        return _sessionId;
    }

    @Override
    public String getSessionName()
    {
        return _sessionName;
    }

    @Override
    public String getPrincipal()
    {
        return _principal;
    }

    @Override
    public String getRemoteAddress()
    {
        return _remoteAddress;
    }

    @Override
    public String getDestination()
    {
        return _destination;
    }

    @Override
    public void setDestination(String destination)
    {
        _destination = destination;
    }

    @Override
    public UUID getDestinationId()
    {
        return _destinationId;
    }

    @Override
    public void setDestinationId(UUID destinationId)
    {
        _destinationId = destinationId;
    }

    @Override
    public DestinationType getDestinationType()
    {
        return _destinationType;
    }

    @Override
    public void setDestinationType(DestinationType destinationType)
    {
        _destinationType = destinationType;
    }

    @Override
    public DeliveryType getDeliveryType()
    {
        return _deliveryType;
    }

    @Override
    public int getMessagesOut()
    {
        return _messagesOut.get();
    }

    @Override
    public long getBytesOut()
    {
        return _bytesOut.get();
    }

    @Override
    public void resetStatistics()
    {
        _bytesOut.set(0);
        _messagesOut.set(0);
    }
}
