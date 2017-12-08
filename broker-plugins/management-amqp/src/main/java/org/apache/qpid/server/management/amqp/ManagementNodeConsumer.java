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
package org.apache.qpid.server.management.amqp;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.Action;

class ManagementNodeConsumer<T extends ConsumerTarget> implements MessageInstanceConsumer<T>, MessageDestination,
                                                                  BaseQueue
{
    private final ManagementNode _managementNode;
    private final List<ManagementResponse> _queue = Collections.synchronizedList(new ArrayList<ManagementResponse>());
    private final T _target;
    private final String _name;
    private final UUID _identifier = UUID.randomUUID();


    public ManagementNodeConsumer(final String consumerName, final ManagementNode managementNode, T target)
    {
        _name = consumerName;
        _managementNode = managementNode;
        _target = target;
    }

    @Override
    public void externalStateChange()
    {
        if(!_queue.isEmpty())
        {
            _target.notifyWork();
        }
    }

    @Override
    public Object getIdentifier()
    {
        return _identifier;
    }

    @Override
    public MessageContainer pullMessage()
    {
        if (!_queue.isEmpty())
        {

            final ManagementResponse managementResponse = _queue.get(0);
            if (!_target.isSuspended() && _target.allocateCredit(managementResponse.getMessage()))
            {
                _queue.remove(0);
                return new MessageContainer(managementResponse, managementResponse.getMessageReference());
            }
        }
        else
        {
            _target.noMessagesAvailable();
        }
        return null;
    }

    @Override
    public void setNotifyWorkDesired(final boolean desired)
    {
        if (desired && !_queue.isEmpty())
        {
            _target.notifyWork();
        }
    }

    AMQPSession<?,?> getSession()
    {
        return _target.getSession();
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    @Override
    public boolean acquires()
    {
        return true;
    }

    @Override
    public void close()
    {
        _queue.forEach(ManagementResponse::delete);
        _managementNode.unregisterConsumer(this);
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _managementNode.getAddressSpace();
    }

    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {
        _managementNode.authorisePublish(token, arguments);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public UUID getId()
    {
        return _identifier;
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.NEVER;
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                               final String routingAddress,
                                                                                               final InstanceProperties instanceProperties)
    {
        RoutingResult<M> result = new RoutingResult<>(message);
        result.addQueue(this);
        return result;
    }

    @Override
    public boolean isDurable()
    {
        return false;
    }

    @Override
    public void linkAdded(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public void linkRemoved(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public MessageDestination getAlternateBindingDestination()
    {
        return null;
    }

    @Override
    public void removeReference(final DestinationReferrer destinationReferrer)
    {
    }

    @Override
    public void addReference(final DestinationReferrer destinationReferrer)
    {
    }

    @Override
    public T getTarget()
    {
        return _target;
    }

    ManagementNode getManagementNode()
    {
        return _managementNode;
    }

    private void send(ManagementResponse responseEntry)
    {
        _queue.add(responseEntry);
        _target.notifyWork();
    }

    @Override
    public void enqueue(final ServerMessage message,
                        final Action<? super MessageInstance> action,
                        final MessageEnqueueRecord record)
    {
        final InternalMessage internalMessage = (InternalMessage) message;
        final ManagementResponse responseEntry = new ManagementResponse(this, internalMessage);

        send(responseEntry);
        if(action != null)
        {
            action.performAction(responseEntry);
        }
    }

    @Override
    public boolean isDeleted()
    {
        return isClosed();
    }
}
