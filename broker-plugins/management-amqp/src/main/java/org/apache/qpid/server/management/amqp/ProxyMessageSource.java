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
import java.security.AccessController;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.StorableMessageMetaData;

public class ProxyMessageSource implements MessageSource, MessageDestination
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyMessageSource.class);
    private final String _name;
    private final UUID _id = UUID.randomUUID();
    private final ManagementAddressSpace _managementAddressSpace;
    private volatile MessageInstanceConsumer<?> _consumer;
    private final AtomicBoolean _consumerSet = new AtomicBoolean(false);
    private Object _connectionReference;

    public ProxyMessageSource(final ManagementAddressSpace managementAddressSpace, final Map<String, Object> attributes)
    {
        _name = String.valueOf(attributes.get(ConfiguredObject.NAME));
        _managementAddressSpace = managementAddressSpace;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _managementAddressSpace;
    }

    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {
        throw new AccessControlException("Sending messages to temporary addresses in a management address space is not supported");
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                               final String routingAddress,
                                                                                               final InstanceProperties instanceProperties)
    {
        return new RoutingResult<>(message);
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
    public UUID getId()
    {
        return _id;
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.NEVER;
    }

    @Override
    public <T extends ConsumerTarget<T>> MessageInstanceConsumer<T> addConsumer(final T target,
                                                                                final FilterManager filters,
                                                                                final Class<? extends ServerMessage> messageClass,
                                                                                final String consumerName,
                                                                                final EnumSet<ConsumerOption> options,
                                                                                final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {
        if(_consumerSet.compareAndSet(false,true))
        {
            Subject currentSubject = Subject.getSubject(AccessController.getContext());
            Set<SessionPrincipal> sessionPrincipals = currentSubject.getPrincipals(SessionPrincipal.class);
            if (!sessionPrincipals.isEmpty())
            {
                _connectionReference = sessionPrincipals.iterator().next().getSession().getConnectionReference();

                WrappingTarget<T> wrapper = new WrappingTarget<>(target, _name);
                _managementAddressSpace.getManagementNode().addConsumer(wrapper, filters, messageClass, _name, options, priority);
                final MessageInstanceConsumer<T> consumer = wrapper.getConsumer();
                _consumer = consumer;
                return consumer;
            }
            else
            {
                return null;
            }
        }
        else
        {
            throw new ExistingExclusiveConsumer();
        }
    }

    @Override
    public Collection<? extends MessageInstanceConsumer> getConsumers()
    {
        return _consumer == null ? Collections.<MessageInstanceConsumer>emptySet() : Collections.singleton(_consumer);
    }

    @Override
    public boolean verifySessionAccess(final AMQPSession<?,?> session)
    {
        return session.getConnectionReference() == _connectionReference;
    }

    @Override
    public void close()
    {
    }

    @Override
    public MessageConversionExceptionHandlingPolicy getMessageConversionExceptionHandlingPolicy()
    {
        return MessageConversionExceptionHandlingPolicy.CLOSE;
    }

    private class WrappingTarget<T extends ConsumerTarget<T>> implements ConsumerTarget<WrappingTarget<T>>
    {
        private final T _underlying;
        private final String _address;
        private MessageInstanceConsumer<T> _consumer;

        public WrappingTarget(final T target, String address)
        {
            _underlying = target;
            _address = address;
        }

        public T getUnderlying()
        {
            return _underlying;
        }

        public MessageInstanceConsumer<T> getConsumer()
        {
            return _consumer;
        }

        @Override
        public void acquisitionRemoved(final MessageInstance node)
        {
            _underlying.acquisitionRemoved(node);
        }

        @Override
        public boolean processPending()
        {
            return _underlying.processPending();
        }

        @Override
        public String getTargetAddress()
        {
            return _address;
        }

        @Override
        public boolean isMultiQueue()
        {
            return false;
        }

        @Override
        public void notifyWork()
        {
            _underlying.notifyWork();
        }

        @Override
        public void updateNotifyWorkDesired()
        {
            _underlying.updateNotifyWorkDesired();
        }

        @Override
        public boolean isNotifyWorkDesired()
        {
            return _underlying.isNotifyWorkDesired();
        }

        @Override
        public State getState()
        {
            return _underlying.getState();
        }

        @Override
        public void consumerAdded(final MessageInstanceConsumer<WrappingTarget<T>> sub)
        {
            _consumer = new UnwrappingWrappingConsumer(sub, this);
            _underlying.consumerAdded(_consumer);
        }

        @Override
        public ListenableFuture<Void> consumerRemoved(final MessageInstanceConsumer<WrappingTarget<T>> sub)
        {
            return _underlying.consumerRemoved(_consumer);
        }

        @Override
        public long getUnacknowledgedBytes()
        {
            return _underlying.getUnacknowledgedBytes();
        }

        @Override
        public long getUnacknowledgedMessages()
        {
            return _underlying.getUnacknowledgedMessages();
        }

        @Override
        public AMQPSession getSession()
        {
            return _underlying.getSession();
        }

        @Override
        public void send(final MessageInstanceConsumer consumer,
                         final MessageInstance entry,
                         final boolean batch)
        {
            _underlying.send(_consumer, entry, batch);
        }

        @Override
        public boolean sendNextMessage()
        {
            return _underlying.sendNextMessage();
        }

        @Override
        public void flushBatched()
        {
            _underlying.flushBatched();
        }

        @Override
        public void noMessagesAvailable()
        {
            _underlying.noMessagesAvailable();
        }

        @Override
        public boolean allocateCredit(final ServerMessage msg)
        {
            return _underlying.allocateCredit(msg);
        }

        @Override
        public void restoreCredit(final ServerMessage queueEntry)
        {
            _underlying.restoreCredit(queueEntry);
        }

        @Override
        public boolean isSuspended()
        {
            return _underlying.isSuspended();
        }

        @Override
        public boolean close()
        {
            _managementAddressSpace.removeProxyMessageSource(_connectionReference, _name);
            ProxyMessageSource.this._consumer = null;
            return _underlying.close();
        }

        @Override
        public void queueDeleted(final Queue queue, final MessageInstanceConsumer sub)
        {
            _underlying.queueDeleted(queue, _consumer);
        }
    }
    private static class UnwrappingWrappingConsumer<T extends ConsumerTarget<T>> implements MessageInstanceConsumer<T>
    {
        private final MessageInstanceConsumer<WrappingTarget<T>> _underlying;
        private final WrappingTarget<T> _target;

        public UnwrappingWrappingConsumer(final MessageInstanceConsumer<WrappingTarget<T>> sub, WrappingTarget<T> wrappedTarget)
        {
            _underlying = sub;
            _target = wrappedTarget;
        }

        @Override
        public boolean isClosed()
        {
            return _underlying.isClosed();
        }

        @Override
        public boolean acquires()
        {
            return _underlying.acquires();
        }

        @Override
        public String getName()
        {
            return _underlying.getName();
        }

        @Override
        public void close()
        {
            _underlying.close();
        }

        @Override
        public void externalStateChange()
        {
            _underlying.externalStateChange();
        }

        @Override
        public Object getIdentifier()
        {
            return _underlying.getIdentifier();
        }

        @Override
        public MessageContainer pullMessage()
        {
            return _underlying.pullMessage();
        }

        @Override
        public T getTarget()
        {
            return _target.getUnderlying();
        }

        @Override
        public void setNotifyWorkDesired(final boolean desired)
        {
            _underlying.setNotifyWorkDesired(desired);
        }
    }
}
