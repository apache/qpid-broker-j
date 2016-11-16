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
package org.apache.qpid.server.virtualhost;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.queue.AbstractQueue;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;

public abstract class AbstractSystemMessageSource implements MessageSource
{
    protected final UUID _id;
    protected final String _name;
    protected final NamedAddressSpace _addressSpace;
    private List<Consumer> _consumers = new CopyOnWriteArrayList<>();

    public AbstractSystemMessageSource(String name, final NamedAddressSpace addressSpace)
    {
        _name = name;
        _id = UUID.nameUUIDFromBytes((getClass().getSimpleName() + "/" + addressSpace.getName() + "/" + name).getBytes(
                StandardCharsets.UTF_8));
        _addressSpace = addressSpace;
    }

    @Override
    public String getName()
    {
        return _name;
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
    public Consumer addConsumer(final ConsumerTarget target,
                                final FilterManager filters,
                                final Class<? extends ServerMessage> messageClass,
                                final String consumerName,
                                final EnumSet<ConsumerImpl.Option> options, final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {
        final Consumer consumer = new Consumer(consumerName, target);
        target.consumerAdded(consumer);
        _consumers.add(consumer);
        return consumer;
    }

    @Override
    public Collection<Consumer> getConsumers()
    {
        return new ArrayList<>(_consumers);
    }

    @Override
    public boolean verifySessionAccess(final AMQSessionModel<?> session)
    {
        return true;
    }

    protected class Consumer implements ConsumerImpl
    {

        private final long _id = ConsumerImpl.CONSUMER_NUMBER_GENERATOR.getAndIncrement();
        private final List<PropertiesMessageInstance> _queue =
                Collections.synchronizedList(new ArrayList<PropertiesMessageInstance>());
        private final ConsumerTarget _target;
        private final String _name;


        public Consumer(final String consumerName, ConsumerTarget target)
        {
            _name = consumerName;
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
        public ConsumerTarget getTarget()
        {
            return _target;
        }

        @Override
        public AbstractQueue.MessageContainer pullMessage()
        {
            if (!_queue.isEmpty())
            {
                final PropertiesMessageInstance propertiesMessageInstance = _queue.get(0);
                if (!_target.isSuspended() && _target.allocateCredit(propertiesMessageInstance.getMessage()))
                {
                    _queue.remove(0);
                    return new AbstractQueue.MessageContainer(propertiesMessageInstance, null, false);
                }
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

        @Override
        public long getBytesOut()
        {
            return 0;
        }

        @Override
        public long getMessagesOut()
        {
            return 0;
        }

        @Override
        public long getUnacknowledgedBytes()
        {
            return 0;
        }

        @Override
        public long getUnacknowledgedMessages()
        {
            return 0;
        }

        @Override
        public AMQSessionModel getSessionModel()
        {
            return _target.getSessionModel();
        }

        @Override
        public MessageSource getMessageSource()
        {
            return AbstractSystemMessageSource.this;
        }

        @Override
        public long getConsumerNumber()
        {
            return _id;
        }

        @Override
        public boolean isSuspended()
        {
            return !isActive();
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
        public boolean seesRequeues()
        {
            return false;
        }

        @Override
        public void close()
        {
            _consumers.remove(this);
        }

        @Override
        public boolean isActive()
        {
            return _target.isNotifyWorkDesired();
        }

        @Override
        public String getName()
        {
            return _name;
        }

        public void send(final InternalMessage response)
        {
            _queue.add(new PropertiesMessageInstance(this, response));
            _target.notifyWork();
        }
    }

    class PropertiesMessageInstance implements MessageInstance
    {
        private final Consumer _consumer;
        private int _deliveryCount;
        private boolean _isRedelivered;
        private boolean _isDelivered;
        private boolean _isDeleted;
        private InternalMessage _message;

        PropertiesMessageInstance(final Consumer consumer, final InternalMessage message)
        {
            _consumer = consumer;
            _message = message;
        }

        @Override
        public int getDeliveryCount()
        {
            return 0;
        }

        @Override
        public void incrementDeliveryCount()
        {
            _deliveryCount++;
        }

        @Override
        public void decrementDeliveryCount()
        {
            _deliveryCount--;
        }

        @Override
        public void addStateChangeListener(final StateChangeListener<? super MessageInstance, EntryState> listener)
        {

        }

        @Override
        public boolean removeStateChangeListener(final StateChangeListener<? super MessageInstance, EntryState> listener)
        {
            return false;
        }


        @Override
        public boolean acquiredByConsumer()
        {
            return !isDeleted();
        }

        @Override
        public ConsumerImpl getAcquiringConsumer()
        {
            return _consumer;
        }

        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return null;
        }

        @Override
        public boolean isAcquiredBy(final ConsumerImpl consumer)
        {
            return consumer == _consumer && !isDeleted();
        }

        @Override
        public boolean removeAcquisitionFromConsumer(final ConsumerImpl consumer)
        {
            return consumer == _consumer;
        }

        @Override
        public void setRedelivered()
        {
            _isRedelivered = true;
        }

        @Override
        public boolean isRedelivered()
        {
            return _isRedelivered;
        }

        @Override
        public Consumer getDeliveredConsumer()
        {
            return isDeleted() ? null : _consumer;
        }

        @Override
        public void reject()
        {
            delete();
        }

        @Override
        public boolean isRejectedBy(final ConsumerImpl consumer)
        {
            return false;
        }

        @Override
        public boolean getDeliveredToConsumer()
        {
            return _isDelivered;
        }

        @Override
        public boolean expired()
        {
            return false;
        }

        @Override
        public boolean acquire(final ConsumerImpl sub)
        {
            return false;
        }

        @Override
        public boolean makeAcquisitionUnstealable(final ConsumerImpl consumer)
        {
            return false;
        }

        @Override
        public boolean makeAcquisitionStealable()
        {
            return false;
        }

        @Override
        public int getMaximumDeliveryCount()
        {
            return 0;
        }

        @Override
        public int routeToAlternate(final Action<? super MessageInstance> action,
                                    final ServerTransaction txn)
        {
            return 0;
        }


        @Override
        public Filterable asFilterable()
        {
            return null;
        }

        @Override
        public boolean isAvailable()
        {
            return false;
        }

        @Override
        public boolean acquire()
        {
            return false;
        }

        @Override
        public boolean isAcquired()
        {
            return !isDeleted();
        }

        @Override
        public void release()
        {
            delete();
        }

        @Override
        public void release(ConsumerImpl consumer)
        {
            release();
        }

        @Override
        public void delete()
        {
            _isDeleted = true;
        }

        @Override
        public boolean isDeleted()
        {
            return _isDeleted;
        }

        @Override
        public boolean isHeld()
        {
            return false;
        }

        @Override
        public ServerMessage getMessage()
        {
            return _message;
        }

        @Override
        public InstanceProperties getInstanceProperties()
        {
            return InstanceProperties.EMPTY;
        }

        @Override
        public TransactionLogResource getOwningResource()
        {
            return AbstractSystemMessageSource.this;
        }

    }
}
