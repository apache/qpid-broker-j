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

import java.util.function.Predicate;

import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;

class ManagementResponse implements MessageInstance
{
    private final ManagementNodeConsumer _consumer;
    private final MessageReference _messageReference;
    private int _deliveryCount;
    private boolean _isRedelivered;
    private boolean _isDelivered;
    private boolean _isDeleted;
    private InternalMessage _message;

    ManagementResponse(final ManagementNodeConsumer consumer,
                       final InternalMessage message)
    {
        _consumer = consumer;
        _message = message;
        _messageReference = _message.newReference(consumer);
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
    public ManagementNodeConsumer getAcquiringConsumer()
    {
        return _consumer;
    }

    @Override
    public MessageEnqueueRecord getEnqueueRecord()
    {
        return null;
    }

    @Override
    public boolean isAcquiredBy(final MessageInstanceConsumer<?> consumer)
    {
        return consumer == _consumer && !isDeleted();
    }

    @Override
    public boolean removeAcquisitionFromConsumer(final MessageInstanceConsumer<?> consumer)
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
    public void reject(final MessageInstanceConsumer<?> consumer)
    {
        delete();
    }

    @Override
    public boolean isRejectedBy(final MessageInstanceConsumer<?> consumer)
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
    public boolean acquire(final MessageInstanceConsumer<?> consumer)
    {
        return false;
    }

    @Override
    public boolean makeAcquisitionUnstealable(final MessageInstanceConsumer<?> consumer)
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
                                final ServerTransaction txn,
                                final Predicate<BaseQueue> predicate)
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
    public void release(final MessageInstanceConsumer<?> consumer)
    {
        release();
    }

    @Override
    public void delete()
    {
        _messageReference.release();
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
    public boolean isPersistent()
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
        return _consumer.getManagementNode();
    }

    public MessageReference getMessageReference()
    {
        return _messageReference;
    }
}
