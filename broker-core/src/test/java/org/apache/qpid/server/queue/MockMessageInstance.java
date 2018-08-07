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
package org.apache.qpid.server.queue;

import java.util.function.Predicate;

import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;

public class MockMessageInstance implements MessageInstance
{

    private ServerMessage _message;

    @Override
    public boolean acquire()
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
    public boolean acquiredByConsumer()
    {
        return false;
    }

    @Override
    public MessageInstanceConsumer getAcquiringConsumer()
    {
        return null;
    }

    @Override
    public MessageEnqueueRecord getEnqueueRecord()
    {
        return null;
    }

    @Override
    public boolean isAcquiredBy(final MessageInstanceConsumer<?> consumer)
    {
        return false;
    }

    @Override
    public boolean removeAcquisitionFromConsumer(final MessageInstanceConsumer<?> consumer)
    {
        return false;
    }

    @Override
    public void delete()
    {

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
    public boolean isAvailable()
    {
        return false;
    }

    @Override
    public boolean getDeliveredToConsumer()
    {
        return false;
    }

    @Override
    public ServerMessage getMessage()
    {
        return _message;
    }

    public long getSize()
    {
        return 0;
    }

    @Override
    public boolean isAcquired()
    {
        return false;
    }

    @Override
    public void reject(final MessageInstanceConsumer<?> consumer)
    {
    }

    @Override
    public boolean isRejectedBy(final MessageInstanceConsumer<?> consumer)
    {
        return false;
    }


    @Override
    public void release()
    {
    }

    @Override
    public void release(final MessageInstanceConsumer<?> consumer)
    {
    }


    @Override
    public void setRedelivered()
    {
    }

    public AMQMessageHeader getMessageHeader()
    {
        return null;
    }

    @Override
    public boolean isPersistent()
    {
        return false;
    }

    @Override
    public boolean isRedelivered()
    {
        return false;
    }

    public void setMessage(ServerMessage msg)
    {
        _message = msg;
    }

    @Override
    public boolean isDeleted()
    {
        return false;
    }

    @Override
    public boolean isHeld()
    {
        return false;
    }

    @Override
    public int getDeliveryCount()
    {
        return 0;
    }

    @Override
    public void incrementDeliveryCount()
    {
    }

    @Override
    public void decrementDeliveryCount()
    {
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
    public Filterable asFilterable()
    {
        return Filterable.Factory.newInstance(_message, getInstanceProperties());
    }

    @Override
    public InstanceProperties getInstanceProperties()
    {
        return InstanceProperties.EMPTY;
    }

    @Override
    public TransactionLogResource getOwningResource()
    {
        return null;
    }
}
