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
package org.apache.qpid.server.message;


import java.util.function.Predicate;

import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;

public interface MessageInstance
{


    /**
     * Number of times this queue entry has been delivered.
     *
     * @return delivery count
     */
    int getDeliveryCount();

    void incrementDeliveryCount();

    void decrementDeliveryCount();

    void addStateChangeListener(StateChangeListener<? super MessageInstance, EntryState> listener);

    boolean removeStateChangeListener(StateChangeListener<? super MessageInstance, EntryState> listener);

    boolean acquiredByConsumer();

    boolean isAcquiredBy(MessageInstanceConsumer<?> consumer);

    boolean removeAcquisitionFromConsumer(MessageInstanceConsumer<?> consumer);

    void setRedelivered();

    boolean isRedelivered();

    void reject(final MessageInstanceConsumer<?> consumer);

    boolean isRejectedBy(MessageInstanceConsumer<?> consumer);

    boolean getDeliveredToConsumer();

    boolean expired();

    boolean acquire(MessageInstanceConsumer<?> consumer);

    boolean makeAcquisitionUnstealable(final MessageInstanceConsumer<?> consumer);

    boolean makeAcquisitionStealable();

    int getMaximumDeliveryCount();

    int routeToAlternate(Action<? super MessageInstance> action,
                         ServerTransaction txn,
                         Predicate<BaseQueue> predicate);

    Filterable asFilterable();

    MessageInstanceConsumer<?> getAcquiringConsumer();

    MessageEnqueueRecord getEnqueueRecord();

    enum State
    {
        AVAILABLE,
        ACQUIRED,
        DEQUEUED,
        DELETED
    }

    abstract class EntryState
    {
        protected EntryState()
        {
        }

        public abstract State getState();

        /**
         * Returns true if state is either DEQUEUED or DELETED.
         *
         * @return true if state is either DEQUEUED or DELETED.
         */
        public boolean isDispensed()
        {
            State currentState = getState();
            return currentState == State.DEQUEUED || currentState == State.DELETED;
        }

    }


    final class AvailableState extends EntryState
    {

        @Override
        public State getState()
        {
            return State.AVAILABLE;
        }

        @Override
        public String toString()
        {
            return getState().name();
        }
    }


    final class DequeuedState extends EntryState
    {

        @Override
        public State getState()
        {
            return State.DEQUEUED;
        }

        @Override
        public String toString()
        {
            return getState().name();
        }
    }


    final class DeletedState extends EntryState
    {

        @Override
        public State getState()
        {
            return State.DELETED;
        }

        @Override
        public String toString()
        {
            return getState().name();
        }
    }

    final class NonConsumerAcquiredState extends EntryState
    {
        @Override
        public State getState()
        {
            return State.ACQUIRED;
        }

        @Override
        public String toString()
        {
            return getState().name();
        }
    }

    abstract class ConsumerAcquiredState<C extends MessageInstanceConsumer> extends EntryState
    {
        public abstract C getConsumer();

        @Override
        public final State getState()
        {
            return State.ACQUIRED;
        }

        @Override
        public String toString()
        {
            return "{" + getState().name() + " : " + getConsumer() +"}";
        }
    }

    final class StealableConsumerAcquiredState<C extends MessageInstanceConsumer> extends ConsumerAcquiredState
    {
        private final C _consumer;
        private final UnstealableConsumerAcquiredState<C> _unstealableState;

        public StealableConsumerAcquiredState(C consumer)
        {
            _consumer = consumer;
            _unstealableState = new UnstealableConsumerAcquiredState<>(this);
        }

        @Override
        public C getConsumer()
        {
            return _consumer;
        }

        public UnstealableConsumerAcquiredState<C> getUnstealableState()
        {
            return _unstealableState;
        }
    }

    final class UnstealableConsumerAcquiredState<C extends MessageInstanceConsumer> extends ConsumerAcquiredState
    {
        private final StealableConsumerAcquiredState<C> _stealableState;

        public UnstealableConsumerAcquiredState(final StealableConsumerAcquiredState<C> stealableState)
        {
            _stealableState = stealableState;
        }

        @Override
        public C getConsumer()
        {
            return _stealableState.getConsumer();
        }

        public StealableConsumerAcquiredState<C> getStealableState()
        {
            return _stealableState;
        }
    }


    EntryState AVAILABLE_STATE = new AvailableState();
    EntryState DELETED_STATE = new DeletedState();
    EntryState DEQUEUED_STATE = new DequeuedState();
    EntryState NON_CONSUMER_ACQUIRED_STATE = new NonConsumerAcquiredState();

    boolean isAvailable();

    boolean acquire();

    boolean isAcquired();

    void release();

    void release(MessageInstanceConsumer<?> consumer);

    void delete();

    boolean isDeleted();

    boolean isHeld();

    boolean isPersistent();

    ServerMessage getMessage();

    InstanceProperties getInstanceProperties();

    TransactionLogResource getOwningResource();
}
