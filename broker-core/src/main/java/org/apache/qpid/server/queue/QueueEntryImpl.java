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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.server.util.StateChangeListenerEntry;

public abstract class QueueEntryImpl implements QueueEntry
{
    private final QueueEntryList _queueEntryList;

    private final MessageReference _message;

    private volatile Set<Object> _rejectedBy = null;
    private static final AtomicReferenceFieldUpdater<QueueEntryImpl, Set> _rejectedByUpdater =
            AtomicReferenceFieldUpdater.newUpdater(QueueEntryImpl.class, Set.class, "_rejectedBy");

    private static final EntryState HELD_STATE = new EntryState()
    {
        @Override
        public State getState()
        {
            return State.AVAILABLE;
        }

        @Override
        public String toString()
        {
            return "HELD";
        }
    };

    private volatile EntryState _state = AVAILABLE_STATE;

    private static final
        AtomicReferenceFieldUpdater<QueueEntryImpl, EntryState>
            _stateUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (QueueEntryImpl.class, EntryState.class, "_state");


    @SuppressWarnings("unused")
    private volatile StateChangeListenerEntry<? super QueueEntry, EntryState> _stateChangeListeners;

    private static final
        AtomicReferenceFieldUpdater<QueueEntryImpl, StateChangeListenerEntry>
                _listenersUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (QueueEntryImpl.class, StateChangeListenerEntry.class, "_stateChangeListeners");


    private static final
        AtomicLongFieldUpdater<QueueEntryImpl>
            _entryIdUpdater =
        AtomicLongFieldUpdater.newUpdater
        (QueueEntryImpl.class, "_entryId");


    @SuppressWarnings("unused")
    private volatile long _entryId;

    private static final int REDELIVERED_FLAG = 1;
    private static final int PERSISTENT_FLAG = 2;
    private static final int MANDATORY_FLAG = 4;
    private static final int IMMEDIATE_FLAG = 8;
    private int _flags;
    private long _expiration;

    /** Number of times this message has been delivered */
    private volatile int _deliveryCount = -1;
    private static final AtomicIntegerFieldUpdater<QueueEntryImpl> _deliveryCountUpdater = AtomicIntegerFieldUpdater
                    .newUpdater(QueueEntryImpl.class, "_deliveryCount");

    private final MessageEnqueueRecord _enqueueRecord;


    QueueEntryImpl(QueueEntryList queueEntryList)
    {
        this(queueEntryList, null, Long.MIN_VALUE, null);
        _state = DELETED_STATE;
    }


    QueueEntryImpl(QueueEntryList queueEntryList,
                   ServerMessage message,
                   final long entryId,
                   final MessageEnqueueRecord enqueueRecord)
    {
        _queueEntryList = queueEntryList;

        _message = message == null ? null : message.newReference(queueEntryList.getQueue());

        _entryIdUpdater.set(this, entryId);
        populateInstanceProperties();
        _enqueueRecord = enqueueRecord;
    }

    QueueEntryImpl(QueueEntryList queueEntryList,
                   ServerMessage message,
                   final MessageEnqueueRecord enqueueRecord)
    {
        _queueEntryList = queueEntryList;
        _message = message == null ? null :  message.newReference(queueEntryList.getQueue());
        populateInstanceProperties();
        _enqueueRecord = enqueueRecord;
    }

    private void populateInstanceProperties()
    {
        if(_message != null)
        {
            if(_message.getMessage().isPersistent())
            {
                setPersistent();
            }
            _expiration = _message.getMessage().getExpiration();
        }
    }

    @Override
    public void setExpiration(long expiration)
    {
        _expiration = expiration;
    }

    @Override
    public InstanceProperties getInstanceProperties()
    {
        return new EntryInstanceProperties();
    }

    void setEntryId(long entryId)
    {
        _entryIdUpdater.set(this, entryId);
    }

    long getEntryId()
    {
        return _entryId;
    }

    @Override
    public Queue<?> getQueue()
    {
        return _queueEntryList.getQueue();
    }

    @Override
    public ServerMessage getMessage()
    {
        return  _message == null ? null : _message.getMessage();
    }

    @Override
    public long getSize()
    {
        return getMessage() == null ? 0 : getMessage().getSize();
    }

    @Override
    public long getSizeWithHeader()
    {
        return getMessage() == null ? 0 : getMessage().getSizeIncludingHeader();
    }

    @Override
    public boolean getDeliveredToConsumer()
    {
        return _deliveryCountUpdater.get(this) != -1;
    }

    @Override
    public boolean expired()
    {
        long expiration = _expiration;
        if (expiration != 0L)
        {
            long now = System.currentTimeMillis();

            return (now > expiration);
        }
        return false;

    }

    @Override
    public boolean isAvailable()
    {
        return _state.getState() == State.AVAILABLE;
    }

    @Override
    public boolean isAcquired()
    {
        return _state.getState() == State.ACQUIRED;
    }

    @Override
    public boolean acquire()
    {
        return acquire(NON_CONSUMER_ACQUIRED_STATE);
    }

    private class DelayedAcquisitionStateListener implements StateChangeListener<MessageInstance, EntryState>
    {
        private final Runnable _task;
        private final AtomicBoolean _run = new AtomicBoolean();

        private DelayedAcquisitionStateListener(final Runnable task)
        {
            _task = task;
        }

        @Override
        public void stateChanged(final MessageInstance object, final EntryState oldState, final EntryState newState)
        {
            if (newState.equals(DELETED_STATE) || newState.equals(DEQUEUED_STATE))
            {
                QueueEntryImpl.this.removeStateChangeListener(this);
            }
            else if (acquireOrSteal(null))
            {
                runTask();
            }
        }

        void runTask()
        {
            QueueEntryImpl.this.removeStateChangeListener(this);
            if(_run.compareAndSet(false,true))
            {
                _task.run();
            }
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final DelayedAcquisitionStateListener that = (DelayedAcquisitionStateListener) o;
            return Objects.equals(_task, that._task);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_task);
        }
    }

    @Override
    public boolean acquireOrSteal(final Runnable delayedAcquisitionTask)
    {
        boolean acquired = acquire();
        if(!acquired)
        {
            QueueConsumer<?,?> consumer = getAcquiringConsumer();
            acquired = removeAcquisitionFromConsumer(consumer);
            if(acquired)
            {
                consumer.acquisitionRemoved(this);
            }
            else if(delayedAcquisitionTask != null)
            {
                DelayedAcquisitionStateListener listener = new DelayedAcquisitionStateListener(delayedAcquisitionTask);
                addStateChangeListener(listener);
                if(acquireOrSteal(null))
                {
                    listener.runTask();
                }
            }
        }
        return acquired;
    }

    private boolean acquire(final EntryState state)
    {
        boolean acquired = false;

        EntryState currentState;

        while((currentState = _state).equals(AVAILABLE_STATE))
        {
            if(acquired = _stateUpdater.compareAndSet(this, currentState, state))
            {
                break;
            }
        }

        if(acquired)
        {
            notifyStateChange(AVAILABLE_STATE, state);
        }

        return acquired;
    }

    @Override
    public boolean acquire(MessageInstanceConsumer<?> consumer)
    {
        final boolean acquired = acquire(((QueueConsumer<?,?>) consumer).getOwningState().getUnstealableState());
        if(acquired)
        {
            _deliveryCountUpdater.compareAndSet(this,-1,0);
        }
        return acquired;
    }

    @Override
    public boolean makeAcquisitionUnstealable(final MessageInstanceConsumer<?> consumer)
    {
        EntryState state = _state;
        if(state instanceof StealableConsumerAcquiredState
           && ((StealableConsumerAcquiredState) state).getConsumer() == consumer)
        {
            UnstealableConsumerAcquiredState unstealableState = ((StealableConsumerAcquiredState) state).getUnstealableState();
            boolean updated = _stateUpdater.compareAndSet(this, state, unstealableState);
            if(updated)
            {
                notifyStateChange(state, unstealableState);
            }
            return updated;
        }
        return state instanceof UnstealableConsumerAcquiredState
               && ((UnstealableConsumerAcquiredState) state).getConsumer() == consumer;
    }

    @Override
    public boolean makeAcquisitionStealable()
    {
        EntryState state = _state;
        if(state instanceof UnstealableConsumerAcquiredState)
        {
            StealableConsumerAcquiredState stealableState = ((UnstealableConsumerAcquiredState) state).getStealableState();
            boolean updated = _stateUpdater.compareAndSet(this, state, stealableState);
            if(updated)
            {
                notifyStateChange(state, stealableState);
            }
            return updated;
        }
        return false;
    }

    @Override
    public boolean acquiredByConsumer()
    {
        return _state instanceof ConsumerAcquiredState;
    }

    @Override
    public QueueConsumer<?,?> getAcquiringConsumer()
    {
        QueueConsumer<?,?> consumer;
        EntryState state = _state;
        if (state instanceof ConsumerAcquiredState)
        {
            consumer = ((ConsumerAcquiredState<QueueConsumer<?,?>>)state).getConsumer();
        }
        else
        {
            consumer = null;
        }
        return consumer;
    }

    @Override
    public boolean isAcquiredBy(MessageInstanceConsumer<?> consumer)
    {
        EntryState state = _state;
        return (state instanceof ConsumerAcquiredState && ((ConsumerAcquiredState)state).getConsumer() == consumer);
    }

    @Override
    public boolean removeAcquisitionFromConsumer(MessageInstanceConsumer<?> consumer)
    {
        EntryState state = _state;
        if(state instanceof StealableConsumerAcquiredState
               && ((StealableConsumerAcquiredState)state).getConsumer() == consumer)
        {
            final boolean stateWasChanged = _stateUpdater.compareAndSet(this, state, NON_CONSUMER_ACQUIRED_STATE);
            if (stateWasChanged)
            {
                notifyStateChange(state, NON_CONSUMER_ACQUIRED_STATE);
            }
            return stateWasChanged;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void release()
    {
        EntryState state = _state;

        if((state.getState() == State.ACQUIRED) && _stateUpdater.compareAndSet(this, state, AVAILABLE_STATE))
        {
            postRelease(state);
        }
    }

    @Override
    public void release(MessageInstanceConsumer<?> consumer)
    {
        EntryState state = _state;
        if(isAcquiredBy(consumer) && _stateUpdater.compareAndSet(this, state, AVAILABLE_STATE))
        {
            postRelease(state);
        }
    }

    private void postRelease(final EntryState previousState)
    {

        if(!getQueue().isDeleted())
        {
            getQueue().requeue(this);
            if (previousState.getState() == State.ACQUIRED)
            {
                notifyStateChange(previousState, AVAILABLE_STATE);
            }

        }
        else if(acquire())
        {
            routeToAlternate(null, null, null);
        }
    }

    @Override
    public boolean checkHeld(final long evaluationTime)
    {
        EntryState state;
        while((state = _state).getState() == State.AVAILABLE)
        {
            boolean isHeld = getQueue().isHeld(this, evaluationTime);
            if(state == AVAILABLE_STATE && isHeld)
            {
                if(!_stateUpdater.compareAndSet(this, state, HELD_STATE))
                {
                    continue;
                }
            }
            else if(state == HELD_STATE && !isHeld)
            {

                if(_stateUpdater.compareAndSet(this, state, AVAILABLE_STATE))
                {
                    postRelease(state);
                }
                else
                {
                    continue;
                }
            }
            return isHeld;

        }
        return false;
    }

    @Override
    public void reject(final MessageInstanceConsumer<?> consumer)
    {
        if (consumer == null)
        {
            throw new IllegalArgumentException("consumer must not be null");
        }

        if (_rejectedBy == null)
        {
            _rejectedByUpdater.compareAndSet(this, null, Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }
        _rejectedBy.add(consumer.getIdentifier());
    }

    @Override
    public boolean isRejectedBy(MessageInstanceConsumer<?> consumer)
    {
        return _rejectedBy != null && _rejectedBy.contains(consumer.getIdentifier());
    }

    private boolean dequeue()
    {
        EntryState state = _state;

        while(state.getState() == State.ACQUIRED && !_stateUpdater.compareAndSet(this, state, DEQUEUED_STATE))
        {
            state = _state;
        }

        if(state.getState() == State.ACQUIRED)
        {
            notifyStateChange(state, DEQUEUED_STATE);
            return true;
        }
        else
        {
            return false;
        }

    }

    private void notifyStateChange(final EntryState oldState, final EntryState newState)
    {
        _queueEntryList.updateStatsOnStateChange(this, oldState, newState);
        StateChangeListenerEntry<? super QueueEntry, EntryState> entry = _listenersUpdater.get(this);
        while(entry != null)
        {
            StateChangeListener<? super QueueEntry, EntryState> l = entry.getListener();
            if(l != null)
            {
                l.stateChanged(this, oldState, newState);
            }
            entry = entry.next();
        }
    }

    private boolean dispose()
    {
        EntryState state = _state;

        if(state != DELETED_STATE && _stateUpdater.compareAndSet(this,state,DELETED_STATE))
        {
            notifyStateChange(state, DELETED_STATE);
            _queueEntryList.entryDeleted(this);
            onDelete();
            _message.release();

            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void delete()
    {
        if(dequeue())
        {
            dispose();
        }
    }

    @Override
    public int routeToAlternate(final Action<? super MessageInstance> action,
                                ServerTransaction txn,
                                final Predicate<BaseQueue> predicate)
    {
        if (!isAcquired())
        {
            throw new IllegalStateException("Illegal queue entry state. " + this + " is not acquired.");
        }

        final Queue<?> currentQueue = getQueue();
        MessageDestination alternateBindingDestination = currentQueue.getAlternateBindingDestination();
        if(alternateBindingDestination == null && currentQueue.getAlternateBinding() != null)
        {
            alternateBindingDestination = currentQueue.getAddressSpace().getAttainedMessageDestination(currentQueue.getAlternateBinding().getDestination(), true);
        }
        boolean autocommit =  txn == null;

        if(autocommit)
        {
            txn = new LocalTransaction(getQueue().getVirtualHost().getMessageStore());
        }

        RoutingResult<?> result;
        ServerMessage<?> message = getMessage();
        if (alternateBindingDestination != null && message.checkValid())
        {
            result = alternateBindingDestination.route(message,
                                                       message.getInitialRoutingAddress(),
                                                       getInstanceProperties());
        }
        else
        {
            result = new RoutingResult<>(message);
        }

        if(predicate != null)
        {
            result.filter(predicate);
        }

        txn.dequeue(getEnqueueRecord(), new ServerTransaction.Action()
        {
            @Override
            public void postCommit()
            {
                delete();
            }

            @Override
            public void onRollback()
            {

            }
        });
        int enqueues = result.send(txn, action);

        if(autocommit)
        {
            txn.commit();
        }
        return enqueues;
    }

    @Override
    public boolean isQueueDeleted()
    {
        return getQueue().isDeleted();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<? super MessageInstance, EntryState> listener)
    {
        StateChangeListenerEntry<? super QueueEntry, EntryState> entry = new StateChangeListenerEntry<>(listener);
        if(!_listenersUpdater.compareAndSet(this,null, entry))
        {
            _listenersUpdater.get(this).add(entry);
        }
    }

    @Override
    public boolean removeStateChangeListener(StateChangeListener<? super MessageInstance, EntryState> listener)
    {
        StateChangeListenerEntry entry = _listenersUpdater.get(this);
        return entry != null && entry.remove(listener);
    }

    @Override
    public int compareTo(final QueueEntry o)
    {
        QueueEntryImpl other = (QueueEntryImpl)o;
        return getEntryId() > other.getEntryId() ? 1 : getEntryId() < other.getEntryId() ? -1 : 0;
    }

    protected void onDelete()
    {
    }

    public QueueEntryList getQueueEntryList()
    {
        return _queueEntryList;
    }

    @Override
    public boolean isDeleted()
    {
        return _state.isDispensed();
    }

    @Override
    public boolean isHeld()
    {
        return checkHeld(System.currentTimeMillis());
    }

    @Override
    public int getDeliveryCount()
    {
        return _deliveryCount == -1 ? 0 : _deliveryCount;
    }

    @Override
    public int getMaximumDeliveryCount()
    {
        return getQueue().getMaximumDeliveryAttempts();
    }

    @Override
    public void incrementDeliveryCount()
    {
        _deliveryCountUpdater.compareAndSet(this,-1,0);
        _deliveryCountUpdater.incrementAndGet(this);
    }

    @Override
    public void decrementDeliveryCount()
    {
        _deliveryCountUpdater.decrementAndGet(this);
    }

    @Override
    public Filterable asFilterable()
    {
        return Filterable.Factory.newInstance(getMessage(), getInstanceProperties());
    }

    @Override
    public String toString()
    {
        return "QueueEntryImpl{" +
                "_entryId=" + _entryId +
                ", _state=" + _state +
                '}';
    }

    @Override
    public TransactionLogResource getOwningResource()
    {
        return getQueue();
    }

    @Override
    public void setRedelivered()
    {
        _flags |= REDELIVERED_FLAG;
    }

    private void setPersistent()
    {
        _flags |= PERSISTENT_FLAG;
    }

    @Override
    public boolean isRedelivered()
    {
        return (_flags & REDELIVERED_FLAG) != 0;
    }

    @Override
    public boolean isPersistent()
    {
        return (_flags & PERSISTENT_FLAG) != 0;
    }

    @Override
    public MessageReference newMessageReference()
    {
        try
        {
            return getMessage().newReference();
        }
        catch (MessageDeletedException mde)
        {
            return null;
        }
    }

    private class EntryInstanceProperties implements InstanceProperties
    {

        @Override
        public Object getProperty(final Property prop)
        {
            switch(prop)
            {

                case REDELIVERED:
                    return (_flags & REDELIVERED_FLAG) != 0;
                case PERSISTENT:
                    return (_flags & PERSISTENT_FLAG) != 0;
                case MANDATORY:
                    return (_flags & MANDATORY_FLAG) != 0;
                case IMMEDIATE:
                    return (_flags & IMMEDIATE_FLAG) != 0;
                case EXPIRATION:
                    return _expiration;
                default:
                    throw new IllegalArgumentException("Unknown property " + prop);
            }
        }

    }

    @Override
    public MessageEnqueueRecord getEnqueueRecord()
    {
        return _enqueueRecord;
    }
}
