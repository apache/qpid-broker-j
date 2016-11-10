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
package org.apache.qpid.server.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.SubscriptionMessages;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.queue.AbstractQueue;
import org.apache.qpid.server.queue.SuspendedConsumerLoggingTicker;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.StateChangeListener;

public abstract class AbstractConsumerTarget implements ConsumerTarget, LogSubject
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumerTarget.class);
    private final AtomicReference<State> _state;

    private final Set<StateChangeListener<ConsumerTarget, State>> _stateChangeListeners = new
            CopyOnWriteArraySet<>();

    private final AtomicInteger _stateActivates = new AtomicInteger();
    private final boolean _isMultiQueue;
    private final SuspendedConsumerLoggingTicker _suspendedConsumerLoggingTicker;
    private final List<ConsumerImpl> _consumers = new CopyOnWriteArrayList<>();

    private Iterator<ConsumerImpl> _pullIterator;
    private boolean _notifyWorkDesired;

    protected AbstractConsumerTarget(final State initialState,
                                     final boolean isMultiQueue,
                                     final AMQPConnection<?> amqpConnection)
    {
        _state = new AtomicReference<State>(initialState);
        _isMultiQueue = isMultiQueue;
        _suspendedConsumerLoggingTicker = isMultiQueue
                ? new SuspendedConsumerLoggingTicker(amqpConnection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD))
                {
                    @Override
                    protected void log(final long period)
                    {
                        amqpConnection.getEventLogger().message(AbstractConsumerTarget.this, SubscriptionMessages.STATE(period));
                    }
                }
                : null;

    }

    public boolean isMultiQueue()
    {
        return _isMultiQueue;
    }

    @Override
    public void notifyWork()
    {
        getSessionModel().notifyWork(this);
    }

    protected final void setNotifyWorkDesired(final boolean desired)
    {
        if (desired != _notifyWorkDesired)
        {
            // TODO - remove once queue is smarter
            if (desired)
            {
                updateState(State.SUSPENDED, State.ACTIVE);
            }
            else
            {
                updateState(State.ACTIVE, State.SUSPENDED);
            }

            for (ConsumerImpl consumer : _consumers)
            {
                consumer.setNotifyWorkDesired(desired);
            }

            _notifyWorkDesired = desired;
            if (desired)
            {
                notifyWork();
            }
        }
    }

    public final boolean isNotifyWorkDesired()
    {
        return _notifyWorkDesired;
    }

    @Override
    public boolean processPending()
    {
        if (!getSessionModel().getAMQPConnection().isIOThread())
        {
            return false;
        }

        // TODO - if not closed
        return sendNextMessage();
    }

    @Override
    public void consumerAdded(final ConsumerImpl sub)
    {
        _consumers.add(sub);
    }

    @Override
    public ListenableFuture<Void> consumerRemoved(final ConsumerImpl sub)
    {
        if(_consumers.contains(sub))
        {
            return doOnIoThreadAsync(
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            consumerRemovedInternal(sub);
                        }
                    });
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    private ListenableFuture<Void> doOnIoThreadAsync(final Runnable task)
    {
        AMQSessionModel<?> sessionModel = getSessionModel();
        return sessionModel.getAMQPConnection().doOnIOThreadAsync(task);
    }

    private void consumerRemovedInternal(final ConsumerImpl sub)
    {
        _consumers.remove(sub);
        if(_consumers.isEmpty())
        {
            close();
        }
    }

    public List<ConsumerImpl> getConsumers()
    {
        return _consumers;
    }


    @Override
    public final boolean isSuspended()
    {
        return !isNotifyWorkDesired();
    }

    public final State getState()
    {
        return _state.get();
    }

    protected final boolean updateState(State from, State to)
    {
        if(_state.compareAndSet(from, to))
        {
            if(!_stateChangeListeners.isEmpty())
            {
                for (StateChangeListener<ConsumerTarget, State> listener : _stateChangeListeners)
                {
                    listener.stateChanged(this, from, to);
                }
            }
            if(_suspendedConsumerLoggingTicker != null)
            {
                if (to == State.SUSPENDED)
                {
                    _suspendedConsumerLoggingTicker.setStartTime(System.currentTimeMillis());
                    getSessionModel().addTicker(_suspendedConsumerLoggingTicker);
                }
                else
                {
                    getSessionModel().removeTicker(_suspendedConsumerLoggingTicker);
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public final void notifyCurrentState()
    {

        for (StateChangeListener<ConsumerTarget, State> listener : _stateChangeListeners)
        {
            State state = getState();
            listener.stateChanged(this, state, state);
        }
    }
    public final void addStateListener(StateChangeListener<ConsumerTarget, State> listener)
    {
        _stateChangeListeners.add(listener);
    }

    @Override
    public void removeStateChangeListener(final StateChangeListener<ConsumerTarget, State> listener)
    {
        _stateChangeListeners.remove(listener);
    }

    @Override
    public final long send(final ConsumerImpl consumer, MessageInstance entry, boolean batch)
    {
        doSend(consumer, entry, batch);

        if (consumer.acquires())
        {
            entry.makeAcquisitionStealable();
        }
        return entry.getMessage().getSize();
    }

    protected abstract void doSend(final ConsumerImpl consumer, MessageInstance entry, boolean batch);


    @Override
    public boolean sendNextMessage()
    {
        AbstractQueue.MessageContainer messageContainer = null;
        ConsumerImpl consumer = null;
        boolean iteratedCompleteList = false;
        while (messageContainer == null)
        {
            if (_pullIterator == null || !_pullIterator.hasNext())
            {
                if (iteratedCompleteList)
                {
                    break;
                }
                iteratedCompleteList = true;

                _pullIterator = getConsumers().iterator();
            }
            if (_pullIterator.hasNext())
            {
                consumer = _pullIterator.next();
                messageContainer = consumer.pullMessage();
            }
        }

        if (messageContainer != null)
        {
            MessageInstance entry = messageContainer._messageInstance;
            try
            {
                send(consumer, entry, false);
            }
            finally
            {
                if (messageContainer._messageReference != null)
                {
                    messageContainer._messageReference.release();
                }
            }
            return true;
        }
        else
        {
            return false;
        }


    }

    final public boolean close()
    {
        boolean closed = false;
        State state = getState();
        List<ConsumerImpl> consumers = new ArrayList<>(_consumers);
        _consumers.clear();

        while(!closed && state != State.CLOSED)
        {
            closed = updateState(state, State.CLOSED);
            if(!closed)
            {
                state = getState();
            }
        }
        setNotifyWorkDesired(false);

        for (ConsumerImpl consumer : consumers)
        {
            consumer.close();
        }
        if(_suspendedConsumerLoggingTicker != null)
        {
            getSessionModel().removeTicker(_suspendedConsumerLoggingTicker);
        }

        return closed;

    }

    @Override
    public String toLogString()
    {

        return "[(** Multi-Queue **)] ";
    }
}
