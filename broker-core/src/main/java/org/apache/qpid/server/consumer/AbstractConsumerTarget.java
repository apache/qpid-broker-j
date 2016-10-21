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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.StateChangeListener;

public abstract class AbstractConsumerTarget implements ConsumerTarget
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumerTarget.class);
    private final AtomicReference<State> _state;

    private final Set<StateChangeListener<ConsumerTarget, State>> _stateChangeListeners = new
            CopyOnWriteArraySet<>();

    private final Lock _stateChangeLock = new ReentrantLock();
    private final AtomicInteger _stateActivates = new AtomicInteger();
    private ConcurrentLinkedQueue<ConsumerMessageInstancePair> _queue = new ConcurrentLinkedQueue();


    protected AbstractConsumerTarget(final State initialState)
    {
        _state = new AtomicReference<State>(initialState);
    }

    @Override
    public boolean processPending()
    {
        if (!getSessionModel().getAMQPConnection().isIOThread())
        {
            return false;
        }
        if(hasMessagesToSend())
        {
            sendNextMessage();
            return true;
        }
        else
        {
            processStateChanged();
            processClosed();
            return false;
        }
    }

    @Override
    public boolean hasPendingWork()
    {
        return hasMessagesToSend() || hasStateChanged() || hasClosed();
    }

    protected abstract boolean hasStateChanged();

    protected abstract boolean hasClosed();

    protected abstract void processStateChanged();

    protected abstract void processClosed();

    @Override
    public final boolean isSuspended()
    {
        return getSessionModel().getAMQPConnection().isMessageAssignmentSuspended() || isFlowSuspended();
    }

    @Override
    public boolean hasCredit()
    {
        return !isFlowSuspended();
    }

    protected abstract boolean isFlowSuspended();

    public final State getState()
    {
        return _state.get();
    }

    protected final boolean updateState(State from, State to)
    {
        if(_state.compareAndSet(from, to))
        {
            if (to == State.ACTIVE && _stateChangeListeners.size() > 1)
            {
                int offset = _stateActivates.incrementAndGet();
                if (offset >= _stateChangeListeners.size())
                {
                    _stateActivates.set(0);
                    offset = 0;
                }

                List<StateChangeListener<ConsumerTarget, State>> holdovers = new ArrayList<>();
                int pos = 0;
                for (StateChangeListener<ConsumerTarget, State> listener : _stateChangeListeners)
                {
                    if (pos++ < offset)
                    {
                        holdovers.add(listener);
                    }
                    else
                    {
                        listener.stateChanged(this, from, to);
                    }
                }
                for (StateChangeListener<ConsumerTarget, State> listener : holdovers)
                {
                    listener.stateChanged(this, from, to);
                }

            }
            else
            {
                for (StateChangeListener<ConsumerTarget, State> listener : _stateChangeListeners)
                {
                    listener.stateChanged(this, from, to);
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

    public final boolean trySendLock()
    {
        return _stateChangeLock.tryLock();
    }

    public final void getSendLock()
    {
        _stateChangeLock.lock();
    }

    public final void releaseSendLock()
    {
        _stateChangeLock.unlock();
    }

    @Override
    public final long send(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
    {
        AMQPConnection<?> amqpConnection = getSessionModel().getAMQPConnection();
        amqpConnection.reserveOutboundMessageSpace(entry.getMessage().getSize());
        _queue.add(new ConsumerMessageInstancePair(consumer, entry, batch));
        amqpConnection.notifyWork();
        return entry.getMessage().getSize();
    }

    protected abstract void doSend(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch);

    @Override
    public boolean hasMessagesToSend()
    {
        return !_queue.isEmpty();
    }

    @Override
    public void sendNextMessage()
    {
        ConsumerMessageInstancePair consumerMessage = _queue.poll();
        if (consumerMessage != null)
        {
            try
            {

                MessageInstanceConsumer consumer = consumerMessage.getConsumer();
                MessageInstance entry = consumerMessage.getEntry();
                boolean batch = consumerMessage.isBatch();
                doSend(consumer, entry, batch);

                if (consumer.acquires())
                {
                    entry.makeAcquisitionStealable();
                }
            }
            finally
            {
                consumerMessage.release();
            }
        }

    }

    final public boolean close()
    {
        boolean closed = false;
        State state = getState();

        getSendLock();
        try
        {
            while(!closed && state != State.CLOSED)
            {
                closed = updateState(state, State.CLOSED);
                if(!closed)
                {
                    state = getState();
                }
            }
            ConsumerMessageInstancePair instance;
            while((instance = _queue.poll()) != null)
            {
                MessageInstance entry = instance.getEntry();
                entry.release(instance.getConsumer());
                instance.release();
            }
            doCloseInternal();
        }
        finally
        {
            releaseSendLock();
        }

        afterCloseInternal();
        return closed;

    }

    protected abstract void afterCloseInternal();

    protected abstract void doCloseInternal();
}
