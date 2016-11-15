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
import java.util.concurrent.CopyOnWriteArrayList;
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

public abstract class AbstractConsumerTarget implements ConsumerTarget
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumerTarget.class);
    private static final LogSubject MULTI_QUEUE_LOG_SUBJECT = new LogSubject()
    {
        @Override
        public String toLogString()
        {
            return "[(** Multi-Queue **)] ";
        }
    };
    private final AtomicReference<State> _state = new AtomicReference<>(State.OPEN);

    private final boolean _isMultiQueue;
    private final SuspendedConsumerLoggingTicker _suspendedConsumerLoggingTicker;
    private final List<ConsumerImpl> _consumers = new CopyOnWriteArrayList<>();

    private Iterator<ConsumerImpl> _pullIterator;
    private boolean _notifyWorkDesired;

    protected AbstractConsumerTarget(final boolean isMultiQueue,
                                     final AMQPConnection<?> amqpConnection)
    {
        _isMultiQueue = isMultiQueue;

        _suspendedConsumerLoggingTicker = new SuspendedConsumerLoggingTicker(amqpConnection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD))
        {
            @Override
            protected void log(final long period)
            {
                amqpConnection.getEventLogger().message(AbstractConsumerTarget.this.getLogSubject(), SubscriptionMessages.STATE(period));
            }
        };
    }

    private LogSubject getLogSubject()
    {
        if (_consumers.size() == 1 && _consumers.get(0) instanceof LogSubject)
        {
            return (LogSubject) _consumers.get(0);
        }
        else
        {
            return MULTI_QUEUE_LOG_SUBJECT;
        }
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
            if(_suspendedConsumerLoggingTicker != null)
            {
                if (desired)
                {
                    getSessionModel().removeTicker(_suspendedConsumerLoggingTicker);
                }
                else
                {
                    _suspendedConsumerLoggingTicker.setStartTime(System.currentTimeMillis());
                    getSessionModel().addTicker(_suspendedConsumerLoggingTicker);
                }
            }

            for (ConsumerImpl consumer : _consumers)
            {
                consumer.setNotifyWorkDesired(desired);
            }

            _notifyWorkDesired = desired;
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
        if (_state.compareAndSet(State.OPEN, State.CLOSED))
        {
            List<ConsumerImpl> consumers = new ArrayList<>(_consumers);
            _consumers.clear();

            setNotifyWorkDesired(false);

            for (ConsumerImpl consumer : consumers)
            {
                consumer.close();
            }
            if (_suspendedConsumerLoggingTicker != null)
            {
                getSessionModel().removeTicker(_suspendedConsumerLoggingTicker);
            }

            return true;
        }
        else
        {
            return false;
        }
    }
}
