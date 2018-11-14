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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.SubscriptionMessages;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.queue.SuspendedConsumerLoggingTicker;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public abstract class AbstractConsumerTarget<T extends AbstractConsumerTarget<T>> implements ConsumerTarget<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumerTarget.class);

    private static final LogSubject MULTI_QUEUE_LOG_SUBJECT = () -> "[(** Multi-Queue **)] ";
    protected final AtomicLong _unacknowledgedBytes = new AtomicLong(0);
    protected final AtomicLong _unacknowledgedCount = new AtomicLong(0);
    private final AtomicReference<State> _state = new AtomicReference<>(State.OPEN);

    private final boolean _isMultiQueue;
    private final SuspendedConsumerLoggingTicker _suspendedConsumerLoggingTicker;
    private final List<MessageInstanceConsumer> _consumers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _scheduled = new AtomicBoolean();

    private volatile Iterator<MessageInstanceConsumer> _pullIterator;
    private volatile boolean _notifyWorkDesired;

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

    @Override
    public void acquisitionRemoved(final MessageInstance node)
    {

    }

    @Override
    public boolean isMultiQueue()
    {
        return _isMultiQueue;
    }

    @Override
    public void notifyWork()
    {
        @SuppressWarnings("unchecked")
        final T target = (T) this;
        getSession().notifyWork(target);
    }

    protected final void setNotifyWorkDesired(final boolean desired)
    {
        if (desired != _notifyWorkDesired)
        {
            if (desired)
            {
                getSession().removeTicker(_suspendedConsumerLoggingTicker);
            }
            else
            {
                _suspendedConsumerLoggingTicker.setStartTime(System.currentTimeMillis());
                getSession().addTicker(_suspendedConsumerLoggingTicker);
            }

            for (MessageInstanceConsumer consumer : _consumers)
            {
                consumer.setNotifyWorkDesired(desired);
            }

            _notifyWorkDesired = desired;
        }
    }

    @Override
    public final boolean isNotifyWorkDesired()
    {
        return _notifyWorkDesired;
    }

    @Override
    public boolean processPending()
    {
        if (getSession() == null || !getSession().getAMQPConnection().isIOThread())
        {
            return false;
        }

        // TODO - if not closed
        return sendNextMessage();
    }

    @Override
    public void consumerAdded(final MessageInstanceConsumer sub)
    {
        _consumers.add(sub);
    }

    @Override
    public ListenableFuture<Void> consumerRemoved(final MessageInstanceConsumer sub)
    {
        if(_consumers.contains(sub))
        {
            return doOnIoThreadAsync(
                    () -> consumerRemovedInternal(sub));
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    private ListenableFuture<Void> doOnIoThreadAsync(final Runnable task)
    {
        return getSession().getAMQPConnection().doOnIOThreadAsync(task);
    }

    private void consumerRemovedInternal(final MessageInstanceConsumer sub)
    {
        _consumers.remove(sub);
        if(_consumers.isEmpty())
        {
            close();
        }
    }

    public List<MessageInstanceConsumer> getConsumers()
    {
        return _consumers;
    }


    @Override
    public final boolean isSuspended()
    {
        return !isNotifyWorkDesired();
    }

    @Override
    public final State getState()
    {
        return _state.get();
    }

    @Override
    public final void send(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
    {
        doSend(consumer, entry, batch);
        getSession().getAMQPConnection().updateLastMessageOutboundTime();
        if (consumer.acquires())
        {
            entry.makeAcquisitionStealable();
        }
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return _unacknowledgedCount.longValue();
    }

    @Override
    public long getUnacknowledgedBytes()
    {
        return _unacknowledgedBytes.longValue();
    }

    protected abstract void doSend(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch);


    @Override
    public boolean sendNextMessage()
    {
        MessageContainer messageContainer = null;
        MessageInstanceConsumer consumer = null;
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
            MessageInstance entry = messageContainer.getMessageInstance();
            try
            {
                send(consumer, entry, false);
            }
            catch (MessageConversionException mce)
            {
                restoreCredit(entry.getMessage());
                final TransactionLogResource owningResource = entry.getOwningResource();
                if (owningResource instanceof MessageSource)
                {
                    final MessageSource.MessageConversionExceptionHandlingPolicy handlingPolicy =
                            ((MessageSource) owningResource).getMessageConversionExceptionHandlingPolicy();
                    switch(handlingPolicy)
                    {
                        case CLOSE:
                            entry.release(consumer);
                            throw new ConnectionScopedRuntimeException(String.format(
                                    "Unable to convert message %s for this consumer",
                                    entry.getMessage()), mce);
                        case ROUTE_TO_ALTERNATE:
                            if (consumer.acquires())
                            {
                                int enqueues = entry.routeToAlternate(null, null, null);
                                if (enqueues == 0)
                                {
                                    LOGGER.info("Failed to convert message {} for this consumer because '{}'."
                                                + "  Message discarded.", entry.getMessage(), mce.getMessage());

                                }
                                else
                                {
                                    LOGGER.info("Failed to convert message {} for this consumer because '{}'."
                                                + "  Message routed to alternate.", entry.getMessage(), mce.getMessage());
                                }
                            }
                            else
                            {
                                LOGGER.info("Failed to convert message {} for this browser because '{}'."
                                            + "  Message skipped.", entry.getMessage(), mce.getMessage());
                            }
                            break;
                        case REJECT:
                            entry.reject(consumer);
                            entry.release(consumer);
                            LOGGER.info("Failed to convert message {} for this consumer because '{}'."
                                        + "  Message skipped.", entry.getMessage(), mce.getMessage());
                            break;
                        default:
                            throw new ServerScopedRuntimeException("Unrecognised policy " + handlingPolicy);
                    }
                }
                else
                {
                    throw new ConnectionScopedRuntimeException(String.format(
                            "Unable to convert message %s for this consumer",
                            entry.getMessage()), mce);
                }
            }
            finally
            {
                if (messageContainer.getMessageReference() != null)
                {
                    messageContainer.getMessageReference().release();
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
    final public boolean close()
    {
        if (_state.compareAndSet(State.OPEN, State.CLOSED))
        {
            setNotifyWorkDesired(false);

            List<MessageInstanceConsumer> consumers = new ArrayList<>(_consumers);
            _consumers.clear();

            for (MessageInstanceConsumer consumer : consumers)
            {
                consumer.close();
            }

            getSession().removeTicker(_suspendedConsumerLoggingTicker);

            return true;
        }
        else
        {
            return false;
        }
    }

    final boolean setScheduled()
    {
        return _scheduled.compareAndSet(false, true);
    }

    final void clearScheduled()
    {
        _scheduled.set(false);
    }

    @Override
    public void queueDeleted(final Queue queue, final MessageInstanceConsumer sub)
    {
        consumerRemoved(sub);
    }

}
