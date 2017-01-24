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
package org.apache.qpid.server.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.transport.TransactionTimeoutTicker;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.transport.network.Ticker;

public abstract class AbstractAMQPSession<S extends AbstractAMQPSession<S, X>, X extends ConsumerTarget<X>>
        extends AbstractConfiguredObject<S>
        implements AMQPSession<S, X>, EventLoggerProvider
{
    private static final String OPEN_TRANSACTION_TIMEOUT_ERROR = "Open transaction timed out";
    private static final String IDLE_TRANSACTION_TIMEOUT_ERROR = "Idle transaction timed out";

    private final Action _deleteModelTask;
    private final Connection<?> _amqpConnection;

    protected AbstractAMQPSession(final Connection<?> parent,
                                  final int sessionId)
    {
        super(parent, createAttributes(sessionId));
        _amqpConnection = parent;

        _deleteModelTask = new Action()
        {
            @Override
            public void performAction(final Object object)
            {
                removeDeleteTask(this);
                deleteAsync();
            }
        };
        setState(State.ACTIVE);
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
        addDeleteTask(_deleteModelTask);
    }

    private static Map<String, Object> createAttributes(final long sessionId)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NAME, sessionId);
        attributes.put(DURABLE, false);
        attributes.put(LIFETIME_POLICY, LifetimePolicy.DELETE_ON_SESSION_END);
        return attributes;
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();
        registerTransactionTimeoutTickers(_amqpConnection);
    }

    @Override
    public abstract int getChannelId();

    @Override
    public boolean isProducerFlowBlocked()
    {
        return getBlocking();
    }

    public abstract boolean getBlocking();

    public abstract Collection<Consumer<?,X>> getConsumers();

    @Override
    public abstract long getConsumerCount();

    @Override
    public int getLocalTransactionOpen()
    {
        long open = getTxnStart() - (getTxnCommits() + getTxnRejects());
        return (open > 0L) ? 1 : 0;
    }

    @Override
    public long getLocalTransactionBegins()
    {
        return getTxnStart();
    }

    public abstract long getTxnRejects();

    public abstract long getTxnCommits();

    public abstract long getTxnStart();

    public long getLocalTransactionRollbacks()
    {
        return getTxnRejects();
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return getUnacknowledgedMessageCount();
    }

    public abstract int getUnacknowledgedMessageCount();

    @Override
    public Date getTransactionStartTime()
    {
        return new Date(getTransactionStartTimeLong());
    }

    @Override
    public Date getTransactionUpdateTime()
    {
        return new Date(getTransactionUpdateTimeLong());
    }

    @StateTransition(currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        deleted();
        setState(State.DELETED);
        removeDeleteTask(_deleteModelTask);
        return Futures.immediateFuture(null);
    }

    @Override
    public abstract EventLogger getEventLogger();

    private void registerTransactionTimeoutTickers(Connection<?> amqpConnection)
    {
        NamedAddressSpace addressSpace = amqpConnection.getAddressSpace();
        if (addressSpace instanceof QueueManagingVirtualHost)
        {
            final EventLogger eventLogger = getEventLogger();
            final QueueManagingVirtualHost<?> virtualhost = (QueueManagingVirtualHost<?>) addressSpace;
            final List<Ticker> tickers = new ArrayList<>(4);

            final Supplier<Long> transactionStartTimeSupplier = new Supplier<Long>()
            {
                @Override
                public Long get()
                {
                    return getTransactionStartTimeLong();
                }
            };
            final Supplier<Long> transactionUpdateTimeSupplier = new Supplier<Long>()
            {
                @Override
                public Long get()
                {
                    return getTransactionUpdateTimeLong();
                }
            };

            long notificationRepeatPeriod =
                    getContextValue(Long.class, Session.TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD);

            if (virtualhost.getStoreTransactionOpenTimeoutWarn() > 0)
            {
                tickers.add(new TransactionTimeoutTicker(
                        virtualhost.getStoreTransactionOpenTimeoutWarn(),
                        notificationRepeatPeriod, transactionStartTimeSupplier,
                        new Action<Long>()
                        {
                            @Override
                            public void performAction(Long age)
                            {
                                eventLogger.message(getLogSubject(), ChannelMessages.OPEN_TXN(age));
                            }
                        }
                ));
            }
            if (virtualhost.getStoreTransactionOpenTimeoutClose() > 0)
            {
                tickers.add(new TransactionTimeoutTicker(
                        virtualhost.getStoreTransactionOpenTimeoutClose(),
                        notificationRepeatPeriod, transactionStartTimeSupplier,
                        new Action<Long>()
                        {
                            @Override
                            public void performAction(Long age)
                            {
                                doTimeoutAction(OPEN_TRANSACTION_TIMEOUT_ERROR);
                            }
                        }
                ));
            }
            if (virtualhost.getStoreTransactionIdleTimeoutWarn() > 0)
            {
                tickers.add(new TransactionTimeoutTicker(
                        virtualhost.getStoreTransactionIdleTimeoutWarn(),
                        notificationRepeatPeriod, transactionUpdateTimeSupplier,
                        new Action<Long>()
                        {
                            @Override
                            public void performAction(Long age)
                            {
                                eventLogger.message(getLogSubject(), ChannelMessages.IDLE_TXN(age));
                            }
                        }
                ));
            }
            if (virtualhost.getStoreTransactionIdleTimeoutClose() > 0)
            {
                tickers.add(new TransactionTimeoutTicker(
                        virtualhost.getStoreTransactionIdleTimeoutClose(),
                        notificationRepeatPeriod, transactionUpdateTimeSupplier,
                        new Action<Long>()
                        {
                            @Override
                            public void performAction(Long age)
                            {
                                doTimeoutAction(IDLE_TRANSACTION_TIMEOUT_ERROR);
                            }
                        }
                ));
            }

            for (Ticker ticker : tickers)
            {
                addTicker(ticker);
            }

            Action deleteTickerTask = new Action()
            {
                @Override
                public void performAction(Object o)
                {
                    removeDeleteTask(this);
                    for (Ticker ticker : tickers)
                    {
                        removeTicker(ticker);
                    }
                }
            };
            addDeleteTask(deleteTickerTask);
        }
    }

    public abstract void addTicker(final Ticker ticker);

    public abstract void removeTicker(final Ticker ticker);

    public abstract void doTimeoutAction(final String idleTransactionTimeoutError);

    public abstract LogSubject getLogSubject();

    public abstract long getTransactionUpdateTimeLong();

    public abstract long getTransactionStartTimeLong();


    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(ChannelMessages.OPERATION(operation));
    }

}
