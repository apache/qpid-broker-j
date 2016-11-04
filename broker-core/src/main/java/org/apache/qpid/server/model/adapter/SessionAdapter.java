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
package org.apache.qpid.server.model.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Publisher;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConsumerListener;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.TransactionTimeoutTicker;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.transport.network.Ticker;

public final class SessionAdapter extends AbstractConfiguredObject<SessionAdapter> implements Session<SessionAdapter>
{
    private static final String OPEN_TRANSACTION_TIMEOUT_ERROR = "Open transaction timed out";
    private static final String IDLE_TRANSACTION_TIMEOUT_ERROR = "Idle transaction timed out";

    // Attributes
    private final AMQSessionModel _session;
    private final Action _deleteModelTask;
    private final AbstractAMQPConnection<?> _amqpConnection;

    public SessionAdapter(final AbstractAMQPConnection<?> amqpConnection,
                          final AMQSessionModel session)
    {
        super(parentsMap(amqpConnection), createAttributes(session));
        _amqpConnection = amqpConnection;
        _session = session;
        _session.addConsumerListener(new ConsumerListener()
        {
            @Override
            public void consumerAdded(final Consumer<?> consumer)
            {
                childAdded(consumer);
            }

            @Override
            public void consumerRemoved(final Consumer<?> consumer)
            {
                childRemoved(consumer);

            }
        });
        session.setModelObject(this);

        _deleteModelTask = new Action()
        {
            @Override
            public void performAction(final Object object)
            {
                session.removeDeleteTask(this);
                deleteAsync();
            }
        };
        session.addDeleteTask(_deleteModelTask);
        setState(State.ACTIVE);
    }

    private static Map<String, Object> createAttributes(final AMQSessionModel session)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ID, UUID.randomUUID());
        attributes.put(NAME, String.valueOf(session.getChannelId()));
        attributes.put(DURABLE, false);
        attributes.put(LIFETIME_POLICY, LifetimePolicy.DELETE_ON_SESSION_END);
        return attributes;
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();
        registerTransactionTimeoutTickers(_amqpConnection, _session);
    }

    @Override
    public int getChannelId()
    {
        return _session.getChannelId();
    }

    @Override
    public boolean isProducerFlowBlocked()
    {
        return _session.getBlocking();
    }

    public Collection<org.apache.qpid.server.model.Consumer> getConsumers()
    {
        return (Collection<Consumer>) _session.getConsumers();
    }

    public Collection<Publisher> getPublishers()
    {
        return Collections.emptySet();  //TODO
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(Class<C> clazz)
    {
        if (clazz == org.apache.qpid.server.model.Consumer.class)
        {
            return (Collection<C>) getConsumers();
        }
        else if (clazz == Publisher.class)
        {
            return (Collection<C>) getPublishers();
        }
        else
        {
            return Collections.emptySet();
        }
    }

    @Override
    public long getConsumerCount()
    {
        return _session.getConsumerCount();
    }

    @Override
    public long getLocalTransactionBegins()
    {
        return _session.getTxnStart();
    }

    @Override
    public int getLocalTransactionOpen()
    {
        long open = _session.getTxnStart() - (_session.getTxnCommits() + _session.getTxnRejects());
        return (open > 0l) ? 1 : 0;
    }

    @Override
    public long getLocalTransactionRollbacks()
    {
        return _session.getTxnRejects();
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return _session.getUnacknowledgedMessageCount();
    }

    @Override
    public Date getTransactionStartTime()
    {
        return new Date(_session.getTransactionStartTime());
    }

    @Override
    public Date getTransactionUpdateTime()
    {
        return new Date(_session.getTransactionUpdateTime());
    }

    @StateTransition(currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        deleted();
        setState(State.DELETED);
        _session.removeDeleteTask(_deleteModelTask);
        return Futures.immediateFuture(null);
    }

    private void registerTransactionTimeoutTickers(AbstractAMQPConnection<?> amqpConnection,
                                                   final AMQSessionModel session)
    {
        NamedAddressSpace addressSpace = amqpConnection.getAddressSpace();
        if (addressSpace instanceof QueueManagingVirtualHost)
        {
            final EventLogger eventLogger = amqpConnection.getEventLogger();
            final QueueManagingVirtualHost<?> virtualhost = (QueueManagingVirtualHost<?>) addressSpace;
            final List<Ticker> tickers = new ArrayList<>(4);

            final Supplier<Long> transactionStartTimeSupplier = new Supplier<Long>()
            {
                @Override
                public Long get()
                {
                    return SessionAdapter.this._session.getTransactionStartTime();
                }
            };
            final Supplier<Long> transactionUpdateTimeSupplier = new Supplier<Long>()
            {
                @Override
                public Long get()
                {
                    return SessionAdapter.this._session.getTransactionUpdateTime();
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
                                eventLogger.message(_session.getLogSubject(), ChannelMessages.OPEN_TXN(age));
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
                                _session.doTimeoutAction(OPEN_TRANSACTION_TIMEOUT_ERROR);
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
                                eventLogger.message(_session.getLogSubject(), ChannelMessages.IDLE_TXN(age));
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
                                _session.doTimeoutAction(IDLE_TRANSACTION_TIMEOUT_ERROR);
                            }
                        }
                ));
            }

            for (Ticker ticker : tickers)
            {
                session.addTicker(ticker);
            }

            Action deleteTickerTask = new Action()
            {
                @Override
                public void performAction(Object o)
                {
                    session.removeDeleteTask(this);
                    for (Ticker ticker : tickers)
                    {
                        session.removeTicker(ticker);
                    }
                }
            };
            session.addDeleteTask(deleteTickerTask);
        }
    }
}
