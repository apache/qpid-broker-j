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

import java.security.AccessControlContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.AbstractConsumerTarget;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.consumer.ScheduledConsumerTargetSet;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.logging.subjects.ChannelLogSubject;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.protocol.PublishAuthorisationCache;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.server.util.Action;

public abstract class AbstractAMQPSession<S extends AbstractAMQPSession<S, X>,
                                          X extends ConsumerTarget<X>>
        extends AbstractConfiguredObject<S>
        implements AMQPSession<S, X>, EventLoggerProvider
{
    private final Action _deleteModelTask;
    private final AMQPConnection<?> _connection;
    private final int _sessionId;

    protected final AccessControlContext _accessControllerContext;
    protected final Subject _subject;
    protected final SecurityToken _token;
    protected final PublishAuthorisationCache _publishAuthCache;

    protected final LogSubject _logSubject;

    protected final List<Action<? super S>> _taskList = new CopyOnWriteArrayList<>();
    private final AtomicInteger _consumerCount = new AtomicInteger();

    protected final Set<AbstractConsumerTarget> _consumersWithPendingWork = new ScheduledConsumerTargetSet<>();
    private Iterator<AbstractConsumerTarget> _processPendingIterator;
    private final Set<Consumer<?,X>> _consumers = ConcurrentHashMap.newKeySet();

    protected AbstractAMQPSession(final Connection<?> parent, final int sessionId)
    {
        super(parent, createAttributes(sessionId));
        _connection = (AMQPConnection) parent;
        _sessionId = sessionId;

        _deleteModelTask = new Action<S>()
        {
            @Override
            public void performAction(final S object)
            {
                removeDeleteTask(this);
                deleteNoChecks();
            }
        };
        _subject = new Subject(false, _connection.getSubject().getPrincipals(),
                               _connection.getSubject().getPublicCredentials(),
                               _connection.getSubject().getPrivateCredentials());
        _subject.getPrincipals().add(new SessionPrincipal(this));

        if  (_connection.getAddressSpace() instanceof ConfiguredObject)
        {
            _token = ((ConfiguredObject) _connection.getAddressSpace()).newToken(_subject);
        }
        else
        {
            final Broker<?> broker = (Broker<?>) _connection.getBroker();
            _token = broker.newToken(_subject);
        }

        _accessControllerContext = _connection.getAccessControlContextFromSubject(_subject);

        final long authCacheTimeout = _connection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT);
        final int authCacheSize = _connection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE);
        _publishAuthCache = new PublishAuthorisationCache(_token, authCacheTimeout, authCacheSize);
        _logSubject = new ChannelLogSubject(this);

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
    public int getChannelId()
    {
        return _sessionId;
    }

    @Override
    public AMQPConnection<?> getAMQPConnection()
    {
        return _connection;
    }

    @Override
    public boolean isProducerFlowBlocked()
    {
        return getBlocking();
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return getUnacknowledgedMessageCount();
    }

    @Override
    public void addDeleteTask(final Action<? super S> task)
    {
        _taskList.add(task);
    }

    @Override
    public void removeDeleteTask(final Action<? super S> task)
    {
        _taskList.remove(task);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        removeDeleteTask(_deleteModelTask);
        return super.onDelete();
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _connection.getEventLogger();
    }

    @Override
    public void addTicker(final Ticker ticker)
    {
        _connection.getAggregateTicker().addTicker(ticker);
        // trigger a wakeup to ensure the ticker will be taken into account
        getAMQPConnection().notifyWork();
    }

    @Override
    public void removeTicker(final Ticker ticker)
    {
        _connection.getAggregateTicker().removeTicker(ticker);
    }

    @Override
    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(ChannelMessages.OPERATION(operation));
    }

    @Override
    public boolean processPending()
    {
        if (!getAMQPConnection().isIOThread() || isClosing())
        {
            return false;
        }

        updateBlockedStateIfNecessary();

        if(!_consumersWithPendingWork.isEmpty() && !getAMQPConnection().isTransportBlockedForWriting())
        {
            if (_processPendingIterator == null || !_processPendingIterator.hasNext())
            {
                _processPendingIterator = _consumersWithPendingWork.iterator();
            }

            if(_processPendingIterator.hasNext())
            {
                AbstractConsumerTarget target = _processPendingIterator.next();
                _processPendingIterator.remove();
                if (target.processPending())
                {
                    _consumersWithPendingWork.add(target);
                }
            }
        }

        return !_consumersWithPendingWork.isEmpty() && !getAMQPConnection().isTransportBlockedForWriting();
    }

    @Override
    public void notifyWork(final X target)
    {
        if(_consumersWithPendingWork.add((AbstractConsumerTarget) target))
        {
            getAMQPConnection().notifyWork(this);
        }
    }

    @Override
    public final long getConsumerCount()
    {
        return _consumerCount.get();
    }

    @Override
    public final void consumerAdded(Consumer<?, X> consumer)
    {
        _consumerCount.incrementAndGet();
        _consumers.add(consumer);
    }

    @Override
    public final void consumerRemoved(Consumer<?, X> consumer)
    {
        _consumerCount.decrementAndGet();
        _consumers.remove(consumer);
    }

    @Override
    public Set<? extends Consumer<?, ?>> getConsumers()
    {
        return Collections.unmodifiableSet(_consumers);
    }

    protected abstract void updateBlockedStateIfNecessary();

    public abstract boolean isClosing();

    @Override
    public ListenableFuture<Void> doOnIOThreadAsync(final Runnable task)
    {
        final ListenableFuture<Void> future = getAMQPConnection().doOnIOThreadAsync(task);
        return doAfter(MoreExecutors.directExecutor(), future, new Runnable()
        {
            @Override
            public void run()
            {
                getAMQPConnection().notifyWork(AbstractAMQPSession.this);
            }
        });
    }
}
