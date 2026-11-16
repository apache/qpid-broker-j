/*
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
 */
package org.apache.qpid.server.virtualhost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ResourceLimitMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerConnectionLimitProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostConnectionLimitProvider;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.limit.CachedConnectionLimiterImpl;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionLimiter.CachedLimiter;
import org.apache.qpid.server.security.limit.ConnectionLimiterService;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;

final class VirtualHostConnectionLimiter extends CachedConnectionLimiterImpl implements CachedLimiter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualHostConnectionLimiter.class);

    private final VirtualHost<?> _virtualHost;

    private final Broker<?> _broker;

    private final Map<ConnectionLimitProvider<?>, ConnectionLimiter> _connectionLimitProviders = new HashMap<>();

    private final Map<ConfiguredObject<?>, ProviderChangeListener> _providerListeners = new ConcurrentHashMap<>();

    private final List<ConnectionLimiterService> _serviceLimiters = new CopyOnWriteArrayList<>();

    private final ChangeListener _virtualHostChangeListener;
    private final ChangeListener _brokerChangeListener;

    private final AtomicBoolean _open = new AtomicBoolean();

    VirtualHostConnectionLimiter(VirtualHost<?> virtualHost, Broker<?> broker)
    {
        super(ConnectionLimiter.noLimits());
        _virtualHost = Objects.requireNonNull(virtualHost);
        _broker = Objects.requireNonNull(broker);
        _virtualHostChangeListener = ChangeListener.virtualHostChangeListener(this);
        _brokerChangeListener = ChangeListener.brokerChangeListener(this);
    }

    public void open()
    {
        if (!_open.compareAndSet(false, true))
        {
            return;
        }
        _virtualHost.addChangeListener(_virtualHostChangeListener);
        _broker.addChangeListener(_brokerChangeListener);

        _virtualHost.getChildren(VirtualHostConnectionLimitProvider.class)
                .forEach(child -> addProvider(child, VirtualHostConnectionLimitProvider.class));
        _broker.getChildren(BrokerConnectionLimitProvider.class)
                .forEach(child -> addProvider(child, BrokerConnectionLimitProvider.class));

        final QpidServiceLoader serviceLoader = new QpidServiceLoader();
        for (final ConnectionLimiterService service : serviceLoader.instancesOf(ConnectionLimiterService.class))
        {
            LOGGER.debug("New connection limiter service found: {}", service.getType());
            _serviceLimiters.add(service);
        }
    }

    public void activate()
    {
        update();
    }

    public void close()
    {
        synchronized (this)
        {
            if (!_open.compareAndSet(true, false))
            {
                return;
            }
            _connectionLimitProviders.clear();
            _serviceLimiters.clear();
            swapLimiter(ConnectionLimiter.noLimits());
        }

        _virtualHost.removeChangeListener(_virtualHostChangeListener);
        _broker.removeChangeListener(_brokerChangeListener);

        _providerListeners.forEach((provider, listener) ->
        {
            if (_providerListeners.remove(provider, listener))
            {
                provider.removeChangeListener(listener);
            }
        });
    }

    private void addProvider(final ConfiguredObject<?> provider, final Class<?> category)
    {
        if (_open.get())
        {
            final ProviderChangeListener listener = _providerListeners.computeIfAbsent(provider,
                    object -> new ProviderChangeListener(this, category));
            provider.addChangeListener(listener);
            if (!_open.get() || _providerListeners.get(provider) != listener)
            {
                _providerListeners.remove(provider, listener);
                provider.removeChangeListener(listener);
            }
        }
    }

    private void removeProvider(final ConfiguredObject<?> provider)
    {
        final ProviderChangeListener listener = _providerListeners.remove(provider);
        if (listener != null)
        {
            provider.removeChangeListener(listener);
        }
        update(provider);
    }

    private synchronized void update(final ConfiguredObject<?> provider, final ProviderChangeListener listener)
    {
        if (_providerListeners.get(provider) == listener)
        {
            update(provider);
        }
    }

    private synchronized void updateState(final ConfiguredObject<?> provider, final ProviderChangeListener listener)
    {
        if (_providerListeners.get(provider) == listener)
        {
            if (provider.getState() != State.ACTIVE)
            {
                _connectionLimitProviders.remove(provider);
            }
            update();
        }
    }

    private synchronized void update(final ConfiguredObject<?> object)
    {
        _connectionLimitProviders.remove(object);
        update();
    }

    private synchronized void update()
    {
        if (_open.get() && !((SystemConfig<?>) _broker.getParent()).isManagementMode())
        {
            swapLimiter(newLimiter(_connectionLimitProviders));
        }
    }

    private ConnectionLimiter newLimiter(final Map<ConnectionLimitProvider<?>, ConnectionLimiter> cache)
    {
        ConnectionLimiter limiter = ConnectionLimiter.noLimits();

        LOGGER.debug("Updating virtual host connection limiters");
        for (final VirtualHostConnectionLimitProvider<?> provider :
                _virtualHost.getChildren(VirtualHostConnectionLimitProvider.class))
        {
            final State state = provider.getState();
            if (state == State.ACTIVE)
            {
                limiter = limiter.append(
                        cache.computeIfAbsent(provider, ConnectionLimitProvider::getConnectionLimiter));
            }
            else if (state == State.UNINITIALIZED || state == State.ERRORED)
            {
                limiter = unavailableProviderLimiter(provider, state);
            }
        }

        LOGGER.debug("Updating broker connection limiters");
        for (final BrokerConnectionLimitProvider<?> provider :
                _broker.getChildren(BrokerConnectionLimitProvider.class))
        {
            final State state = provider.getState();
            if (state == State.ACTIVE)
            {
                limiter = limiter.append(
                        cache.computeIfAbsent(provider, ConnectionLimitProvider::getConnectionLimiter));
            }
            else if (state == State.UNINITIALIZED || state == State.ERRORED)
            {
                limiter = unavailableProviderLimiter(provider, state);
            }
        }

        LOGGER.debug("Updating service based connection limiters");
        for (final ConnectionLimiterService service : _serviceLimiters)
        {
            limiter = limiter.append(service);
        }
        return limiter;
    }

    private ConnectionLimiter unavailableProviderLimiter(final ConnectionLimitProvider<?> provider,
                                                         final State blockingState)
    {
        return new ConnectionLimiter()
        {
            @Override
            public ConnectionSlot register(final AMQPConnection<?> connection)
            {
                final State currentState = provider.getState();
                final State state = currentState == State.UNINITIALIZED || currentState == State.ERRORED ?
                        currentState : blockingState;
                final String message = String.format(
                        "Connection limit provider '%s' (%s) is %s (state %s); "
                        + "new connections to virtual host '%s' are rejected",
                        provider.getName(), provider.getCategoryClass().getSimpleName(),
                        state == State.UNINITIALIZED ? "initializing" : "errored", state, _virtualHost.getName());
                final String principal = connection.getPrincipal();
                final LogMessage logMessage = state == State.UNINITIALIZED
                        ? ResourceLimitMessages.REJECTED_INITIALIZING("Opening", "connection", principal, message)
                        : ResourceLimitMessages.REJECTED_ERRORED("Opening", "connection", principal, message);
                connection.getEventLogger().message(logMessage);
                throw new ConnectionLimitException(message);
            }

            @Override
            public ConnectionLimiter append(final ConnectionLimiter limiter)
            {
                Objects.requireNonNull(limiter);
                return this;
            }
        };
    }

    private abstract static class AbstractChangeListener extends AbstractConfigurationChangeListener
    {
        final VirtualHostConnectionLimiter _limiter;

        final Class<?> _providerClazz;

        AbstractChangeListener(VirtualHostConnectionLimiter limiter, Class<?> providerClazz)
        {
            super();
            _limiter = Objects.requireNonNull(limiter);
            _providerClazz = Objects.requireNonNull(providerClazz);
        }

        void addProvider(final ConfiguredObject<?> provider)
        {
            _limiter.addProvider(provider, _providerClazz);
            _limiter.update();
        }

        void removeProvider(final ConfiguredObject<?> provider)
        {
            _limiter.removeProvider(provider);
        }
    }

    private static final class ChangeListener extends AbstractChangeListener
    {
        private final Class<?> _categoryClass;

        static ChangeListener virtualHostChangeListener(VirtualHostConnectionLimiter limiter)
        {
            return new ChangeListener(limiter, VirtualHost.class, VirtualHostConnectionLimitProvider.class);
        }

        static ChangeListener brokerChangeListener(VirtualHostConnectionLimiter limiter)
        {
            return new ChangeListener(limiter, Broker.class, BrokerConnectionLimitProvider.class);
        }

        private ChangeListener(VirtualHostConnectionLimiter limiter,
                               Class<?> categoryClass, Class<?> childCategoryClass)
        {
            super(limiter, childCategoryClass);
            _categoryClass = categoryClass;
        }

        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            super.childAdded(object, child);
            if (object.getCategoryClass() == _categoryClass && child.getCategoryClass() == _providerClazz)
            {
                addProvider(child);
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            super.childRemoved(object, child);
            if (object.getCategoryClass() == _categoryClass && child.getCategoryClass() == _providerClazz)
            {
                removeProvider(child);
            }
        }
    }

    private static final class ProviderChangeListener extends AbstractChangeListener
    {
        private final AtomicBoolean _bulkChange = new AtomicBoolean();

        ProviderChangeListener(final VirtualHostConnectionLimiter limiter, final Class<?> clazz)
        {
            super(limiter, clazz);
        }

        @Override
        public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
        {
            if (object.getCategoryClass() == _providerClazz)
            {
                _limiter.updateState(object, this);
            }
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
            if (object.getCategoryClass() == _providerClazz && !_bulkChange.get())
            {
                updateProvider(object);
            }
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
            super.bulkChangeStart(object);
            _bulkChange.set(true);
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            super.bulkChangeEnd(object);
            if (_bulkChange.getAndSet(false))
            {
                updateProvider(object);
            }
        }

        private void updateProvider(final ConfiguredObject<?> provider)
        {
            _limiter.update(provider, this);
        }
    }
}
