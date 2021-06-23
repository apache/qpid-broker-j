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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerConnectionLimitProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostConnectionLimitProvider;
import org.apache.qpid.server.security.limit.CachedConnectionLimiterImpl;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionLimiter.CachedLimiter;

final class VirtualHostConnectionLimiter extends CachedConnectionLimiterImpl implements CachedLimiter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualHostConnectionLimiter.class);

    private final VirtualHost<?> _virtualHost;

    private final Broker<?> _broker;

    private final Map<ConnectionLimitProvider<?>, ConnectionLimiter> _connectionLimitProviders = new ConcurrentHashMap<>();

    private final ChangeListener _virtualHostChangeListener;
    private final ChangeListener _brokerChangeListener;

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
        _virtualHost.addChangeListener(_virtualHostChangeListener);
        _broker.addChangeListener(_brokerChangeListener);

        _virtualHost.getChildren(VirtualHostConnectionLimitProvider.class)
                .forEach(child -> child.addChangeListener(ProviderChangeListener.virtualHostChangeListener(this)));
        _broker.getChildren(BrokerConnectionLimitProvider.class)
                .forEach(child -> child.addChangeListener(ProviderChangeListener.brokerChangeListener(this)));
    }

    public void activate()
    {
        update();
    }

    public void close()
    {
        _virtualHost.removeChangeListener(_virtualHostChangeListener);
        _broker.removeChangeListener(_brokerChangeListener);

        final ProviderChangeListener virtualHostChangeListener = ProviderChangeListener.virtualHostChangeListener(this);
        _virtualHost.getChildren(VirtualHostConnectionLimitProvider.class)
                .forEach(child -> child.removeChangeListener(virtualHostChangeListener));

        final ProviderChangeListener brokerChangeListener = ProviderChangeListener.brokerChangeListener(this);
        _broker.getChildren(BrokerConnectionLimitProvider.class)
                .forEach(child -> child.removeChangeListener(brokerChangeListener));

        swapLimiter(ConnectionLimiter.noLimits());
    }

    private void update(ConfiguredObject<?> object)
    {
        _connectionLimitProviders.remove(object);
        update();
    }

    private void update()
    {
        if (!((SystemConfig<?>) _broker.getParent()).isManagementMode())
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
            if (provider.getState() == State.ACTIVE)
            {
                limiter = limiter.append(
                        cache.computeIfAbsent(provider, ConnectionLimitProvider::getConnectionLimiter));
            }
            else if (provider.getState() == State.ERRORED)
            {
                limiter = ConnectionLimiter.blockEveryone();
            }
        }

        LOGGER.debug("Updating broker connection limiters");
        for (final BrokerConnectionLimitProvider<?> provider :
                _broker.getChildren(BrokerConnectionLimitProvider.class))
        {
            if (provider.getState() == State.ACTIVE)
            {
                limiter = limiter.append(
                        cache.computeIfAbsent(provider, ConnectionLimitProvider::getConnectionLimiter));
            }
            else if (provider.getState() == State.ERRORED)
            {
                limiter = ConnectionLimiter.blockEveryone();
            }
        }
        return limiter;
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

        void addProvider(ConfiguredObject<?> provider)
        {
            provider.addChangeListener(new ProviderChangeListener(_limiter, _providerClazz));
            _limiter.update();
        }

        void removeProvider(ConfiguredObject<?> provider)
        {
            provider.removeChangeListener(new ProviderChangeListener(_limiter, _providerClazz));
            _limiter.update(provider);
        }

        void updateProvider(ConfiguredObject<?> provider)
        {
            _limiter.update(provider);
        }

        @Override
        public int hashCode()
        {
            return 31 * _limiter.hashCode() + _providerClazz.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AbstractChangeListener)
            {
                final AbstractChangeListener changeListener = (AbstractChangeListener) obj;
                return _limiter == changeListener._limiter && _providerClazz == changeListener._providerClazz;
            }
            return false;
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
        private final Map<ConfiguredObject<?>, Boolean> _bulkChanges = new ConcurrentHashMap<>();

        static ProviderChangeListener virtualHostChangeListener(VirtualHostConnectionLimiter limiter)
        {
            return new ProviderChangeListener(limiter, VirtualHostConnectionLimitProvider.class);
        }

        static ProviderChangeListener brokerChangeListener(VirtualHostConnectionLimiter limiter)
        {
            return new ProviderChangeListener(limiter, BrokerConnectionLimitProvider.class);
        }

        ProviderChangeListener(VirtualHostConnectionLimiter limiter, Class<?> clazz)
        {
            super(limiter, clazz);
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
            if (object.getCategoryClass() == _providerClazz && !_bulkChanges.containsKey(object))
            {
                updateProvider(object);
            }
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
            super.bulkChangeStart(object);
            _bulkChanges.put(object, Boolean.TRUE);
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            super.bulkChangeEnd(object);
            if (Optional.ofNullable(_bulkChanges.remove(object)).orElse(Boolean.FALSE))
            {
                updateProvider(object);
            }
        }
    }
}
