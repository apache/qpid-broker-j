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
package org.apache.qpid.server.security;

import java.util.Collection;

import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.plugin.SystemNodeCreator;

@PluggableService
public class TrustStoreMessageSourceCreator implements SystemNodeCreator
{

    @Override
    public String getType()
    {
        return "TRUSTSTORE-MESSAGE-SOURCE";
    }

    @Override
    public void register(final SystemNodeRegistry registry)
    {
        final VirtualHost<?> vhost = registry.getVirtualHost();
        VirtualHostNode<?> virtualHostNode = (VirtualHostNode<?>) vhost.getParent();
        final Broker<?> broker = (Broker<?>) virtualHostNode.getParent();

        final Collection<TrustStore> trustStores = broker.getChildren(TrustStore.class);

        final TrustStoreChangeListener trustStoreChangeListener = new TrustStoreChangeListener(registry);

        for(final TrustStore trustStore : trustStores)
        {
            updateTrustStoreSourceRegistration(registry, trustStore);
            trustStore.addChangeListener(trustStoreChangeListener);
        }
        AbstractConfigurationChangeListener brokerListener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
            {
                if (child instanceof TrustStore)
                {
                    TrustStore<?> trustStore = (TrustStore<?>) child;

                    updateTrustStoreSourceRegistration(registry, trustStore);
                    trustStore.addChangeListener(trustStoreChangeListener);
                }
            }

            @Override
            public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
            {

                if (child instanceof TrustStore)
                {
                    TrustStore<?> trustStore = (TrustStore<?>) child;

                    trustStore.removeChangeListener(trustStoreChangeListener);
                    registry.removeSystemNode(TrustStoreMessageSource.getSourceNameFromTrustStore(trustStore));
                }
                else if (child == virtualHostNode)
                {
                    object.removeChangeListener(this);
                    broker.getChildren(TrustStore.class).forEach(t -> t.removeChangeListener(trustStoreChangeListener));
                }
            }
        };
        broker.addChangeListener(brokerListener);
        virtualHostNode.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
            {
                if (child == vhost)
                {
                    broker.removeChangeListener(brokerListener);
                    object.removeChangeListener(this);
                    broker.getChildren(TrustStore.class).forEach(t -> t.removeChangeListener(trustStoreChangeListener));
                }
            }
        });
    }


    private boolean isTrustStoreExposedAsMessageSource(VirtualHostNode<?> virtualHostNode, final TrustStore trustStore)
    {
        return trustStore.getState() == State.ACTIVE && trustStore.isExposedAsMessageSource()
               && (trustStore.getIncludedVirtualHostNodeMessageSources().contains(virtualHostNode)
                   || (trustStore.getIncludedVirtualHostNodeMessageSources().isEmpty()
                       && !trustStore.getExcludedVirtualHostNodeMessageSources().contains(virtualHostNode)));
    }


    private void updateTrustStoreSourceRegistration(SystemNodeRegistry registry, TrustStore<?> trustStore)
    {
        final String sourceName = TrustStoreMessageSource.getSourceNameFromTrustStore(trustStore);
        if (isTrustStoreExposedAsMessageSource(registry.getVirtualHostNode(), trustStore))
        {
            if(!registry.hasSystemNode(sourceName))
            {

                registry.registerSystemNode(new TrustStoreMessageSource(trustStore, registry.getVirtualHost()));

            }
        }
        else
        {
            registry.removeSystemNode(sourceName);
        }
    }

    private class TrustStoreChangeListener extends AbstractConfigurationChangeListener
    {

        private final SystemNodeRegistry _registry;

        public TrustStoreChangeListener(SystemNodeRegistry registry)
        {
            _registry = registry;
        }

        @Override
        public void stateChanged(final ConfiguredObject<?> object,
                                 final State oldState,
                                 final State newState)
        {
            updateTrustStoreSourceRegistration(_registry, (TrustStore<?>)object);
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            updateTrustStoreSourceRegistration(_registry, (TrustStore<?>)object);
        }
    }
}
