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
package org.apache.qpid.server.configuration.startup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.BrokerOptions;
import org.apache.qpid.server.configuration.ConfigurationEntry;
import org.apache.qpid.server.configuration.ConfiguredObjectRecoverer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.RecovererProvider;
import org.apache.qpid.server.configuration.store.StoreConfigurationChangeListener;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.LogRecorder;
import org.apache.qpid.server.logging.RootMessageLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.adapter.AuthenticationProviderFactory;
import org.apache.qpid.server.model.adapter.BrokerAdapter;
import org.apache.qpid.server.model.adapter.GroupProviderFactory;
import org.apache.qpid.server.model.adapter.PortFactory;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.virtualhost.VirtualHostRegistry;

public class BrokerRecoverer implements ConfiguredObjectRecoverer<Broker>
{
    private final StatisticsGatherer _statisticsGatherer;
    private final VirtualHostRegistry _virtualHostRegistry;
    private final LogRecorder _logRecorder;
    private final RootMessageLogger _rootMessageLogger;
    private final AuthenticationProviderFactory _authenticationProviderFactory;
    private final PortFactory _portFactory;
    private final TaskExecutor _taskExecutor;
    private final BrokerOptions _brokerOptions;
    private final GroupProviderFactory _groupProviderFactory;

    public BrokerRecoverer(AuthenticationProviderFactory authenticationProviderFactory, GroupProviderFactory groupProviderFactory,
            PortFactory portFactory, StatisticsGatherer statisticsGatherer, VirtualHostRegistry virtualHostRegistry,
            LogRecorder logRecorder, RootMessageLogger rootMessageLogger, TaskExecutor taskExecutor, BrokerOptions brokerOptions)
    {
        _groupProviderFactory = groupProviderFactory;
        _portFactory = portFactory;
        _authenticationProviderFactory = authenticationProviderFactory;
        _statisticsGatherer = statisticsGatherer;
        _virtualHostRegistry = virtualHostRegistry;
        _logRecorder = logRecorder;
        _rootMessageLogger = rootMessageLogger;
        _taskExecutor = taskExecutor;
        _brokerOptions = brokerOptions;
    }

    @Override
    public Broker create(RecovererProvider recovererProvider, ConfigurationEntry entry, ConfiguredObject... parents)
    {
        StoreConfigurationChangeListener storeChangeListener = new StoreConfigurationChangeListener(entry.getStore());
        BrokerAdapter broker = new BrokerAdapter(entry.getId(), entry.getAttributes(), _statisticsGatherer, _virtualHostRegistry,
                _logRecorder, _rootMessageLogger, _authenticationProviderFactory, _groupProviderFactory, _portFactory,
                _taskExecutor, entry.getStore(), _brokerOptions);

        broker.addChangeListener(storeChangeListener);

        //Recover the SSL keystores / truststores first, then others that depend on them
        Map<String, Collection<ConfigurationEntry>> childEntries = new HashMap<String, Collection<ConfigurationEntry>>(entry.getChildren());
        Map<String, Collection<ConfigurationEntry>> priorityChildEntries = new HashMap<String, Collection<ConfigurationEntry>>(childEntries);
        List<String> types = new ArrayList<String>(childEntries.keySet());

        for(String type : types)
        {
            if(KeyStore.class.getSimpleName().equals(type) || TrustStore.class.getSimpleName().equals(type)
                        || AuthenticationProvider.class.getSimpleName().equals(type))
            {
                childEntries.remove(type);
            }
            else
            {
                priorityChildEntries.remove(type);
            }
        }

        for (String type : priorityChildEntries.keySet())
        {
            recoverType(recovererProvider, storeChangeListener, broker, priorityChildEntries, type);
        }
        for (String type : childEntries.keySet())
        {
            recoverType(recovererProvider, storeChangeListener, broker, childEntries, type);
        }

        return broker;
    }

    private void recoverType(RecovererProvider recovererProvider,
                             StoreConfigurationChangeListener storeChangeListener,
                             BrokerAdapter broker,
                             Map<String, Collection<ConfigurationEntry>> childEntries,
                             String type)
    {
        ConfiguredObjectRecoverer<?> recoverer = recovererProvider.getRecoverer(type);
        if (recoverer == null)
        {
            throw new IllegalConfigurationException("Cannot recover entry for the type '" + type + "' from broker");
        }
        Collection<ConfigurationEntry> entries = childEntries.get(type);
        for (ConfigurationEntry childEntry : entries)
        {
            ConfiguredObject object = recoverer.create(recovererProvider, childEntry, broker);
            if (object == null)
            {
                throw new IllegalConfigurationException("Cannot create configured object for the entry " + childEntry);
            }
            broker.recoverChild(object);
            object.addChangeListener(storeChangeListener);
        }
    }
}
