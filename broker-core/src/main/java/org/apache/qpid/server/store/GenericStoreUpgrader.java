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
package org.apache.qpid.server.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.BrokerModel;

public class GenericStoreUpgrader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericStoreUpgrader.class);

    private final Map<UUID, ConfiguredObjectRecord> _records = new HashMap<UUID, ConfiguredObjectRecord>();
    private final Map<String, StoreUpgraderPhase> _upgraders;
    private final DurableConfigurationStore _store;
    private final String _rootCategory;
    private final String _modelVersionAttributeName;

    public GenericStoreUpgrader(String rootCategory, String rootModelVersionAttributeName, DurableConfigurationStore configurationStore, Map<String, StoreUpgraderPhase> upgraders)
    {
        super();
        _upgraders = upgraders;
        _store = configurationStore;
        _rootCategory = rootCategory;
        _modelVersionAttributeName = rootModelVersionAttributeName;
    }


    public List<ConfiguredObjectRecord> getRecords()
    {
        return new ArrayList<ConfiguredObjectRecord>(_records.values());
    }

    public void upgrade(List<ConfiguredObjectRecord> records)
    {
        _records.clear();
        for(ConfiguredObjectRecord record : records)
        {
            _records.put(record.getId(), record);
        }
        performUpgrade();

    }

    private void performUpgrade()
    {
        String version = getCurrentVersion();

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info(_rootCategory + " store has model version " + version + ". Number of record(s) " + _records.size());
        }

        Map<UUID, ConfiguredObjectRecord> updatedRecords = new HashMap<>();
        Map<UUID, ConfiguredObjectRecord> records = new HashMap<>(_records);
        for(DurableConfigurationStoreUpgrader upgrader: buildUpgraderList(version))
        {
            for(ConfiguredObjectRecord record : records.values())
            {
                upgrader.configuredObject(record);
            }

            upgrader.complete();

            Set<UUID> deleted = upgrader.getDeletedRecords().keySet();
            updatedRecords.putAll(upgrader.getUpdatedRecords());
            updatedRecords.keySet().removeAll(deleted);

            records.keySet().removeAll(deleted);
            records.putAll(updatedRecords);
        }

        Map<UUID, ConfiguredObjectRecord> deletedRecords = new HashMap<>(_records);
        deletedRecords.keySet().removeAll(records.keySet());

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(_rootCategory + " store upgrade is about to complete. " + _records.size() + " total record(s)."
                    + " Records to update " + updatedRecords.size()
                    + " Records to delete " + deletedRecords.size());
        }

        _store.update(true, updatedRecords.values().toArray(new ConfiguredObjectRecord[updatedRecords.size()]));
        _store.remove(deletedRecords.values().toArray(new ConfiguredObjectRecord[deletedRecords.size()]));

        _records.keySet().removeAll(deletedRecords.keySet());
        _records.putAll(updatedRecords);
    }

    private List<DurableConfigurationStoreUpgrader> buildUpgraderList(String version)
    {
        List<DurableConfigurationStoreUpgrader> result = new LinkedList<>();
        while(!BrokerModel.MODEL_VERSION.equals(version))
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Adding " + _rootCategory + " store upgrader from model version: " + version);
            }

            StoreUpgraderPhase upgrader = _upgraders.get(version);
            if (upgrader == null)
            {
                throw new IllegalConfigurationException("No phase upgrader for version " + version);
            }
            version = upgrader.getToVersion();
            result.add(upgrader);
        }
        return result;
    }

    private String getCurrentVersion()
    {
        for(ConfiguredObjectRecord record : _records.values())
        {
            if(_rootCategory.equals(record.getType()))
            {
                return (String) record.getAttributes().get(_modelVersionAttributeName);
            }
        }
        return BrokerModel.MODEL_VERSION;
    }

}
