/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.server.store;


import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.QpidTestCase;

public class VirtualHostStoreUpgraderAndRecovererTest extends QpidTestCase
{
    private VirtualHostNode<?> _virtualHostNode;
    private VirtualHostStoreUpgraderAndRecoverer _upgraderAndRecoverer;
    private DurableConfigurationStore _store;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _virtualHostNode = mock(VirtualHostNode.class);
        _store = mock(DurableConfigurationStore.class);
        _upgraderAndRecoverer = new VirtualHostStoreUpgraderAndRecoverer(_virtualHostNode);
    }

    public void testUpgradeForFlowControlFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);
        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put("name", "queue");
        queueAttributes.put("queueFlowControlSizeBytes", 1000);
        queueAttributes.put("queueFlowResumeSizeBytes", 700);
        ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Queue", queueAttributes,
                                                                            Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));
        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, queueRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded queue record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertEquals("Unexpected maximumQueueDepthBytes", 1000, upgradedAttributes.get("maximumQueueDepthBytes"));
        assertEquals("Unexpected queue.queueFlowResumeLimit",
                     "70.00",
                     ((Map<String, String>) upgradedAttributes.get("context")).get("queue.queueFlowResumeLimit"));
    }

    private ConfiguredObjectRecord findRecordById(UUID id, List<ConfiguredObjectRecord> records)
    {
        for (ConfiguredObjectRecord record : records)
        {
            if (id.equals(record.getId()))
            {
                return record;
            }
        }
        return null;
    }
}
