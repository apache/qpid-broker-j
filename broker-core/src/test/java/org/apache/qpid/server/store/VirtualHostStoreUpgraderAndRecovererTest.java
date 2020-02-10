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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostStoreUpgraderAndRecovererTest extends UnitTestBase
{
    private VirtualHostNode<?> _virtualHostNode;
    private VirtualHostStoreUpgraderAndRecoverer _upgraderAndRecoverer;
    private DurableConfigurationStore _store;

    @Before
    public void setUp() throws Exception
    {

        final Broker broker = mock(Broker.class);
        _virtualHostNode = mock(VirtualHostNode.class);
        when(_virtualHostNode.getParent()).thenReturn(broker);
        _store = mock(DurableConfigurationStore.class);
        _upgraderAndRecoverer = new VirtualHostStoreUpgraderAndRecoverer(_virtualHostNode);
    }

    @Test
    public void testUpgradeFlowControlFrom_6_1() throws Exception
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

        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMinimumFractionDigits(2);

        assertEquals("Unexpected queue.queueFlowResumeLimit",
                            formatter.format(70L),
                            ((Map<String, String>) upgradedAttributes.get("context")).get("queue.queueFlowResumeLimit"));

        assertEquals("Unexpected overflowPolicy",
                            OverflowPolicy.PRODUCER_FLOW_CONTROL.name(),
                            String.valueOf(upgradedAttributes.get("overflowPolicy")));
    }

    @Test
    public void testUpgradeQueueAlternateExchangeFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);
        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put("name", "queue");
        queueAttributes.put("alternateExchange", "testExchange");

        ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Queue", queueAttributes,
                                                                            Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put("name", "testExchange");
        ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", exchangeAttributes,
                                                                               Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));
        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, queueRecord, exchangeRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded queue record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertTrue("Attribute 'alternateBinding' was not added",
                          upgradedAttributes.containsKey("alternateBinding"));
        assertEquals("Unexpected alternateBinding",
                            new HashMap<>(Collections.singletonMap("destination", "testExchange")),
                            new HashMap<>(((Map<String, String>) upgradedAttributes.get("alternateBinding"))));
        assertFalse("Attribute 'alternateExchange' was not removed",
                           upgradedAttributes.containsKey("alternateExchange"));

    }

    @Test
    public void testUpgradeExchangeAlternateExchangeFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);

        final Map<String, Object> alternateExchangeAttributes = new HashMap<>();
        alternateExchangeAttributes.put("name", "testExchange");
        ConfiguredObjectRecord alternateExchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", alternateExchangeAttributes,
                                                                               Collections.singletonMap(rootRecord.getType(),
                                                                                                        rootRecord.getId()));

        Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put("name", "exchange");
        exchangeAttributes.put("alternateExchange", "testExchange");

        ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", exchangeAttributes,
                                                                            Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));

        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, exchangeRecord, alternateExchangeRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(exchangeRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded exchange record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertTrue("Attribute 'alternateBinding' was not added",
                          upgradedAttributes.containsKey("alternateBinding"));
        assertEquals("Unexpected alternateBinding",
                            new HashMap<>(Collections.singletonMap("destination", "testExchange")),
                            new HashMap<>(((Map<String, String>) upgradedAttributes.get("alternateBinding"))));
        assertFalse("Attribute 'alternateExchange' was not removed",
                           upgradedAttributes.containsKey("alternateExchange"));
    }
    @Test
    public void testUpgradeExchangeAlternateExchangeSpecifiedWithUUIDFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);

        final Map<String, Object> alternateExchangeAttributes = new HashMap<>();
        alternateExchangeAttributes.put("name", "testExchange");
        UUID alternateExchangeId = UUID.randomUUID();
        ConfiguredObjectRecord alternateExchangeRecord = new ConfiguredObjectRecordImpl(alternateExchangeId, "Exchange", alternateExchangeAttributes,
                                                                                        Collections.singletonMap(rootRecord.getType(),
                                                                                                                 rootRecord.getId()));
        Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put("name", "exchange");
        exchangeAttributes.put("alternateExchange", alternateExchangeId.toString());

        ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", exchangeAttributes,
                                                                               Collections.singletonMap(rootRecord.getType(),
                                                                                                        rootRecord.getId()));

        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, exchangeRecord, alternateExchangeRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(exchangeRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded exchange record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertTrue("Attribute 'alternateBinding' was not added",
                          upgradedAttributes.containsKey("alternateBinding"));
        assertEquals("Unexpected alternateBinding",
                            new HashMap<>(Collections.singletonMap("destination", "testExchange")),
                            new HashMap<>(((Map<String, String>) upgradedAttributes.get("alternateBinding"))));
        assertFalse("Attribute 'alternateExchange' was not removed",
                           upgradedAttributes.containsKey("alternateExchange"));
    }

    @Test
    public void testUpgradeQueueSharedMessageGroupsFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);
        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put("messageGroupKey", "myheader");
        queueAttributes.put("messageGroupSharedGroups", true);

        ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Queue", queueAttributes,
                                                                            Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put("name", "testExchange");
        ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", exchangeAttributes,
                                                                               Collections.singletonMap(rootRecord.getType(),
                                                                                                        rootRecord.getId()));
        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, queueRecord, exchangeRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded queue record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertFalse("Attribute 'messageGroupKey' was not removed",
                           upgradedAttributes.containsKey("messageGroupKey"));
        assertFalse("Attribute 'messageGroupSharedGroups' was not removed",
                           upgradedAttributes.containsKey("messageGroupSharedGroups"));

        assertTrue("Attribute 'messageGroupKeyOverride' was not added",
                          upgradedAttributes.containsKey("messageGroupKeyOverride"));
        assertEquals("Unexpected messageGroupKeyOverride",
                            "myheader",
                            upgradedAttributes.get("messageGroupKeyOverride"));
        assertTrue("Attribute 'messageGroupType' was not added",
                          upgradedAttributes.containsKey("messageGroupType"));
        assertEquals("Unexpected messageGroupType", "SHARED_GROUPS", upgradedAttributes.get("messageGroupType"));
    }

    @Test
    public void testUpgradeQueueStandardMessageGroupsFrom_6_1() throws Exception
    {
        Map<String, Object> rootAttributes = new HashMap<>();
        rootAttributes.put("modelVersion", "6.1");
        rootAttributes.put("name", "root");
        ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost", rootAttributes);
        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put("messageGroupKey", "JMSXGroupId");
        queueAttributes.put("messageGroupSharedGroups", false);

        ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Queue", queueAttributes,
                                                                            Collections.singletonMap(rootRecord.getType(),
                                                                                                     rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put("name", "testExchange");
        ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Exchange", exchangeAttributes,
                                                                               Collections.singletonMap(rootRecord.getType(),
                                                                                                        rootRecord.getId()));
        List<ConfiguredObjectRecord> records = Arrays.asList(rootRecord, queueRecord, exchangeRecord);
        List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull("Upgraded queue record not found ", upgradedQueueRecord);

        Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull("Upgraded attributes not found", upgradedAttributes);

        assertFalse("Attribute 'messageGroupKey' was not removed",
                           upgradedAttributes.containsKey("messageGroupKey"));
        assertFalse("Attribute 'messageGroupSharedGroups' was not removed",
                           upgradedAttributes.containsKey("messageGroupSharedGroups"));
        assertFalse("Attribute 'messageGroupKeyOverride' was added",
                           upgradedAttributes.containsKey("messageGroupKeyOverride"));

        assertTrue("Attribute 'messageGroupType' was not added",
                          upgradedAttributes.containsKey("messageGroupType"));
        assertEquals("Unexpected messageGroupType", "STANDARD", upgradedAttributes.get("messageGroupType"));
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
