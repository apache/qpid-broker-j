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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostStoreUpgraderAndRecovererTest extends UnitTestBase
{
    private static final Map<String, Object> ROOT_ATTRIBUTES = Map.of("modelVersion", "6.1", "name", "root");

    private VirtualHostStoreUpgraderAndRecoverer _upgraderAndRecoverer;
    private DurableConfigurationStore _store;

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setUp() throws Exception
    {
        final Broker broker = mock(Broker.class);
        final VirtualHostNode<?> virtualHostNode = mock(VirtualHostNode.class);
        when(virtualHostNode.getParent()).thenReturn(broker);
        _store = mock(DurableConfigurationStore.class);
        _upgraderAndRecoverer = new VirtualHostStoreUpgraderAndRecoverer(virtualHostNode);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpgradeFlowControlFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);
        final Map<String, Object> queueAttributes = Map.of("name", "queue",
                "queueFlowControlSizeBytes", 1000,
                "queueFlowResumeSizeBytes", 700);
        final ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Queue", queueAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));
        final List<ConfiguredObjectRecord> records = List.of(rootRecord, queueRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded queue record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertEquals(1000, upgradedAttributes.get("maximumQueueDepthBytes"), "Unexpected maximumQueueDepthBytes");

        final NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMinimumFractionDigits(2);

        assertEquals(formatter.format(70L), ((Map<String, String>) upgradedAttributes.get("context"))
                .get("queue.queueFlowResumeLimit"), "Unexpected queue.queueFlowResumeLimit");

        assertEquals(OverflowPolicy.PRODUCER_FLOW_CONTROL.name(), String.valueOf(upgradedAttributes.get("overflowPolicy")),
                "Unexpected overflowPolicy");
    }

    @Test
    public void testUpgradeQueueAlternateExchangeFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);
        final Map<String, Object> queueAttributes = Map.of("name", "queue",
                "alternateExchange", "testExchange");

        final ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Queue", queueAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = Map.of("name", "testExchange");
        final ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", exchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));
        final List<ConfiguredObjectRecord> records = List.of(rootRecord, queueRecord, exchangeRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded queue record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertTrue(upgradedAttributes.containsKey("alternateBinding"), "Attribute 'alternateBinding' was not added");
        assertEquals(Map.of("destination", "testExchange"), upgradedAttributes.get("alternateBinding"),
                "Unexpected alternateBinding");
        assertFalse(upgradedAttributes.containsKey("alternateExchange"), "Attribute 'alternateExchange' was not removed");
    }

    @Test
    public void testUpgradeExchangeAlternateExchangeFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);

        final Map<String, Object> alternateExchangeAttributes = new HashMap<>();
        alternateExchangeAttributes.put("name", "testExchange");
        final ConfiguredObjectRecord alternateExchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", alternateExchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = Map.of("name", "exchange",
                "alternateExchange", "testExchange");

        final ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", exchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final List<ConfiguredObjectRecord> records = List.of(rootRecord, exchangeRecord, alternateExchangeRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(exchangeRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded exchange record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertTrue(upgradedAttributes.containsKey("alternateBinding"), "Attribute 'alternateBinding' was not added");
        assertEquals(Map.of("destination", "testExchange"), upgradedAttributes.get("alternateBinding"),
                "Unexpected alternateBinding");
        assertFalse(upgradedAttributes.containsKey("alternateExchange"), "Attribute 'alternateExchange' was not removed");
    }
    @Test
    public void testUpgradeExchangeAlternateExchangeSpecifiedWithUUIDFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);

        final Map<String, Object> alternateExchangeAttributes = Map.of("name", "testExchange");
        final UUID alternateExchangeId = randomUUID();
        final ConfiguredObjectRecord alternateExchangeRecord = new ConfiguredObjectRecordImpl(alternateExchangeId, "Exchange", alternateExchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));
        final Map<String, Object> exchangeAttributes = Map.of("name", "exchange",
                "alternateExchange", alternateExchangeId.toString());

        final ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", exchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final List<ConfiguredObjectRecord> records = List.of(rootRecord, exchangeRecord, alternateExchangeRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(exchangeRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded exchange record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertTrue(upgradedAttributes.containsKey("alternateBinding"), "Attribute 'alternateBinding' was not added");
        assertEquals(Map.of("destination", "testExchange"), upgradedAttributes.get("alternateBinding"),
                "Unexpected alternateBinding");
        assertFalse(upgradedAttributes.containsKey("alternateExchange"),
                "Attribute 'alternateExchange' was not removed");
    }

    @Test
    public void testUpgradeQueueSharedMessageGroupsFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);
        final Map<String, Object> queueAttributes = Map.of("messageGroupKey", "myheader",
                "messageGroupSharedGroups", true);

        final ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Queue", queueAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = Map.of("name", "testExchange");
        final ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", exchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));
        final List<ConfiguredObjectRecord> records = List.of(rootRecord, queueRecord, exchangeRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded queue record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertFalse(upgradedAttributes.containsKey("messageGroupKey"),
                "Attribute 'messageGroupKey' was not removed");
        assertFalse(upgradedAttributes.containsKey("messageGroupSharedGroups"),
                "Attribute 'messageGroupSharedGroups' was not removed");

        assertTrue(upgradedAttributes.containsKey("messageGroupKeyOverride"),
                "Attribute 'messageGroupKeyOverride' was not added");
        assertEquals("myheader", upgradedAttributes.get("messageGroupKeyOverride"),
                "Unexpected messageGroupKeyOverride");
        assertTrue(upgradedAttributes.containsKey("messageGroupType"),
                "Attribute 'messageGroupType' was not added");
        assertEquals("SHARED_GROUPS", upgradedAttributes.get("messageGroupType"),
                "Unexpected messageGroupType");
    }

    @Test
    public void testUpgradeQueueStandardMessageGroupsFrom_6_1()
    {
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", ROOT_ATTRIBUTES);
        final Map<String, Object> queueAttributes = Map.of("messageGroupKey", "JMSXGroupId",
                "messageGroupSharedGroups", false);

        final ConfiguredObjectRecord queueRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Queue", queueAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));

        final Map<String, Object> exchangeAttributes = Map.of("name", "testExchange");
        final ConfiguredObjectRecord exchangeRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Exchange", exchangeAttributes,
                Map.of(rootRecord.getType(), rootRecord.getId()));
        final List<ConfiguredObjectRecord> records = List.of(rootRecord, queueRecord, exchangeRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedQueueRecord = findRecordById(queueRecord.getId(), upgradedRecords);
        assertNotNull(upgradedQueueRecord, "Upgraded queue record not found ");

        final Map<String, Object> upgradedAttributes = upgradedQueueRecord.getAttributes();
        assertNotNull(upgradedAttributes, "Upgraded attributes not found");

        assertFalse(upgradedAttributes.containsKey("messageGroupKey"),
                "Attribute 'messageGroupKey' was not removed");
        assertFalse(upgradedAttributes.containsKey("messageGroupSharedGroups"),
                "Attribute 'messageGroupSharedGroups' was not removed");
        assertFalse(upgradedAttributes.containsKey("messageGroupKeyOverride"),
                "Attribute 'messageGroupKeyOverride' was added");

        assertTrue(upgradedAttributes.containsKey("messageGroupType"),
                "Attribute 'messageGroupType' was not added");
        assertEquals("STANDARD", upgradedAttributes.get("messageGroupType"), "Unexpected messageGroupType");
    }

    @Test
    public void testContextVariableUpgradeForTLSProtocolsSetOnVirtualHost()
    {
        final Map<String, String> context = Map.of("qpid.security.tls.protocolWhiteList", ".*",
                "qpid.security.tls.protocolBlackList", "Ssl.*");
        final Map<String, Object> rootAttributes = Map.of("modelVersion", "8.0",
                "name", "root",
                "context", context);
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", rootAttributes);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, List.of(rootRecord), "VirtualHost", "modelVersion");

        final Map<String, Object> newContext = getContextForRecordWithGivenId(rootRecord.getId(), upgradedRecords);
        assertEquals(".*", newContext.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST));
        assertEquals("Ssl.*", newContext.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST));
    }

    @Test
    public void testContextVariableUpgradeForTLSCipherSuitesSetOnVirtualHostAccessControlProvider()
    {
        final Map<String, Object> rootAttributes = Map.of("modelVersion", "8.0","name", "root");
        final ConfiguredObjectRecord rootRecord =
                new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", rootAttributes);

        final Map<String, String> context = Map.of("qpid.security.tls.cipherSuiteWhiteList", ".*",
                "qpid.security.tls.cipherSuiteBlackList", "Ssl.*");
        final ConfiguredObjectRecord accessControlProviderRecord =
                createMockRecordForGivenCategoryTypeAndContext("VirtualHostAccessControlProvider", "test", context);

        final List<ConfiguredObjectRecord> records = List.of(rootRecord, accessControlProviderRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final Map<String, Object> newContext =
                getContextForRecordWithGivenId(accessControlProviderRecord.getId(), upgradedRecords);
        assertEquals(".*", newContext.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST));
        assertEquals("Ssl.*", newContext.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST));
    }

    @ParameterizedTest
    @CsvSource(value =
    {
            "4,20,5,80,20", "0,20,5,40,20", "null,20,5,40,20", "4,null,null,40,20", "null,null,null,40,20"
    }, nullValues = { "null" })
    public void testContextVariableUpgradeFromBoneCPToHikariCPProvider(final String partitionCount,
                                                                       final String maxConnectionsPerPartition,
                                                                       final String minConnectionsPerPartition,
                                                                       final String maximumPoolSize,
                                                                       final String minimumIdle)
    {
        final Map<String, Object> rootAttributes = Map.of("modelVersion", "9.0", "name", "root");
        final ConfiguredObjectRecord rootRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", rootAttributes);

        final Map<String, String> context = new HashMap<>();
        context.put("qpid.jdbcstore.bonecp.partitionCount", partitionCount);
        context.put("qpid.jdbcstore.bonecp.maxConnectionsPerPartition", maxConnectionsPerPartition);
        context.put("qpid.jdbcstore.bonecp.minConnectionsPerPartition", minConnectionsPerPartition);
        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "modelVersion", "9.0",
                "type", "JDBC",
                "connectionPoolType", "BONECP",
                "context", context);
        final ConfiguredObjectRecord virtualHostRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostRecord.getId()).thenReturn(randomUUID());
        when(virtualHostRecord.getType()).thenReturn("VirtualHost");
        when(virtualHostRecord.getAttributes()).thenReturn(attributes);

        final List<ConfiguredObjectRecord> records = List.of(rootRecord, virtualHostRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedVirtualHost = upgradedRecords.stream()
                .filter(record -> record.getId().equals(virtualHostRecord.getId())).findFirst()
                .orElse(null);
        final Map<String, String> contextMap = (Map<String, String>) upgradedVirtualHost.getAttributes().get("context");

        assertNotNull(upgradedVirtualHost);
        assertEquals(maximumPoolSize, contextMap.get("qpid.jdbcstore.hikaricp.maximumPoolSize"));
        assertEquals(minimumIdle, contextMap.get("qpid.jdbcstore.hikaricp.minimumIdle"));
        assertEquals("HIKARICP", upgradedVirtualHost.getAttributes().get("connectionPoolType"));
    }

    @Test
    public void testContextVariableUpgradeFromDefaultCPToHikariCPProvider()
    {
        final Map<String, Object> rootAttributes = Map.of("modelVersion", "9.0", "name", "root");
        final ConfiguredObjectRecord rootRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost", rootAttributes);

        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "modelVersion", "9.0",
                "type", "JDBC",
                "connectionPoolType", "NONE",
                "context", new HashMap<>());
        final ConfiguredObjectRecord virtualHostRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostRecord.getId()).thenReturn(randomUUID());
        when(virtualHostRecord.getType()).thenReturn("VirtualHost");
        when(virtualHostRecord.getAttributes()).thenReturn(attributes);

        final List<ConfiguredObjectRecord> records = List.of(rootRecord, virtualHostRecord);
        final List<ConfiguredObjectRecord> upgradedRecords =
                _upgraderAndRecoverer.upgrade(_store, records, "VirtualHost", "modelVersion");

        final ConfiguredObjectRecord upgradedVirtualHost = upgradedRecords.stream()
                .filter(record -> record.getId().equals(virtualHostRecord.getId())).findFirst()
                .orElse(null);
        final Map<String, String> contextMap = (Map<String, String>) upgradedVirtualHost.getAttributes().get("context");

        assertNotNull(upgradedVirtualHost);
        assertNull(contextMap.get("qpid.jdbcstore.hikaricp.maximumPoolSize"));
        assertNull(contextMap.get("qpid.jdbcstore.hikaricp.minimumIdle"));
        assertEquals("NONE", virtualHostRecord.getAttributes().get("connectionPoolType"));
    }

    private ConfiguredObjectRecord findRecordById(final UUID id, final List<ConfiguredObjectRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().orElse(null);
    }

    private ConfiguredObjectRecord createMockRecordForGivenCategoryTypeAndContext(final String category,
                                                                                  final String type,
                                                                                  final Map<String, String> context)
    {
        final ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(record.getId()).thenReturn(randomUUID());
        when(record.getType()).thenReturn(category);
        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "type", type,
                "context", context);
        when(record.getAttributes()).thenReturn(attributes);
        return record;
    }

    private Map<String, Object> getContextForRecordWithGivenId(final UUID rootRecordId,
                                                               final List<ConfiguredObjectRecord> upgradedRecords)
    {
        final ConfiguredObjectRecord upgradedRecord = findRecordById(rootRecordId, upgradedRecords);
        assertNotNull(upgradedRecord);
        final Map<String, Object> attributes = upgradedRecord.getAttributes();
        assertNotNull(attributes);

        final Object context = attributes.get("context");
        assertTrue(context instanceof Map);
        @SuppressWarnings("unchecked")
        final Map<String, Object> contextMap = (Map<String, Object>) context;
        return contextMap;
    }
}
