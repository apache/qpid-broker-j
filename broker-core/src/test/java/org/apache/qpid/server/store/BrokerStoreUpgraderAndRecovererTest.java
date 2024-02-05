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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.qpid.server.configuration.CommonProperties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.test.utils.UnitTestBase;

public class BrokerStoreUpgraderAndRecovererTest extends UnitTestBase
{
    private static final long BROKER_CREATE_TIME = 1401385808828L;
    private static final String BROKER_NAME = "Broker";
    private static final String VIRTUALHOST_NAME = "test";
    private static final long VIRTUALHOST_CREATE_TIME = 1401385905260L;
    private static final String VIRTUALHOST_CREATED_BY = "webadmin";

    private ConfiguredObjectRecord _brokerRecord;
    private SystemConfig<?> _systemConfig;
    private List<Map<String, Object>> _virtualHosts;
    private UUID _hostId;
    private UUID _brokerId;

    @BeforeEach
    public void setUp() throws Exception
    {
        _virtualHosts = new ArrayList<>();
        _hostId = randomUUID();
        _brokerId = randomUUID();
        final Map<String, Object> brokerAttributes = new HashMap<>();
        brokerAttributes.put("createdTime", BROKER_CREATE_TIME);
        brokerAttributes.put("defaultVirtualHost", VIRTUALHOST_NAME);
        brokerAttributes.put("modelVersion", "1.3");
        brokerAttributes.put("name", BROKER_NAME);
        brokerAttributes.put("virtualhosts", _virtualHosts);

        _brokerRecord = mock(ConfiguredObjectRecord.class);
        when(_brokerRecord.getId()).thenReturn(_brokerId);
        when(_brokerRecord.getType()).thenReturn("Broker");
        when(_brokerRecord.getAttributes()).thenReturn(brokerAttributes);

        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _systemConfig = new JsonSystemConfigImpl(taskExecutor, mock(EventLogger.class), null, Map.of());
    }

    @Test
    public void testUpgradeVirtualHostWithJDBCStoreAndBoneCPPool()
    {
        final Map<String, Object> hostAttributes = ImmutableMap.<String, Object>builder()
                .put("name", VIRTUALHOST_NAME)
                .put("modelVersion", "0.4")
                .put("connectionPool", "BONECP")
                .put("connectionURL", "jdbc:derby://localhost:1527/tmp/vh/test;create=true")
                .put("createdBy", VIRTUALHOST_CREATED_BY)
                .put("createdTime", VIRTUALHOST_CREATE_TIME)
                .put("maxConnectionsPerPartition", 7)
                .put("minConnectionsPerPartition", 6)
                .put("partitionCount", 2)
                .put("storeType", "jdbc")
                .put("type", "STANDARD")
                .put("jdbcBigIntType", "mybigint")
                .put("jdbcBlobType", "myblob")
                .put("jdbcVarbinaryType", "myvarbinary")
                .put("jdbcBytesForBlob", true).build();

        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String, Object> context = Map.of("qpid.jdbcstore.bigIntType", "mybigint",
                "qpid.jdbcstore.varBinaryType", "myvarbinary",
                "qpid.jdbcstore.blobType", "myblob",
                "qpid.jdbcstore.useBytesForBlob", true,
                "qpid.jdbcstore.hikaricp.maximumPoolSize", "14",
                "qpid.jdbcstore.hikaricp.minimumIdle", "12");
        final Map<String,Object> expectedAttributes = Map.of("connectionPoolType", "HIKARICP",
                "connectionUrl", "jdbc:derby://localhost:1527/tmp/vh/test;create=true",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "JDBC",
                "defaultVirtualHostNode", "true",
                "context", context);

        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(),
                "Unexpected attributes");
        assertBrokerRecord(records);
    }

    private List<ConfiguredObjectRecord> upgrade(final DurableConfigurationStore dcs,
                                                 final BrokerStoreUpgraderAndRecoverer recoverer)
    {
        final RecordRetrievingConfiguredObjectRecordHandler handler =
                new RecordRetrievingConfiguredObjectRecordHandler();
        dcs.openConfigurationStore(handler);
        return recoverer.upgrade(dcs, handler.getRecords());
    }

    @Test
    public void testUpgradeVirtualHostWithJDBCStoreAndDefaultPool()
    {
        final Map<String, Object> hostAttributes = ImmutableMap.<String, Object>builder()
                .put("name", VIRTUALHOST_NAME)
                .put("modelVersion", "0.4")
                .put("connectionPool", "DEFAULT")
                .put("connectionURL", "jdbc:derby://localhost:1527/tmp/vh/test;create=true")
                .put("createdBy", VIRTUALHOST_CREATED_BY)
                .put("createdTime", VIRTUALHOST_CREATE_TIME)
                .put("storeType", "jdbc")
                .put("type", "STANDARD")
                .put("jdbcBigIntType", "mybigint")
                .put("jdbcBlobType", "myblob")
                .put("jdbcVarbinaryType", "myvarbinary")
                .put("jdbcBytesForBlob", true).build();


        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String, Object> context = Map.of("qpid.jdbcstore.bigIntType", "mybigint",
                "qpid.jdbcstore.varBinaryType", "myvarbinary",
                "qpid.jdbcstore.blobType", "myblob",
                "qpid.jdbcstore.useBytesForBlob", true);
        final Map<String,Object> expectedAttributes = Map.of("connectionPoolType", "NONE",
                "connectionUrl", "jdbc:derby://localhost:1527/tmp/vh/test;create=true",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "JDBC",
                "defaultVirtualHostNode", "true",
                "context", context);

        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(),
                "Unexpected attributes");
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithDerbyStore()
    {
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.4",
                "storePath", "/tmp/vh/derby",
                "storeType", "derby",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD");

        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedAttributes = Map.of("storePath", "/tmp/vh/derby",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "DERBY",
                "defaultVirtualHostNode", "true");

        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(), "Unexpected attributes");
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithBDBStore()
    {
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.4",
                "storePath", "/tmp/vh/bdb",
                "storeType", "bdb",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD",
                "bdbEnvironmentConfig", Map.of("je.stats.collect", "false"));

        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedAttributes = Map.of("storePath", "/tmp/vh/bdb",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "BDB",
                "defaultVirtualHostNode", "true",
                "context", Map.of("je.stats.collect", "false"));
        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(), "Unexpected attributes");
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithBDBHAStore()
    {
        final Map<String, Object> hostAttributes = ImmutableMap.<String, Object>builder()
                .put("name", VIRTUALHOST_NAME)
                .put("modelVersion", "0.4")
                .put("createdBy", VIRTUALHOST_CREATED_BY)
                .put("createdTime", VIRTUALHOST_CREATE_TIME)
                .put("type", "BDB_HA")
                .put("storePath", "/tmp/vh/bdbha")
                .put("haCoalescingSync", "true")
                .put("haDesignatedPrimary", "true")
                .put("haGroupName", "ha")
                .put("haHelperAddress", "localhost:7000")
                .put("haNodeAddress", "localhost:7000")
                .put("haNodeName", "n1")
                .put("haReplicationConfig", Map.of("je.stats.collect", "false"))
                .put("bdbEnvironmentConfig", Map.of("je.rep.feederTimeout", "1 m")).build();

        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedContext = Map.of("je.stats.collect", "false",
                "je.rep.feederTimeout", "1 m");

        final Map<String,Object> expectedAttributes = ImmutableMap.<String, Object>builder()
                .put("createdBy", VIRTUALHOST_CREATED_BY)
                .put("createdTime", VIRTUALHOST_CREATE_TIME)
                .put("type", "BDB_HA")
                .put("storePath", "/tmp/vh/bdbha")
                .put("designatedPrimary", "true")
                .put("groupName", "ha")
                .put("address", "localhost:7000")
                .put("helperAddress", "localhost:7000")
                .put("name", "n1")
                .put("context", expectedContext)
                .put("defaultVirtualHostNode", "true").build();

        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(), "Unexpected attributes");
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithMemoryStore()
    {
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.4",
                "storeType", "memory",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD");

        final ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(randomUUID(), "VirtualHost",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedAttributes = Map.of("createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "Memory",
                "defaultVirtualHostNode", "true");

        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(), "Unexpected attributes");
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_0()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.0");
        _brokerRecord.getAttributes().put("virtualhosts", _virtualHosts);
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.1",
                "storeType", "memory",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "id", _hostId);
        _virtualHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_1()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.1");
        _brokerRecord.getAttributes().put("virtualhosts", _virtualHosts);
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.2",
                "storeType", "memory",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD",
                "id", _hostId);
        _virtualHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_2()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.2");
        _brokerRecord.getAttributes().put("virtualhosts", _virtualHosts);
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.3",
                "storeType", "memory",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD",
                "id", _hostId);
        _virtualHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_3()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.3");
        _brokerRecord.getAttributes().put("virtualhosts", _virtualHosts);
        final Map<String, Object> hostAttributes = Map.of("name", VIRTUALHOST_NAME,
                "modelVersion", "0.4",
                "storeType", "memory",
                "createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "type", "STANDARD",
                "id", _hostId);
        _virtualHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeNonAMQPPort()
    {
        final Map<String, Object> hostAttributes = Map.of("name", "nonAMQPPort",
                "type", "HTTP");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");


        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Port",
                hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertTrue(findRecordByType("VirtualHostAlias", records).isEmpty(),
                "No virtualhostalias rescords should be returned");
    }

    @Test
    public void testUpgradeImpliedAMQPPort()
    {
        final Map<String, Object> hostAttributes = Map.of("name", "impliedPort");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertFalse(findRecordByType("VirtualHostAlias", records).isEmpty(),
                "VirtualHostAlias rescords should be returned");

    }


    @Test
    public void testUpgradeImpliedNonAMQPPort()
    {
        final Map<String, Object> hostAttributes = Map.of("name", "nonAMQPPort",
                "protocols", "HTTP");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", hostAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertTrue(findRecordByType("VirtualHostAlias", records).isEmpty(),
                "No virtualhostalias rescords should be returned");
    }

    @Test
    public void testUpgradeBrokerType()
    {
        _brokerRecord.getAttributes().put("modelVersion", "3.0");
        _brokerRecord.getAttributes().put("type", "broker");

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> brokerRecords = findRecordByType("Broker", records);

        assertEquals(1, (long) brokerRecords.size(), "Unexpected number of broker records");
        assertFalse(brokerRecords.get(0).getAttributes().containsKey("type"), "Unexpected type");
    }

    @Test
    public void testUpgradeAMQPPortWithNetworkBuffers()
    {
        final Map<String, Object> portAttributes = Map.of("name", getTestName(),
                "type", "AMQP",
                "receiveBufferSize", "1",
                "sendBufferSize", "2");

        _brokerRecord.getAttributes().put("modelVersion", "3.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(randomUUID(), "Port",
                portAttributes, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);

        assertEquals(1, (long) ports.size(), "Unexpected port size");
        final ConfiguredObjectRecord upgradedRecord = ports.get(0);
        final Map<String, Object> attributes = upgradedRecord.getAttributes();
        assertFalse(attributes.containsKey("receiveBufferSize"),
                "receiveBufferSize is found " + attributes.get("receiveBufferSize"));
        assertFalse(attributes.containsKey("sendBufferSize"),
                "sendBufferSize is found " + attributes.get("sendBufferSize"));
        assertEquals(getTestName(), attributes.get("name"), "Unexpected name");
    }

    @Test
    public void testUpgradeRemoveJmxPlugin()
    {
        final Map<String, Object> jmxPlugin = Map.of("name", getTestName(),
                "type", "MANAGEMENT-JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Plugin", jmxPlugin, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final List<ConfiguredObjectRecord> plugins = findRecordByType("Plugin", records);
        assertTrue(plugins.isEmpty(), "JMX Plugin was not removed");
    }

    @Test
    public void testUpgradeRemoveJmxPortByType()
    {
        final Map<String, Object> jmxPort = Map.of("name", "jmx1",
                "type", "JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", jmxPort, Map.of("Broker", _brokerRecord.getId()));

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue(ports.isEmpty(), "Port was not removed");
    }

    @Test
    public void testUpgradeRemoveRmiPortByType()
    {
        final Map<String, Object> rmiPort = Map.of("name", "rmi1",
                "type", "RMI");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", rmiPort, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue(ports.isEmpty(), "Port was not removed");
    }

    @Test
    public void testUpgradeRemoveJmxPortByProtocol()
    {
        final Map<String, Object> jmxPort = Map.of("name", "jmx2",
                "protocols", Set.of("JMX_RMI"));

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", jmxPort, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue(ports.isEmpty(), "Port was not removed");
    }

    @Test
    public void testUpgradeRemoveRmiPortByProtocol()
    {
        final Map<String, Object> rmiPort2 = Map.of("name", "rmi2",
                "protocols", Set.of("RMI"));

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(
                randomUUID(), "Port", rmiPort2, Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue(ports.isEmpty(), "Port was not removed");
    }

    @Test
    public void testUpgradeRemovePreferencesProviderNonJsonLikeStore()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final Map<String, Object> authenticationProvider = Map.of("name", "anonymous",
                "type", "Anonymous");
        final ConfiguredObjectRecord authenticationProviderRecord = new ConfiguredObjectRecordImpl(
                randomUUID(),
                "AuthenticationProvider",
                authenticationProvider,
                Map.of("Broker", _brokerRecord.getId()));
        final ConfiguredObjectRecord preferencesProviderRecord = new ConfiguredObjectRecordImpl(
                randomUUID(),
                "PreferencesProvider",
                Map.of(),
                Map.of("AuthenticationProvider", authenticationProviderRecord.getId()));
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord,
                                                                                authenticationProviderRecord,
                                                                                preferencesProviderRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> preferencesProviders = findRecordByType("PreferencesProvider", records);
        assertTrue(preferencesProviders.isEmpty(), "PreferencesProvider was not removed");

        final List<ConfiguredObjectRecord> authenticationProviders = findRecordByType("AuthenticationProvider", records);
        assertEquals(1, (long) authenticationProviders.size(), "AuthenticationProvider was removed");
    }

    @Test
    public void testUpgradeRemovePreferencesProviderJsonLikeStore()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final Map<String, Object> authenticationProvider = Map.of("name", "anonymous",
                "type", "Anonymous",
                "preferencesproviders", Map.of());
        final ConfiguredObjectRecord authenticationProviderRecord = new ConfiguredObjectRecordImpl(
                randomUUID(),
                "AuthenticationProvider",
                authenticationProvider,
                Map.of("Broker", _brokerRecord.getId()));
        final DurableConfigurationStore dcs =
                new DurableConfigurationStoreStub(_brokerRecord, authenticationProviderRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final List<ConfiguredObjectRecord> authenticationProviders = findRecordByType("AuthenticationProvider", records);
        assertEquals(1, (long) authenticationProviders.size(), "AuthenticationProviders was removed");
        assertFalse(authenticationProviders.get(0).getAttributes().containsKey("preferencesproviders"),
                               "PreferencesProvider was not removed");
    }

    @Test
    public void testUpgradeTrustStoreRecordsFrom_6_0()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");
        final Map<String, UUID> parents = Map.of("Broker", _brokerRecord.getId());

        final Map<String, Object> trustStoreAttributes1 = Map.of("name", "truststore1",
                "type", "FileTrustStore",
                "path", "${json:test.ssl.resources}/java_broker_truststore1.jks",
                "password", "password");
        final ConfiguredObjectRecord trustStore1 = new ConfiguredObjectRecordImpl(randomUUID(), "TrustStore",
                                                                        trustStoreAttributes1,
                                                                        parents);

        final Map<String, Object> trustStoreAttributes2 = Map.of("name", "truststore2",
                "type", "FileTrustStore",
                "path", "${json:test.ssl.resources}/java_broker_truststore2.jks",
                "password", "password",
                "includedVirtualHostMessageSources", "true");
        final ConfiguredObjectRecord trustStore2 = new ConfiguredObjectRecordImpl(randomUUID(), "TrustStore",
                                                                            trustStoreAttributes2,
                                                                            parents);

        final Map<String, Object> trustStoreAttributes3 = Map.of("name", "truststore3",
                "type", "FileTrustStore",
                "path", "${json:test.ssl.resources}/java_broker_truststore3.jks",
                "password", "password",
                "excludedVirtualHostMessageSources", "true");
        final ConfiguredObjectRecord trustStore3 = new ConfiguredObjectRecordImpl(randomUUID(), "TrustStore",
                                                                            trustStoreAttributes3,
                                                                            parents);

        final DurableConfigurationStore dcs =
                new DurableConfigurationStoreStub(_brokerRecord, trustStore1, trustStore2, trustStore3);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final ConfiguredObjectRecord trustStore1Upgraded = findRecordById(trustStore1.getId(), records);
        final ConfiguredObjectRecord trustStore2Upgraded = findRecordById(trustStore2.getId(), records);
        final ConfiguredObjectRecord trustStore3Upgraded = findRecordById(trustStore3.getId(), records);

        assertNotNull(trustStore1Upgraded, "Trust store 1 is not found after upgrade");
        assertNotNull(trustStore2Upgraded, "Trust store 2 is not found after upgrade");
        assertNotNull(trustStore3Upgraded, "Trust store 3 is not found after upgrade");

        assertEquals(trustStoreAttributes1, new HashMap<>(trustStore1Upgraded.getAttributes()),
                "Unexpected attributes after upgrade for Trust store 1");

        assertEquals("true", trustStore2Upgraded.getAttributes().get("includedVirtualHostNodeMessageSources"),
                "includedVirtualHostNodeMessageSources is not found");
        assertNull(trustStore2Upgraded.getAttributes().get("includedVirtualHostMessageSources"),
                "includedVirtualHostMessageSources is  found");

        assertEquals("true", trustStore3Upgraded.getAttributes().get("excludedVirtualHostNodeMessageSources"),
                "includedVirtualHostNodeMessageSources is not found");
        assertNull(trustStore3Upgraded.getAttributes().get("excludedVirtualHostMessageSources"),
                "includedVirtualHostMessageSources is  found");
        assertModelVersionUpgraded(records);
    }

    @Test
    public void testUpgradeJmxRecordsFrom_3_0()
    {
        _brokerRecord.getAttributes().put("modelVersion", "3.0");
        final Map<String, UUID> parents = Map.of("Broker", _brokerRecord.getId());

        final Map<String, Object> jmxPortAttributes = Map.of("name", "jmx1",
                "type", "JMX");
        final ConfiguredObjectRecord jmxPort = new ConfiguredObjectRecordImpl(randomUUID(), "Port",
                                                                        jmxPortAttributes,
                                                                        parents);
        final Map<String, Object> rmiPortAttributes = Map.of("name", "rmi1",
                "type", "RMI");
        final ConfiguredObjectRecord rmiPort = new ConfiguredObjectRecordImpl(randomUUID(), "Port",
                                                                        rmiPortAttributes,
                                                                        parents);

        final Map<String, Object> jmxPluginAttributes = Map.of("name", getTestName(),
                "type", "MANAGEMENT-JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final ConfiguredObjectRecord jmxManagement = new ConfiguredObjectRecordImpl(randomUUID(),
                                                                           "Plugin",
                                                                           jmxPluginAttributes,
                                                                           parents);

        final DurableConfigurationStore dcs =
                new DurableConfigurationStoreStub(_brokerRecord, jmxPort, rmiPort, jmxManagement);

        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertNull(findRecordById(jmxPort.getId(), records), "Jmx port is not removed");
        assertNull(findRecordById(rmiPort.getId(), records), "Rmi port is not removed");
        assertNull(findRecordById(jmxManagement.getId(), records), "Jmx plugin is not removed");

        assertModelVersionUpgraded(records);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpgradeHttpPortFrom_6_0()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        final Map<String, UUID> parents = Map.of("Broker", _brokerRecord.getId());
        final Map<String, String> context = Map.of("port.http.additionalInternalThreads", "6",
                "port.http.maximumQueuedRequests", "1000");
        final Map<String, Object> httpPortAttributes = Map.of("name", "http",
                "protocols", Set.of("HTTP"),
                "context", context);

        final ConfiguredObjectRecord httpPort = new ConfiguredObjectRecordImpl(randomUUID(), "Port",
                                                                        httpPortAttributes,
                                                                        parents);
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, httpPort);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final ConfiguredObjectRecord upgradedPort = findRecordById(httpPort.getId(), records);
        assertNotNull(upgradedPort, "Http port is not found");
        final Map<String, Object> upgradedAttributes = upgradedPort.getAttributes();

        final Map<String, String> upgradedContext = (Map<String, String>) upgradedAttributes.get("context");
        assertFalse(upgradedContext.containsKey("port.http.additionalInternalThreads"),
                "Context variable \"port.http.additionalInternalThreads\" is not removed");
        assertFalse(upgradedContext.containsKey("port.http.maximumQueuedRequests"),
                "Context variable \"port.http.maximumQueuedRequests\" is not removed");
        assertEquals("1000", upgradedContext.get("qpid.port.http.acceptBacklog"),
                "Context variable \"port.http.maximumQueuedRequests\" is not renamed");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBrokerConnectionAttributesRemoval()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.1");
        _brokerRecord.getAttributes().put("connection.sessionCountLimit", "512");
        _brokerRecord.getAttributes().put("connection.heartBeatDelay", "300");
        _brokerRecord.getAttributes().put("connection.closeWhenNoRoute", "false");

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        final ConfiguredObjectRecord upgradedBroker = findRecordById(_brokerRecord.getId(), records);
        assertNotNull(upgradedBroker, "Upgraded broker record is not found");
        final Map<String, Object> upgradedAttributes = upgradedBroker.getAttributes();

        final Map<String, String> expectedContext = Map.of("qpid.port.sessionCountLimit", "512",
                "qpid.port.heartbeatDelay", "300",
                "qpid.port.closeWhenNoRoute", "false");

        final Object upgradedContext = upgradedAttributes.get("context");
        final boolean condition = upgradedContext instanceof Map;
        assertTrue(condition, "Unpexcted context");
        assertEquals(expectedContext, new HashMap<>(((Map<String, String>) upgradedContext)), "Unexpected context");

        assertFalse(upgradedAttributes.containsKey("connection.sessionCountLimit"),
                "Session count limit is not removed");
        assertFalse(upgradedAttributes.containsKey("connection.heartBeatDelay"), "Heart beat delay is not removed");
        assertFalse(upgradedAttributes.containsKey("conection.closeWhenNoRoute"),
                "Close when no route is not removed");
    }

    @Test
    public void testContextVariableUpgradeForTLSProtocolsSetOnBroker()
    {
        final Map<String, String> context = Map.of("qpid.security.tls.protocolWhiteList", ".*",
                "qpid.security.tls.protocolBlackList", "Ssl.*");

        _brokerRecord.getAttributes().put("modelVersion", "8.0");
        _brokerRecord.getAttributes().put("context", context);

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("Broker", records);

        assertEquals(".*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST));
        assertEquals("Ssl.*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST));
    }

    @Test
    public void testContextVariableUpgradeForTLSCipherSuitesSetOnBroker()
    {
        final Map<String, String> context = Map.of("qpid.security.tls.cipherSuiteWhiteList", ".*",
                "qpid.security.tls.cipherSuiteBlackList", "Ssl.*");

        _brokerRecord.getAttributes().put("modelVersion", "8.0");
        _brokerRecord.getAttributes().put("context", context);

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("Broker", records);

        assertEquals(".*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST));
        assertEquals("Ssl.*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST));
    }

    @Test
    public void testContextVariableUpgradeForTLSProtocolsSetOnPort()
    {
        _brokerRecord.getAttributes().put("modelVersion", "8.0");

        final Map<String, String> context = Map.of("qpid.security.tls.protocolWhiteList", ".*",
                "qpid.security.tls.protocolBlackList", "Ssl.*");

        final ConfiguredObjectRecord portRecord =
                createMockRecordForGivenCategoryTypeAndContext("Port", "AMQP", context);

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(portRecord, _brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("Port", records);

        assertEquals(".*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST));
        assertEquals("Ssl.*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST));
    }

    @Test
    public void testContextVariableUpgradeForTLSCipherSuitesSetOnAuthenticationProvider()
    {
        _brokerRecord.getAttributes().put("modelVersion", "8.0");

        final Map<String, String> context = Map.of("qpid.security.tls.cipherSuiteWhiteList", ".*",
                "qpid.security.tls.cipherSuiteBlackList", "Ssl.*");
        final ConfiguredObjectRecord authenticationProviderRecord =
                createMockRecordForGivenCategoryTypeAndContext("AuthenticationProvider", "OAuth2", context);

        final DurableConfigurationStore dcs =
                new DurableConfigurationStoreStub(authenticationProviderRecord, _brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("AuthenticationProvider", records);

        assertEquals(".*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST));
        assertEquals("Ssl.*", contextMap.get(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST));
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
        _brokerRecord.getAttributes().put("modelVersion", "9.0");

        final Map<String, String> context = new HashMap<>();
        context.put("qpid.jdbcstore.bonecp.partitionCount", partitionCount);
        context.put("qpid.jdbcstore.bonecp.maxConnectionsPerPartition", maxConnectionsPerPartition);
        context.put("qpid.jdbcstore.bonecp.minConnectionsPerPartition", minConnectionsPerPartition);
        context.put("qpid.jdbcstore.property1", "1");
        context.put("qpid.jdbcstore.property2", "two");
        context.put("qpid.jdbcstore.property3", "_3_");

        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "type", "JDBC",
                "connectionPoolType", "BONECP",
                "context", context);

        _brokerRecord.getAttributes().put("context", context);

        final ConfiguredObjectRecord systemConfigRecord = mock(ConfiguredObjectRecord.class);;
        when(systemConfigRecord.getId()).thenReturn(randomUUID());
        when(systemConfigRecord.getType()).thenReturn("SystemConfig");
        when(systemConfigRecord.getAttributes()).thenReturn(Map.copyOf(attributes));

        final ConfiguredObjectRecord virtualHostNodeRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostNodeRecord.getId()).thenReturn(randomUUID());
        when(virtualHostNodeRecord.getType()).thenReturn("VirtualHostNode");
        when(virtualHostNodeRecord.getAttributes()).thenReturn(Map.copyOf(attributes));

        final ConfiguredObjectRecord virtualHostRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostRecord.getId()).thenReturn(randomUUID());
        when(virtualHostRecord.getType()).thenReturn("VirtualHost");
        when(virtualHostRecord.getAttributes()).thenReturn(Map.copyOf(attributes));

        final ConfiguredObjectRecord jdbcBrokerLoggerRecord = mock(ConfiguredObjectRecord.class);;
        when(jdbcBrokerLoggerRecord.getId()).thenReturn(randomUUID());
        when(jdbcBrokerLoggerRecord.getType()).thenReturn("BrokerLogger");
        when(jdbcBrokerLoggerRecord.getAttributes()).thenReturn(Map.copyOf(attributes));

        final ConfiguredObjectRecord jdbcVirtualHostLoggerRecord = mock(ConfiguredObjectRecord.class);;
        when(jdbcVirtualHostLoggerRecord.getId()).thenReturn(randomUUID());
        when(jdbcVirtualHostLoggerRecord.getType()).thenReturn("VirtualHostLogger");
        when(jdbcVirtualHostLoggerRecord.getAttributes()).thenReturn(Map.copyOf(attributes));

        final DurableConfigurationStore dcs =
                new DurableConfigurationStoreStub(jdbcVirtualHostLoggerRecord, jdbcBrokerLoggerRecord, virtualHostRecord,
                                                  virtualHostNodeRecord, systemConfigRecord, _brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        records.forEach(record ->
        {
            final Map<String, String> upgradedContext =
                    (Map<String, String>) record.getAttributes().get("context");

            assertNull(upgradedContext.get("qpid.jdbcstore.bonecp.partitionCount"));
            assertEquals(maximumPoolSize, upgradedContext.get("qpid.jdbcstore.hikaricp.maximumPoolSize"));
            assertEquals(minimumIdle, upgradedContext.get("qpid.jdbcstore.hikaricp.minimumIdle"));
            assertEquals("1", upgradedContext.get("qpid.jdbcstore.property1"));
            assertEquals("two", upgradedContext.get("qpid.jdbcstore.property2"));
            assertEquals("_3_", upgradedContext.get("qpid.jdbcstore.property3"));
            if (!"Broker".equals(record.getType()))
            {
                assertEquals("HIKARICP", record.getAttributes().get("connectionPoolType"));
            }
        });
    }

    @Test
    public void testUpgradeFromBoneCPToHikariCPProviderWithEmptyContext()
    {
        _brokerRecord.getAttributes().put("modelVersion", "9.0");

        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "type", "JDBC",
                "connectionPoolType", "BONECP",
                "context", new HashMap<>());
        final ConfiguredObjectRecord virtualHostRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostRecord.getId()).thenReturn(randomUUID());
        when(virtualHostRecord.getType()).thenReturn("VirtualHost");
        when(virtualHostRecord.getAttributes()).thenReturn(attributes);

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(virtualHostRecord, _brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final ConfiguredObjectRecord upgradedVirtualHost = records.stream()
                .filter(record -> "VirtualHost".equals(record.getType())).findFirst().orElse(null);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("VirtualHost", records);

        assertEquals("40", contextMap.get("qpid.jdbcstore.hikaricp.maximumPoolSize"));
        assertEquals("20", contextMap.get("qpid.jdbcstore.hikaricp.minimumIdle"));
        assertEquals("HIKARICP", upgradedVirtualHost.getAttributes().get("connectionPoolType"));
    }

    @Test
    public void testContextVariableUpgradeFromDefaultCPToHikariCPProvider()
    {
        _brokerRecord.getAttributes().put("modelVersion", "9.0");

        final Map<String, Object> attributes = Map.of("name", getTestName(),
                "type", "JDBC",
                "connectionPoolType", "NONE",
                "context", new HashMap<>());
        final ConfiguredObjectRecord virtualHostRecord = mock(ConfiguredObjectRecord.class);;
        when(virtualHostRecord.getId()).thenReturn(randomUUID());
        when(virtualHostRecord.getType()).thenReturn("VirtualHost");
        when(virtualHostRecord.getAttributes()).thenReturn(attributes);

        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(virtualHostRecord, _brokerRecord);
        final BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);
        final Map<String, String> contextMap = findCategoryRecordAndGetContext("VirtualHost", records);

        assertNull(contextMap.get("qpid.jdbcstore.hikaricp.maximumPoolSize"));
        assertNull(contextMap.get("qpid.jdbcstore.hikaricp.minimumIdle"));
        assertEquals("NONE", virtualHostRecord.getAttributes().get("connectionPoolType"));
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

    @SuppressWarnings("unchecked")
    private Map<String, String> findCategoryRecordAndGetContext(final String category,
                                                                final List<ConfiguredObjectRecord> records)
    {
        final List<ConfiguredObjectRecord> foundRecords = findRecordByType(category, records);
        assertEquals(1, foundRecords.size(), "Unexpected number of records");
        final Map<String, Object> attributes = foundRecords.get(0).getAttributes();
        assertNotNull(attributes);
        final Object context = attributes.get("context");
        assertTrue(context instanceof Map);
        return (Map<String, String>) context;
    }

    private void assertModelVersionUpgraded(final List<ConfiguredObjectRecord> records)
    {
        final ConfiguredObjectRecord upgradedBrokerRecord = findRecordById(_brokerRecord.getId(), records);
        assertEquals(BrokerModel.MODEL_VERSION, upgradedBrokerRecord.getAttributes().get(Broker.MODEL_VERSION),
                "Unexpected model version");
    }

    private void upgradeBrokerRecordAndAssertUpgradeResults()
    {
        final DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        final List<ConfiguredObjectRecord> records = upgrade(dcs, new BrokerStoreUpgraderAndRecoverer(_systemConfig));

        assertVirtualHost(records, true);
        assertBrokerRecord(records);
    }

    private void assertVirtualHost(final List<ConfiguredObjectRecord> records, final boolean isDefaultVHN)
    {
        final ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(_hostId, records);
        assertEquals("VirtualHostNode", upgradedVirtualHostNodeRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedAttributes = Map.of("createdBy", VIRTUALHOST_CREATED_BY,
                "createdTime", VIRTUALHOST_CREATE_TIME,
                "name", VIRTUALHOST_NAME,
                "type", "Memory",
                "defaultVirtualHostNode", Boolean.toString(isDefaultVHN));
        assertEquals(expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes(), "Unexpected attributes");
    }

    private void assertBrokerRecord(final List<ConfiguredObjectRecord> records)
    {
        final ConfiguredObjectRecord upgradedBrokerRecord = findRecordById(_brokerId, records);
        assertEquals("Broker", upgradedBrokerRecord.getType(), "Unexpected type");
        final Map<String,Object> expectedAttributes = Map.of("name", "Broker",
                "modelVersion", BrokerModel.MODEL_VERSION,
                "createdTime", 1401385808828L);
        assertEquals(expectedAttributes, upgradedBrokerRecord.getAttributes(), "Unexpected broker attributes");
    }

    private ConfiguredObjectRecord findRecordById(final UUID id, final List<ConfiguredObjectRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().orElse(null);
    }

    private List<ConfiguredObjectRecord> findRecordByType(final String type, final List<ConfiguredObjectRecord> records)
    {
        return records.stream().filter(record -> record.getType().equals(type)).collect(Collectors.toList());
    }

    static class DurableConfigurationStoreStub implements DurableConfigurationStore
    {
        private final List<ConfiguredObjectRecord> records;

        public DurableConfigurationStoreStub(ConfiguredObjectRecord... records)
        {
            super();
            this.records = List.of(records);
        }

        @Override
        public void init(ConfiguredObject<?> parent) throws StoreException
        {
        }

        @Override
        public void upgradeStoreStructure() throws StoreException
        {

        }

        @Override
        public void create(ConfiguredObjectRecord object) throws StoreException
        {
        }

        @Override
        public UUID[] remove(ConfiguredObjectRecord... objects) throws StoreException
        {
            return null;
        }

        @Override
        public void update(final boolean createIfNecessary, final ConfiguredObjectRecord... records) throws StoreException
        {
        }

        @Override
        public void closeConfigurationStore() throws StoreException
        {
        }

        @Override
        public void onDelete(ConfiguredObject<?> parent)
        {
        }

        @Override
        public boolean openConfigurationStore(final ConfiguredObjectRecordHandler handler,
                                              final ConfiguredObjectRecord... initialRecords) throws StoreException
        {
            records.forEach(handler::handle);
            return false;
        }


        @Override
        public void reload(ConfiguredObjectRecordHandler handler) throws StoreException
        {
            records.forEach(handler::handle);
        }
    }

    private static class RecordRetrievingConfiguredObjectRecordHandler implements ConfiguredObjectRecordHandler
    {
        private final List<ConfiguredObjectRecord> _records = new ArrayList<>();

        @Override
        public void handle(final ConfiguredObjectRecord record)
        {
            _records.add(record);
        }

        public List<ConfiguredObjectRecord> getRecords()
        {
            return _records;
        }
    }
}
