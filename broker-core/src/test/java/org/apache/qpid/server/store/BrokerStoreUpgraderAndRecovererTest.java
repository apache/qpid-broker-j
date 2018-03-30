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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
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
    private static final long BROKER_CREATE_TIME = 1401385808828l;
    private static final String BROKER_NAME = "Broker";
    private static final String VIRTUALHOST_NAME = "test";
    private static final long VIRTUALHOST_CREATE_TIME = 1401385905260l;
    private static final String VIRTUALHOST_CREATED_BY = "webadmin";

    private ConfiguredObjectRecord _brokerRecord;
    private CurrentThreadTaskExecutor _taskExecutor;
    private SystemConfig<?> _systemConfig;
    private List<Map<String, Object>> _virtaulHosts;
    private UUID _hostId;
    private UUID _brokerId;

    @Before
    public void setUp() throws Exception
    {
        _virtaulHosts = new ArrayList<>();
        _hostId = UUID.randomUUID();
        _brokerId = UUID.randomUUID();
        Map<String, Object> brokerAttributes = new HashMap<>();
        brokerAttributes.put("createdTime", BROKER_CREATE_TIME);
        brokerAttributes.put("defaultVirtualHost", VIRTUALHOST_NAME);
        brokerAttributes.put("modelVersion", "1.3");
        brokerAttributes.put("name", BROKER_NAME);
        brokerAttributes.put("virtualhosts", _virtaulHosts);

        _brokerRecord = mock(ConfiguredObjectRecord.class);
        when(_brokerRecord.getId()).thenReturn(_brokerId);
        when(_brokerRecord.getType()).thenReturn("Broker");
        when(_brokerRecord.getAttributes()).thenReturn(brokerAttributes);

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _systemConfig = new JsonSystemConfigImpl(_taskExecutor,
                                                 mock(EventLogger.class),
                                                 null, new HashMap<String,Object>());
    }

    @Test
    public void testUpgradeVirtualHostWithJDBCStoreAndBoneCPPool()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("connectionPool", "BONECP");
        hostAttributes.put("connectionURL", "jdbc:derby://localhost:1527/tmp/vh/test;create=true");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("maxConnectionsPerPartition", 7);
        hostAttributes.put("minConnectionsPerPartition", 6);
        hostAttributes.put("partitionCount", 2);
        hostAttributes.put("storeType", "jdbc");
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("jdbcBigIntType", "mybigint");
        hostAttributes.put("jdbcBlobType", "myblob");
        hostAttributes.put("jdbcVarbinaryType", "myvarbinary");
        hostAttributes.put("jdbcBytesForBlob", true);


        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("connectionPoolType", "BONECP");
        expectedAttributes.put("connectionUrl", "jdbc:derby://localhost:1527/tmp/vh/test;create=true");
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "JDBC");
        expectedAttributes.put("defaultVirtualHostNode", "true");

        final Map<String, Object> context = new HashMap<>();
        context.put("qpid.jdbcstore.bigIntType", "mybigint");
        context.put("qpid.jdbcstore.varBinaryType", "myvarbinary");
        context.put("qpid.jdbcstore.blobType", "myblob");
        context.put("qpid.jdbcstore.useBytesForBlob", true);

        context.put("qpid.jdbcstore.bonecp.maxConnectionsPerPartition", 7);
        context.put("qpid.jdbcstore.bonecp.minConnectionsPerPartition", 6);
        context.put("qpid.jdbcstore.bonecp.partitionCount", 2);
        expectedAttributes.put("context", context);

        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    private List<ConfiguredObjectRecord> upgrade(final DurableConfigurationStore dcs,
                                                 final BrokerStoreUpgraderAndRecoverer recoverer)
    {
        RecordRetrievingConfiguredObjectRecordHandler handler = new RecordRetrievingConfiguredObjectRecordHandler();
        dcs.openConfigurationStore(handler);
        return recoverer.upgrade(dcs, handler.getRecords());
    }

    @Test
    public void testUpgradeVirtualHostWithJDBCStoreAndDefaultPool()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("connectionPool", "DEFAULT");
        hostAttributes.put("connectionURL", "jdbc:derby://localhost:1527/tmp/vh/test;create=true");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("storeType", "jdbc");
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("jdbcBigIntType", "mybigint");
        hostAttributes.put("jdbcBlobType", "myblob");
        hostAttributes.put("jdbcVarbinaryType", "myvarbinary");
        hostAttributes.put("jdbcBytesForBlob", true);


        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("connectionPoolType", "NONE");
        expectedAttributes.put("connectionUrl", "jdbc:derby://localhost:1527/tmp/vh/test;create=true");
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "JDBC");
        expectedAttributes.put("defaultVirtualHostNode", "true");


        final Map<String, Object> context = new HashMap<>();
        context.put("qpid.jdbcstore.bigIntType", "mybigint");
        context.put("qpid.jdbcstore.varBinaryType", "myvarbinary");
        context.put("qpid.jdbcstore.blobType", "myblob");
        context.put("qpid.jdbcstore.useBytesForBlob", true);

        expectedAttributes.put("context", context);

        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithDerbyStore()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("storePath", "/tmp/vh/derby");
        hostAttributes.put("storeType", "derby");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");

        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("storePath", "/tmp/vh/derby");
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "DERBY");
        expectedAttributes.put("defaultVirtualHostNode", "true");

        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithBDBStore()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("storePath", "/tmp/vh/bdb");
        hostAttributes.put("storeType", "bdb");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("bdbEnvironmentConfig", Collections.singletonMap("je.stats.collect", "false"));


        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("storePath", "/tmp/vh/bdb");
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "BDB");
        expectedAttributes.put("defaultVirtualHostNode", "true");
        expectedAttributes.put("context", Collections.singletonMap("je.stats.collect", "false"));
        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithBDBHAStore()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "BDB_HA");
        hostAttributes.put("storePath", "/tmp/vh/bdbha");
        hostAttributes.put("haCoalescingSync", "true");
        hostAttributes.put("haDesignatedPrimary", "true");
        hostAttributes.put("haGroupName", "ha");
        hostAttributes.put("haHelperAddress", "localhost:7000");
        hostAttributes.put("haNodeAddress", "localhost:7000");
        hostAttributes.put("haNodeName", "n1");
        hostAttributes.put("haReplicationConfig", Collections.singletonMap("je.stats.collect", "false"));
        hostAttributes.put("bdbEnvironmentConfig", Collections.singletonMap("je.rep.feederTimeout", "1 m"));


        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedContext = new HashMap<>();
        expectedContext.put("je.stats.collect", "false");
        expectedContext.put("je.rep.feederTimeout", "1 m");

        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("type", "BDB_HA");
        expectedAttributes.put("storePath", "/tmp/vh/bdbha");
        expectedAttributes.put("designatedPrimary", "true");
        expectedAttributes.put("groupName", "ha");
        expectedAttributes.put("address", "localhost:7000");
        expectedAttributes.put("helperAddress", "localhost:7000");
        expectedAttributes.put("name", "n1");
        expectedAttributes.put("context", expectedContext);
        expectedAttributes.put("defaultVirtualHostNode", "true");

        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeVirtualHostWithMemoryStore()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("storeType", "memory");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");

        ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "VirtualHost",
                hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, virtualHostRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(virtualHostRecord.getId(), records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "Memory");
        expectedAttributes.put("defaultVirtualHostNode", "true");

        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
        assertBrokerRecord(records);
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_0()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.0");
        _brokerRecord.getAttributes().put("virtualhosts", _virtaulHosts);
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.1");
        hostAttributes.put("storeType", "memory");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("id", _hostId);
        _virtaulHosts.add(hostAttributes);


        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_1()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.1");
        _brokerRecord.getAttributes().put("virtualhosts", _virtaulHosts);
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.2");
        hostAttributes.put("storeType", "memory");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("id", _hostId);
        _virtaulHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_2()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.2");
        _brokerRecord.getAttributes().put("virtualhosts", _virtaulHosts);
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.3");
        hostAttributes.put("storeType", "memory");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("id", _hostId);
        _virtaulHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeBrokerRecordWithModelVersion1_3()
    {
        _brokerRecord.getAttributes().put("modelVersion", "1.3");
        _brokerRecord.getAttributes().put("virtualhosts", _virtaulHosts);
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", VIRTUALHOST_NAME);
        hostAttributes.put("modelVersion", "0.4");
        hostAttributes.put("storeType", "memory");
        hostAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        hostAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        hostAttributes.put("type", "STANDARD");
        hostAttributes.put("id", _hostId);
        _virtaulHosts.add(hostAttributes);

        upgradeBrokerRecordAndAssertUpgradeResults();
    }

    @Test
    public void testUpgradeNonAMQPPort()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", "nonAMQPPort");
        hostAttributes.put("type", "HTTP");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");


        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                                  hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertTrue("No virtualhostalias rescords should be returned",
                          findRecordByType("VirtualHostAlias", records).isEmpty());

    }

    @Test
    public void testUpgradeImpliedAMQPPort()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", "impliedPort");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");


        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                           hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertFalse("VirtualHostAlias rescords should be returned",
                           findRecordByType("VirtualHostAlias", records).isEmpty());

    }


    @Test
    public void testUpgradeImpliedNonAMQPPort()
    {
        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put("name", "nonAMQPPort");
        hostAttributes.put("protocols", "HTTP");

        _brokerRecord.getAttributes().put("modelVersion", "2.0");


        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                           hostAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertTrue("No virtualhostalias rescords should be returned",
                          findRecordByType("VirtualHostAlias", records).isEmpty());
    }

    @Test
    public void testUpgradeBrokerType()
    {
        _brokerRecord.getAttributes().put("modelVersion", "3.0");
        _brokerRecord.getAttributes().put("type", "broker");

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> brokerRecords = findRecordByType("Broker", records);
        assertEquals("Unexpected number of broker records", (long) 1, (long) brokerRecords.size());
        assertFalse("Unexpected type", brokerRecords.get(0).getAttributes().containsKey("type"));
    }

    @Test
    public void testUpgradeAMQPPortWithNetworkBuffers()
    {
        Map<String, Object> portAttributes = new HashMap<>();
        portAttributes.put("name", getTestName());
        portAttributes.put("type", "AMQP");
        portAttributes.put("receiveBufferSize", "1");
        portAttributes.put("sendBufferSize", "2");

        _brokerRecord.getAttributes().put("modelVersion", "3.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                portAttributes, Collections.singletonMap("Broker", _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertEquals("Unexpected port size", (long) 1, (long) ports.size());
        ConfiguredObjectRecord upgradedRecord = ports.get(0);
        Map<String, Object> attributes = upgradedRecord.getAttributes();
        assertFalse("receiveBufferSize is found " + attributes.get("receiveBufferSize"),
                           attributes.containsKey("receiveBufferSize"));
        assertFalse("sendBufferSize is found " + attributes.get("sendBufferSize"),
                           attributes.containsKey("sendBufferSize"));
        assertEquals("Unexpected name", getTestName(), attributes.get("name"));
    }

    @Test
    public void testUpgradeRemoveJmxPlugin()
    {
        Map<String, Object> jmxPlugin = new HashMap<>();
        jmxPlugin.put("name", getTestName());
        jmxPlugin.put("type", "MANAGEMENT-JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                           "Plugin",
                                                                           jmxPlugin,
                                                                           Collections.singletonMap("Broker",
                                                                                                    _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> plugins = findRecordByType("Plugin", records);
        assertTrue("JMX Plugin was not removed", plugins.isEmpty());
    }

    @Test
    public void testUpgradeRemoveJmxPortByType()
    {
        Map<String, Object> jmxPort = new HashMap<>();
        jmxPort.put("name", "jmx1");
        jmxPort.put("type", "JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                           jmxPort,
                                                                           Collections.singletonMap("Broker",
                                                                                                    _brokerRecord.getId()));

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue("Port was not removed", ports.isEmpty());
    }

    @Test
    public void testUpgradeRemoveRmiPortByType()
    {
        Map<String, Object> rmiPort = new HashMap<>();
        rmiPort.put("name", "rmi1");
        rmiPort.put("type", "RMI");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                           rmiPort,
                                                                           Collections.singletonMap("Broker",
                                                                                                    _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue("Port was not removed", ports.isEmpty());
    }

    @Test
    public void testUpgradeRemoveJmxPortByProtocol()
    {
        Map<String, Object> jmxPort = new HashMap<>();
        jmxPort.put("name", "jmx2");
        jmxPort.put("protocols", Collections.singleton("JMX_RMI"));

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                           "Port",
                                                                           jmxPort,
                                                                           Collections.singletonMap("Broker",
                                                                                                    _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue("Port was not removed", ports.isEmpty());
    }

    @Test
    public void testUpgradeRemoveRmiPortByProtocol()
    {
        Map<String, Object> rmiPort2 = new HashMap<>();
        rmiPort2.put("name", "rmi2");
        rmiPort2.put("protocols", Collections.singleton("RMI"));

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord portRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                            "Port",
                                                                            rmiPort2,
                                                                            Collections.singletonMap("Broker",
                                                                                                     _brokerRecord.getId()));
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, portRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> ports = findRecordByType("Port", records);
        assertTrue("Port was not removed", ports.isEmpty());
    }

    @Test
    public void testUpgradeRemovePreferencesProviderNonJsonLikeStore()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        Map<String, Object> authenticationProvider = new HashMap<>();
        authenticationProvider.put("name", "anonymous");
        authenticationProvider.put("type", "Anonymous");

        ConfiguredObjectRecord authenticationProviderRecord = new ConfiguredObjectRecordImpl(
                UUID.randomUUID(),
                "AuthenticationProvider",
                authenticationProvider,
                Collections.singletonMap("Broker", _brokerRecord.getId()));

        ConfiguredObjectRecord preferencesProviderRecord = new ConfiguredObjectRecordImpl(
                UUID.randomUUID(),
                "PreferencesProvider",
                Collections.<String, Object>emptyMap(),
                Collections.singletonMap("AuthenticationProvider", authenticationProviderRecord.getId()));

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord,
                                                                          authenticationProviderRecord,
                                                                          preferencesProviderRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> preferencesProviders = findRecordByType("PreferencesProvider", records);
        assertTrue("PreferencesProvider was not removed", preferencesProviders.isEmpty());

        List<ConfiguredObjectRecord> authenticationProviders = findRecordByType("AuthenticationProvider", records);
        assertEquals("AuthenticationProvider was removed", (long) 1, (long) authenticationProviders.size());
    }

    @Test
    public void testUpgradeRemovePreferencesProviderJsonLikeStore()
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        Map<String, Object> authenticationProvider = new HashMap<>();
        authenticationProvider.put("name", "anonymous");
        authenticationProvider.put("type", "Anonymous");
        authenticationProvider.put("preferencesproviders", Collections.emptyMap());

        ConfiguredObjectRecord authenticationProviderRecord = new ConfiguredObjectRecordImpl(
                UUID.randomUUID(),
                "AuthenticationProvider",
                authenticationProvider,
                Collections.singletonMap("Broker", _brokerRecord.getId()));

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, authenticationProviderRecord);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        List<ConfiguredObjectRecord> authenticationProviders = findRecordByType("AuthenticationProvider", records);
        assertEquals("AuthenticationProviders was removed", (long) 1, (long) authenticationProviders.size());
        assertFalse("PreferencesProvider was not removed",
                           authenticationProviders.get(0).getAttributes().containsKey("preferencesproviders"));
    }

    @Test
    public void testUpgradeTrustStoreRecordsFrom_6_0() throws Exception
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");
        Map<String, UUID> parents = Collections.singletonMap("Broker", _brokerRecord.getId());

        Map<String, Object> trustStoreAttributes1 = new HashMap<>();
        trustStoreAttributes1.put("name", "truststore1");
        trustStoreAttributes1.put("type", "FileTrustStore");
        trustStoreAttributes1.put("path", "${json:test.ssl.resources}/java_broker_truststore1.jks");
        trustStoreAttributes1.put("password", "password");
        ConfiguredObjectRecord trustStore1 = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "TrustStore",
                                                                        trustStoreAttributes1,
                                                                        parents);

        Map<String, Object> trustStoreAttributes2 = new HashMap<>();
        trustStoreAttributes2.put("name", "truststore2");
        trustStoreAttributes2.put("type", "FileTrustStore");
        trustStoreAttributes2.put("path", "${json:test.ssl.resources}/java_broker_truststore2.jks");
        trustStoreAttributes2.put("password", "password");
        trustStoreAttributes2.put("includedVirtualHostMessageSources", "true");
        ConfiguredObjectRecord trustStore2 = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "TrustStore",
                                                                            trustStoreAttributes2,
                                                                            parents);

        Map<String, Object> trustStoreAttributes3 = new HashMap<>();
        trustStoreAttributes3.put("name", "truststore3");
        trustStoreAttributes3.put("type", "FileTrustStore");
        trustStoreAttributes3.put("path", "${json:test.ssl.resources}/java_broker_truststore3.jks");
        trustStoreAttributes3.put("password", "password");
        trustStoreAttributes3.put("excludedVirtualHostMessageSources", "true");
        ConfiguredObjectRecord trustStore3 = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "TrustStore",
                                                                            trustStoreAttributes3,
                                                                            parents);

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, trustStore1, trustStore2, trustStore3);
        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord trustStore1Upgraded = findRecordById(trustStore1.getId(), records);
        ConfiguredObjectRecord trustStore2Upgraded = findRecordById(trustStore2.getId(), records);
        ConfiguredObjectRecord trustStore3Upgraded = findRecordById(trustStore3.getId(), records);

        assertNotNull("Trust store 1 is not found after upgrade", trustStore1Upgraded);
        assertNotNull("Trust store 2 is not found after upgrade", trustStore2Upgraded);
        assertNotNull("Trust store 3 is not found after upgrade", trustStore3Upgraded);

        assertEquals("Unexpected attributes after upgrade for Trust store 1",
                            trustStoreAttributes1,
                            new HashMap<>(trustStore1Upgraded.getAttributes()));

        assertEquals("includedVirtualHostNodeMessageSources is not found",
                            "true",
                            trustStore2Upgraded.getAttributes().get("includedVirtualHostNodeMessageSources"));
        assertNull("includedVirtualHostMessageSources is  found",
                          trustStore2Upgraded.getAttributes().get("includedVirtualHostMessageSources"));

        assertEquals("includedVirtualHostNodeMessageSources is not found",
                            "true",
                            trustStore3Upgraded.getAttributes().get("excludedVirtualHostNodeMessageSources"));
        assertNull("includedVirtualHostMessageSources is  found",
                          trustStore3Upgraded.getAttributes().get("excludedVirtualHostMessageSources"));
        assertModelVersionUpgraded(records);
    }

    @Test
    public void testUpgradeJmxRecordsFrom_3_0() throws Exception
    {
        _brokerRecord.getAttributes().put("modelVersion", "3.0");
        Map<String, UUID> parents = Collections.singletonMap("Broker", _brokerRecord.getId());

        Map<String, Object> jmxPortAttributes = new HashMap<>();
        jmxPortAttributes.put("name", "jmx1");
        jmxPortAttributes.put("type", "JMX");
        ConfiguredObjectRecord jmxPort = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                        jmxPortAttributes,
                                                                        parents);
        Map<String, Object> rmiPortAttributes = new HashMap<>();
        rmiPortAttributes.put("name", "rmi1");
        rmiPortAttributes.put("type", "RMI");
        ConfiguredObjectRecord rmiPort = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                        rmiPortAttributes,
                                                                        parents);

        Map<String, Object> jmxPluginAttributes = new HashMap<>();
        jmxPluginAttributes.put("name", getTestName());
        jmxPluginAttributes.put("type", "MANAGEMENT-JMX");

        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        ConfiguredObjectRecord jmxManagement = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                           "Plugin",
                                                                           jmxPluginAttributes,
                                                                           parents);

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, jmxPort, rmiPort, jmxManagement);

        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        assertNull("Jmx port is not removed", findRecordById(jmxPort.getId(), records));
        assertNull("Rmi port is not removed", findRecordById(rmiPort.getId(), records));
        assertNull("Jmx plugin is not removed", findRecordById(jmxManagement.getId(), records));

        assertModelVersionUpgraded(records);
    }

    @Test
    public void testUpgradeHttpPortFrom_6_0() throws Exception
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.0");

        Map<String, UUID> parents = Collections.singletonMap("Broker", _brokerRecord.getId());

        Map<String, Object> httpPortAttributes = new HashMap<>();
        httpPortAttributes.put("name", "http");
        httpPortAttributes.put("protocols", Collections.singletonList("HTTP"));
        final Map<String, String > context = new HashMap<>();
        httpPortAttributes.put("context", context);

        context.put("port.http.additionalInternalThreads", "6");
        context.put("port.http.maximumQueuedRequests", "1000");

        ConfiguredObjectRecord httpPort = new ConfiguredObjectRecordImpl(UUID.randomUUID(), "Port",
                                                                        httpPortAttributes,
                                                                        parents);

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord, httpPort);
        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);


        ConfiguredObjectRecord upgradedPort = findRecordById(httpPort.getId(), records);
        assertNotNull("Http port is not found", upgradedPort);
        Map<String, Object> upgradedAttributes = upgradedPort.getAttributes();

        Map<String, String> upgradedContext = (Map<String, String>) upgradedAttributes.get("context");
        assertFalse("Context variable \"port.http.additionalInternalThreads\" is not removed",
                           upgradedContext.containsKey("port.http.additionalInternalThreads"));
        assertFalse("Context variable \"port.http.maximumQueuedRequests\" is not removed",
                           upgradedContext.containsKey("port.http.maximumQueuedRequests"));
        assertEquals("Context variable \"port.http.maximumQueuedRequests\" is not renamed",
                            "1000",
                            upgradedContext.get("qpid.port.http.acceptBacklog"));
    }

    @Test
    public void testBrokerConnectionAttributesRemoval() throws Exception
    {
        _brokerRecord.getAttributes().put("modelVersion", "6.1");
        _brokerRecord.getAttributes().put("connection.sessionCountLimit", "512");
        _brokerRecord.getAttributes().put("connection.heartBeatDelay", "300");
        _brokerRecord.getAttributes().put("connection.closeWhenNoRoute", "false");

        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
        List<ConfiguredObjectRecord> records = upgrade(dcs, recoverer);

        ConfiguredObjectRecord upgradedBroker = findRecordById(_brokerRecord.getId(), records);
        assertNotNull("Upgraded broker record is not found", upgradedBroker);
        Map<String, Object> upgradedAttributes = upgradedBroker.getAttributes();

        final Map<String, String> expectedContext = new HashMap<>();
        expectedContext.put("qpid.port.sessionCountLimit", "512");
        expectedContext.put("qpid.port.heartbeatDelay", "300");
        expectedContext.put("qpid.port.closeWhenNoRoute", "false");

        Object upgradedContext = upgradedAttributes.get("context");
        final boolean condition = upgradedContext instanceof Map;
        assertTrue("Unpexcted context", condition);
        assertEquals("Unexpected context",
                            expectedContext,
                            new HashMap<>(((Map<String, String>) upgradedContext)));

        assertFalse("Session count limit is not removed",
                           upgradedAttributes.containsKey("connection.sessionCountLimit"));
        assertFalse("Heart beat delay is not removed",
                           upgradedAttributes.containsKey("connection.heartBeatDelay"));
        assertFalse("Close when no route is not removed",
                           upgradedAttributes.containsKey("conection.closeWhenNoRoute"));
    }

    private void assertModelVersionUpgraded(final List<ConfiguredObjectRecord> records)
    {
        ConfiguredObjectRecord upgradedBrokerRecord = findRecordById(_brokerRecord.getId(), records);
        assertEquals("Unexpected model version",
                            BrokerModel.MODEL_VERSION,
                            upgradedBrokerRecord.getAttributes().get(Broker.MODEL_VERSION));
    }

    private void upgradeBrokerRecordAndAssertUpgradeResults()
    {
        DurableConfigurationStore dcs = new DurableConfigurationStoreStub(_brokerRecord);
        List<ConfiguredObjectRecord> records = upgrade(dcs, new BrokerStoreUpgraderAndRecoverer(_systemConfig));

        assertVirtualHost(records, true);
        assertBrokerRecord(records);
    }

    private void assertVirtualHost(List<ConfiguredObjectRecord> records, final boolean isDefaultVHN)
    {
        ConfiguredObjectRecord upgradedVirtualHostNodeRecord = findRecordById(_hostId, records);
        assertEquals("Unexpected type", "VirtualHostNode", upgradedVirtualHostNodeRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("createdBy", VIRTUALHOST_CREATED_BY);
        expectedAttributes.put("createdTime", VIRTUALHOST_CREATE_TIME);
        expectedAttributes.put("name", VIRTUALHOST_NAME);
        expectedAttributes.put("type", "Memory");
        expectedAttributes.put("defaultVirtualHostNode", Boolean.toString(isDefaultVHN));
        assertEquals("Unexpected attributes", expectedAttributes, upgradedVirtualHostNodeRecord.getAttributes());
    }

    private void assertBrokerRecord(List<ConfiguredObjectRecord> records)
    {
        ConfiguredObjectRecord upgradedBrokerRecord = findRecordById(_brokerId, records);
        assertEquals("Unexpected type", "Broker", upgradedBrokerRecord.getType());
        Map<String,Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("name", "Broker");
        expectedAttributes.put("modelVersion", BrokerModel.MODEL_VERSION);
        expectedAttributes.put("createdTime", 1401385808828l);
        assertEquals("Unexpected broker attributes", expectedAttributes, upgradedBrokerRecord.getAttributes());
    }

    private ConfiguredObjectRecord findRecordById(UUID id, List<ConfiguredObjectRecord> records)
    {
        for (ConfiguredObjectRecord configuredObjectRecord : records)
        {
            if (configuredObjectRecord.getId().equals(id))
            {
                return configuredObjectRecord;
            }
        }
        return null;
    }

    private List<ConfiguredObjectRecord> findRecordByType(String type, List<ConfiguredObjectRecord> records)
    {
        List<ConfiguredObjectRecord> results = new ArrayList<>();

        for (ConfiguredObjectRecord configuredObjectRecord : records)
        {
            if (configuredObjectRecord.getType().equals(type))
            {
                results.add(configuredObjectRecord);
            }
        }
        return results;
    }

    class DurableConfigurationStoreStub implements DurableConfigurationStore
    {
        private ConfiguredObjectRecord[] records;

        public DurableConfigurationStoreStub(ConfiguredObjectRecord... records)
        {
            super();
            this.records = records;
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
        public void update(boolean createIfNecessary, ConfiguredObjectRecord... records) throws StoreException
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
        public boolean openConfigurationStore(ConfiguredObjectRecordHandler handler,
                                              final ConfiguredObjectRecord... initialRecords) throws StoreException
        {
            for (ConfiguredObjectRecord record : records)
            {
                handler.handle(record);
            }
            return false;
        }


        @Override
        public void reload(ConfiguredObjectRecordHandler handler) throws StoreException
        {
            for (ConfiguredObjectRecord record : records)
            {
                handler.handle(record);
            }
        }
    }

    private class RecordRetrievingConfiguredObjectRecordHandler implements ConfiguredObjectRecordHandler
    {
        private List<ConfiguredObjectRecord> _records = new ArrayList<>();

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
