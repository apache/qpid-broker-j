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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfig;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.util.FileUtils;


public class BrokerStoreUpgraderAndRecovererTest extends QpidTestCase
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

    public void setUp() throws Exception
    {
        super.setUp();
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
        assertEquals("Unexpected port size", 1, ports.size());
        ConfiguredObjectRecord upgradedRecord = ports.get(0);
        Map<String, Object> attributes = upgradedRecord.getAttributes();
        assertFalse("receiveBufferSize is found " + attributes.get("receiveBufferSize"), attributes.containsKey("receiveBufferSize"));
        assertFalse("sendBufferSize is found " + attributes.get("sendBufferSize"), attributes.containsKey("sendBufferSize"));
        assertEquals("Unexpected name", getTestName(), attributes.get("name"));
    }

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
        assertEquals("AuthenticationProvider was removed", 1, authenticationProviders.size());
    }

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
        assertEquals("AuthenticationProviders was removed", 1, authenticationProviders.size());
        assertFalse("PreferencesProvider was not removed",
                    authenticationProviders.get(0).getAttributes().containsKey("preferencesproviders"));
    }

    public void testEndToEndUpgradeFromModelVersion1() throws Exception
    {
        final File workDir = TestFileUtils.createTestDirectory("qpid.work_dir", true);
        try
        {
            File pathToStore_1_0 = copyResource("configuration/broker-config-1.0.json", workDir);
            File pathToStoreLatest = copyResource("configuration/broker-config-latest.json", workDir);

            List<ConfiguredObjectRecord> expected = getStoreRecords(pathToStoreLatest);

            BrokerStoreUpgraderAndRecoverer recoverer = new BrokerStoreUpgraderAndRecoverer(_systemConfig);
            List<ConfiguredObjectRecord> records =
                    recoverer.upgrade(mock(DurableConfigurationStore.class), getStoreRecords(pathToStore_1_0));

            assertEquals("Unexpected number of records after upgrade", expected.size(), records.size());

            for (ConfiguredObjectRecord expectedRecord : expected)
            {
                ConfiguredObjectRecord actualRecord = findActualForExpected(expectedRecord, expected, records);
                assertNotNull("Missing record after upgrade :" + expectedRecord, actualRecord);
                assertEquals("Unexpected record attributes after upgrade",
                             getRecordAttributes(expectedRecord.getAttributes()),
                             getRecordAttributes(actualRecord.getAttributes()));
            }
        }
        finally
        {
            FileUtils.delete(workDir, true);
        }
    }

    private Map<String, Object> getRecordAttributes(final Map<String, Object> attributes)
    {
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Object> attribute : attributes.entrySet())
        {
            if (!(attribute.getValue() instanceof Collection) || !("id".equals(attribute.getKey())))
            {
                result.put(attribute.getKey(), attribute.getValue());
            }
        }
        return result;
    }

    private ConfiguredObjectRecord findActualForExpected(final ConfiguredObjectRecord expectedRecord,
                                                         final List<ConfiguredObjectRecord> expectedRecords,
                                                         final List<ConfiguredObjectRecord> actualRecords)
    {
        for (ConfiguredObjectRecord actual : actualRecords)
        {
            if (actual.getId().equals(expectedRecord.getId()))
            {
                return actual;
            }
        }
        String expectedName = String.valueOf(expectedRecord.getAttributes().get("name"));
        List<ConfiguredObjectRecord> expectedParents = findParents(expectedRecord.getParents(), expectedRecords);
        for (ConfiguredObjectRecord actual : actualRecords)
        {
            String actualName = String.valueOf(actual.getAttributes().get("name"));
            List<ConfiguredObjectRecord> actualParents = findParents(actual.getParents(), actualRecords);
            if (actual.getType().equals(expectedRecord.getType())
                && expectedName.equals(actualName)
                && equals(expectedParents, actualParents))
            {
                return actual;
            }
        }
        return null;
    }

    private List<ConfiguredObjectRecord> findParents(final Map<String, UUID> parents,
                                                     final List<ConfiguredObjectRecord> records)
    {
        List<ConfiguredObjectRecord> results = new ArrayList<>();
        for (Map.Entry<String, UUID> parent : parents.entrySet())
        {
            ConfiguredObjectRecord parentRecord = null;
            for (ConfiguredObjectRecord record : records)
            {
                if (record.getId().equals(parent.getValue()) && record.getType().equals(parent.getKey()))
                {
                    parentRecord = record;
                    break;
                }
            }
            if (parentRecord == null)
            {
                throw new RuntimeException("Parent record is not found for " + parent);
            }
            results.add(parentRecord);
        }
        return results;
    }

    private boolean equals(final List<ConfiguredObjectRecord> expectedRecords,
                           final List<ConfiguredObjectRecord> actualRecords)
    {
        for (ConfiguredObjectRecord expectedRecord : expectedRecords)
        {
            String expectedName = String.valueOf(expectedRecord.getAttributes().get("name"));
            for (ConfiguredObjectRecord actual : actualRecords)
            {
                if (actual.getId().equals(expectedRecord.getId()))
                {
                    return true;
                }
                String actualName = String.valueOf(actual.getAttributes().get("name"));
                if (actual.getType().equals(expectedRecord.getType())
                    && expectedName.equals(actualName))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private File copyResource(String resource, File workDir) throws IOException
    {
        File copy = new File(workDir, new File(resource).getName());
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource))
        {
            Files.copy(is, copy.toPath());
        }
        return copy;
    }

    private List<ConfiguredObjectRecord> getStoreRecords(final File pathToStore)
    {
        JsonSystemConfig systemConfig = getJsonSystemConfigMock(pathToStore.getAbsolutePath());
        JsonFileConfigStore jsonFileConfigStore = new JsonFileConfigStore(Broker.class);
        try
        {
            jsonFileConfigStore.init(systemConfig);
            RecordRetrievingConfiguredObjectRecordHandler handler = new RecordRetrievingConfiguredObjectRecordHandler();
            jsonFileConfigStore.openConfigurationStore(handler);
            return handler.getRecords();
        }
        finally
        {
            jsonFileConfigStore.closeConfigurationStore();
        }
    }

    private JsonSystemConfig getJsonSystemConfigMock(final String pathToStore)
    {
        JsonSystemConfig systemConfig = mock(JsonSystemConfig.class);
        when(systemConfig.getName()).thenReturn("test");
        when(systemConfig.getStorePath()).thenReturn(pathToStore);
        when(systemConfig.getModel()).thenReturn(BrokerModel.getInstance());
        return systemConfig;
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
