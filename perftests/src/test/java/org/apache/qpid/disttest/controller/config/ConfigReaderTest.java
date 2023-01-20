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
 *
 */
package org.apache.qpid.disttest.controller.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.qpid.disttest.ConfigFileTestHelper;
import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.test.utils.TestFileUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ConfigReaderTest extends UnitTestBase
{
    private Config _config;

    @BeforeEach
    public void setUp() throws Exception
    {
        ConfigReader configReader = new ConfigReader();
        Reader reader = ConfigFileTestHelper.getConfigFileReader(getClass(), "sampleConfig.json");
        _config = configReader.readConfig(reader);
    }

    @Test
    public void testReadTest()
    {
        List<TestConfig> tests = _config.getTestConfigs();
        assertEquals(2, (long) tests.size(), "Unexpected number of tests");
        TestConfig test1Config = tests.get(0);
        assertNotNull(test1Config, "Test 1 configuration is expected");
        assertEquals("Test 1", test1Config.getName(), "Unexpected test name");

        TestConfig test2Config = tests.get(1);
        assertNotNull(test2Config, "Test 2 configuration is expected");
    }

    @Test
    public void testReadsTestWithQueues()
    {
        TestConfig test1Config =  _config.getTestConfigs().get(0);
        List<QueueConfig> queues = test1Config.getQueues();
        assertEquals(2, (long) queues.size(), "Unexpected number of queues");
        QueueConfig queue1Config = queues.get(0);
        assertNotNull(queue1Config, "Expected queue 1 config");
        assertEquals("Json-Queue-Name", queue1Config.getName(), "Unexpected queue name");
        assertTrue(queue1Config.getAttributes().isEmpty(), "Unexpected attributes");
        assertFalse(queue1Config.isDurable(), "Unexpected durable");

        QueueConfig queue2Config = queues.get(1);
        assertNotNull(queue2Config, "Expected queue 2 config");
        assertEquals("Json Queue Name 2", queue2Config.getName(), "Unexpected queue name");
        assertTrue(queue2Config.isDurable(), "Unexpected durable");
        Map<String, Object> attributes =  queue2Config.getAttributes();
        assertNotNull(attributes, "Expected attributes");
        assertFalse(attributes.isEmpty(), "Attributes are not loaded");
        assertEquals(1, (long) attributes.size(), "Unexpected number of attributes");
        assertEquals(10, (long) ((Number) attributes.get("x-qpid-priorities")).intValue(),
                "Unexpected attribute 'x-qpid-priorities' value");
    }

    @Test
    public void testReadsTestWithIterations()
    {
        TestConfig testConfig = _config.getTestConfigs().get(0);
        List<IterationValue> iterationValues = testConfig.getIterationValues();
        assertEquals(2, (long) iterationValues.size(), "Unexpected number of iterations");

        IterationValue iteration1 = iterationValues.get(0);

        String messageSizeProperty = "_messageSize";

        assertEquals("100", iteration1.getIterationPropertyValuesWithUnderscores().get(messageSizeProperty),
                "Unexpected value for property " + messageSizeProperty);
    }

    @Test
    public void testReadsMessageProviders()
    {
        TestConfig testConfig = _config.getTestConfigs().get(0);
        ClientConfig clientConfig = testConfig.getClients().get(0);
        List<MessageProviderConfig> configs = clientConfig.getMessageProviders();
        assertNotNull(configs, "Message provider configs should not be null");
        assertEquals(1, (long) configs.size(), "Unexpected number of message providers");
        MessageProviderConfig messageProvider = configs.get(0);
        assertNotNull(messageProvider, "Message provider config should not be null");
        assertEquals("testProvider1", messageProvider.getName(), "Unexpected provider name");
        Map<String, PropertyValue> properties = messageProvider.getMessageProperties();
        assertNotNull(properties, "Message properties should not be null");
        assertEquals(3, (long) properties.size(), "Unexpected number of message properties");
        assertNotNull(properties.get("test"), "test property is not found");
        assertNotNull(properties.get("priority"), "priority property is not found");
        assertNotNull(properties.get("id"), "id property is not found");
    }

    @Test
    public void testReadsJS() throws Exception
    {
        ConfigReader configReader = new ConfigReader();
        String path = TestFileUtils.createTempFileFromResource(this, "ConfigReaderTest-test-config.js").getAbsolutePath();
        _config = configReader.getConfigFromFile(path);

        List<TestConfig> testConfigs = _config.getTestConfigs();
        assertEquals(2, (long) testConfigs.size(), "Unexpected number of tests");
        TestConfig testConfig1 = _config.getTestConfigs().get(0);
        List<ClientConfig> cleintConfigs = testConfig1.getClients();
        assertEquals(2, (long) cleintConfigs.size(), "Unexpected number of test 1 clients");
        List<QueueConfig> queueConfigs = testConfig1.getQueues();
        assertEquals(1, (long) queueConfigs.size(), "Unexpected number of test 1 queue");
        assertEquals("Json-Queue-Name", queueConfigs.get(0).getName(), "Unexpected queue name");
        ClientConfig clientConfig = cleintConfigs.get(0);
        List<ConnectionConfig> connectionConfigs = clientConfig.getConnections();
        assertEquals(1, (long) connectionConfigs.size(), "Unexpected number of connections");
        List<SessionConfig> sessionConfigs = connectionConfigs.get(0).getSessions();
        assertEquals(1, (long) sessionConfigs.size(), "Unexpected number of sessions");
        assertEquals(0, (long) sessionConfigs.get(0).getAcknowledgeMode(), "Unexpected ack mode");

        TestConfig testConfig2 = _config.getTestConfigs().get(1);
        List<ClientConfig> cleintConfigs2 = testConfig2.getClients();
        assertEquals(2, (long) cleintConfigs2.size(), "Unexpected number of test 1 clients");
        List<QueueConfig> queueConfigs2 = testConfig2.getQueues();
        assertEquals(1, (long) queueConfigs2.size(), "Unexpected number of test 1 queue");
        assertEquals("Json-Queue-Name", queueConfigs2.get(0).getName(), "Unexpected queue name");
        ClientConfig clientConfig2 = cleintConfigs2.get(0);
        List<ConnectionConfig> connectionConfigs2 = clientConfig2.getConnections();
        assertEquals(1, (long) connectionConfigs2.size(), "Unexpected number of connections");
        List<SessionConfig> sessionConfigs2 = connectionConfigs2.get(0).getSessions();
        assertEquals(1, (long) sessionConfigs2.size(), "Unexpected number of sessions");
        assertEquals(1, (long) sessionConfigs2.get(0).getAcknowledgeMode(), "Unexpected ack mode");
    }

}
