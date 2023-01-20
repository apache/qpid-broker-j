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
package org.apache.qpid.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class SystemLauncherTest extends UnitTestBase
{
    private static final String INITIAL_SYSTEM_PROPERTY = "test";
    private static final String INITIAL_SYSTEM_PROPERTY_VALUE = "testValue";

    private File _initialSystemProperties;
    private File _initialConfiguration;
    private File _brokerWork;
    private SystemLauncher _systemLauncher;

    @BeforeEach
    public void setUp() throws Exception
    {

        // create empty initial configuration
        final Map<String,Object> initialConfig = Map.of(ConfiguredObject.NAME, "test",
                org.apache.qpid.server.model.Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);

        final ObjectMapper mapper = new ObjectMapper();
        final String config = mapper.writeValueAsString(initialConfig);
        _initialConfiguration = TestFileUtils.createTempFile(this, ".initial-config.json", config);
        _brokerWork = TestFileUtils.createTestDirectory("qpid-work", true);
        _initialSystemProperties = TestFileUtils.createTempFile(this, ".initial-system.properties",
                INITIAL_SYSTEM_PROPERTY + "=" + INITIAL_SYSTEM_PROPERTY_VALUE
                + "\nQPID_WORK=" +  _brokerWork.getAbsolutePath() + "_test");
        setTestSystemProperty("QPID_WORK", _brokerWork.getAbsolutePath());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_systemLauncher != null)
        {
            _systemLauncher.shutdown();
        }
        System.clearProperty(INITIAL_SYSTEM_PROPERTY);
        FileUtils.delete(_brokerWork, true);
        FileUtils.delete(_initialSystemProperties, false);
        FileUtils.delete(_initialConfiguration, false);
    }

    @Test
    public void testInitialSystemPropertiesAreSetOnBrokerStartup() throws Exception
    {
        final Map<String,Object> attributes = Map.of(SystemConfig.INITIAL_SYSTEM_PROPERTIES_LOCATION, _initialSystemProperties.getAbsolutePath(),
                SystemConfig.INITIAL_CONFIGURATION_LOCATION, _initialConfiguration.getAbsolutePath(),
                SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE,
                SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.TRUE);
        _systemLauncher = new SystemLauncher();
        _systemLauncher.startup(attributes);

        // test JVM system property should be set from initial system config file
        assertEquals(INITIAL_SYSTEM_PROPERTY_VALUE, System.getProperty(INITIAL_SYSTEM_PROPERTY),
                "Unexpected JVM system property");


        // existing system property should not be overridden
        assertEquals(_brokerWork.getAbsolutePath(), System.getProperty("QPID_WORK"),
                "Unexpected QPID_WORK system property");
    }

    @Test
    public void testConsoleLogsOnSuccessfulStartup() throws Exception
    {
        final byte[] outputBytes = startBrokerAndCollectSystemOutput();
        final String output = new String(outputBytes);
        assertFalse(output.contains("Exception"), "Detected unexpected Exception: " + output);
        assertTrue(output.contains(BrokerMessages.READY().toString()),
                "Output does not contain Broker Ready Message");
    }

    @Test
    public void testConsoleLogsOnUnsuccessfulStartup() throws Exception
    {
        final Map<String,Object> initialConfig = Map.of();

        final ObjectMapper mapper = new ObjectMapper();
        final String config = mapper.writeValueAsString(initialConfig);
        TestFileUtils.saveTextContentInFile(config, _initialConfiguration);

        final byte[] outputBytes = startBrokerAndCollectSystemOutput();
        final String output = new String(outputBytes);
        assertTrue(output.contains("Exception"), "No Exception detected in output: " + output);
        assertFalse(output.contains(BrokerMessages.READY().toString()),
                "Output contains Broker Ready Message");
    }

    private byte[] startBrokerAndCollectSystemOutput() throws Exception
    {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream())
        {

            final PrintStream originalOutput = System.out;
            try
            {
                System.setOut(new PrintStream(out));

                final Map<String,Object> attributes = Map.of(SystemConfig.INITIAL_CONFIGURATION_LOCATION, _initialConfiguration.getAbsolutePath(),
                        SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);

                _systemLauncher = new SystemLauncher();
                _systemLauncher.startup(attributes);
            }
            finally
            {
                System.setOut(originalOutput);
            }
            return out.toByteArray();
        }
    }
}
