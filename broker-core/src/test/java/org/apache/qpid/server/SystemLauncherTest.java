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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() throws Exception
    {

        // create empty initial configuration
        Map<String,Object> initialConfig = new HashMap<>();
        initialConfig.put(ConfiguredObject.NAME, "test");
        initialConfig.put(org.apache.qpid.server.model.Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);

        ObjectMapper mapper = new ObjectMapper();
        String config = mapper.writeValueAsString(initialConfig);
        _initialConfiguration = TestFileUtils.createTempFile(this, ".initial-config.json", config);
        _brokerWork = TestFileUtils.createTestDirectory("qpid-work", true);
        _initialSystemProperties = TestFileUtils.createTempFile(this, ".initial-system.properties",
                INITIAL_SYSTEM_PROPERTY + "=" + INITIAL_SYSTEM_PROPERTY_VALUE
                + "\nQPID_WORK=" +  _brokerWork.getAbsolutePath() + "_test");
        setTestSystemProperty("QPID_WORK", _brokerWork.getAbsolutePath());
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
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
    }

    @Test
    public void testInitialSystemPropertiesAreSetOnBrokerStartup() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SystemConfig.INITIAL_SYSTEM_PROPERTIES_LOCATION, _initialSystemProperties.getAbsolutePath());
        attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, _initialConfiguration.getAbsolutePath());
        attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);
        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.TRUE);
        _systemLauncher = new SystemLauncher();
        _systemLauncher.startup(attributes);

        // test JVM system property should be set from initial system config file
        assertEquals("Unexpected JVM system property",
                            INITIAL_SYSTEM_PROPERTY_VALUE,
                            System.getProperty(INITIAL_SYSTEM_PROPERTY));


        // existing system property should not be overridden
        assertEquals("Unexpected QPID_WORK system property",
                            _brokerWork.getAbsolutePath(),
                            System.getProperty("QPID_WORK"));
    }

    @Test
    public void testConsoleLogsOnSuccessfulStartup() throws Exception
    {
        byte[] outputBytes = startBrokerAndCollectSystemOutput();
        String output = new String(outputBytes);
        assertFalse("Detected unexpected Exception: " + output, output.contains("Exception"));
        assertTrue("Output does not contain Broker Ready Message",
                          output.contains(BrokerMessages.READY().toString()));
    }

    @Test
    public void testConsoleLogsOnUnsuccessfulStartup() throws Exception
    {
        Map<String,Object> initialConfig = new HashMap<>();

        ObjectMapper mapper = new ObjectMapper();
        String config = mapper.writeValueAsString(initialConfig);
        TestFileUtils.saveTextContentInFile(config, _initialConfiguration);

        byte[] outputBytes = startBrokerAndCollectSystemOutput();
        String output = new String(outputBytes);
        assertTrue("No Exception detected in output: " + output, output.contains("Exception"));
        assertFalse("Output contains Broker Ready Message", output.contains(BrokerMessages.READY().toString()));
    }

    private byte[] startBrokerAndCollectSystemOutput() throws Exception
    {
        try(ByteArrayOutputStream out = new ByteArrayOutputStream())
        {

            PrintStream originalOutput = System.out;
            try
            {
                System.setOut(new PrintStream(out));

                Map<String,Object> attributes = new HashMap<>();
                attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, _initialConfiguration.getAbsolutePath());
                attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);

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
