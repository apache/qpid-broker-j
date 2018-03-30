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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Test to verify the command line parsing within the Main class, by
 * providing it a series of command line arguments and verifying the
 * BrokerOptions emerging for use in starting the Broker instance.
 */
public class MainTest extends UnitTestBase
{
    private Exception _startupException;

    @Test
    public void testNoOptionsSpecified()
    {
        String qpidWork = "/qpid/work";
        setTestSystemProperty(SystemConfig.PROPERTY_QPID_WORK, qpidWork);
        String qpidHome = "/qpid/home";
        setTestSystemProperty(Main.PROPERTY_QPID_HOME, qpidHome);

        Map<String,Object> attributes = startDummyMain("");

        assertEquals("JSON", attributes.get(SystemConfig.TYPE));
        assertNotEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
    }


    @Test
    public void testConfigurationStoreLocation()
    {
        Map<String,Object> attributes = startDummyMain("-sp abcd/config.xml");
        assertEquals("abcd/config.xml", attributes.get("storePath"));

        attributes = startDummyMain("-store-path abcd/config2.xml");
        assertEquals("abcd/config2.xml", attributes.get("storePath"));
    }

    @Test
    public void testConfigurationStoreType()
    {
        Map<String,Object> attributes = startDummyMain("-st dby");
        assertEquals("dby", attributes.get(SystemConfig.TYPE));

        attributes = startDummyMain("-store-type bdb");
        assertEquals("bdb", attributes.get(SystemConfig.TYPE));
    }

    @Test
    public void testVersion()
    {
        final TestMain main = new TestMain("-v".split("\\s"));

        assertNotNull("Command line not parsed correctly", main.getCommandLine());
        assertTrue("Parsed command line didnt pick up version option", main.getCommandLine().hasOption("v"));
    }

    @Test
    public void testHelp()
    {
        final TestMain main = new TestMain("-h".split("\\s"));

        assertNotNull("Command line not parsed correctly", main.getCommandLine());
        assertTrue("Parsed command line didnt pick up help option", main.getCommandLine().hasOption("h"));
    }

    @Test
    public void testInitialConfigurationLocation()
    {
        Map<String,Object> attributes = startDummyMain("-icp abcd/initial-config.json");
        assertEquals("abcd/initial-config.json", attributes.get(SystemConfig.INITIAL_CONFIGURATION_LOCATION));

        attributes = startDummyMain("-initial-config-path abcd/initial-config.json");
        assertEquals("abcd/initial-config.json", attributes.get(SystemConfig.INITIAL_CONFIGURATION_LOCATION));
    }

    @Test
    public void testManagementMode()
    {
        Map<String,Object> attributes = startDummyMain("-mm");
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));

        attributes = startDummyMain("--management-mode");
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
    }

    @Test
    public void testManagementModeHttpPortOverride()
    {
        Map<String,Object> attributes = startDummyMain("-mm -mmhttp 9999");
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
        assertEquals("9999", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE)));

        attributes = startDummyMain("-mm --management-mode-http-port 9999");
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
        assertEquals("9999", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE)));

        attributes = startDummyMain("-mmhttp 9999");
        assertNotEquals("9999", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE)));
    }

    @Test
    public void testManagementModePassword()
    {
        String password = getTestName();
        Map<String,Object> attributes = startDummyMain("-mm -mmpass " + password);
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
        assertEquals(password, attributes.get(SystemConfig.MANAGEMENT_MODE_PASSWORD));

        attributes = startDummyMain("-mm --management-mode-password " + password);
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
        assertEquals(password, attributes.get(SystemConfig.MANAGEMENT_MODE_PASSWORD));

        attributes = startDummyMain("-mm -mmpass " + password);
        assertEquals(password, attributes.get(SystemConfig.MANAGEMENT_MODE_PASSWORD));
    }

    @Test
    public void testDefaultManagementModePassword()
    {
        Map<String,Object> attributes = startDummyMain("-mm");
        assertEquals("true", String.valueOf(attributes.get(SystemConfig.MANAGEMENT_MODE)));
        assertNotNull(attributes.get(SystemConfig.MANAGEMENT_MODE_PASSWORD));
    }

    @Test
    public void testSetConfigProperties()
    {
        //short name
        String newPort = "12345";
        Map<String,Object> attributes = startDummyMain("-prop name=value -prop " + org.apache.qpid.server.model.Broker.QPID_AMQP_PORT + "=" + newPort);

        Map<String, String> props = (Map<String,String>) attributes.get(SystemConfig.CONTEXT);

        assertEquals(newPort, props.get(org.apache.qpid.server.model.Broker.QPID_AMQP_PORT));
        assertEquals("value", props.get("name"));

        //long name
        newPort = "678910";
        attributes = startDummyMain("--config-property name2=value2 --config-property " + org.apache.qpid.server.model.Broker.QPID_AMQP_PORT + "=" + newPort);

        props = (Map<String,String>) attributes.get(SystemConfig.CONTEXT);

        assertEquals(newPort, props.get(org.apache.qpid.server.model.Broker.QPID_AMQP_PORT));
        assertEquals("value2", props.get("name2"));
    }

    @Test
    public void testSetConfigPropertiesInvalidFormat()
    {
        //missing equals
        startDummyMain("-prop namevalue");
        final boolean condition1 = _startupException instanceof IllegalArgumentException;
        assertTrue("expected exception did not occur", condition1);

        //no name specified
        startDummyMain("-prop =value");
        final boolean condition = _startupException instanceof IllegalArgumentException;
        assertTrue("expected exception did not occur", condition);
    }

    private Map<String,Object> startDummyMain(String commandLine)
    {
        return (new TestMain(commandLine.split("\\s"))).getAttributes();
    }

    private class TestMain extends Main
    {
        private Map<String, Object> _attributes;

        public TestMain(String[] args)
        {
            super(args);
        }

        @Override
        protected void execute()
        {
            try
            {
                super.execute();
            }
            catch(Exception re)
            {
                MainTest.this._startupException = re;
            }
        }

        @Override
        protected void startBroker(Map<String,Object> attributes)
        {
            _attributes = attributes;
        }

        @Override
        protected void setExceptionHandler()
        {
        }

        public Map<String,Object> getAttributes()
        {
            return _attributes;
        }

        public CommandLine getCommandLine()
        {
            return _commandLine;
        }
    }
}
