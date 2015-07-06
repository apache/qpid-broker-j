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
 *
 *
 */
package org.apache.qpid.server;

import java.io.File;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.server.logging.AbstractTestLogging;
import org.apache.qpid.util.FileUtils;

/**
 * Series of tests to validate the external Java broker starts up as expected.
 */
public class BrokerStartupTest extends AbstractTestLogging
{
    @Override
    public void startBroker()
    {
        // Each test starts its own broker so no-op here because it is called from super.setup()
    }

    /**
     * This test simply tests that the broker will startup even if there is no config file (i.e. that it can use the
     * currently packaged initial config file (all system tests by default generate their own config file).
     *
     *
     * @throws Exception
     */
    public void testStartupWithNoConfig() throws Exception
    {
        if (isJavaBroker())
        {
            int port = getPort(0);
            int managementPort = getManagementPort(port);
            int connectorServerPort = managementPort + JMXPORT_CONNECTORSERVER_OFFSET;

            setTestSystemProperty("qpid.amqp_port",String.valueOf(port));
            setTestSystemProperty("qpid.jmx_port",String.valueOf(managementPort));
            setTestSystemProperty("qpid.rmi_port",String.valueOf(connectorServerPort));
            setTestSystemProperty("qpid.http_port",String.valueOf(DEFAULT_HTTP_MANAGEMENT_PORT));

            File brokerConfigFile = new File(getTestConfigFile(port));
            if (brokerConfigFile.exists())
            {
                FileUtils.delete(brokerConfigFile, true);
            }

            startBroker(port, null);

            AMQConnectionURL url = new AMQConnectionURL(String.format("amqp://"
                                                                      + GUEST_USERNAME
                                                                      + ":"
                                                                      + GUEST_PASSWORD
                                                                      + "@clientid/?brokerlist='localhost:%d'", port));
            Connection conn = getConnection(url);
            assertNotNull(conn);
            conn.close();
        }
    }

    public void testStartupWithErroredChildrenCanBeConfigured() throws Exception
    {
        if (isJavaBroker())
        {
            int port = getPort(0);
            int managementPort = getManagementPort(port);
            int connectorServerPort = managementPort + JMXPORT_CONNECTORSERVER_OFFSET;

            setTestSystemProperty("qpid.amqp_port",String.valueOf(port));
            setTestSystemProperty("qpid.jmx_port",String.valueOf(managementPort));
            setTestSystemProperty("qpid.rmi_port",String.valueOf(connectorServerPort));

            //Purposely set the HTTP port to be the same as the AMQP port so that the port object becomes ERRORED
            setTestSystemProperty("qpid.http_port",String.valueOf(port));

            // Set broker to fail on startup if it has ERRORED children
            setTestSystemProperty("broker.failStartupWithErroredChild", String.valueOf(Boolean.TRUE));

            File brokerConfigFile = new File(getTestConfigFile(port));
            if (brokerConfigFile.exists())
            {
                // Config exists from previous test run, delete it.
                FileUtils.delete(brokerConfigFile, true);
            }

            startBroker(port, null);

            AMQConnectionURL url = new AMQConnectionURL(String.format("amqp://"
                    + GUEST_USERNAME
                    + ":"
                    + GUEST_PASSWORD
                    + "@clientid/?brokerlist='localhost:%d'", port));

            try
            {
                Connection conn = getConnection(url);
                fail("Connection should fail as broker startup should have failed due to ERRORED children (port)");
                conn.close();
            }
            catch (JMSException jmse)
            {
                //pass
            }
        }
    }

}
