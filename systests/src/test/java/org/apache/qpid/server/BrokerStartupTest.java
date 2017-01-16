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

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.qpid.server.logging.AbstractTestLogging;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

/**
 * Series of tests to validate the external Apache Qpid Broker for Java starts up as expected.
 */
public class BrokerStartupTest extends AbstractTestLogging
{
    @Override
    public void startDefaultBroker()
    {
        // noop; we do not want to start broker
    }

    /**
     * This test simply tests that the broker will startup even if there is no config file (i.e. that it can use the
     * currently packaged initial config file (all system tests by default generate their own config file).
     *
     * @throws Exception
     */
    public void testStartupWithNoConfig() throws Exception
    {
        if (isJavaBroker())
        {
            super.startDefaultBroker();
            Connection conn = getConnection();
            assertNotNull(conn);
        }
    }

    public void testStartupWithErroredChildrenCanBeConfigured() throws Exception
    {
        if (isJavaBroker())
        {
            // Purposely set the HTTP port to -1 so that the port object becomes ERRORED
            final int invalidHttpPort = -1;
            setTestSystemProperty("test.hport", String.valueOf(invalidHttpPort));
            getDefaultBrokerConfiguration().addHttpManagementConfiguration();
            getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, Port.PORT, invalidHttpPort);

            // Set broker to fail on startup if it has ERRORED children
            setTestSystemProperty("broker.failStartupWithErroredChild", String.valueOf(Boolean.TRUE));

            super.startDefaultBroker();

            try
            {
                Connection conn = getConnection();
                fail("Connection should fail as broker startup should have failed due to ERRORED children (port)");
            }
            catch (JMSException jmse)
            {
                //pass
            }
        }
    }

}
