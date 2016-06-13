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

package org.apache.qpid.client.connection;

import java.net.Socket;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.transport.network.io.IoNetworkTransport;

public class ExistingSocketTest extends QpidBrokerTestCase
{
    public static final String SOCKET_NAME = "mysock";

    public void testExistingSocket_SuccessfulConnection() throws Exception
    {
        // Suppress negotiation
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, getBrokerProtocol().getProtocolVersion());

        try(Socket sock = new Socket("localhost", getDefaultAmqpPort()))
        {

            IoNetworkTransport.registerOpenSocket(SOCKET_NAME, sock);

            String url = String.format("amqp://guest:guest@/test?brokerlist='socket://%s'", SOCKET_NAME);

            Connection conn = getConnection(new AMQConnectionURL(url));
            conn.createSession(true, javax.jms.Session.SESSION_TRANSACTED);
            conn.close();
        }
    }

    public void testExistingSocket_UnknownSocket() throws Exception
    {
        final Object unknownSockName = "unknownSock";

        String url = String.format("amqp://guest:guest@/test?brokerlist='socket://%s'", unknownSockName);

        try
        {
            getConnection(new AMQConnectionURL(url));
        }
        catch (JMSException e)
        {
            String expected = String.format("Error creating connection: No socket registered with id '%s'",
                                            unknownSockName);
            assertEquals(expected, e.getMessage());
        }
    }
}
