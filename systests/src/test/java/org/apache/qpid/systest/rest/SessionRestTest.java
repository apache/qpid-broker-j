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
package org.apache.qpid.systest.rest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNodeCreator;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class SessionRestTest extends QpidRestTestCase
{

    private javax.jms.Connection _connection;
    private javax.jms.Session _session;
    private Destination _queue;
    private MessageProducer _producer;

    @Override
    public void setUp() throws Exception
    {
        // disable the virtualhostPropertiesNode as it messes with the statistics counts since causes the client to
        // create a session and then it sends a message
        setTestSystemProperty("qpid.plugin.disabled:systemnodecreator."+ VirtualHostPropertiesNodeCreator.TYPE, "true");

        super.setUp();

        javax.jms.Connection managementConnection = getConnection();
        try
        {
            managementConnection.start();
            _queue = createTestQueue(managementConnection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE));
        }
        finally
        {
            managementConnection.close();
        }
        _connection = getConnection();
        _session = _connection.createSession(true, javax.jms.Session.SESSION_TRANSACTED);
        MessageConsumer consumer = _session.createConsumer(_queue);
        _producer = _session.createProducer(_queue);
        _connection.start();

        _producer.send(_session.createTextMessage("Test"));
        _session.commit();

        Message m = consumer.receive(getReceiveTimeout());
        assertNotNull("First message was not received", m);
        // Session left open with uncommitted transaction
    }

    public void testGetAllSessions() throws Exception
    {
        List<Map<String, Object>> sessions = getRestTestHelper().getJsonAsList("session");
        assertEquals("Unexpected number of sessions", 1, sessions.size());
        assertSession(sessions.get(0));
    }

    public void testGetPortSessions() throws Exception
    {
        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;

        List<Map<String, Object>> sessions = getRestTestHelper().getJsonAsList("session/" + portName);
        assertEquals("Unexpected number of sessions", 1, sessions.size());
        assertSession(sessions.get(0));
    }

    public void testGetConnectionSessions() throws Exception
    {
        String connectionName = getConnectionName();
        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;

        List<Map<String, Object>> sessions = getRestTestHelper().getJsonAsList("session/" + portName + "/"
                + getRestTestHelper().encodeAsUTF(connectionName));
        assertEquals("Unexpected number of sessions", 1, sessions.size());
        assertSession(sessions.get(0));
    }

    public void testGetSessionByName() throws Exception
    {
        String connectionName = getConnectionName();
        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;

        Map<String, Object> sessionDetails = getRestTestHelper().getJsonAsMap("session/" + portName + "/"
                + getRestTestHelper().encodeAsUTF(connectionName) + "/" + ((AMQSession<?, ?>) _session).getChannelId());
        assertSession(sessionDetails);
    }

    private void assertSession(Map<String, Object> sessionData)
    {
        assertNotNull("Session map cannot be null", sessionData);
        Asserts.assertAttributesPresent(sessionData, BrokerModel.getInstance().getTypeRegistry().getAttributeNames(
                Session.class),
                                        ConfiguredObject.TYPE,
                                        ConfiguredObject.CREATED_BY,
                                        ConfiguredObject.CREATED_TIME,
                                        ConfiguredObject.LAST_UPDATED_BY,
                                        ConfiguredObject.LAST_UPDATED_TIME,
                                        ConfiguredObject.DESCRIPTION,
                                        ConfiguredObject.CONTEXT,
                                        ConfiguredObject.DESIRED_STATE,
                                        Session.STATE,
                                        Session.DURABLE,
                                        Session.LIFETIME_POLICY);
        assertEquals("Unexpected value of attribute " + Session.PRODUCER_FLOW_BLOCKED, Boolean.FALSE,
                sessionData.get(Session.PRODUCER_FLOW_BLOCKED));
        assertNotNull("Unexpected value of attribute " + Session.NAME, sessionData.get(Session.NAME));
        assertNotNull("Unexpected value of attribute " + Session.CHANNEL_ID , sessionData.get(Session.CHANNEL_ID));

        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) sessionData.get(Asserts.STATISTICS_ATTRIBUTE);
        Asserts.assertAttributesPresent(statistics, "consumerCount",
                                        "unacknowledgedMessages");

        assertEquals("Unexpected value of statistic attribute " + "unacknowledgedMessages",  1,
                statistics.get("unacknowledgedMessages"));

        assertEquals("Unexpected value of statistic attribute " + "consumerCount", 1,
                     statistics.get("consumerCount"));
    }

    private String getConnectionName() throws IOException
    {
        List<Map<String, Object>> connections = getRestTestHelper().getJsonAsList("virtualhost/test/test/getConnections");
        assertEquals("Unexpected number of connections", 1, connections.size());
        Map<String, Object> connection = connections.get(0);
        String connectionName = (String) connection.get(Connection.NAME);
        return connectionName;
    }
}
