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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNodeCreator;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class ConnectionRestTest extends QpidRestTestCase
{
    private javax.jms.Connection _connection;
    private javax.jms.Session _session;
    private MessageProducer _producer;
    private long _startTime;
    private Destination _queue;

    @Override
    public void setUp() throws Exception
    {
        // disable the virtualhostPropertiesNode as it messes with the statistics counts since causes the client to
        // create a session and then it sends a message
        setTestSystemProperty("qpid.plugin.disabled:systemnodecreator." + VirtualHostPropertiesNodeCreator.TYPE,
                              "true");

        super.setUp();

        javax.jms.Connection managementConnection = getConnection();
        managementConnection.start();
        _queue = createTestQueue(managementConnection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE));
        managementConnection.close();

        _startTime = System.currentTimeMillis();
        _connection = getConnectionBuilder().setSyncPublish(true).build();
        _session = _connection.createSession(true, javax.jms.Session.SESSION_TRANSACTED);
        _producer = _session.createProducer(_queue);
        _connection.start();
    }

    public void testGetAllConnections() throws Exception
    {
        List<Map<String, Object>> connections = getRestTestHelper().getJsonAsList("connection");
        assertEquals("Unexpected number of connections", 1, connections.size());
        Asserts.assertConnection(connections.get(0), isBroker10() ? 2 : 1);
    }

    public void testGetVirtualHostConnections() throws Exception
    {
        List<Map<String, Object>> connections =
                getRestTestHelper().getJsonAsList("virtualhost/test/test/getConnections");
        assertEquals("Unexpected number of connections", 1, connections.size());
        Asserts.assertConnection(connections.get(0), isBroker10() ? 2 : 1);
    }

    public void testDeleteConnection() throws Exception
    {
        String connectionName = getConnectionName();
        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;

        List<Map<String, Object>> connections = getRestTestHelper().getJsonAsList("connection/" + portName);
        assertEquals("Unexpected number of connections before deletion", 1, connections.size());

        String connectionUrl = "connection/" + portName + "/" + getRestTestHelper().encodeAsUTF(connectionName);
        getRestTestHelper().submitRequest(connectionUrl, "DELETE", HttpServletResponse.SC_OK);

        connections = getRestTestHelper().getJsonAsList("connection/" + portName);
        assertEquals("Unexpected number of connections before deletion", 0, connections.size());

        try
        {
            _connection.createSession(true, javax.jms.Session.SESSION_TRANSACTED);
            fail("Exception not thrown");
        }
        catch (JMSException je)
        {
            // PASS
        }
    }

    public void testConnectionTransactionCountStatistics() throws Exception
    {
        String connectionUrl = getVirtualHostConnectionUrl();

        _producer.send(_session.createTextMessage("Test-0"));
        _session.commit();

        MessageConsumer consumer = _session.createConsumer(_queue);
        Message m = consumer.receive(getReceiveTimeout());
        assertNotNull("Subsequent messages were not received", m);

        Map<String, Object> statistics = getConnectionStatistics(connectionUrl);

        assertEquals("Unexpected value of statistic attribute localTransactionBegins", 2,
                     statistics.get("localTransactionBegins"));
        assertEquals("Unexpected value of statistic attribute localTransactionRollbacks", 0,
                     statistics.get("localTransactionRollbacks"));
        assertEquals("Unexpected value of statistic attribute localTransactionOpen", 1,
                     statistics.get("localTransactionOpen"));

        _session.rollback();
        m = consumer.receive(getReceiveTimeout());
        assertNotNull("Subsequent messages were not received", m);

        final Map<String, Object> statistics2 = getConnectionStatistics(connectionUrl);

        assertEquals("Unexpected value of statistic attribute localTransactionBegins", 3,
                     statistics2.get("localTransactionBegins"));
        assertEquals("Unexpected value of statistic attribute localTransactionRollbacks", 1,
                     statistics2.get("localTransactionRollbacks"));
        assertEquals("Unexpected value of statistic attribute localTransactionOpen", 1,
                     statistics2.get("localTransactionOpen"));

        _producer.send(_session.createMessage());
        consumer.close();
        _session.close();

        final Map<String, Object> statistics3 = getConnectionStatistics(connectionUrl);

        assertEquals("Unexpected value of statistic attribute localTransactionBegins", 3,
                     statistics3.get("localTransactionBegins"));
        assertEquals("Unexpected value of statistic attribute localTransactionRollbacks", 2,
                     statistics3.get("localTransactionRollbacks"));
        assertEquals("Unexpected value of statistic attribute localTransactionOpen", 0,
                     statistics3.get("localTransactionOpen"));
    }

    public void testConnectionMessageCountStatistics() throws Exception
    {
        _producer.send(_session.createTextMessage("Test-0"));
        _session.commit();

        MessageConsumer consumer = _session.createConsumer(_queue);
        Message m = consumer.receive(getReceiveTimeout());
        assertNotNull("First message was not received", m);
        _session.commit();

        _session.close();

        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _producer = _session.createProducer(_queue);
        _producer.send(_session.createTextMessage("Test-1"));
        consumer = _session.createConsumer(_queue);
        m = consumer.receive(getReceiveTimeout());
        assertNotNull("First message was not received", m);

        // close session to make sure message ack/disposition reaches the broker before rest request is made
        _session.close();

        String connectionUrl = getVirtualHostConnectionUrl();

        Map<String, Object> statistics = getConnectionStatistics(connectionUrl);
        assertTrue("Unexpected value of connection statistics attribute bytesIn",
                   Long.parseLong(String.valueOf(statistics.get("bytesIn"))) > 0);
        assertTrue("Unexpected value of connection statistics attribute bytesOut",
                   Long.parseLong(String.valueOf(statistics.get("bytesOut"))) > 0);
        assertEquals("Unexpected value of connection statistics attribute messagesIn", 2,
                     statistics.get("messagesIn"));
        assertEquals("Unexpected value of connection statistics attribute messagesOut",
                     2, statistics.get("messagesOut"));

        assertEquals("Unexpected value of statistic attribute transactedMessagesIn", 1,
                     statistics.get("transactedMessagesIn"));

        assertEquals("Unexpected value of statistic attribute transactedMessagesOut", 1,
                     statistics.get("transactedMessagesOut"));
    }

    public void testOldestTransactionStartTime() throws Exception
    {
        String connectionUrl = getVirtualHostConnectionUrl();

        _producer.send(_session.createTextMessage("Test"));

        Map<String, Object> statistics = getConnectionStatistics(connectionUrl);
        long oldestTransactionStartTime = ((Number) statistics.get("oldestTransactionStartTime")).longValue();
        assertTrue("Unexpected transaction oldestTransactionStartTime  for connection with work "
                   + oldestTransactionStartTime, oldestTransactionStartTime >= _startTime);

        _session.commit();
        statistics = getConnectionStatistics(connectionUrl);
        assertNull(String.format(
                "Unexpected transaction oldestTransactionStartTime %s for connection with no work",
                statistics.get("oldestTransactionStartTime")), statistics.get("oldestTransactionStartTime"));

        _producer.send(_session.createTextMessage("Test"));
        _session.close();

        statistics = getConnectionStatistics(connectionUrl);
        assertNull("Unexpected transaction oldestTransactionStartTime for connection with no session",
                   statistics.get("oldestTransactionStartTime"));
    }

    private String getConnectionName() throws IOException
    {
        List<Map<String, Object>> connections =
                getRestTestHelper().getJsonAsList("virtualhost/test/test/getConnections");
        assertEquals("Unexpected number of connections", 1, connections.size());
        Map<String, Object> connection = connections.get(0);
        String connectionName = (String) connection.get(Connection.NAME);
        return connectionName;
    }

    private String getVirtualHostConnectionUrl() throws IOException
    {
        String connectionName = getConnectionName();
        return "virtualhost/test/test/getConnection?name=" + getRestTestHelper().encodeAsUTF(connectionName);
    }

    private Map<String, Object> getConnectionStatistics(final String connectionUrl) throws IOException
    {
        Map<String, Object> connection = getRestTestHelper().getJsonAsMap(connectionUrl);
        return (Map<String, Object>) connection.get(Asserts.STATISTICS_ATTRIBUTE);
    }
}
