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
package org.apache.qpid.systest.management.amqp;

import static org.apache.qpid.server.model.Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.queue.PriorityQueue;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class AmqpManagementTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private Session _session;
    private Queue _queue;
    private Queue _replyAddress;
    private Queue _replyConsumer;
    private MessageConsumer _consumer;
    private MessageProducer _producer;

    private void setupSession() throws Exception
    {
        _connection.start();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if(isBroker10())
        {
            _queue = _session.createQueue("$management");
            _replyAddress = _session.createTemporaryQueue();
            _replyConsumer = _replyAddress;
        }
        else
        {
            _queue = _session.createQueue("ADDR:$management");
            _replyAddress = _session.createQueue("ADDR:!response");
            _replyConsumer = _session.createQueue(
                    "ADDR:$management ; {assert : never, node: { type: queue }, link:{name: \"!response\"}}");
        }
        _consumer = _session.createConsumer(_replyConsumer);
        _producer = _session.createProducer(_queue);
    }

    private void setupBrokerManagementConnection() throws Exception
    {
        ConnectionFactory management =
                isBroker10() ? getConnectionFactory("default", "$management", UUID.randomUUID().toString())
                        : getConnectionFactory("management");

        _connection = management.createConnection(GUEST_USERNAME, GUEST_PASSWORD);
        setupSession();
    }

    private void setupVirtualHostManagementConnection() throws Exception
    {
        _connection = getConnection();
        setupSession();
    }

    // test get types on $management
    public void testGetTypesOnBrokerManagement() throws Exception
    {
        setupBrokerManagementConnection();

        Message message = _session.createBytesMessage();

        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "GET-TYPES");

        message.setJMSReplyTo(_replyAddress);

        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertEquals("The correlation id does not match the sent message's messageId", message.getJMSMessageID(), responseMessage.getJMSCorrelationID());
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));

        checkResponseIsMapType(responseMessage);
        assertNotNull("The response did not include the org.amqp.Management type", getValueFromMapResponse(responseMessage, "org.amqp.management"));
        assertNotNull("The response did not include the org.apache.qpid.Port type", getValueFromMapResponse(responseMessage, "org.apache.qpid.Port"));

    }

    private void checkResponseIsMapType(final Message responseMessage) throws JMSException
    {
        if (isBroker10())
        {
            assertTrue("The response was not an Object Message", responseMessage instanceof ObjectMessage);
            assertTrue("The Object Message did not contain a Map",
                       ((ObjectMessage) responseMessage).getObject() instanceof Map);
        }
        else
        {
            assertTrue("The response was not a MapMessage", responseMessage instanceof MapMessage);
        }
    }

    Object getValueFromMapResponse(final Message responseMessage, String name) throws JMSException
    {
        if (isBroker10())
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).get(name);
        }
        else
        {
            return ((MapMessage) responseMessage).getObject(name);
        }
    }

    Collection getMapResponseKeys(final Message responseMessage) throws JMSException
    {
        if (isBroker10())
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).keySet();
        }
        else
        {
            return Collections.list(((MapMessage) responseMessage).getMapNames());
        }
    }

    // test get types on $management
    public void testQueryBrokerManagement() throws Exception
    {
        setupBrokerManagementConnection();

        MapMessage message = _session.createMapMessage();

        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setObject("attributeNames", new ArrayList<>());
        message.setJMSReplyTo(_replyAddress);

        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertEquals("The correlation id does not match the sent message's messageId", message.getJMSMessageID(), responseMessage.getJMSCorrelationID());
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        ArrayList resultMessageKeys = new ArrayList(getMapResponseKeys(responseMessage));
        assertEquals("The response map has two entries", 2, resultMessageKeys.size());
        assertTrue("The response map does not contain attribute names", resultMessageKeys.contains("attributeNames"));
        assertTrue("The response map does not contain results ", resultMessageKeys.contains("results"));
        Object attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
        assertTrue("The attribute names are not a list", attributeNames instanceof Collection);
        Collection attributeNamesCollection = (Collection)attributeNames;
        assertTrue("The attribute names do not contain identity", attributeNamesCollection.contains("identity"));
        assertTrue("The attribute names do not contain name", attributeNamesCollection.contains("name"));

        assertTrue("The attribute names do not contain qpid-type", attributeNamesCollection.contains("qpid-type"));

        // Now test filtering by type
        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setStringProperty("entityType", "org.apache.qpid.Exchange");

        message.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertEquals("The correlation id does not match the sent message's messageId", message.getJMSMessageID(), responseMessage.getJMSCorrelationID());
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        resultMessageKeys = new ArrayList(getMapResponseKeys(responseMessage));
        assertEquals("The response map has two entries", 2, resultMessageKeys.size());
        assertTrue("The response map does not contain attribute names", resultMessageKeys.contains("attributeNames"));
        assertTrue("The response map does not contain results ", resultMessageKeys.contains("results"));
        attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
        assertTrue("The attribute names are not a list", attributeNames instanceof Collection);
        attributeNamesCollection = (Collection)attributeNames;
        assertEquals("The attributeNames are no as expected", Arrays.asList("name", "identity", "type"), attributeNamesCollection);
        Object resultsObject = ((MapMessage) responseMessage).getObject("results");
        assertTrue("results is not a collection", resultsObject instanceof Collection);
        Collection results = (Collection)resultsObject;

        final int numberOfExchanges = results.size();
        assertTrue("results should have at least 4 elements", numberOfExchanges >= 4);

        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setStringProperty("entityType", "org.apache.qpid.DirectExchange");

        message.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        final Collection directExchanges = (Collection) getValueFromMapResponse(responseMessage, "results");
        assertTrue("There are the same number of results when searching for direct exchanges as when searching for all exchanges", directExchanges.size() < numberOfExchanges);
        assertTrue("The list of direct exchanges is not a proper subset of the list of all exchanges", results.containsAll(directExchanges));
    }


    // test get types on a virtual host
    public void testGetTypesOnVhostManagement() throws Exception
    {
        setupVirtualHostManagementConnection();

        Message message = _session.createBytesMessage();

        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "GET-TYPES");
        byte[] correlationID = "some correlation id".getBytes();
        message.setJMSCorrelationIDAsBytes(correlationID);

        message.setJMSReplyTo(_replyAddress);

        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The correlation id does not match the sent message's correlationId", Arrays.equals(correlationID, responseMessage.getJMSCorrelationIDAsBytes()));

        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        assertTrue("The response was not a MapMessage", responseMessage instanceof MapMessage);
        assertNotNull("The response did not include the org.amqp.Management type",
                      ((MapMessage) responseMessage).getObject("org.amqp.management"));
        assertNull("The response included the org.apache.qpid.Port type",
                   ((MapMessage) responseMessage).getObject("org.apache.qpid.Port"));



    }

    // create / update / read / delete a queue via $management
    public void testCreateQueueOnBrokerManagement() throws Exception
    {
        setupBrokerManagementConnection();

        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 100L);
        String path = "test/test/" + getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 201, responseMessage.getIntProperty("statusCode"));
        assertTrue("The response was not a MapMessage", responseMessage instanceof MapMessage);
        assertEquals("The created queue was not a standard queue", "org.apache.qpid.StandardQueue", ((MapMessage)responseMessage).getString("type"));
        assertEquals("The created queue was not a standard queue", "standard", ((MapMessage)responseMessage).getString("qpid-type"));
        assertEquals("the created queue did not have the correct alerting threshold", 100L, ((MapMessage)responseMessage).getLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES));
        Object identity = ((MapMessage) responseMessage).getObject("identity");

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "UPDATE");
        message.setObjectProperty("identity", identity);
        message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        assertTrue("The response was not a MapMessage", responseMessage instanceof MapMessage);
        assertEquals("the created queue did not have the correct alerting threshold", 250L, ((MapMessage)responseMessage).getLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES));

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "DELETE");
        message.setObjectProperty("index", "object-path");
        message.setObjectProperty("key", path);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 204, responseMessage.getIntProperty("statusCode"));

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "READ");
        message.setObjectProperty("identity", identity);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate not found", 404, responseMessage.getIntProperty("statusCode"));

    }
    // create / update / read / delete a queue via vhost

    public void testCreateQueueOnVhostManagement() throws Exception
    {
        setupVirtualHostManagementConnection();

        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        message.setInt(PriorityQueue.PRIORITIES, 13);
        String path = getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 201, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        assertEquals("The created queue was not a priority queue", "org.apache.qpid.PriorityQueue", getValueFromMapResponse(responseMessage, "type"));
        assertEquals("The created queue was not a standard queue", "priority", getValueFromMapResponse(responseMessage, "qpid-type"));
        assertEquals("the created queue did not have the correct number of priorities", 13, Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString()).intValue());
        Object identity = getValueFromMapResponse(responseMessage, "identity");

        // Trying to create a second queue with the same name should cause a conflict
        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        message.setInt(PriorityQueue.PRIORITIES, 7);
        message.setString("object-path", getTestName());
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate conflict", 409, responseMessage.getIntProperty("statusCode"));

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "READ");
        message.setObjectProperty("identity", identity);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        assertEquals("the queue did not have the correct number of priorities", 13, Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString()).intValue());
        assertEquals("the queue did not have the expected path", getTestName(), getValueFromMapResponse(responseMessage, "object-path"));


        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "UPDATE");
        message.setObjectProperty("identity", identity);
        message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        assertEquals("The updated queue did not have the correct alerting threshold", 250L, Long.valueOf(getValueFromMapResponse(responseMessage, ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES).toString()).longValue());


        message = _session.createMapMessage();
        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "DELETE");
        message.setObjectProperty("index", "object-path");
        message.setObjectProperty("key", path);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 204, responseMessage.getIntProperty("statusCode"));

        message = _session.createMapMessage();
        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "DELETE");
        message.setObjectProperty("index", "object-path");
        message.setObjectProperty("key", path);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate not found", 404, responseMessage.getIntProperty("statusCode"));
    }

    // read virtual host from virtual host management
    public void testReadVirtualHost() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.VirtualHost");
        message.setStringProperty("operation", "READ");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", "");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 200, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        assertEquals("The name of the virtual host is not as expected", "test", getValueFromMapResponse(responseMessage, "name"));
    }

    // create a virtual host from $management
    public void testCreateVirtualHost() throws Exception
    {
        setupBrokerManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.JsonVirtualHostNode");
        message.setStringProperty("operation", "CREATE");
        String virtualHostName = "newMemoryVirtualHost";
        message.setString("name", virtualHostName);
        message.setString(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, "{ \"type\" : \"Memory\" }");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 201, responseMessage.getIntProperty("statusCode"));
        _connection.close();
        _connection = getConnectionForVHost("/"+virtualHostName);
        setupSession();

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.VirtualHost");
        message.setStringProperty("operation", "READ");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", "");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 200, responseMessage.getIntProperty("statusCode"));
        assertTrue("The response was not a MapMessage", responseMessage instanceof MapMessage);
        assertEquals("The name of the virtual host is not as expected", virtualHostName, ((MapMessage)responseMessage).getString("name"));
        assertEquals("The type of the virtual host is not as expected", "Memory", ((MapMessage)responseMessage).getString("qpid-type"));


    }
    // attempt to delete the virtual host via the virtual host
    public void testDeleteVirtualHost() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.VirtualHost");
        message.setStringProperty("operation", "DELETE");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", "");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 501, responseMessage.getIntProperty("statusCode"));
    }

    // create a queue with the qpid type
    public void testCreateQueueWithQpidType() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        message.setString("qpid-type", "lvq");
        String path = getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 201, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        assertEquals("The created queue did not have the correct type", "org.apache.qpid.LastValueQueue", getValueFromMapResponse(responseMessage, "type"));
    }

    // create a queue using the AMQP type
    public void testCreateQueueWithAmqpType() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.SortedQueue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        String path = getTestName();
        message.setString("object-path", path);
        message.setString("sortKey", "foo");
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 201, responseMessage.getIntProperty("statusCode"));
        checkResponseIsMapType(responseMessage);
        assertEquals("The created queue did not have the correct type", "sorted", getValueFromMapResponse(responseMessage, "qpid-type"));
    }

    // attempt to create an exchange without a type
    public void testCreateExchangeWithoutType() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Exchange");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        String path = getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("Incorrect response code", 400, responseMessage.getIntProperty("statusCode"));
    }



    // attempt to create a connection
    public void testCreateConnectionOnVhostManagement() throws Exception
    {
        setupVirtualHostManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Connection");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        String path = getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate not implemented", 501, responseMessage.getIntProperty("statusCode"));
    }

    public void testCreateConnectionOnBrokerManagement() throws Exception
    {
        setupBrokerManagementConnection();
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Connection");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", getTestName());
        String path = getTestName();
        message.setString("object-path", path);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate not implemented", 501, responseMessage.getIntProperty("statusCode"));
    }

    // create a binding
    public void testCreateBindingOnVhostManagement() throws Exception
    {
        setupVirtualHostManagementConnection();
        String exchangeName = getTestName() + "_Exchange";
        String queueName = getTestName() + "_Queue";
        String exchangePath = exchangeName;
        String queuePath = queueName;

        doTestCreateBinding(exchangeName, queueName, exchangePath, queuePath);

    }

    public void testCreateBindingOnBrokerManagement() throws Exception
    {
        setupBrokerManagementConnection();
        String exchangeName = getTestName() + "_Exchange";
        String queueName = getTestName() + "_Queue";
        String exchangePath = "test/test/"+exchangeName;
        String queuePath = "test/test/"+exchangeName;

        doTestCreateBinding(exchangeName, queueName, exchangePath, queuePath);

    }

    private void doTestCreateBinding(final String exchangeName,
                                     final String queueName,
                                     final String exchangePath,
                                     final String queuePath) throws JMSException
    {
        MapMessage message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Queue");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", queueName);
        message.setString("object-path", queuePath);
        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        Message responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 201, responseMessage.getIntProperty("statusCode"));

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.FanoutExchange");
        message.setStringProperty("operation", "CREATE");
        message.setString("name", exchangeName);
        message.setString("object-path", exchangePath);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 201, responseMessage.getIntProperty("statusCode"));

        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Binding");
        message.setStringProperty("operation", "CREATE");
        message.setString("name",  "binding1");
        message.setString("object-path", exchangePath + "/" + queueName + "/binding1");

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 201, responseMessage.getIntProperty("statusCode"));

        // use an operation to bind
        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Exchange");
        message.setStringProperty("operation", "bind");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", exchangePath);
        message.setStringProperty("bindingKey",  "binding2");
        message.setStringProperty("queue", queueName);

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));

        // read the new binding
        message = _session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.Binding");
        message.setStringProperty("operation", "READ");
        message.setStringProperty("index",  "object-path");
        message.setStringProperty("key", exchangePath + "/" + queueName + "/binding2");

        message.setJMSReplyTo(_replyAddress);
        _producer.send(message);

        responseMessage = _consumer.receive(getReceiveTimeout());
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success", 200, responseMessage.getIntProperty("statusCode"));
    }



}
