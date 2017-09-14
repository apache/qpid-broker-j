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

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;

public class QueueRestTest extends QpidRestTestCase
{

    /**
     * Message number to publish into queue
     */
    private static final int MESSAGE_NUMBER = 2;
    private static final int MESSAGE_PAYLOAD_SIZE = 6;
    private static final int MESSAGE_SIZE_OVERHEAD_0_9 = 160;
    private static final int MESSAGE_SIZE_OVERHEAD_0_10 = 123;
    private static final int MESSAGE_SIZE_OVERHEAD_1_0 = 181;
    private static final int ENQUEUED_MESSAGES = 1;
    private static final int DEQUEUED_MESSAGES = 1;

    private Connection _connection;
    private Session _session;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination queue = createTestQueue(_session);
        _session.commit();
        MessageConsumer consumer = _session.createConsumer(queue);
        MessageProducer producer = _session.createProducer(queue);

        for (int i = 0; i < MESSAGE_NUMBER; i++)
        {
            producer.send(_session.createTextMessage("Test-" + i));
        }
        _session.commit();
        _connection.start();
        Message m = consumer.receive(getReceiveTimeout());
        assertNotNull("Message is not received", m);
        _session.commit();
    }

    public void testGetByName() throws Exception
    {
        String queueName = getTestQueueName();

        Map<String, Object> queueDetails =
                getRestTestHelper().getJsonAsMap(String.format("queue/test/test/%s", queueName));
        Asserts.assertQueue(queueName, "standard", queueDetails);
    }

    public void testGetByNameAsList() throws Exception
    {
        String queueName = getTestQueueName();

        Map<String, Object> queueDetails =
                getRestTestHelper().getJsonAsSingletonList(String.format("queue/test/test/%s?singletonModelObjectResponseAsList=true", queueName));
        Asserts.assertQueue(queueName, "standard", queueDetails);
    }

    public void testGetById() throws Exception
    {
        String queueName = getTestQueueName();
        Map<String, Object> queueDetails =
                getRestTestHelper().getJsonAsMap(String.format("queue/test/test/%s", queueName));
        String queueId = (String) queueDetails.get(Queue.ID);

        List<Map<String, Object>> filteredResults = getRestTestHelper().getJsonAsList("queue/test/test?id=" + queueId);
        assertEquals("Unexpected number of queues when querying by id filter", 1, filteredResults.size());
    }

    public void testGetByName_NotFound() throws Exception
    {
        String queueName = "notfound";
        getRestTestHelper().submitRequest(String.format("queue/test/test/%s", queueName), "GET", SC_NOT_FOUND);
    }

    public void testGetAllQueues() throws Exception
    {
        List<Map<String, Object>> brokerQueues = getRestTestHelper().getJsonAsList("queue");
        assertEquals("Unexpected number of queues queried broker-wide", 1, brokerQueues.size());

        List<Map<String, Object>> vhnQueues = getRestTestHelper().getJsonAsList("queue/test");
        assertEquals("Unexpected number of queues queried virtualhostnode scoped", 1, vhnQueues.size());

        List<Map<String, Object>> vhQueues = getRestTestHelper().getJsonAsList("queue/test/test");
        assertEquals("Unexpected number of queues queried virtualhost scoped", 1, vhQueues.size());

        TemporaryQueue queue = _session.createTemporaryQueue();
        queue.getQueueName();

        vhQueues = getRestTestHelper().getJsonAsList("queue/test/test");
        assertEquals("Unexpected number of queues", 2, vhQueues.size());
    }

    public void testGetAllQueuesWildcards() throws Exception
    {
        List<Map<String, Object>> vhnQueues = getRestTestHelper().getJsonAsList("queue/*");
        assertEquals("Unexpected number of queues queried virtualhostnode scoped", 1, vhnQueues.size());

        getRestTestHelper().submitRequest("queue/*/test", "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    public void testGetAll_NotFound() throws Exception
    {
        List<Map<String, Object>> vhn2Queues = getRestTestHelper().getJsonAsList(String.format("queue/%s", TEST2_VIRTUALHOST));
        assertEquals("Unexpected number of queues on vhn : " + TEST2_VIRTUALHOST  , 0, vhn2Queues.size());

        List<Map<String, Object>> vh2Queues = getRestTestHelper().getJsonAsList(String.format("queue/%s/%s", TEST2_VIRTUALHOST, TEST2_VIRTUALHOST));
        assertEquals("Unexpected number of queues on vhn " + TEST2_VIRTUALHOST, 0, vh2Queues.size());
    }

    public void testUpdateQueue() throws Exception
    {
        String queueName = getTestName();

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, queueName);

        String queueUrl = "queue/test/test/" + queueName;
        getRestTestHelper().submitRequest(queueUrl, "PUT", attributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> queueDetails = getRestTestHelper().getJsonAsMap(queueUrl);
        Asserts.assertQueue(queueName, "standard", queueDetails);

        attributes = new HashMap<>();
        attributes.put(Queue.NAME, queueName);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000);
        attributes.put(Queue.ALERT_REPEAT_GAP, 10000);
        attributes.put(Queue.ALERT_THRESHOLD_MESSAGE_AGE, 20000);
        attributes.put(Queue.ALERT_THRESHOLD_MESSAGE_SIZE, 30000);
        attributes.put(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES, 40000);
        attributes.put(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 50000);
        attributes.put(Queue.MAXIMUM_DELIVERY_ATTEMPTS, 10);

        getRestTestHelper().submitRequest(queueUrl, "PUT", attributes, HttpServletResponse.SC_OK);

        Map<String, Object> queueData = getRestTestHelper().getJson(queueUrl, Map.class);
        assertEquals("Unexpected " + Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000, queueData.get(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES));
        assertEquals("Unexpected " + Queue.ALERT_REPEAT_GAP, 10000, queueData.get(Queue.ALERT_REPEAT_GAP) );
        assertEquals("Unexpected " + Queue.ALERT_THRESHOLD_MESSAGE_AGE, 20000, queueData.get(Queue.ALERT_THRESHOLD_MESSAGE_AGE) );
        assertEquals("Unexpected " + Queue.ALERT_THRESHOLD_MESSAGE_SIZE, 30000, queueData.get(Queue.ALERT_THRESHOLD_MESSAGE_SIZE) );
        assertEquals("Unexpected " + Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES, 40000, queueData.get(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES) );
        assertEquals("Unexpected " + Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 50000, queueData.get(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES) );
    }

    private void assertConsumer(Map<String, Object> consumer)
    {
        assertNotNull("Consumer map should not be null", consumer);
        Asserts.assertAttributesPresent(consumer,
                                        BrokerModel.getInstance().getTypeRegistry().getAttributeNames(Consumer.class), Consumer.STATE,
                Consumer.SETTLEMENT_MODE, Consumer.EXCLUSIVE, Consumer.SELECTOR,
                Consumer.NO_LOCAL,
                ConfiguredObject.TYPE,
                ConfiguredObject.CREATED_BY,
                ConfiguredObject.CREATED_TIME,
                ConfiguredObject.LAST_UPDATED_BY,
                ConfiguredObject.LAST_UPDATED_TIME,
                ConfiguredObject.DESCRIPTION,
                ConfiguredObject.CONTEXT,
                ConfiguredObject.DESIRED_STATE);

        assertEquals("Unexpected consumer attribute " + Consumer.DURABLE, Boolean.FALSE, consumer.get(Consumer.DURABLE));
        assertEquals("Unexpected consumer attribute " + Consumer.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_SESSION_END.name(),
                consumer.get(Consumer.LIFETIME_POLICY));
        assertEquals("Unexpected consumer attribute " + Consumer.DISTRIBUTION_MODE, "MOVE",
                consumer.get(Consumer.DISTRIBUTION_MODE));

        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) consumer.get(Asserts.STATISTICS_ATTRIBUTE);
        assertNotNull("Consumer statistics is not present", statistics);
        Asserts.assertAttributesPresent(statistics, "bytesOut", "messagesOut", "unacknowledgedBytes", "unacknowledgedMessages");
    }

    private void assertStatistics(Map<String, Object> queueDetails)
    {
        final int messageSize;
        if (isBrokerPre010())
        {
            messageSize = MESSAGE_SIZE_OVERHEAD_0_9 + MESSAGE_PAYLOAD_SIZE;
        }
        else if (isBroker010())
        {
            messageSize = MESSAGE_SIZE_OVERHEAD_0_10 + MESSAGE_PAYLOAD_SIZE;
        }
        else if (isBroker10())
        {
            messageSize = MESSAGE_SIZE_OVERHEAD_1_0 + MESSAGE_PAYLOAD_SIZE;
        }
        else
        {
            fail("unexpected AMQP version");
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) queueDetails.get(Asserts.STATISTICS_ATTRIBUTE);
        assertEquals("Unexpected queue statistics attribute " + "persistentDequeuedMessages", DEQUEUED_MESSAGES,
                statistics.get("persistentDequeuedMessages"));
        assertEquals("Unexpected queue statistics attribute " + "queueDepthMessages", ENQUEUED_MESSAGES,
                statistics.get("queueDepthMessages"));
        assertEquals("Unexpected queue statistics attribute " + "consumerCount", 1,
                statistics.get("consumerCount"));
        assertEquals("Unexpected queue statistics attribute " + "consumerCountWithCredit", 1,
                statistics.get("consumerCountWithCredit"));
        assertEquals("Unexpected queue statistics attribute " + "bindingCount", isBroker10() ? 0 : 1, statistics.get("bindingCount"));
        assertEquals("Unexpected queue statistics attribute " + "persistentDequeuedMessages", DEQUEUED_MESSAGES,
                statistics.get("persistentDequeuedMessages"));
        assertEquals("Unexpected queue statistics attribute " + "totalDequeuedMessages", DEQUEUED_MESSAGES,
                statistics.get("totalDequeuedMessages"));
        assertEquals("Unexpected queue statistics attribute " + "totalDequeuedBytes", messageSize,
                statistics.get("totalDequeuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "persistentDequeuedBytes", messageSize,
                statistics.get("totalDequeuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "persistentEnqueuedBytes", 2 * messageSize,
                     statistics.get("persistentEnqueuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "totalEnqueuedBytes", 2 * messageSize,
                statistics.get("totalEnqueuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "queueDepthBytes", messageSize,
                statistics.get("queueDepthBytes"));
    }
}
