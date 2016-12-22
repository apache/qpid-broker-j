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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;

public class QueueRestTest extends QpidRestTestCase
{
    private static final String QUEUE_ATTRIBUTE_CONSUMERS = "consumers";

    /**
     * Message number to publish into queue
     */
    private static final int MESSAGE_NUMBER = 2;
    private static final int MESSAGE_PAYLOAD_SIZE = 6;
    private static final int ENQUEUED_MESSAGES = 1;
    private static final int DEQUEUED_MESSAGES = 1;

    private Connection _connection;

    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        Session session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination queue = createTestQueue(session);
        session.commit();
        MessageConsumer consumer = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MESSAGE_NUMBER; i++)
        {
            producer.send(session.createTextMessage("Test-" + i));
        }
        session.commit();
        _connection.start();
        Message m = consumer.receive(1000l);
        assertNotNull("Message is not received", m);
        session.commit();
    }

    public void testGetVirtualHostQueues() throws Exception
    {
        String queueName = getTestQueueName();
        List<Map<String, Object>> queues = getRestTestHelper().getJsonAsList("queue/test");
        assertEquals("Unexpected number of queues", 1, queues.size());
        String[] expectedQueues = new String[]{queueName};

        for (String name : expectedQueues)
        {
            Map<String, Object> queueDetails = getRestTestHelper().find(Queue.NAME, name, queues);
            Asserts.assertQueue(name, "standard", queueDetails);
        }
    }

    public void testGetByName() throws Exception
    {
        String queueName = getTestQueueName();
        Map<String, Object> queueDetails = getRestTestHelper().getJsonAsSingletonList("queue/test/test/" + queueName);
        Asserts.assertQueue(queueName, "standard", queueDetails);
        assertStatistics(queueDetails);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> consumers = (List<Map<String, Object>>) queueDetails.get(QUEUE_ATTRIBUTE_CONSUMERS);
        assertNotNull("Queue consumers are not found", consumers);
        assertEquals("Unexpected number of consumers", 1, consumers.size());
        assertConsumer(consumers.get(0));
    }

    public void testUpdateQueue() throws Exception
    {
        String queueName = getTestName();

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, queueName);

        String queueUrl = "queue/test/test/" + queueName;
        int responseCode =  getRestTestHelper().submitRequest(queueUrl, "PUT", attributes);
        assertEquals("Queue was not created", 201, responseCode);

        Map<String, Object> queueDetails = getRestTestHelper().getJsonAsSingletonList(queueUrl);
        Asserts.assertQueue(queueName, "standard", queueDetails);

        attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, queueName);
        attributes.put(Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES, 100000);
        attributes.put(Queue.QUEUE_FLOW_RESUME_SIZE_BYTES, 80000);
        attributes.put(Queue.ALERT_REPEAT_GAP, 10000);
        attributes.put(Queue.ALERT_THRESHOLD_MESSAGE_AGE, 20000);
        attributes.put(Queue.ALERT_THRESHOLD_MESSAGE_SIZE, 30000);
        attributes.put(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES, 40000);
        attributes.put(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 50000);
        attributes.put(Queue.MAXIMUM_DELIVERY_ATTEMPTS, 10);

        responseCode = getRestTestHelper().submitRequest(queueUrl, "PUT", attributes);
        assertEquals("Setting of queue attributes should be allowed", 200, responseCode);

        Map<String, Object> queueData = getRestTestHelper().getJsonAsSingletonList(queueUrl);
        assertEquals("Unexpected " + Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES, 100000, queueData.get(Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES) );
        assertEquals("Unexpected " + Queue.QUEUE_FLOW_RESUME_SIZE_BYTES, 80000, queueData.get(Queue.QUEUE_FLOW_RESUME_SIZE_BYTES) );
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
        assertEquals("Unexpected queue statistics attribute " + "totalDequeuedBytes", isBroker10() ? 11 : MESSAGE_PAYLOAD_SIZE,
                statistics.get("totalDequeuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "persistentDequeuedBytes", isBroker10() ? 11 : MESSAGE_PAYLOAD_SIZE,
                statistics.get("totalDequeuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "persistentEnqueuedBytes", isBroker10() ? 22 : 2*MESSAGE_PAYLOAD_SIZE,
                     statistics.get("persistentEnqueuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "totalEnqueuedBytes", isBroker10() ? 22 : 2*MESSAGE_PAYLOAD_SIZE,
                statistics.get("totalEnqueuedBytes"));
        assertEquals("Unexpected queue statistics attribute " + "queueDepthBytes", isBroker10() ? 11 : MESSAGE_PAYLOAD_SIZE,
                statistics.get("queueDepthBytes"));
    }
}
