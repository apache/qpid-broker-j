/*
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

package org.apache.qpid.server.queue;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.client.RejectBehaviour;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class LiveQueueOperationsTest extends QpidBrokerTestCase
{

    private static final int MAX_DELIVERY_COUNT = 2;

    @Override
    protected void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        setTestSystemProperty("queue.deadLetterQueueEnabled","true");
        setTestSystemProperty("queue.maximumDeliveryAttempts", String.valueOf(MAX_DELIVERY_COUNT));

        // Set client-side flag to allow the server to determine if messages
        // dead-lettered or requeued.
        if (!isBroker010())
        {
            setTestClientSystemProperty(ClientProperties.REJECT_BEHAVIOUR_PROP_NAME, RejectBehaviour.SERVER.toString());
        }

        super.setUp();
    }

    public void testClearQueueOperationWithActiveConsumerDlqAll() throws Exception
    {
        final String virtualHostName = TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST;
        RestTestHelper restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        Connection conn = getConnection();
        conn.start();
        final Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        final Queue queue = createTestQueue(session);
        session.createConsumer(queue).close();

        sendMessage(session, queue, 250);

        MessageConsumer consumer = session.createConsumer(queue);

        final CountDownLatch clearQueueLatch = new CountDownLatch(10);
        final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
        consumer.setMessageListener(new MessageListener()
        {
            @Override
            public void onMessage(final Message message)
            {
                try
                {
                    clearQueueLatch.countDown();
                    session.rollback();
                }
                catch (Throwable t)
                {
                    throwableAtomicReference.set(t);
                }
            }
        });


        boolean ready = clearQueueLatch.await(30, TimeUnit.SECONDS);
        assertTrue("Consumer did not reach expected point within timeout", ready);

        final String queueUrl = "queue/" + virtualHostName + "/" + virtualHostName + "/" + queue.getQueueName();

        String clearOperationUrl = queueUrl + "/clearQueue";
        restTestHelper.submitRequest(clearOperationUrl, "POST", Collections.<String,Object>emptyMap(), 200);

        int queueDepthMessages = 0;
        for (int i = 0; i < 20; ++i)
        {
            Map<String, Object> statistics = getStatistics(restTestHelper, queueUrl);
            queueDepthMessages = (int) statistics.get("queueDepthMessages");
            if (queueDepthMessages == 0)
            {
                break;
            }
            Thread.sleep(250);
        }
        assertEquals("Queue depth did not reach 0 within expected time", 0, queueDepthMessages);

        consumer.close();

        Map<String, Object> statistics = getStatistics(restTestHelper, queueUrl);
        queueDepthMessages = (int) statistics.get("queueDepthMessages");
        assertEquals("Unexpected queue depth after consumer close", 0, queueDepthMessages);

        assertNull("Unexpected exception thrown", throwableAtomicReference.get());
    }

    private Map<String, Object> getStatistics(final RestTestHelper restTestHelper, final String objectUrl) throws Exception
    {
        Map<String, Object> object = restTestHelper.getJsonAsSingletonList(objectUrl);
        return (Map<String, Object>) object.get("statistics");
    }
}
