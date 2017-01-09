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
package org.apache.qpid.test.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.server.logging.AbstractTestLogging;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class ProducerFlowControlTest extends AbstractTestLogging
{
    private static final long TIMEOUT = 5000;
    private Queue _queue;
    private Connection _producerConnection;
    private Session _producerSession;
    private RestTestHelper _restTestHelper;
    private String _queueUrl;
    private MessageProducer _producer;
    private BytesMessage _message;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();

        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        setTestClientSystemProperty("qpid.flow_control_wait_failure","3000");
        setTestClientSystemProperty("qpid.flow_control_wait_notify_period","1000");

        _producerConnection = getConnectionWithSyncPublishing();
        _producerSession = _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String queueName = getTestQueueName();
        _queueUrl = String.format("queue/%1$s/%1$s/%2$s", TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST, queueName);
        _queue = createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 800);

        _producer = _producerSession.createProducer(_queue);
        _message = _producerSession.createBytesMessage();
        _message.writeBytes(new byte[1100]);

        _monitor.markDiscardPoint();
    }

    public void testClientLogMessages() throws Exception
    {
        _producer.send(_message);
        _restTestHelper.waitForAttributeChanged(_queueUrl, org.apache.qpid.server.model.Queue.QUEUE_FLOW_STOPPED, Boolean.TRUE);
        ((AMQSession<?, ?>) _producerSession).sync();  // Ensure that the client has processed the flow control.
        try
        {
            _producer.send(_message);
            fail("Producer should be blocked by flow control");
        }
        catch (JMSException e)
        {
            final String expectedMsg = isBroker010() ? "Exception when sending message:timed out waiting for message credit"
                    : "Unable to send message for 3 seconds due to broker enforced flow control";
            assertEquals("Unexpected exception reason", expectedMsg, e.getMessage());
        }

        List<String> results = waitAndFindMatches("Message send delayed by", TIMEOUT);
        assertTrue("No delay messages logged by client",results.size()!=0);

        List<String> failedMessages = waitAndFindMatches("Message send failed due to timeout waiting on broker enforced"
                                                         + " flow control", TIMEOUT);
        assertEquals("Incorrect number of send failure messages logged by client (got " + results.size() + " delay "
                     + "messages)",1,failedMessages.size());
    }


    private Queue createAndBindQueueWithFlowControlEnabled(Session session,
                                                           String queueName,
                                                           int capacity,
                                                           int resumeCapacity) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("x-qpid-capacity", capacity);
        arguments.put("x-qpid-flow-resume-capacity", resumeCapacity);
        ((AMQSession<?, ?>) session).createQueue(queueName, true, false, false, arguments);
        Queue queue = session.createQueue("direct://amq.direct/"
                                    + queueName
                                    + "/"
                                    + queueName
                                    + "?durable='"
                                    + false
                                    + "'&autodelete='"
                                    + true
                                    + "'");
        ((AMQSession<?, ?>) session).declareAndBind((AMQDestination) queue);
        return queue;
    }
}

