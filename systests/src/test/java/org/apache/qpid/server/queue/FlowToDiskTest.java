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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.QpidException;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class FlowToDiskTest extends QpidBrokerTestCase
{
    private Session _session;
    private Queue _queue;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        Connection connection = getConnection();
        connection.start();
        _session = connection.createSession(true, Session.SESSION_TRANSACTED);

        _queue = createQueue();
    }

    private Queue createQueue() throws QpidException, JMSException
    {
        final Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, OverflowPolicy.FLOW_TO_DISK.name());
        arguments.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 0L);
        createEntityUsingAmqpManagement(getTestQueueName(), _session, "org.apache.qpid.Queue", arguments);
        return getQueueFromName(_session, getTestQueueName());
    }

    public void testRoundtripWithFlowToDisk() throws Exception
    {
        Map<String, Object> statistics = (Map<String, Object>) performOperationUsingAmqpManagement("test", "getStatistics", _session, "org.apache.qpid.VirtualHost", Collections.singletonMap("statistics", Collections.singletonList("bytesEvacuatedFromMemory")));
        Long originalBytesEvacuatedFromMemory = (Long) statistics.get("bytesEvacuatedFromMemory");

        TextMessage message = _session.createTextMessage("testMessage");
        MessageProducer producer = _session.createProducer(_queue);
        producer.send(message);
        _session.commit();

        // make sure we are flowing to disk
        Map<String, Object> statistics2 = (Map<String, Object>) performOperationUsingAmqpManagement("test", "getStatistics", _session, "org.apache.qpid.VirtualHost", Collections.singletonMap("statistics", Collections.singletonList("bytesEvacuatedFromMemory")));
        Long bytesEvacuatedFromMemory = (Long) statistics2.get("bytesEvacuatedFromMemory");
        assertTrue("Message was not evacuated from memory", bytesEvacuatedFromMemory > originalBytesEvacuatedFromMemory);

        MessageConsumer consumer = _session.createConsumer(_queue);
        Message receivedMessage = consumer.receive(getReceiveTimeout());
        assertNotNull("Did not receive message", receivedMessage);
        assertThat("Unexpected message type", receivedMessage, is(instanceOf(TextMessage.class)));
        assertEquals("Unexpected message content", message.getText(), ((TextMessage) receivedMessage).getText());
    }
}
