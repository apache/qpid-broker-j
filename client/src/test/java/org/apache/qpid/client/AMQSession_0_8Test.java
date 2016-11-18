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
package org.apache.qpid.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.message.UnprocessedMessage;
import org.apache.qpid.client.transport.TestNetworkConnection;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.AMQBody;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicConsumeOkBody;
import org.apache.qpid.framing.ChannelFlowOkBody;
import org.apache.qpid.framing.ExchangeDeclareOkBody;
import org.apache.qpid.framing.QueueDeclareOkBody;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.url.AMQBindingURL;

public class AMQSession_0_8Test extends QpidTestCase
{
    private AMQConnection _connection;

    public void setUp() throws Exception
    {
        super.setUp();
        _connection = new MockAMQConnection("amqp://guest:guest@/test?brokerlist='tcp://localhost:5672'");
        NetworkConnection network = new TestNetworkConnection();
        _connection.getProtocolHandler().setNetworkConnection(network);
    }

    public void testQueueNameIsGeneratedOnDeclareQueueWithEmptyQueueName() throws Exception
    {
        final String testQueueName = "tmp_127_0_0_1_1_1";

        _connection.setConnectionListener(new MockReceiveConnectionListener(_connection, 1,
                new QueueDeclareOkBody(AMQShortString.valueOf(testQueueName), 0, 0)));

        AMQSession_0_8 session = new AMQSession_0_8(_connection, 1, true, 0, 1, 1);

        AMQBindingURL bindingURL = new AMQBindingURL("topic://amq.topic//?routingkey='testTopic'");
        AMQQueue queue = new AMQQueue(bindingURL);

        assertEquals("Unexpected queue name", "", queue.getAMQQueueName());

        session.declareQueue(queue, true);

        assertEquals("Unexpected queue name", testQueueName, queue.getAMQQueueName());
    }

    public void testResubscribe() throws Exception
    {
        // to verify producer resubscribe set qpid.declare_exchanges=true
        setTestSystemProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "true");
        setTestSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");
        setTestSystemProperty(ClientProperties.QPID_BIND_QUEUES_PROP_NAME, "false");

        AMQSession_0_8 session = new AMQSession_0_8(_connection, 1, true, 0, 1, 1);

        AMQQueue queue1 = new AMQQueue(new AMQBindingURL("direct://amq.direct//test1?routingkey='test1'"));

        // expecting exchange declare Ok
        MockReceiveConnectionListener listener = new MockReceiveConnectionListener(_connection, 1, new ExchangeDeclareOkBody());
        _connection.setConnectionListener(listener);
        session.createProducer(queue1);
        assertTrue("Not all expected commands have been sent on producer1 creation", listener.responsesEmpty());

        // expecting exchange declare Ok, Channel flow Ok, Consume Ok
        listener = new MockReceiveConnectionListener(_connection, 1,
                new ExchangeDeclareOkBody(), new ChannelFlowOkBody(false), new BasicConsumeOkBody(AMQShortString.valueOf("1")));
        _connection.setConnectionListener(listener );
        BasicMessageConsumer_0_8 consumer1 = (BasicMessageConsumer_0_8)session.createConsumer(queue1);
        assertTrue("Not all expected commands have been sent on consumer1 creation", listener.responsesEmpty());

        // expecting exchange declare Ok
        listener = new MockReceiveConnectionListener(_connection, 1, new ExchangeDeclareOkBody());
        _connection.setConnectionListener(listener);
        AMQQueue queue2 = new AMQQueue(new AMQBindingURL("direct://amq.direct//test2?routingkey='test2'"));
        session.createProducer(queue2);
        assertTrue("Not all expected commands have been sent on producer2 creation", listener.responsesEmpty());


        // expecting exchange declare Ok, Consume Ok
        listener = new MockReceiveConnectionListener(_connection, 1,
                new ExchangeDeclareOkBody(), new BasicConsumeOkBody(AMQShortString.valueOf("2")));
        _connection.setConnectionListener(listener);

        BasicMessageConsumer_0_8 consumer2 = (BasicMessageConsumer_0_8)session.createConsumer(queue2);
        assertTrue("Not all expected commands have been sent on consumer2 creation", listener.responsesEmpty());

        UnprocessedMessage[] messages = new UnprocessedMessage[4];
        for (int i =0; i< messages.length;i++ )
        {
            int consumerTag = i % 2 == 0 ? consumer1.getConsumerTag() : consumer2.getConsumerTag();
            int deliveryTag = i + 1;
            messages[i]= createMockMessage(deliveryTag, consumerTag);
            session.messageReceived(messages[i]);
            if (deliveryTag % 2 == 0)
            {
                session.addDeliveredMessage(deliveryTag);
            }
            else
            {
                session.addUnacknowledgedMessage(deliveryTag);
            }
        }

        assertEquals("Unexpected highest delivery tag", messages.length, session.getHighestDeliveryTag().get());
        assertFalse("Unexpected delivered message tags", session.getDeliveredMessageTags().isEmpty());
        assertFalse("Unexpected unacknowledged message tags", session.getUnacknowledgedMessageTags().isEmpty());
        assertEquals("Unexpected consumers", new HashSet<>(Arrays.asList(consumer1, consumer2)), new HashSet<>(session.getConsumers()));

        listener = new MockReceiveConnectionListener(_connection, 1,
                new ExchangeDeclareOkBody(), // first producer resubscribe
                new ExchangeDeclareOkBody(), // second producer resubscribe
                new ExchangeDeclareOkBody(), new BasicConsumeOkBody(AMQShortString.valueOf("1")),  // first consumer resubscribe
                new ExchangeDeclareOkBody(), new BasicConsumeOkBody(AMQShortString.valueOf("2"))); // second consumer resubscribe
        _connection.setConnectionListener(listener);

        session.resubscribe();

        assertTrue("Not all expected commands have been sent on session resubscribe", listener.responsesEmpty());

        assertEquals("Unexpected highest delivery tag", -1, session.getHighestDeliveryTag().get());
        assertTrue("Unexpected unacknowledged message tags", session.getUnacknowledgedMessageTags().isEmpty());
        assertTrue("Unexpected delivered message tags", session.getDeliveredMessageTags().isEmpty());
        assertTrue("Unexpected pre-fetched message tags", session.getPrefetchedMessageTags().isEmpty());
        assertEquals("Unexpected consumers", new HashSet<>(Arrays.asList(consumer1, consumer2)), new HashSet<>(session.getConsumers()));
    }

    private UnprocessedMessage createMockMessage(long deliveryTag, int consumerTag)
    {
        UnprocessedMessage message = mock(UnprocessedMessage.class);
        when(message.getConsumerTag()).thenReturn(consumerTag);
        when(message.getDeliveryTag()).thenReturn(deliveryTag);
        return message;
    }

    static class MockReceiveConnectionListener extends ConnectionListenerSupport
    {
        private final AMQConnection _connection;
        private final List<AMQBody> _responses;
        private final int _channelId;

        MockReceiveConnectionListener(AMQConnection connection, int channelId, AMQBody... response)
        {
            _connection = connection;
            _responses = new ArrayList<>(Arrays.asList(response));
            _channelId = channelId;
        }

        @Override
        public void bytesSent(long count)
        {
            try
            {
                AMQBody response = _responses.remove(0);
                _connection.getProtocolHandler().methodBodyReceived(_channelId, response);
            }
            catch (QpidException e)
            {
                throw new RuntimeException(e);
            }
        }

        public boolean responsesEmpty()
        {
            return _responses.isEmpty();
        }
    }
}
