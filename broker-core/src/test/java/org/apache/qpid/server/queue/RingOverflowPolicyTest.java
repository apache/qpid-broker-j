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
package org.apache.qpid.server.queue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.TestConsumerTarget;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class RingOverflowPolicyTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private QueueManagingVirtualHost<?> _virtualHost;
    private AtomicLong _messageId;

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setUp() throws Exception
    {
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();
        final String name = getClass().getName();
        final VirtualHostNode<?> virtualHostNode = BrokerTestHelper.createVirtualHostNodeMock(
                name, true, BrokerTestHelper.createAccessControlMock(), BrokerTestHelper.createBrokerMock());
        when(virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);
        when(virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);

        final Map<String, Object> virtualHostAttributes = Map
                .of(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                VirtualHost.NAME, name,
                QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, 2,
                QueueManagingVirtualHost.NUMBER_OF_SELECTORS, 1);

        final ConfiguredObjectFactory objectFactory = virtualHostNode.getObjectFactory();
        final QueueManagingVirtualHost<?> host = (QueueManagingVirtualHost<?>)objectFactory.create(VirtualHost.class, virtualHostAttributes, virtualHostNode);
        final AbstractVirtualHost abstractVirtualHost = (AbstractVirtualHost) host;
        abstractVirtualHost.start();
        when(virtualHostNode.getVirtualHost()).thenReturn(abstractVirtualHost);
        _virtualHost = host;
        _messageId = new AtomicLong();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stop();
    }

    @Test
    public void testEnqueueWithOverflowWhenLeastSignificantEntryIsAcquiredByConsumer() throws Exception
    {
        final Queue<?> queue = createTestRingQueue(2);
        final ServerMessage<?> message1 = enqueueTestMessage(queue);
        final TestConsumerTarget consumerTarget = createTestConsumerTargetAndConsumer(queue);
        final boolean received = consumerTarget.processPending();
        assertThat(received, is(true));

        final MessageInstance receivedMessage = consumerTarget.getMessages().remove(0);
        assertThat(receivedMessage, is(notNullValue()));
        assertThat(receivedMessage.isAcquired(), is(true));
        assertThat(receivedMessage.getMessage(), is(message1));

        final ServerMessage<?> message2 = enqueueTestMessage(queue);
        assertThat(queue.getQueueDepthMessages(), is(equalTo(2)));

        final ServerMessage<?> message3 = enqueueTestMessage(queue);
        assertThat(queue.getQueueDepthMessages(), is(equalTo(2)));
        assertThat(message2.isReferenced(queue), is(equalTo(false)));
        assertThat(message3.isReferenced(queue), is(equalTo(true)));
    }

    @Test
    public void testLeastSignificantEntryAcquiredByConsumerIsDeletedAfterRelease() throws Exception
    {
        final Queue<?> queue = createTestRingQueue(1);
        final ServerMessage<?> message1 = enqueueTestMessage(queue);
        final TestConsumerTarget consumerTarget = createTestConsumerTargetAndConsumer(queue);
        final boolean received = consumerTarget.processPending();
        assertThat(received, is(true));

        final MessageInstance receivedMessage = consumerTarget.getMessages().remove(0);
        assertThat(receivedMessage, is(notNullValue()));
        assertThat(receivedMessage.isAcquired(), is(true));
        assertThat(receivedMessage.getMessage(), is(message1));

        final ServerMessage<?> message2 = enqueueTestMessage(queue);
        assertThat(queue.getQueueDepthMessages(), is(equalTo(2)));
        assertThat(message1.isReferenced(queue), is(equalTo(true)));
        assertThat(message2.isReferenced(queue), is(equalTo(true)));

        receivedMessage.release();
        assertThat(queue.getQueueDepthMessages(), is(equalTo(1)));
        assertThat(message1.isReferenced(queue), is(equalTo(false)));
        assertThat(message2.isReferenced(queue), is(equalTo(true)));
    }

    private Queue<?> createTestRingQueue(final int messageLimit)
    {
        final Map<String, Object> attributes = Map.of(Queue.NAME, getTestName(),
                Queue.OVERFLOW_POLICY, OverflowPolicy.RING.name(),
                Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, messageLimit);
        return _virtualHost.createChild(Queue.class, attributes);
    }

    private TestConsumerTarget createTestConsumerTargetAndConsumer(final Queue<?> queue) throws Exception
    {
        final TestConsumerTarget consumerTarget = new TestConsumerTarget();
        queue.addConsumer(consumerTarget,
                          null,
                          InternalMessage.class,
                          getTestName(),
                          EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES),
                          0);
        return consumerTarget;
    }

    private ServerMessage<?> enqueueTestMessage(final Queue<?> queue)
    {
        final ServerMessage<?> message = createMessage(_messageId.incrementAndGet(), queue.getName());
        final MessageEnqueueRecord record = createMessageEnqueueRecord(queue.getId(), message.getMessageNumber());
        queue.enqueue(message, null, record);
        return message;
    }

    private MessageEnqueueRecord createMessageEnqueueRecord(final UUID queueId, final long messageNumber)
    {
        return new MessageEnqueueRecord()
        {
            @Override
            public UUID getQueueId()
            {
                return queueId;
            }

            @Override
            public long getMessageNumber()
            {
                return messageNumber;
            }
        };
    }

    private ServerMessage<?> createMessage(final long messageNumber, final String queueName)
    {
        final AMQMessageHeader amqpHeader = mock(AMQMessageHeader.class);
        when(amqpHeader.getMessageId()).thenReturn(String.valueOf(messageNumber));
        when(amqpHeader.getExpiration()).thenReturn(0L);
        final Serializable messageContent = String.format("test message %d", messageNumber);
        return InternalMessage.createMessage(_virtualHost.getMessageStore(),
                                             amqpHeader,
                                             messageContent,
                                             false,
                                             queueName);
    }
}
