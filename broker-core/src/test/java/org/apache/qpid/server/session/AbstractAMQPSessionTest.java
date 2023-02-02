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

package org.apache.qpid.server.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.verify;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Producer;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractAMQPSessionTest extends UnitTestBase
{
    public static final String TEST_USERNAME = "testUser";

    private AMQPConnection<?> _connection;
    private AbstractAMQPSession<?, ?> mockAMQPSession;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _connection = mock(AMQPConnection.class);
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_connection.getChildExecutor()).thenReturn(_taskExecutor);
        final Model model = BrokerModel.getInstance();
        when(_connection.getModel()).thenReturn(model);
        final Subject testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
        when(_connection.getSubject()).thenReturn(testSubject);
        final Broker broker = BrokerTestHelper.mockWithSystemPrincipal(Broker.class, mock(Principal.class));
        when(_connection.getBroker()).thenReturn(broker);
        final QueueManagingVirtualHost<?> virtualHost = mock(QueueManagingVirtualHost.class);
        when(_connection.getAddressSpace()).thenReturn(virtualHost);
        when(_connection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(_connection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        mockAMQPSession = new MockAMQPSession(_connection, 123);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            _taskExecutor.stop();
        }
        finally
        {
            // remove
            _connection.closeSessionAsync(mockAMQPSession, AMQPConnection.CloseReason.MANAGEMENT,"Test Execution completed");
        }
    }

    @Test
    public void testRegisterMessageDelivered()
    {
        mockAMQPSession.registerMessageDelivered(100);
        assertEquals(1, mockAMQPSession.getMessagesOut());
        assertEquals(100, mockAMQPSession.getBytesOut());
        verify(_connection).registerMessageDelivered(100);
    }

    @Test
    public void testRegisterMessageReceived()
    {
        mockAMQPSession.registerMessageReceived(100);
        assertEquals(1, mockAMQPSession.getMessagesIn());
        assertEquals(100, mockAMQPSession.getBytesIn());
        verify(_connection).registerMessageReceived(100);
    }

    @Test
    public void testRegisterTransactedMessageDelivered()
    {
        mockAMQPSession.registerTransactedMessageDelivered();
        assertEquals(1, mockAMQPSession.getTransactedMessagesOut());
        verify(_connection).registerTransactedMessageDelivered();
    }

    @Test
    public void testRegisterTransactedMessageReceived()
    {
        mockAMQPSession.registerTransactedMessageReceived();
        assertEquals(1, mockAMQPSession.getTransactedMessagesIn());
        verify(_connection).registerTransactedMessageReceived();
    }

    @Test
    public void resetStatistics()
    {
        mockAMQPSession.resetStatistics();

        mockAMQPSession.registerMessageDelivered(100);
        mockAMQPSession.registerMessageReceived(100);
        mockAMQPSession.registerTransactedMessageDelivered();
        mockAMQPSession.registerTransactedMessageReceived();

        final Map<String, Object> statisticsBeforeReset = mockAMQPSession.getStatistics();
        assertEquals(100L, statisticsBeforeReset.get("bytesIn"));
        assertEquals(100L, statisticsBeforeReset.get("bytesOut"));
        assertEquals(1L, statisticsBeforeReset.get("messagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("messagesOut"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesOut"));

        mockAMQPSession.resetStatistics();

        final Map<String, Object> statisticsAfterReset = mockAMQPSession.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("bytesIn"));
        assertEquals(0L, statisticsAfterReset.get("bytesOut"));
        assertEquals(0L, statisticsAfterReset.get("messagesIn"));
        assertEquals(0L, statisticsAfterReset.get("messagesOut"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesIn"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesOut"));
    }

    @Test
    public void producer() throws Exception
    {
        final PublishingLink link = mock(PublishingLink.class);
        when(link.getName()).thenReturn("test-link");
        when(link.getType()).thenReturn(PublishingLink.TYPE_LINK);

        final Queue<?> queue = mock(Queue.class);

        final Producer<?> producer = mockAMQPSession.addProducer(link, queue);

        Map<String, Object> statistics = producer.getStatistics();

        assertEquals(0, statistics.get("messagesOut"));
        assertEquals(0L, statistics.get("bytesOut"));

        producer.registerMessageDelivered(100L);

        statistics = producer.getStatistics();

        assertEquals(1, statistics.get("messagesOut"));
        assertEquals(100L, statistics.get("bytesOut"));

        producer.resetStatistics();
        statistics = producer.getStatistics();

        assertEquals(0, statistics.get("messagesOut"));
        assertEquals(0L, statistics.get("bytesOut"));
    }

    private static class MockAMQPSession extends AbstractAMQPSession
    {

        protected MockAMQPSession(final Connection parent, final int sessionId)
        {
            super(parent, sessionId);
        }

        @Override
        protected void updateBlockedStateIfNecessary()
        {

        }

        @Override
        public boolean isClosing()
        {
            return false;
        }

        @Override
        public Object getConnectionReference()
        {
            return null;
        }

        @Override
        public void block()
        {

        }

        @Override
        public void unblock()
        {

        }

        @Override
        public boolean getBlocking()
        {
            return false;
        }

        @Override
        public int getUnacknowledgedMessageCount()
        {
            return 0;
        }

        @Override
        public long getTransactionStartTimeLong()
        {
            return 0;
        }

        @Override
        public long getTransactionUpdateTimeLong()
        {
            return 0;
        }

        @Override
        public void transportStateChanged()
        {

        }

        @Override
        public void unblock(final Queue queue)
        {

        }

        @Override
        public void block(final Queue queue)
        {

        }
    }
}
