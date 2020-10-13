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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.verify;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractAMQPSessionTest extends UnitTestBase
{
    private AMQPConnection _connection;
    private AbstractAMQPSession mockAMQPSession;
    private QueueManagingVirtualHost<?> _virtualHost;
    private Broker _broker;
    private TaskExecutor _taskExecutor;
    private Subject _testSubject;
    public static final String TEST_USERNAME = "testUser";

    @Before
    public void setUp() throws Exception
    {
        _connection = mock(AMQPConnection.class);
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_connection.getChildExecutor()).thenReturn(_taskExecutor);
        Model model = BrokerModel.getInstance();
        when(_connection.getModel()).thenReturn(model);
        _testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
        when(_connection.getSubject()).thenReturn(_testSubject);
        _broker = BrokerTestHelper.mockWithSystemPrincipal(Broker.class, mock(Principal.class));
        when(_connection.getBroker()).thenReturn(_broker);
        _virtualHost = mock(QueueManagingVirtualHost.class);
        when(_connection.getAddressSpace()).thenReturn((VirtualHost)_virtualHost);
        when(_connection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(_connection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        mockAMQPSession = new MockAMQPSession(_connection,123);

    }

    @After
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

    private class MockAMQPSession extends AbstractAMQPSession{

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
