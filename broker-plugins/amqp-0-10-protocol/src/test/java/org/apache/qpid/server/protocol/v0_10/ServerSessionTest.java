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
 */
package org.apache.qpid.server.protocol.v0_10;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.transport.Binary;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class ServerSessionTest extends UnitTestBase
{

    private VirtualHost<?> _virtualHost;
    private CurrentThreadTaskExecutor _taskExecutor;

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestName(), this);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_virtualHost != null)
            {
                _virtualHost.close();
            }
        }
        finally
        {
            try
            {
                if (_taskExecutor != null)
                {
                    _taskExecutor.stop();
                }
            }
            finally
            {
                BrokerTestHelper.tearDown();
            }
        }
    }

    @Test
    public void testOverlargeMessageTest() throws Exception
    {
        final Broker<?> broker = mock(Broker.class);
        when(broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0l);

        AmqpPort port = createMockPort();

        final AMQPConnection_0_10 modelConnection = mock(AMQPConnection_0_10.class);
        when(modelConnection.getCategoryClass()).thenReturn(Connection.class);
        when(modelConnection.getTypeClass()).thenReturn(AMQPConnection_0_10.class);
        when(modelConnection.closeAsync()).thenReturn(Futures.immediateFuture(null));
        when(modelConnection.getAddressSpace()).thenReturn(_virtualHost);
        when(modelConnection.getContextProvider()).thenReturn(_virtualHost);
        when(modelConnection.getBroker()).thenReturn(broker);
        when(modelConnection.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(modelConnection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(modelConnection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        when(modelConnection.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(Connection.DEFAULT_MAX_UNCOMMITTED_IN_MEMORY_SIZE);
        when(modelConnection.getChildExecutor()).thenReturn(_taskExecutor);
        when(modelConnection.getModel()).thenReturn(BrokerModel.getInstance());
        when(modelConnection.getPort()).thenReturn(port);

        final AuthenticatedPrincipal principal =
                new AuthenticatedPrincipal(new UsernamePrincipal(getTestName(), mock(AuthenticationProvider.class)));
        final Subject subject =
                new Subject(false, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
        when(modelConnection.getSubject()).thenReturn(subject);
        when(modelConnection.getMaxMessageSize()).thenReturn(1024l);
        when(modelConnection.getCreatedTime()).thenReturn(new Date());
        ServerConnection connection = new ServerConnection(1, broker, port, Transport.TCP, modelConnection);
        connection.setVirtualHost(_virtualHost);

        final List<Method> invokedMethods = new ArrayList<>();
        ServerSession session = new ServerSession(connection, new ServerSessionDelegate(),
                                                  new Binary(getTestName().getBytes()), 0)
        {
            @Override
            public void invoke(final Method m)
            {
                invokedMethods.add(m);
            }
        };
        Session_0_10 modelSession = new Session_0_10(modelConnection, 1, session);
        session.setModelObject(modelSession);
        ServerSessionDelegate delegate = new ServerSessionDelegate();

        MessageTransfer xfr = new MessageTransfer();
        byte[] body1 = new byte[2048];
        xfr.setBody(QpidByteBuffer.wrap(body1));
        delegate.messageTransfer(session, xfr);

        assertFalse("No methods invoked - expecting at least 1", invokedMethods.isEmpty());
        Method firstInvoked = invokedMethods.get(0);
        final boolean condition = firstInvoked instanceof ExecutionException;
        assertTrue("First invoked method not execution error", condition);
        assertEquals(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED,
                            ((ExecutionException)firstInvoked).getErrorCode());


        invokedMethods.clear();

        // test the boundary condition

        byte[] body = new byte[1024];
        xfr.setBody(QpidByteBuffer.wrap(body));
        delegate.messageTransfer(session, xfr);

        assertTrue("Methods invoked when not expecting any", invokedMethods.isEmpty());
    }

    public AmqpPort createMockPort()
    {
        AmqpPort port = mock(AmqpPort.class);
        TaskExecutor childExecutor = new TaskExecutorImpl();
        childExecutor.start();
        when(port.getChildExecutor()).thenReturn(childExecutor);
        when(port.getCategoryClass()).thenReturn(Port.class);
        when(port.getModel()).thenReturn(BrokerModel.getInstance());
        return port;
    }


}
