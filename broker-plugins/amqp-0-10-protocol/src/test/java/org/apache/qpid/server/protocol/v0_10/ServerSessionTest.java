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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

@SuppressWarnings({"rawtypes"})
class ServerSessionTest extends UnitTestBase
{
    private VirtualHost<?> _virtualHost;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    void setUp() throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestName(), this);
    }

    @AfterEach
    void tearDown()
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
            if (_taskExecutor != null)
            {
                _taskExecutor.stop();
            }
        }
    }

    @Test
    void overlargeMessageTest()
    {
        final Broker<?> broker = mock(Broker.class);
        when(broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0L);

        final AmqpPort<?> port = createMockPort();

        final AMQPConnection_0_10 modelConnection = mock(AMQPConnection_0_10.class);
        when(modelConnection.getCategoryClass()).thenReturn(Connection.class);
        when(modelConnection.getTypeClass()).thenReturn(AMQPConnection_0_10.class);
        when(modelConnection.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
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
        final Subject subject = new Subject(false, Set.of(principal), Set.of(), Set.of());
        when(modelConnection.getSubject()).thenReturn(subject);
        when(modelConnection.getMaxMessageSize()).thenReturn(1024L);
        when(modelConnection.getCreatedTime()).thenReturn(new Date());
        final ServerConnection connection = new ServerConnection(1, broker, port, Transport.TCP, modelConnection);
        connection.setVirtualHost(_virtualHost);

        final List<Method> invokedMethods = new ArrayList<>();
        final ServerSession session =
                new ServerSession(connection, new ServerSessionDelegate(), new Binary(getTestName().getBytes()), 0)
        {
            @Override
            public void invoke(final Method m)
            {
                invokedMethods.add(m);
            }
        };
        final Session_0_10 modelSession = new Session_0_10(modelConnection, 1, session, getTestName());
        session.setModelObject(modelSession);
        final ServerSessionDelegate delegate = new ServerSessionDelegate();

        final MessageTransfer xfr = new MessageTransfer();
        final byte[] body1 = new byte[2048];
        xfr.setBody(QpidByteBuffer.wrap(body1));
        delegate.messageTransfer(session, xfr);

        assertFalse(invokedMethods.isEmpty(), "No methods invoked - expecting at least 1");
        final Method firstInvoked = invokedMethods.get(0);
        final boolean condition = firstInvoked instanceof ExecutionException;
        assertTrue(condition, "First invoked method not execution error");
        assertEquals(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED, ((ExecutionException)firstInvoked).getErrorCode());

        invokedMethods.clear();

        // test the boundary condition

        final byte[] body = new byte[1024];
        xfr.setBody(QpidByteBuffer.wrap(body));
        delegate.messageTransfer(session, xfr);

        assertTrue(invokedMethods.isEmpty(), "Methods invoked when not expecting any");
    }

    AmqpPort<?> createMockPort()
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
