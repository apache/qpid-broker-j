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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.Binary;
import org.apache.qpid.transport.ExecutionErrorCode;
import org.apache.qpid.transport.ExecutionException;
import org.apache.qpid.transport.MessageTransfer;
import org.apache.qpid.transport.Method;

public class ServerSessionTest extends QpidTestCase
{

    private VirtualHost<?> _virtualHost;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        BrokerTestHelper.setUp();
        _virtualHost = BrokerTestHelper.createVirtualHost(getName());
    }

    @Override
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
            BrokerTestHelper.tearDown();
            super.tearDown();
        }
    }

    public void testOverlargeMessageTest() throws Exception
    {
        final Broker<?> broker = mock(Broker.class);
        when(broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0l);

        AmqpPort port = createMockPort();

        final AMQPConnection_0_10 modelConnection = mock(AMQPConnection_0_10.class);
        when(modelConnection.getAddressSpace()).thenReturn(_virtualHost);
        when(modelConnection.getContextProvider()).thenReturn(_virtualHost);
        when(modelConnection.getBroker()).thenReturn((Broker)broker);
        when(modelConnection.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(modelConnection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(modelConnection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        Subject subject = new Subject();
        when(modelConnection.getSubject()).thenReturn(subject);
        when(modelConnection.getMaxMessageSize()).thenReturn(1024l);
        ServerConnection connection = new ServerConnection(1, broker, port, Transport.TCP, modelConnection);
        connection.setVirtualHost(_virtualHost);
        final List<Method> invokedMethods = new ArrayList<>();
        ServerSession session = new ServerSession(connection, new ServerSessionDelegate(),
                                                   new Binary(getName().getBytes()), 0)
        {
            @Override
            public void invoke(final Method m)
            {
                invokedMethods.add(m);
            }
        };

        ServerSessionDelegate delegate = new ServerSessionDelegate();

        MessageTransfer xfr = new MessageTransfer();
        xfr.setBody(new byte[2048]);
        delegate.messageTransfer(session, xfr);

        assertFalse("No methods invoked - expecting at least 1", invokedMethods.isEmpty());
        Method firstInvoked = invokedMethods.get(0);
        assertTrue("First invoked method not execution error", firstInvoked instanceof ExecutionException);
        assertEquals(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED, ((ExecutionException)firstInvoked).getErrorCode());

        invokedMethods.clear();

        // test the boundary condition

        xfr.setBody(new byte[1024]);
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
