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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Collections;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQPConnection_0_8Test extends UnitTestBase
{
    private static final String VIRTUAL_HOST_NAME = "vhost";
    private static final byte[] SASL_RESPONSE = "response".getBytes();
    private static final AMQShortString LOCALE = AMQShortString.createAMQShortString("en_US");
    private static final AMQShortString SASL_MECH = AMQShortString.createAMQShortString("MECH");

    private TaskExecutorImpl _taskExecutor;
    private Broker _broker;
    private VirtualHostNode _virtualHostNode;
    private QueueManagingVirtualHost _virtualHost;
    private AmqpPort _port;
    private ServerNetworkConnection _network;
    private Transport _transport;
    private Protocol _protocol;
    private AggregateTicker _ticker;
    private ByteBufferSender _sender;

    @Before
    public void setUp() throws Exception
    {

        EventLogger value = new EventLogger();

        SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        Model model = BrokerModel.getInstance();

        _broker = mock(Broker.class);
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getEventLogger()).thenReturn(value);
        when(_broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0l);

        _virtualHostNode = mock(VirtualHostNode.class);
        when(_virtualHostNode.getParent()).thenReturn(_broker);
        when(_virtualHostNode.getModel()).thenReturn(model);
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);

        _virtualHost = mock(QueueManagingVirtualHost.class);
        VirtualHostPrincipal virtualHostPrincipal = new VirtualHostPrincipal(_virtualHost);
        when(_virtualHost.getParent()).thenReturn(_virtualHostNode);
        when(_virtualHost.getModel()).thenReturn(model);
        when(_virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(_virtualHost.getState()).thenReturn(State.ACTIVE);
        when(_virtualHost.isActive()).thenReturn(true);

        when(_virtualHost.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHost.getPrincipal()).thenReturn(virtualHostPrincipal);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1024);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1024l);
        when(_virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_virtualHost.authoriseCreateConnection(any(AMQPConnection.class))).thenReturn(true);
        when(_virtualHost.getEventLogger()).thenReturn(value);

        SubjectCreator subjectCreator = mock(SubjectCreator.class);


        SaslNegotiator saslNegotiator = mock(SaslNegotiator.class);
        when(subjectCreator.createSaslNegotiator(eq(SASL_MECH.toString()), any(SaslSettings.class))).thenReturn(saslNegotiator);
        when(subjectCreator.authenticate(saslNegotiator, SASL_RESPONSE)).thenReturn(new SubjectAuthenticationResult(
                new AuthenticationResult(new AuthenticatedPrincipal(new UsernamePrincipal("username", null))), new Subject()));

        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(Collections.singletonList(SASL_MECH.toString()));

        _port = mock(AmqpPort.class);
        when(_port.getParent()).thenReturn(_broker);
        when(_port.getCategoryClass()).thenReturn(Port.class);
        when(_port.getChildExecutor()).thenReturn(_taskExecutor);
        when(_port.getModel()).thenReturn(model);
        when(_port.getAuthenticationProvider()).thenReturn(authenticationProvider);
        when(_port.getAddressSpace(VIRTUAL_HOST_NAME)).thenReturn(_virtualHost);
        when(_port.getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)).thenReturn(2500l);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(Connection.DEFAULT_MAX_MESSAGE_SIZE);
        when(_port.getSubjectCreator(eq(false), anyString())).thenReturn(subjectCreator);

        _sender = mock(ByteBufferSender.class);

        _network = mock(ServerNetworkConnection.class);
        when(_network.getSender()).thenReturn(_sender);
        when(_network.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(_network.getSelectedHost()).thenReturn("localhost");

        _transport = Transport.TCP;
        _protocol = Protocol.AMQP_0_8;
        _ticker = new AggregateTicker();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            _taskExecutor.stopImmediately();
        }
        finally
        {
        }
    }

    @Test
    public void testCloseOnNoRoute()
    {
        {
            AMQPConnection_0_8Impl
                    conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            FieldTable startFieldTable = FieldTable.convertToFieldTable(Collections.<String, Object>singletonMap(
                    ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE,
                    Boolean.TRUE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);

            assertTrue("Unexpected closeWhenNoRoute value", conn.isCloseWhenNoRoute());
        }

        {
            AMQPConnection_0_8Impl
                    conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            FieldTable startFieldTable = FieldTable.convertToFieldTable(Collections.<String, Object>singletonMap(
                    ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE,
                    Boolean.FALSE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);
            assertFalse("Unexpected closeWhenNoRoute value", conn.isCloseWhenNoRoute());
        }
    }

    @Test
    public void testConnectionEnforcesMaxSessions() throws Exception
    {
        AMQPConnection_0_8Impl
                conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
        conn.create();

        conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        conn.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Collections.emptyMap()), SASL_MECH, SASL_RESPONSE, LOCALE);
        int maxChannels = 10;
        conn.receiveConnectionTuneOk(maxChannels, 65535, 0);
        conn.receiveConnectionOpen(AMQShortString.createAMQShortString(VIRTUAL_HOST_NAME), AMQShortString.EMPTY_STRING, false);

        // check the channel count is correct
        int channelCount = conn.getSessionModels().size();
        assertEquals("Initial channel count wrong", (long) 0, (long) channelCount);

        assertEquals("Number of channels not correctly set.",
                            (long) maxChannels,
                            (long) conn.getSessionCountLimit());


        assertFalse("Connection should not be closed after opening " + maxChannels + " channels",
                           conn.isClosing());
        for (long currentChannel = 1L; currentChannel <= maxChannels; currentChannel++)
        {
            conn.receiveChannelOpen((int) currentChannel);
        }

        assertFalse("Connection should not be closed after opening " + maxChannels + " channels",
                           conn.isClosing());
        assertEquals("Maximum number of channels not set.",
                            (long) maxChannels,
                            (long) conn.getSessionModels().size());
        conn.receiveChannelOpen(maxChannels + 1);
        assertTrue("Connection should be closed after opening " + (maxChannels + 1) + " channels",
                          conn.isClosing());
    }

}
