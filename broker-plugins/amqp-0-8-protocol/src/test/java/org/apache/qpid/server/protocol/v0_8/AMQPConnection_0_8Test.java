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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

@SuppressWarnings({"rawtypes"})
class AMQPConnection_0_8Test extends UnitTestBase
{
    private static final String VIRTUAL_HOST_NAME = "vhost";
    private static final byte[] SASL_RESPONSE = "response".getBytes();
    private static final AMQShortString LOCALE = AMQShortString.createAMQShortString("en_US");
    private static final AMQShortString SASL_MECH = AMQShortString.createAMQShortString("MECH");

    private TaskExecutorImpl _taskExecutor;
    private Broker _broker;
    private QueueManagingVirtualHost _virtualHost;
    private AmqpPort _port;
    private ServerNetworkConnection _network;
    private Transport _transport;
    private Protocol _protocol;
    private AggregateTicker _ticker;

    @BeforeEach
    void setUp() throws Exception
    {
        final EventLogger value = new EventLogger();

        final SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        final Model model = BrokerModel.getInstance();

        _broker = mock(Broker.class);
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getEventLogger()).thenReturn(value);
        when(_broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(0L);

        final VirtualHostNode virtualHostNode = mock(VirtualHostNode.class);
        when(virtualHostNode.getParent()).thenReturn(_broker);
        when(virtualHostNode.getModel()).thenReturn(model);
        when(virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);

        _virtualHost = mock(QueueManagingVirtualHost.class);
        final VirtualHostPrincipal virtualHostPrincipal = new VirtualHostPrincipal(_virtualHost);
        when(_virtualHost.getParent()).thenReturn(virtualHostNode);
        when(_virtualHost.getModel()).thenReturn(model);
        when(_virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(_virtualHost.getState()).thenReturn(State.ACTIVE);
        when(_virtualHost.isActive()).thenReturn(true);

        when(_virtualHost.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHost.getPrincipal()).thenReturn(virtualHostPrincipal);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1024);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1024L);
        when(_virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_virtualHost.authoriseCreateConnection(any(AMQPConnection.class))).thenReturn(true);
        when(_virtualHost.getEventLogger()).thenReturn(value);

        final SubjectCreator subjectCreator = mock(SubjectCreator.class);

        final SaslNegotiator saslNegotiator = mock(SaslNegotiator.class);
        when(subjectCreator.createSaslNegotiator(eq(SASL_MECH.toString()), any(SaslSettings.class))).thenReturn(saslNegotiator);
        when(subjectCreator.authenticate(saslNegotiator, SASL_RESPONSE)).thenReturn(new SubjectAuthenticationResult(
                new AuthenticationResult(new AuthenticatedPrincipal(new UsernamePrincipal("username", null))), new Subject()));

        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(List.of(SASL_MECH.toString()));

        _port = mock(AmqpPort.class);
        when(_port.getParent()).thenReturn(_broker);
        when(_port.getCategoryClass()).thenReturn(Port.class);
        when(_port.getChildExecutor()).thenReturn(_taskExecutor);
        when(_port.getModel()).thenReturn(model);
        when(_port.getAuthenticationProvider()).thenReturn(authenticationProvider);
        when(_port.getAddressSpace(VIRTUAL_HOST_NAME)).thenReturn(_virtualHost);
        when(_port.getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)).thenReturn(2500L);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(Connection.DEFAULT_MAX_MESSAGE_SIZE);
        when(_port.getSubjectCreator(eq(false), anyString())).thenReturn(subjectCreator);

        final ByteBufferSender sender = mock(ByteBufferSender.class);

        _network = mock(ServerNetworkConnection.class);
        when(_network.getSender()).thenReturn(sender);
        when(_network.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(_network.getSelectedHost()).thenReturn("localhost");

        _transport = Transport.TCP;
        _protocol = Protocol.AMQP_0_8;
        _ticker = new AggregateTicker();
    }

    @AfterEach
    void tearDown()
    {
        _taskExecutor.stopImmediately();
    }

    @Test
    void closeOnNoRoute()
    {
        {
            final AMQPConnection_0_8Impl
                    conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            final FieldTable startFieldTable = FieldTable
                    .convertToFieldTable(Map.of(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, Boolean.TRUE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);

            assertTrue(conn.isCloseWhenNoRoute(), "Unexpected closeWhenNoRoute value");
        }

        {
            final AMQPConnection_0_8Impl
                    conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
            conn.create();
            conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));

            final FieldTable startFieldTable = FieldTable
                    .convertToFieldTable(Map.of(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, Boolean.FALSE));
            conn.receiveConnectionStartOk(startFieldTable, SASL_MECH, SASL_RESPONSE, LOCALE);
            assertFalse(conn.isCloseWhenNoRoute(), "Unexpected closeWhenNoRoute value");
        }
    }

    @Test
    void connectionEnforcesMaxSessions()
    {
        final AMQPConnection_0_8Impl
                conn = new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
        conn.create();

        conn.receiveProtocolHeader(new ProtocolInitiation(ProtocolVersion.v0_8));
        conn.receiveConnectionStartOk(FieldTableFactory.createFieldTable(Map.of()), SASL_MECH, SASL_RESPONSE, LOCALE);
        final int maxChannels = 10;
        conn.receiveConnectionTuneOk(maxChannels, 65535, 0);
        conn.receiveConnectionOpen(AMQShortString.createAMQShortString(VIRTUAL_HOST_NAME), AMQShortString.EMPTY_STRING, false);

        // check the channel count is correct
        final int channelCount = conn.getSessionModels().size();
        assertEquals(0, (long) channelCount, "Initial channel count wrong");

        assertEquals(maxChannels, (long) conn.getSessionCountLimit(), "Number of channels not correctly set.");


        assertFalse(conn.isClosing(), "Connection should not be closed after opening " + maxChannels + " channels");
        for (long currentChannel = 1L; currentChannel <= maxChannels; currentChannel++)
        {
            conn.receiveChannelOpen((int) currentChannel);
        }

        assertFalse(conn.isClosing(), "Connection should not be closed after opening " + maxChannels + " channels");
        assertEquals(maxChannels, (long) conn.getSessionModels().size(), "Maximum number of channels not set.");
        conn.receiveChannelOpen(maxChannels + 1);
        assertTrue(conn.isClosing(), "Connection should be closed after opening " + (maxChannels + 1) + " channels");
    }

    @Test
    void resetStatistics()
    {
        final AMQPConnection_0_8Impl connection =
                new AMQPConnection_0_8Impl(_broker, _network, _port, _transport, _protocol, 0, _ticker);
        connection.create();
        connection.setAddressSpace(_virtualHost);
        connection.registerMessageReceived(100L);
        connection.registerMessageDelivered(100L);
        connection.registerTransactedMessageReceived();
        connection.registerTransactedMessageDelivered();

        final Map<String, Object> statisticsBeforeReset = connection.getStatistics();
        assertEquals(100L, statisticsBeforeReset.get("bytesIn"));
        assertEquals(100L, statisticsBeforeReset.get("bytesOut"));
        assertEquals(1L, statisticsBeforeReset.get("messagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("messagesOut"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesOut"));

        connection.resetStatistics();

        final Map<String, Object> statisticsAfterReset = connection.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("bytesIn"));
        assertEquals(0L, statisticsAfterReset.get("bytesOut"));
        assertEquals(0L, statisticsAfterReset.get("messagesIn"));
        assertEquals(0L, statisticsAfterReset.get("messagesOut"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesIn"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesOut"));
    }
}
