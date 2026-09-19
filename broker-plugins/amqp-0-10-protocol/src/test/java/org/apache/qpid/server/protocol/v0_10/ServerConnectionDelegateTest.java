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

package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.qpid.server.protocol.v0_10.ServerConnectionDelegate.BASE64_LIMIT;
import static org.apache.qpid.server.protocol.v0_10.ServerConnectionDelegate.MESSAGE_DIGEST_SHA1;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.security.auth.Subject;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionCloseCode;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolHeader;
import org.apache.qpid.server.protocol.v0_10.transport.SessionAttach;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class ServerConnectionDelegateTest extends UnitTestBase
{
    private ServerConnectionDelegate _delegate;
    private ServerConnection _serverConnection;
    private TaskExecutor _taskExecutor;
    private AuthenticationProvider _authenticationProvider;
    private AmqpPort<?> _port;
    private SubjectCreator _subjectCreator;
    private AMQPConnection_0_10<?> _amqpConnection;

    @BeforeEach
    void setUp()
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final Broker broker = mock(Broker.class);
        when(broker.getNetworkBufferSize()).thenReturn(0xffff);
        when(broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(Long.MAX_VALUE);
        when(broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());
        _authenticationProvider = mock(AuthenticationProvider.class);
        when(_authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(List.of("PLAIN"));
        _subjectCreator = mock(SubjectCreator.class);
        _port = mock(AmqpPort.class);
        when(_port.getAuthenticationProvider()).thenReturn(_authenticationProvider);
        when(_port.getSubjectCreator(true, "test")).thenReturn(_subjectCreator);
        when(_port.getParent()).thenReturn(broker);

        _delegate = new ServerConnectionDelegate(_port, true, "test");
        _delegate.setState(ServerConnectionDelegate.ConnectionState.OPEN);
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        when(addressSpace.getConnections()).thenReturn(List.of());

        final Subject subject = new Subject();
        subject.setReadOnly();
        _amqpConnection = mock(AMQPConnection_0_10.class, withSettings().extraInterfaces(SaslSettings.class));
        when(_amqpConnection.getParent()).thenReturn(broker);
        when(_amqpConnection.getBroker()).thenReturn(broker);
        when(_amqpConnection.getChildExecutor()).thenReturn(_taskExecutor);
        when(_amqpConnection.getModel()).thenReturn(BrokerModel.getInstance());
        when(_amqpConnection.getSubject()).thenReturn(subject);
        when(_amqpConnection.getContextValue(Long.class,
                                             org.apache.qpid.server.model.Session.PRODUCER_AUTH_CACHE_TIMEOUT))
                .thenReturn(Long.MAX_VALUE);
        when(_amqpConnection.getContextValue(Integer.class,
                                             org.apache.qpid.server.model.Session.PRODUCER_AUTH_CACHE_SIZE))
                .thenReturn(Integer.MAX_VALUE);
        when(_amqpConnection.getEventLogger()).thenReturn(mock(EventLogger.class));

        _serverConnection = mock(ServerConnection.class);
        when(_serverConnection.getAddressSpace()).thenReturn(addressSpace);
        when(_serverConnection.getBroker()).thenReturn(broker);
        when(_serverConnection.getAmqpConnection()).thenReturn(_amqpConnection);
    }

    @AfterEach
    void tearDown()
    {
        _taskExecutor.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN", "DISABLED", "SECURE_ONLY"})
    void unadvertisedSaslMechanismRejected(final String mechanismName)
    {
        final ServerConnectionDelegate delegate = createDelegateAwaitingStartOk();

        delegate.connectionStartOk(_serverConnection, createConnectionStartOk(mechanismName));

        verify(_serverConnection).sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED,
                "Sasl mechanism was not advertised");
        verify(_subjectCreator, never()).createSaslNegotiator(eq(mechanismName), any(SaslSettings.class));
    }

    @Test
    void mechanismAddedAfterAdvertisementRejected()
    {
        final String mechanismName = "NEWLY_ENABLED";
        final ServerConnectionDelegate delegate = createDelegateAwaitingStartOk();
        when(_authenticationProvider.getAvailableMechanisms(true))
                .thenReturn(List.of("PLAIN", mechanismName));

        delegate.connectionStartOk(_serverConnection, createConnectionStartOk(mechanismName));

        verify(_serverConnection).sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED,
                "Sasl mechanism was not advertised");
        verify(_subjectCreator, never()).createSaslNegotiator(eq(mechanismName), any(SaslSettings.class));
    }

    @Test
    void advertisedSaslMechanismPassedToSubjectCreator()
    {
        final String mechanismName = "PLAIN";
        final ServerConnectionDelegate delegate = createDelegateAwaitingStartOk();

        delegate.connectionStartOk(_serverConnection, createConnectionStartOk(mechanismName));

        verify(_subjectCreator).createSaslNegotiator(eq(mechanismName), any(SaslSettings.class));
    }

    @Test
    void sessionAttachWhenNameIsUUID()
    {
        final String name = UUID.randomUUID().toString();
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session<?> session = serverSession.getModelObject();
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(name)));
    }

    @Test
    void sessionAttachWhenNameIsNotUUID()
    {
        final String name = "ABC";
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session<?> session = serverSession.getModelObject();
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(Base64.getEncoder().encodeToString(name.getBytes(UTF_8)))));
    }

    @Test
    void sessionAttachWhenNameExceedsSizeLimit() throws Exception
    {
        final String name = Stream.generate(() ->
                String.valueOf('a')).limit(BASE64_LIMIT + 1).collect(Collectors.joining());;
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session<?> session = serverSession.getModelObject();
        final String digest = Base64.getEncoder().encodeToString(MessageDigest.getInstance(MESSAGE_DIGEST_SHA1)
                .digest(name.getBytes(UTF_8)));
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(digest)));
    }

    private SessionAttach createSessionAttach(final String name)
    {
        final SessionAttach attach = new SessionAttach();
        attach.setName(name.getBytes(UTF_8));
        return attach;
    }

    private ServerConnectionDelegate createDelegateAwaitingStartOk()
    {
        final ServerConnectionDelegate delegate = new ServerConnectionDelegate(_port, true, "test");
        delegate.init(_serverConnection, new ProtocolHeader(1, 0, 10));
        return delegate;
    }

    private ConnectionStartOk createConnectionStartOk(final String mechanismName)
    {
        return new ConnectionStartOk(Map.of(), mechanismName, new byte[0], "en_US");
    }
}
