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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.transport.SessionAttach;
import org.apache.qpid.test.utils.UnitTestBase;

public class ServerConnectionDelegateTest extends UnitTestBase
{

    private ServerConnectionDelegate _delegate;
    private ServerConnection _serverConnection;
    private TaskExecutor _taskExecutor;
    private AccessControlContext _accessControlContext;

    @Before
    public void setUp()
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final Broker broker = mock(Broker.class);
        when(broker.getNetworkBufferSize()).thenReturn(0xffff);
        when(broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(Long.MAX_VALUE);
        when(broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());
        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(Collections.singletonList("PLAIN"));
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getAuthenticationProvider()).thenReturn(authenticationProvider);
        when(port.getParent()).thenReturn(broker);

        _delegate = new ServerConnectionDelegate(port, true, "test");
        _delegate.setState(ServerConnectionDelegate.ConnectionState.OPEN);
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        when(addressSpace.getConnections()).thenReturn(Collections.emptyList());

        final Subject subject = new Subject();
        subject.setReadOnly();
         _accessControlContext = AccessController.getContext();
        final AMQPConnection_0_10 amqpConnection = mock(AMQPConnection_0_10.class);
        when(amqpConnection.getParent()).thenReturn(broker);
        when(amqpConnection.getBroker()).thenReturn(broker);
        when(amqpConnection.getChildExecutor()).thenReturn(_taskExecutor);
        when(amqpConnection.getModel()).thenReturn(BrokerModel.getInstance());
        when(amqpConnection.getSubject()).thenReturn(subject);
        when(amqpConnection.getContextValue(Long.class, org.apache.qpid.server.model.Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Long.MAX_VALUE);
        when(amqpConnection.getContextValue(Integer.class, org.apache.qpid.server.model.Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Integer.MAX_VALUE);
        doAnswer((Answer<AccessControlContext>) invocationOnMock -> {
            Subject subject1 = (Subject) invocationOnMock.getArgument(0);
            return AccessController.doPrivileged(
                    (PrivilegedAction<AccessControlContext>) () ->
                            new AccessControlContext(_accessControlContext, new SubjectDomainCombiner(subject1)));
        }).when(amqpConnection).getAccessControlContextFromSubject(any());
        when(amqpConnection.getEventLogger()).thenReturn(mock(EventLogger.class));

        _serverConnection = mock(ServerConnection.class);
        when(_serverConnection.getAddressSpace()).thenReturn(addressSpace);
        when(_serverConnection.getBroker()).thenReturn(broker);
        when(_serverConnection.getAmqpConnection()).thenReturn(amqpConnection);
    }

    @After
    public void tearDown()
    {
        _taskExecutor.stop();
    }

    @Test
    public void sessionAttachWhenNameIsUUID()
    {
        final String name = UUID.randomUUID().toString();
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session session = serverSession.getModelObject();
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(name)));
    }

    @Test
    public void sessionAttachWhenNameIsNotUUID()
    {
        final String name = "ABC";
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session session = serverSession.getModelObject();
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(Base64.getEncoder().encodeToString(name.getBytes(UTF_8)))));
    }

    @Test
    public void sessionAttachWhenNameExceedsSizeLimit() throws Exception
    {
        final String name = Stream.generate(() -> String.valueOf('a')).limit(BASE64_LIMIT + 1).collect(Collectors.joining());;
        final SessionAttach attach = createSessionAttach(name);

        _delegate.sessionAttach(_serverConnection, attach);

        final ArgumentCaptor<ServerSession> sessionCaptor = ArgumentCaptor.forClass(ServerSession.class);
        verify(_serverConnection).registerSession(sessionCaptor.capture());

        final ServerSession serverSession = sessionCaptor.getValue();
        final Session session = serverSession.getModelObject();
        final String digest = Base64.getEncoder().encodeToString(MessageDigest.getInstance(MESSAGE_DIGEST_SHA1).digest(name.getBytes(UTF_8)));
        assertThat(session.getPeerSessionName(), CoreMatchers.is(equalTo(digest)));
    }

    private SessionAttach createSessionAttach(final String name)
    {
        final SessionAttach attach = new SessionAttach();
        attach.setName(name.getBytes(UTF_8));
        return attach;
    }
}