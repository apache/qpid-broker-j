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
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.framing.SASLFrame;
import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManagerFactory;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.virtualhost.ConnectionEstablishmentPolicy;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class ProtocolEngine_1_0_0Test extends UnitTestBase
{
    private AMQPConnection_1_0Impl _protocolEngine_1_0_0;
    private ServerNetworkConnection _networkConnection;
    private Broker<?> _broker;
    private AmqpPort _port;
    private AuthenticationProvider _authenticationProvider;
    private FrameWriter _frameWriter;
    private AMQPConnection _connection;
    private VirtualHost<?> _virtualHost;

    @Before
    public void setUp() throws Exception
    {
        _networkConnection = mock(ServerNetworkConnection.class);
        when(_networkConnection.getLocalAddress()).thenReturn(new InetSocketAddress(0));
        when(_networkConnection.getSelectedHost()).thenReturn("localhost");
        _broker = mock(Broker.class);
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());
        when(_broker.getNetworkBufferSize()).thenReturn(256*1026);
        final TaskExecutor taskExecutor = new TaskExecutorImpl();
        taskExecutor.start();
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getId()).thenReturn(UUID.randomUUID());
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(((Broker) _broker).getCategoryClass()).thenReturn(Broker.class);
        _port = mock(AmqpPort.class);
        when(_port.getChildExecutor()).thenReturn(taskExecutor);
        when(_port.getCategoryClass()).thenReturn(Port.class);
        when(_port.getModel()).thenReturn(BrokerModel.getInstance());
        final SubjectCreator subjectCreator = mock(SubjectCreator.class);
        _authenticationProvider = mock(AuthenticationProvider.class);

        when(_port.getAuthenticationProvider()).thenReturn(_authenticationProvider);
        _virtualHost = mock(VirtualHost.class);
        when(_virtualHost.getChildExecutor()).thenReturn(taskExecutor);
        when(_virtualHost.getModel()).thenReturn(BrokerModel.getInstance());
        when(_virtualHost.getState()).thenReturn(State.ACTIVE);
        when(_virtualHost.isActive()).thenReturn(true);

        final ArgumentCaptor<AMQPConnection> connectionCaptor = ArgumentCaptor.forClass(AMQPConnection.class);
        final ArgumentCaptor<ConnectionEstablishmentPolicy> establishmentPolicyCaptor = ArgumentCaptor.forClass(ConnectionEstablishmentPolicy.class);
        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                _connection = connectionCaptor.getValue();
                return null;
            }
        }).when(_virtualHost).registerConnection(connectionCaptor.capture(), establishmentPolicyCaptor.capture());
        when(_virtualHost.getPrincipal()).thenReturn(mock(VirtualHostPrincipal.class));
        when(_port.getAddressSpace(anyString())).thenReturn(_virtualHost);
        when(_port.getSubjectCreator(anyBoolean(), anyString())).thenReturn(subjectCreator);

        final ArgumentCaptor<Principal> userCaptor = ArgumentCaptor.forClass(Principal.class);
        when(subjectCreator.createSubjectWithGroups(userCaptor.capture())).then(new Answer<Subject>()
        {
            @Override
            public Subject answer(final InvocationOnMock invocation) throws Throwable
            {
                Subject subject = new Subject();
                subject.getPrincipals().add(userCaptor.getValue());
                return subject;
            }
        });

        final ByteBufferSender sender = mock(ByteBufferSender.class);
        when(_networkConnection.getSender()).thenReturn(sender);

        AMQPDescribedTypeRegistry registry = AMQPDescribedTypeRegistry.newInstance().registerTransportLayer()
                .registerMessagingLayer()
                .registerTransactionLayer()
                .registerSecurityLayer();

        _frameWriter = new FrameWriter(registry, new ByteBufferSender()
                                                {

                                                    @Override
                                                    public boolean isDirectBufferPreferred()
                                                    {
                                                        return false;
                                                    }

                                                    @Override
                                                    public void send(final QpidByteBuffer msg)
                                                    {
                                                        _protocolEngine_1_0_0.received(msg);
                                                    }

                                                    @Override
                                                    public void flush()
                                                    {

                                                    }

                                                    @Override
                                                    public void close()
                                                    {

                                                    }
                                                });
    }

    @Test
    public void testProtocolEngineWithNoSaslNonTLSandAnon() throws Exception
    {
        final Map<String, Object> attrs = Collections.singletonMap(ConfiguredObject.NAME, getTestName());
        final AnonymousAuthenticationManager anonymousAuthenticationManager =
                (new AnonymousAuthenticationManagerFactory()).create(null, attrs, _broker);
        when(_port.getAuthenticationProvider()).thenReturn(anonymousAuthenticationManager);
        allowMechanisms(AnonymousAuthenticationManager.MECHANISM_NAME);

        createEngine(Transport.TCP);

        _protocolEngine_1_0_0.received(QpidByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance()
                                                                   .getHeaderIdentifier()));

        Open open = new Open();
        open.setContainerId("testContainerId");
        _frameWriter.send(new TransportFrame((int) (short) 0, open));

        verify(_virtualHost).registerConnection(any(AMQPConnection.class), any(ConnectionEstablishmentPolicy.class));
        AuthenticatedPrincipal principal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(principal);
        assertEquals(principal,
                            new AuthenticatedPrincipal(anonymousAuthenticationManager.getAnonymousPrincipal()));

    }


    @Test
    public void testProtocolEngineWithNoSaslNonTLSandNoAnon() throws Exception
    {
        allowMechanisms("foo");

        createEngine(Transport.TCP);

        _protocolEngine_1_0_0.received(QpidByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        open.setContainerId("testContainerId");
        _frameWriter.send(new TransportFrame((int) (short) 0, open));

        verify(_virtualHost, never()).registerConnection(any(AMQPConnection.class), any(ConnectionEstablishmentPolicy.class));
        verify(_networkConnection).close();
    }


    @Test
    public void testProtocolEngineWithNoSaslTLSandExternal() throws Exception
    {
        final Principal principal = () -> "test";
        when(_networkConnection.getPeerPrincipal()).thenReturn(principal);

        allowMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME);

        createEngine(Transport.SSL);
        _protocolEngine_1_0_0.received(QpidByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        open.setContainerId("testContainerId");
        _frameWriter.send(new TransportFrame((int) (short) 0, open));

        verify(_virtualHost).registerConnection(any(AMQPConnection.class), any(ConnectionEstablishmentPolicy.class));
        AuthenticatedPrincipal authPrincipal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(authPrincipal);
        assertEquals(authPrincipal, new AuthenticatedPrincipal(principal));
    }

    @Test
    public void testProtocolEngineWithSaslNonTLSandAnon() throws Exception
    {
        final Map<String, Object> attrs = Collections.singletonMap(ConfiguredObject.NAME, getTestName());
        final AnonymousAuthenticationManager anonymousAuthenticationManager =
                (new AnonymousAuthenticationManagerFactory()).create(null, attrs, _broker);
        when(_port.getAuthenticationProvider()).thenReturn(anonymousAuthenticationManager);
        when(_port.getSubjectCreator(anyBoolean(), anyString())).thenReturn(new SubjectCreator(anonymousAuthenticationManager, Collections.emptyList(), null));

        allowMechanisms(AnonymousAuthenticationManager.MECHANISM_NAME);

        createEngine(Transport.TCP);

        _protocolEngine_1_0_0.received(QpidByteBuffer.wrap(ProtocolEngineCreator_1_0_0_SASL.getInstance()
                                                                   .getHeaderIdentifier()));

        SaslInit init = new SaslInit();
        init.setMechanism(Symbol.valueOf("ANONYMOUS"));
        _frameWriter.send(new SASLFrame(init));

        _protocolEngine_1_0_0.received(QpidByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance()
                                                                   .getHeaderIdentifier()));

        Open open = new Open();
        open.setContainerId("testContainerId");
        _frameWriter.send(new TransportFrame((int) (short) 0, open));

        verify(_virtualHost).registerConnection(any(AMQPConnection.class), any(ConnectionEstablishmentPolicy.class));
        AuthenticatedPrincipal principal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(principal);
        assertEquals(principal,
                            new AuthenticatedPrincipal(anonymousAuthenticationManager.getAnonymousPrincipal()));
    }


    private void createEngine(Transport transport)
    {
        _protocolEngine_1_0_0 =
                new AMQPConnection_1_0Impl(_broker, _networkConnection, _port, transport, 1, new AggregateTicker());
    }

    private void allowMechanisms(String... mechanisms)
    {
        when(_authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(Arrays.asList(mechanisms));
    }
}
