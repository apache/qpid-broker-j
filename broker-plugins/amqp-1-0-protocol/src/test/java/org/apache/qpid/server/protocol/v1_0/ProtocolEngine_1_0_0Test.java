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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.amqp_1_0.codec.FrameWriter;
import org.apache.qpid.amqp_1_0.framing.AMQFrame;
import org.apache.qpid.amqp_1_0.framing.SASLFrame;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.amqp_1_0.type.security.SaslInit;
import org.apache.qpid.amqp_1_0.type.transport.Open;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.connection.ConnectionRegistry;
import org.apache.qpid.server.connection.IConnectionRegistry;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQConnectionModel;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManagerFactory;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.transport.NetworkConnectionScheduler;
import org.apache.qpid.server.transport.NonBlockingConnection;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;

public class ProtocolEngine_1_0_0Test extends QpidTestCase
{
    private ProtocolEngine_1_0_0 _protocolEngine_1_0_0;
    private NetworkConnection _networkConnection;
    private Broker<?> _broker;
    private AmqpPort<?> _port;
    private SubjectCreator _subjectCreator;
    private AuthenticationProvider _authenticationProvider;
    private List<ByteBuffer> _sentBuffers;
    private FrameWriter _frameWriter;
    private IConnectionRegistry _connectionRegistry;
    private Connection_1_0 _connection;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _networkConnection = mock(NonBlockingConnection.class);
        _broker = mock(Broker.class);
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());
        final TaskExecutor taskExecutor = new TaskExecutorImpl();
        taskExecutor.start();
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getId()).thenReturn(UUID.randomUUID());
        _port = mock(AmqpPort.class);
        _subjectCreator = mock(SubjectCreator.class);
        _authenticationProvider = mock(AuthenticationProvider.class);
        when(_port.getAuthenticationProvider()).thenReturn(_authenticationProvider);
        VirtualHostImpl virtualHost = mock(VirtualHostImpl.class);

        _connectionRegistry = mock(IConnectionRegistry.class);
        final ArgumentCaptor<AMQConnectionModel> connectionCaptor = ArgumentCaptor.forClass(AMQConnectionModel.class);
        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                _connection = (Connection_1_0) connectionCaptor.getValue();
                return null;
            }
        }).when(_connectionRegistry).registerConnection(connectionCaptor.capture());
        when(virtualHost.getConnectionRegistry()).thenReturn(_connectionRegistry);
        when(_port.getVirtualHost(anyString())).thenReturn(virtualHost);
        when(_authenticationProvider.getSubjectCreator(anyBoolean())).thenReturn(_subjectCreator);

        final ArgumentCaptor<Principal> userCaptor = ArgumentCaptor.forClass(Principal.class);
        when(_subjectCreator.createSubjectWithGroups(userCaptor.capture())).then(new Answer<Subject>()
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

        final ArgumentCaptor<ByteBuffer> byteBufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                _sentBuffers.add(byteBufferCaptor.getValue());
                return null;
            }
        }).when(sender).send(byteBufferCaptor.capture());
        _sentBuffers = new ArrayList<>();

        AMQPDescribedTypeRegistry registry = AMQPDescribedTypeRegistry.newInstance().registerTransportLayer()
                .registerMessagingLayer()
                .registerTransactionLayer()
                .registerSecurityLayer();

        _frameWriter = new FrameWriter(registry);
    }

    public void testProtocolEngineWithNoSaslNonTLSandAnon() throws Exception
    {
        allowMechanisms(AnonymousAuthenticationManager.MECHANISM_NAME);
        final boolean useSASL = false;

        createEngine(useSASL, Transport.TCP);

        _protocolEngine_1_0_0.received(ByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        _frameWriter.setValue(AMQFrame.createAMQFrame((short)0,open));
        ByteBuffer buf = ByteBuffer.allocate(64*1024);
        _frameWriter.writeToBuffer(buf);
        buf.flip();
        _protocolEngine_1_0_0.received(buf);

        verify(_connectionRegistry).registerConnection(any(AMQConnectionModel.class));
        AuthenticatedPrincipal principal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(principal);
        assertEquals(principal, new AuthenticatedPrincipal(AnonymousAuthenticationManager.ANONYMOUS_PRINCIPAL));
    }


    public void testProtocolEngineWithNoSaslNonTLSandNoAnon() throws Exception
    {
        allowMechanisms("foo");
        final boolean useSASL = false;

        createEngine(useSASL, Transport.TCP);

        _protocolEngine_1_0_0.received(ByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        _frameWriter.setValue(AMQFrame.createAMQFrame((short)0,open));
        ByteBuffer buf = ByteBuffer.allocate(64*1024);
        _frameWriter.writeToBuffer(buf);
        buf.flip();
        _protocolEngine_1_0_0.received(buf);

        verify(_connectionRegistry, never()).registerConnection(any(AMQConnectionModel.class));
        verify(_networkConnection).close();
    }


    public void testProtocolEngineWithNoSaslTLSandExternal() throws Exception
    {
        final Principal principal = new Principal()
        {
            @Override
            public String getName()
            {
                return "test";
            }
        };
        when(_networkConnection.getPeerPrincipal()).thenReturn(principal);

        allowMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME);
        final boolean useSASL = false;

        createEngine(useSASL, Transport.SSL);
        _protocolEngine_1_0_0.received(ByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        _frameWriter.setValue(AMQFrame.createAMQFrame((short)0,open));
        ByteBuffer buf = ByteBuffer.allocate(64*1024);
        _frameWriter.writeToBuffer(buf);
        buf.flip();
        _protocolEngine_1_0_0.received(buf);

        verify(_connectionRegistry).registerConnection(any(AMQConnectionModel.class));
        AuthenticatedPrincipal authPrincipal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(authPrincipal);
        assertEquals(authPrincipal, new AuthenticatedPrincipal(principal));
    }

    public void testProtocolEngineWithSaslNonTLSandAnon() throws Exception
    {
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put(ConfiguredObject.NAME, getTestName());
        final AuthenticationProvider anonymousAuthenticationManager =
                (new AnonymousAuthenticationManagerFactory())
                        .create(
                                null,
                                attrs,
                                _broker);
        when(_port.getAuthenticationProvider()).thenReturn(anonymousAuthenticationManager);
        allowMechanisms(AnonymousAuthenticationManager.MECHANISM_NAME);
        final boolean useSASL = true;

        createEngine(useSASL, Transport.TCP);

        _protocolEngine_1_0_0.received(ByteBuffer.wrap(ProtocolEngineCreator_1_0_0_SASL.getInstance().getHeaderIdentifier()));

        SaslInit init = new SaslInit();
        init.setMechanism(Symbol.valueOf("ANONYMOUS"));
        _frameWriter.setValue(new SASLFrame(init));
        ByteBuffer buf = ByteBuffer.allocate(64*1024);
        _frameWriter.writeToBuffer(buf);

        buf.flip();
        _protocolEngine_1_0_0.received(buf);

        _protocolEngine_1_0_0.received(ByteBuffer.wrap(ProtocolEngineCreator_1_0_0.getInstance().getHeaderIdentifier()));

        Open open = new Open();
        _frameWriter.setValue(AMQFrame.createAMQFrame((short)0,open));
        buf = ByteBuffer.allocate(64*1024);
        _frameWriter.writeToBuffer(buf);
        buf.flip();
        _protocolEngine_1_0_0.received(buf);

        verify(_connectionRegistry).registerConnection(any(AMQConnectionModel.class));
        AuthenticatedPrincipal principal = (AuthenticatedPrincipal) _connection.getAuthorizedPrincipal();
        assertNotNull(principal);
        assertEquals(principal, new AuthenticatedPrincipal(AnonymousAuthenticationManager.ANONYMOUS_PRINCIPAL));
    }


    private void createEngine(final boolean useSASL, Transport transport)
    {
        _protocolEngine_1_0_0 = new ProtocolEngine_1_0_0(_networkConnection,
                                                         _broker, 1, _port, transport, new AggregateTicker(),
                                                         useSASL);
    }

    private void allowMechanisms(String... mechanisms)
    {
        when(_subjectCreator.getMechanisms()).thenReturn(Arrays.asList(mechanisms));
    }
}
