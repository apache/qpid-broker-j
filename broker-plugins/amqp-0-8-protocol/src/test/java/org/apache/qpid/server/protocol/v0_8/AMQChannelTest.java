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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.NullMessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMemoryMessage;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQChannelTest extends UnitTestBase
{
    public static final AMQShortString ROUTING_KEY = AMQShortString.valueOf("routingKey");

    private QueueManagingVirtualHost<?> _virtualHost;
    private AMQPConnection_0_8 _amqConnection;
    private MessageStore _messageStore;
    private AmqpPort<?> _port;
    private Broker<?> _broker;
    private ProtocolOutputConverter _protocolOutputConverter;
    private MessageDestination _messageDestination;

    @Before
    public void setUp() throws Exception
    {

        TaskExecutor taskExecutor = mock(TaskExecutor.class);

        _broker = mock(Broker.class);
        when(_broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(_broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(1l);

        _messageStore = mock(MessageStore.class);

        _virtualHost = mock(QueueManagingVirtualHost.class);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1l);
        when(_virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_virtualHost.getPrincipal()).thenReturn(mock(Principal.class));
        when(_virtualHost.getEventLogger()).thenReturn(mock(EventLogger.class));

        _port = mock(AmqpPort.class);
        when(_port.getChildExecutor()).thenReturn(taskExecutor);
        when(_port.getModel()).thenReturn(BrokerModel.getInstance());
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(1);

        AuthenticatedPrincipal authenticatedPrincipal = new AuthenticatedPrincipal(new UsernamePrincipal("user", null));
        Set<Principal> authenticatedUser = Collections.<Principal>singleton(authenticatedPrincipal);
        Subject authenticatedSubject = new Subject(true, authenticatedUser, Collections.<Principal>emptySet(), Collections.<Principal>emptySet());

        _protocolOutputConverter = mock(ProtocolOutputConverter.class);

        _amqConnection = mock(AMQPConnection_0_8.class);
        when(_amqConnection.getSubject()).thenReturn(authenticatedSubject);
        when(_amqConnection.getAuthorizedPrincipal()).thenReturn(authenticatedPrincipal);
        when(_amqConnection.getAddressSpace()).thenReturn((VirtualHost)_virtualHost);
        when(_amqConnection.getProtocolOutputConverter()).thenReturn(_protocolOutputConverter);
        when(_amqConnection.getBroker()).thenReturn((Broker) _broker);
        when(_amqConnection.getMethodRegistry()).thenReturn(new MethodRegistry(ProtocolVersion.v0_9));
        when(_amqConnection.getContextProvider()).thenReturn(_virtualHost);
        when(_amqConnection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(_amqConnection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        when(_amqConnection.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(Connection.DEFAULT_MAX_UNCOMMITTED_IN_MEMORY_SIZE);
        when(_amqConnection.getContextValue(Boolean.class, AMQPConnection_0_8.FORCE_MESSAGE_VALIDATION)).thenReturn(true);
        when(_amqConnection.getTaskExecutor()).thenReturn(taskExecutor);
        when(_amqConnection.getChildExecutor()).thenReturn(taskExecutor);
        when(_amqConnection.getModel()).thenReturn(BrokerModel.getInstance());

        when(_amqConnection.getContextValue(Long.class, AMQPConnection_0_8.BATCH_LIMIT)).thenReturn(AMQPConnection_0_8.DEFAULT_BATCH_LIMIT);
        when(_amqConnection.getContextValue(Long.class, AMQPConnection_0_8.HIGH_PREFETCH_LIMIT)).thenReturn(AMQPConnection_0_8.DEFAULT_BATCH_LIMIT);

        when(_amqConnection.getEventLogger()).thenReturn(mock(EventLogger.class));
        _messageDestination = mock(MessageDestination.class);
    }

    @Test
    public void testReceiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasBindings() throws Exception
    {
        String testExchangeName = getTestName();
        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(true);
        doReturn(exchange).when(_virtualHost).getAttainedMessageDestination(eq(testExchangeName), anyBoolean());

        AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);

        channel.receiveExchangeDelete(AMQShortString.valueOf(testExchangeName), true, false);

        verify(_amqConnection).closeChannelAndWriteFrame(eq(channel),
                                                         eq(ErrorCodes.IN_USE),
                                                         eq("Exchange has bindings"));
    }

    @Test
    public void testReceiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasNoBinding() throws Exception
    {
        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(false);
        doReturn(exchange).when(_virtualHost).getAttainedMessageDestination(eq(getTestName()), anyBoolean());

        AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);
        channel.receiveExchangeDelete(AMQShortString.valueOf(getTestName()), true, false);

        verify(exchange).delete();
    }

    @Test
    public void testOversizedMessageClosesChannel() throws Exception
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(mock(MessageDestination.class));

        long maximumMessageSize = 1024l;
        when(_amqConnection.getMaxMessageSize()).thenReturn(maximumMessageSize);
        AMQChannel channel = new AMQChannel(_amqConnection, 1, _virtualHost.getMessageStore());

        BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);
        channel.receiveMessageHeader(properties, maximumMessageSize + 1);

        verify(_amqConnection).closeChannelAndWriteFrame(eq(channel),
                                                         eq(ErrorCodes.MESSAGE_TOO_LARGE),
                                                         eq("Message size of 1025 greater than allowed maximum of 1024"));

    }

    @Test
    public void testPublishContentHeaderWhenMessageAuthorizationFails() throws Exception
    {
        final String impostorId = "impostor";
        doThrow(new AccessControlException("fail")).when(_amqConnection).checkAuthorizedMessagePrincipal(eq(impostorId));
        when(_virtualHost.getDefaultDestination()).thenReturn(mock(MessageDestination.class));
        when(_virtualHost.getMessageStore()).thenReturn(new NullMessageStore()
        {
            @Override
            public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
            {
                MessageHandle messageHandle = new StoredMemoryMessage(1, metaData);
                return messageHandle;
            }
        });


        int channelId = 1;
        AMQChannel channel = new AMQChannel(_amqConnection, channelId, _virtualHost.getMessageStore());

        BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setUserId(impostorId);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);
        channel.receiveMessageHeader(properties, 0);

        verify(_amqConnection).sendConnectionClose(eq(ErrorCodes.ACCESS_REFUSED), anyString(), eq(channelId));
        verifyNoInteractions(_messageDestination);
    }

    @Test
    public void testPublishContentHeaderWhenMessageAuthorizationSucceeds() throws Exception
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);
        when(_virtualHost.getMessageStore()).thenReturn(new NullMessageStore()
        {
            @Override
            public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
            {
                MessageHandle messageHandle = new StoredMemoryMessage(1, metaData);
                return messageHandle;
            }
        });
        final ArgumentCaptor<ServerMessage> messageCaptor = ArgumentCaptor.forClass(ServerMessage.class);
        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                ServerMessage message = messageCaptor.getValue();
                return new RoutingResult(message);
            }
        }).when(_messageDestination).route(messageCaptor.capture(), eq(ROUTING_KEY.toString()), any(InstanceProperties.class));
        AMQChannel channel = new AMQChannel(_amqConnection, 1, _virtualHost.getMessageStore());

        BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setUserId(_amqConnection.getAuthorizedPrincipal().getName());
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 0);

        verify(_messageDestination).route((ServerMessage) any(),
                                         eq(ROUTING_KEY.toString()),
                                         any(InstanceProperties.class));
    }
}
