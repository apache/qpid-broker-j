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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.security.AccessDeniedException;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.NullMessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMemoryMessage;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class AMQChannelTest extends UnitTestBase
{
    public static final AMQShortString ROUTING_KEY = AMQShortString.valueOf("routingKey");

    private QueueManagingVirtualHost<?> _virtualHost;
    private AMQPConnection_0_8 _amqConnection;
    private MessageStore _messageStore;
    private MessageDestination _messageDestination;

    @BeforeEach
    void setUp() throws Exception
    {
        final TaskExecutor taskExecutor = mock(TaskExecutor.class);

        final Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(broker.getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)).thenReturn(1L);

        _messageStore = mock(MessageStore.class);

        _virtualHost = mock(QueueManagingVirtualHost.class);
        when(_virtualHost.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(1);
        when(_virtualHost.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(1L);
        when(_virtualHost.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_virtualHost.getPrincipal()).thenReturn(mock(Principal.class));
        when(_virtualHost.getEventLogger()).thenReturn(mock(EventLogger.class));

        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getChildExecutor()).thenReturn(taskExecutor);
        when(port.getModel()).thenReturn(BrokerModel.getInstance());
        when(port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(1);

        final AuthenticatedPrincipal authenticatedPrincipal = new AuthenticatedPrincipal(new UsernamePrincipal("user", null));
        final Set<Principal> authenticatedUser = Set.of(authenticatedPrincipal);
        final Subject authenticatedSubject = new Subject(true, authenticatedUser, Set.of(), Set.of());

        final ProtocolOutputConverter protocolOutputConverter = mock(ProtocolOutputConverter.class);

        _amqConnection = mock(AMQPConnection_0_8.class);
        when(_amqConnection.getSubject()).thenReturn(authenticatedSubject);
        when(_amqConnection.getAuthorizedPrincipal()).thenReturn(authenticatedPrincipal);
        when(_amqConnection.getAddressSpace()).thenReturn(_virtualHost);
        when(_amqConnection.getProtocolVersion()).thenReturn(ProtocolVersion.v0_9);
        when(_amqConnection.getProtocolOutputConverter()).thenReturn(protocolOutputConverter);
        when(_amqConnection.getBroker()).thenReturn(broker);
        when(_amqConnection.getMethodRegistry()).thenReturn(new MethodRegistry(ProtocolVersion.v0_9));
        when(_amqConnection.getContextProvider()).thenReturn(_virtualHost);
        when(_amqConnection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(_amqConnection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        when(_amqConnection.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(Connection.DEFAULT_MAX_UNCOMMITTED_IN_MEMORY_SIZE);
        when(_amqConnection.getContextValue(Boolean.class, AMQPConnection_0_8.FORCE_MESSAGE_VALIDATION)).thenReturn(true);
        when(_amqConnection.getContextValue(Integer.class, AMQPConnection_0_8.CONNECTION_MAX_CONTENT_BODY_FRAMES_PER_MESSAGE))
                .thenReturn(AMQPConnection_0_8.DEFAULT_MAX_CONTENT_BODY_FRAMES_PER_MESSAGE);
        when(_amqConnection.getTaskExecutor()).thenReturn(taskExecutor);
        when(_amqConnection.getChildExecutor()).thenReturn(taskExecutor);
        when(_amqConnection.getModel()).thenReturn(BrokerModel.getInstance());

        when(_amqConnection.getContextValue(Long.class, AMQPConnection_0_8.BATCH_LIMIT)).thenReturn(AMQPConnection_0_8.DEFAULT_BATCH_LIMIT);
        when(_amqConnection.getContextValue(Long.class, AMQPConnection_0_8.HIGH_PREFETCH_LIMIT)).thenReturn(AMQPConnection_0_8.DEFAULT_BATCH_LIMIT);

        when(_amqConnection.getEventLogger()).thenReturn(mock(EventLogger.class));
        _messageDestination = mock(MessageDestination.class);
    }

    @Test
    void receiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasBindings()
    {
        final String testExchangeName = getTestName();
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(true);
        doReturn(exchange).when(_virtualHost).getAttainedMessageDestination(eq(testExchangeName), anyBoolean());

        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);

        channel.receiveExchangeDelete(AMQShortString.valueOf(testExchangeName), true, false);

        verify(_amqConnection).closeChannelAndWriteFrame(channel, ErrorCodes.IN_USE, "Exchange has bindings");
    }

    @Test
    void constructorLogsUnderSubject()
    {
        final AtomicReference<Subject> loggedSubject = new AtomicReference<>();
        final EventLogger eventLogger = mock(EventLogger.class);
        doAnswer(invocation ->
        {
            loggedSubject.set(SubjectExecutionContext.currentSubject());
            return null;
        }).when(eventLogger).message(any(LogMessage.class));
        when(_amqConnection.getEventLogger()).thenReturn(eventLogger);

        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);

        final Subject subject = loggedSubject.get();
        assertNotNull(subject, "Subject not captured during log");
        assertTrue(subject.getPrincipals().containsAll(_amqConnection.getSubject().getPrincipals()),
                   "Missing principals from connection subject");
        final Set<SessionPrincipal> sessionPrincipals = subject.getPrincipals(SessionPrincipal.class);
        assertTrue(sessionPrincipals.size() == 1, "Expected single SessionPrincipal");
        assertSame(channel, sessionPrincipals.iterator().next().getSession(),
                   "SessionPrincipal should reference the channel");
    }

    @Test
    void receiveExchangeDeleteWhenIfUsedIsSetAndExchangeHasNoBinding()
    {
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.hasBindings()).thenReturn(false);
        doReturn(exchange).when(_virtualHost).getAttainedMessageDestination(eq(getTestName()), anyBoolean());

        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);
        channel.receiveExchangeDelete(AMQShortString.valueOf(getTestName()), true, false);

        verify(exchange).delete();
    }

    @Test
    void oversizedMessageClosesChannel()
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(mock(MessageDestination.class));

        final long maximumMessageSize = 1024L;
        when(_amqConnection.getMaxMessageSize()).thenReturn(maximumMessageSize);
        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _virtualHost.getMessageStore());

        final BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);
        channel.receiveMessageHeader(properties, maximumMessageSize + 1);

        verify(_amqConnection).closeChannelAndWriteFrame(channel,
                                                         ErrorCodes.MESSAGE_TOO_LARGE,
                                                         "Message size of 1025 greater than allowed maximum of 1024");

    }

    @Test
    void unsignedOversizedMessageClosesChannel()
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);

        channel.receiveOversizedMessageHeader(Long.MIN_VALUE);

        verify(_amqConnection).closeChannelAndWriteFrame(channel, ErrorCodes.MESSAGE_TOO_LARGE,
                "Content body size 9223372036854775808 exceeds the supported range");
    }

    @Test
    void unsignedOversizedMessageWithoutPublishClosesConnection()
    {
        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);

        channel.receiveOversizedMessageHeader(Long.MIN_VALUE);

        verify(_amqConnection).sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                "Attempt to send a content header without first sending a publish frame", channelId);
    }

    @Test
    void unsignedOversizedDuplicateHeaderUsesContentSequenceError()
    {
        when(_amqConnection.getProtocolVersion()).thenReturn(ProtocolVersion.v0_91);
        when(_amqConnection.getMaxMessageSize()).thenReturn(1L);
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);
        final BasicContentHeaderProperties properties = mock(BasicContentHeaderProperties.class);
        when(properties.checkValid()).thenReturn(true);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 1L);

        channel.receiveOversizedMessageHeader(Long.MIN_VALUE);

        verify(properties).dispose();
        verify(_amqConnection).sendConnectionClose(ErrorCodes.UNEXPECTED_FRAME,
                "Attempt to send a duplicate content header", channelId);
    }

    @Test
    void contentBodyFrameLimitDisposesIncompleteMessageAndClosesConnection()
    {
        final int maximumContentBodyFrames = 2;
        when(_amqConnection.getContextValue(Integer.class,
                AMQPConnection_0_8.CONNECTION_MAX_CONTENT_BODY_FRAMES_PER_MESSAGE))
                .thenReturn(maximumContentBodyFrames);
        when(_amqConnection.getMaxMessageSize()).thenReturn(3L);
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final BasicContentHeaderProperties properties = mock(BasicContentHeaderProperties.class);
        when(properties.checkValid()).thenReturn(true);
        final QpidByteBuffer content = mock(QpidByteBuffer.class);
        final QpidByteBuffer firstRetainedContent = mock(QpidByteBuffer.class);
        final QpidByteBuffer secondRetainedContent = mock(QpidByteBuffer.class);
        when(content.remaining()).thenReturn(1);
        when(content.duplicate()).thenReturn(firstRetainedContent, secondRetainedContent);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 3L);

        channel.receiveMessageContent(content);
        channel.receiveMessageContent(content);
        channel.receiveMessageContent(content);

        verify(properties).dispose();
        verify(firstRetainedContent).dispose();
        verify(secondRetainedContent).dispose();
        verify(_amqConnection).sendConnectionClose(ErrorCodes.RESOURCE_ERROR,
                "Message exceeds maximum number of content body frames (" + maximumContentBodyFrames + ")", channelId);
        channel.dispose();
    }

    @Test
    void excessiveContentBodyDisposesIncompleteMessageAndClosesConnection()
    {
        when(_amqConnection.getMaxMessageSize()).thenReturn(2L);
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final BasicContentHeaderProperties properties = mock(BasicContentHeaderProperties.class);
        when(properties.checkValid()).thenReturn(true);
        final QpidByteBuffer content = mock(QpidByteBuffer.class);
        final QpidByteBuffer retainedContent = mock(QpidByteBuffer.class);
        when(content.remaining()).thenReturn(1);
        when(content.duplicate()).thenReturn(retainedContent);
        final QpidByteBuffer excessiveContent = mock(QpidByteBuffer.class);
        when(excessiveContent.remaining()).thenReturn(2);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 2L);
        channel.receiveMessageContent(content);

        channel.receiveMessageContent(excessiveContent);

        verify(properties).dispose();
        verify(retainedContent).dispose();
        verify(excessiveContent, never()).duplicate();
        verify(_amqConnection).sendConnectionClose(ErrorCodes.FRAME_ERROR,
                "More message data received than content header defined", channelId);
        channel.dispose();
    }

    @Test
    void contentBodyBeforeHeaderClosesConnection()
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);

        try (final QpidByteBuffer emptyContent = QpidByteBuffer.wrap(new byte[0]))
        {
            channel.receiveMessageContent(emptyContent);
        }

        verify(_amqConnection).sendConnectionClose(ErrorCodes.FRAME_ERROR,
                "Attempt to send a content body before sending a content header", channelId);
        channel.dispose();
    }

    @Test
    void methodFrameRejectionDisposesIncompleteMessageAndClosesConnection()
    {
        when(_amqConnection.getMaxMessageSize()).thenReturn(2L);
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final BasicContentHeaderProperties properties = mock(BasicContentHeaderProperties.class);
        when(properties.checkValid()).thenReturn(true);
        final QpidByteBuffer content = mock(QpidByteBuffer.class);
        final QpidByteBuffer retainedContent = mock(QpidByteBuffer.class);
        when(content.remaining()).thenReturn(1);
        when(content.duplicate()).thenReturn(retainedContent);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        assertFalse(channel.rejectMethodFrameIfContentIncomplete());
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 2L);
        channel.receiveMessageContent(content);

        assertTrue(channel.rejectMethodFrameIfContentIncomplete());

        verify(properties).dispose();
        verify(retainedContent).dispose();
        verify(_amqConnection).sendConnectionClose(ErrorCodes.FRAME_ERROR,
                "Method frame received before completing the previous message", channelId);
        channel.dispose();
    }

    @Test
    void duplicateContentHeaderDisposesBothHeadersAndClosesConnection()
    {
        when(_amqConnection.getProtocolVersion()).thenReturn(ProtocolVersion.v0_91);
        when(_amqConnection.getMaxMessageSize()).thenReturn(1L);
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);

        final BasicContentHeaderProperties firstProperties = mock(BasicContentHeaderProperties.class);
        when(firstProperties.checkValid()).thenReturn(true);
        final BasicContentHeaderProperties secondProperties = mock(BasicContentHeaderProperties.class);

        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _messageStore);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(firstProperties, 1L);

        channel.receiveMessageHeader(secondProperties, 1L);

        verify(firstProperties).dispose();
        verify(secondProperties).dispose();
        verify(_amqConnection).sendConnectionClose(ErrorCodes.UNEXPECTED_FRAME,
                "Attempt to send a duplicate content header", channelId);
        channel.dispose();
    }

    @Test
    void publishContentHeaderWhenMessageAuthorizationFails()
    {
        final String impostorId = "impostor";
        doThrow(new AccessDeniedException("fail")).when(_amqConnection).checkAuthorizedMessagePrincipal(impostorId);
        when(_virtualHost.getDefaultDestination()).thenReturn(mock(MessageDestination.class));
        when(_virtualHost.getMessageStore()).thenReturn(new NullMessageStore()
        {
            @Override
            public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
            {
                return (MessageHandle) new StoredMemoryMessage(1, metaData);
            }
        });


        final int channelId = 1;
        final AMQChannel channel = new AMQChannel(_amqConnection, channelId, _virtualHost.getMessageStore());

        final BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setUserId(impostorId);
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, AMQShortString.EMPTY_STRING, false, false);
        channel.receiveMessageHeader(properties, 0);

        verify(_amqConnection).sendConnectionClose(eq(ErrorCodes.ACCESS_REFUSED), anyString(), eq(channelId));
        verifyNoInteractions(_messageDestination);
    }

    @Test
    void publishContentHeaderWhenMessageAuthorizationSucceeds()
    {
        when(_virtualHost.getDefaultDestination()).thenReturn(_messageDestination);
        when(_virtualHost.getMessageStore()).thenReturn(new NullMessageStore()
        {
            @Override
            public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
            {
                return (MessageHandle) new StoredMemoryMessage(1, metaData);
            }
        });
        final ArgumentCaptor<ServerMessage> messageCaptor = ArgumentCaptor.forClass(ServerMessage.class);
        doAnswer(invocation ->
        {
            final ServerMessage message = messageCaptor.getValue();
            return new RoutingResult(message);
        }).when(_messageDestination).route(messageCaptor.capture(), eq(ROUTING_KEY.toString()), any(InstanceProperties.class));
        final AMQChannel channel = new AMQChannel(_amqConnection, 1, _virtualHost.getMessageStore());

        final BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setUserId(_amqConnection.getAuthorizedPrincipal().getName());
        channel.receiveBasicPublish(AMQShortString.EMPTY_STRING, ROUTING_KEY, false, false);
        channel.receiveMessageHeader(properties, 0);

        verify(_messageDestination).route((ServerMessage) any(), eq(ROUTING_KEY.toString()), any(InstanceProperties.class));
    }
}
