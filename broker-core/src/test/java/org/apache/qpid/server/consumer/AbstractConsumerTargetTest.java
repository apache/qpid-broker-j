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

package org.apache.qpid.server.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mockito.InOrder;

import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.MessageSource.MessageConversionExceptionHandlingPolicy;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.mimecontentconverter.AmqpCompoundMimeContentToObjectConverter;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractConsumerTargetTest extends UnitTestBase
{
    private final AMQPConnection<?> _connection = mock(AMQPConnection.class);
    @SuppressWarnings("unchecked")
    private final AMQPSession<?,TestAbstractConsumerTarget> _session = mock(AMQPSession.class);

    private TestAbstractConsumerTarget _consumerTarget;
    private Consumer<?, ?> _consumer;
    private MessageSource _messageSource;
    private MessageInstance _messageInstance;
    private MessageReference<?> _messageReference;

    @BeforeEach
    public void setUp() throws Exception
    {
        when(_connection.getContextValue(eq(Long.class), eq(Consumer.SUSPEND_NOTIFICATION_PERIOD))).thenReturn(1000000L);

        _consumer = mock(Consumer.class);
        _messageSource = mock(MessageSource.class);
        when(_messageSource.getMessageConversionExceptionHandlingPolicy())
                .thenReturn(MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE);
        _messageInstance = mock(MessageInstance.class);
        when(_messageInstance.getOwningResource()).thenReturn(_messageSource);
        _messageReference = mock(MessageReference.class);
        final MessageContainer messageContainer = new MessageContainer(_messageInstance, _messageReference);
        when(_consumer.pullMessage()).thenReturn(messageContainer);
        _consumerTarget = new TestAbstractConsumerTarget();
        _consumerTarget.consumerAdded(_consumer);
    }

    @Test
    public void testClose()
    {
        _consumerTarget = new TestAbstractConsumerTarget();
        assertEquals(0, (long) _consumerTarget.getConsumers().size(), "Unexpected number of consumers");

        _consumerTarget.consumerAdded(_consumer);
        assertEquals(1, (long) _consumerTarget.getConsumers().size(), "Unexpected number of consumers after add");

        _consumerTarget.close();
        assertEquals(0, (long) _consumerTarget.getConsumers().size(), "Unexpected number of consumers after close");

        verify(_consumer, times(1)).close();
    }

    @Test
    public void testNotifyWork()
    {
        final InOrder order = inOrder(_consumer);

        _consumerTarget = new TestAbstractConsumerTarget();
        assertEquals(0, (long) _consumerTarget.getConsumers().size(), "Unexpected number of consumers");

        _consumerTarget.consumerAdded(_consumer);

        _consumerTarget.setNotifyWorkDesired(true);
        order.verify(_consumer, times(1)).setNotifyWorkDesired(true);

        _consumerTarget.setNotifyWorkDesired(false);
        order.verify(_consumer, times(1)).setNotifyWorkDesired(false);

        _consumerTarget.setNotifyWorkDesired(true);
        order.verify(_consumer, times(1)).setNotifyWorkDesired(true);

        _consumerTarget.setNotifyWorkDesired(true);
        // no change of state - should not be propagated to the consumer

        _consumerTarget.close();
        order.verify(_consumer, times(1)).setNotifyWorkDesired(false);
        order.verify(_consumer, times(1)).close();

        verifyNoMoreInteractions(_consumer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    public void testConversionExceptionPolicyClose(final boolean compoundDecoderRejection)
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE, compoundDecoderRejection);

        final ConnectionScopedRuntimeException thrown = assertThrows(ConnectionScopedRuntimeException.class,
                () -> _consumerTarget.sendNextMessage(), "Exception not thrown");
        final boolean condition = thrown.getCause() instanceof MessageConversionException;
        assertTrue(condition, String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                                            thrown.getCause().getClass().getSimpleName()));

        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConversionExceptionPolicyCloseForNonAcquiringConsumer(final boolean compoundDecoderRejection)
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE,
                compoundDecoderRejection);

        final ConnectionScopedRuntimeException thrown = assertThrows(ConnectionScopedRuntimeException.class,
                () -> _consumerTarget.sendNextMessage(), "Exception not thrown");
        final boolean condition = thrown.getCause() instanceof MessageConversionException;
        assertTrue(condition, String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                thrown.getCause().getClass().getSimpleName()));
        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConversionExceptionPolicyReroute(final boolean compoundDecoderRejection)
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.ROUTE_TO_ALTERNATE,
                compoundDecoderRejection);

        _consumerTarget.sendNextMessage();
        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance).routeToAlternate(null, null, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConversionExceptionPolicyRerouteForNonAcquiringConsumer(final boolean compoundDecoderRejection)
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.ROUTE_TO_ALTERNATE,
                compoundDecoderRejection);

        _consumerTarget.sendNextMessage();
        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConversionExceptionPolicyReject(final boolean compoundDecoderRejection)
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.REJECT,
                compoundDecoderRejection);

        _consumerTarget.sendNextMessage();

        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance).reject(_consumer);
        verify(_messageInstance).release(_consumer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConversionExceptionPolicyRejectForNonAcquiringConsumer(final boolean compoundDecoderRejection)
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.REJECT,
                compoundDecoderRejection);

        _consumerTarget.sendNextMessage();
        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance).reject(_consumer);
        verify(_messageInstance).release(_consumer);
    }

    @Test
    public void testConversionExceptionPolicyWhenOwningResourceIsNotMessageSource()
    {
        final TransactionLogResource owningResource = mock(TransactionLogResource.class);
        when(_messageInstance.getOwningResource()).thenReturn(owningResource);

        final ConnectionScopedRuntimeException thrown = assertThrows(ConnectionScopedRuntimeException.class,
                () -> _consumerTarget.sendNextMessage(), "Exception not thrown");
        final boolean condition = thrown.getCause() instanceof MessageConversionException;
        assertTrue(condition, String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                                            thrown.getCause().getClass().getSimpleName()));
        assertTrue(_consumerTarget.isCreditRestored(), "message credit was not restored");
        verify(_messageReference).release();
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    private void configureBehaviour(final boolean acquires,
                                    final MessageConversionExceptionHandlingPolicy exceptionHandlingPolicy,
                                    final boolean compoundDecoderRejection)
    {
        when(_consumer.acquires()).thenReturn(acquires);
        when(_messageSource.getMessageConversionExceptionHandlingPolicy()).thenReturn(exceptionHandlingPolicy);
        if (compoundDecoderRejection)
        {
            final AmqpCompoundMimeContentToObjectConverter<?> converter =
                    mock(AmqpCompoundMimeContentToObjectConverter.class);
            final byte[] content = new byte[0];
            when(converter.toObject(content, 0, 1)).thenThrow(new IllegalArgumentException("Compound limit exceeded"));
            _consumerTarget._sendAction = () ->
                    AmqpCompoundMimeContentToObjectConverter.toObject(converter, content, 0, 1);
        }
    }

    @SuppressWarnings("rawtypes")
    private class TestAbstractConsumerTarget extends AbstractConsumerTarget<TestAbstractConsumerTarget>
    {
        private Runnable _sendAction = () ->
        {
            throw new MessageConversionException("testException");
        };
        private boolean _creditRestored;

        TestAbstractConsumerTarget()
        {
            super(false, _connection);
        }

        @Override
        protected void doSend(final MessageInstanceConsumer consumer, final MessageInstance entry, final boolean batch)
        {
            _sendAction.run();
        }

        @Override
        public String getTargetAddress()
        {
            return null;
        }

        @Override
        public void updateNotifyWorkDesired()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AMQPSession<?, TestAbstractConsumerTarget> getSession()
        {
            return _session;
        }

        @Override
        public void flushBatched()
        {

        }

        @Override
        public void noMessagesAvailable()
        {

        }

        @Override
        public boolean allocateCredit(final ServerMessage msg)
        {
            return false;
        }

        @Override
        public void restoreCredit(final ServerMessage queueEntry)
        {
            _creditRestored = true;
        }

        public boolean isCreditRestored()
        {
            return _creditRestored;
        }
    }
}
