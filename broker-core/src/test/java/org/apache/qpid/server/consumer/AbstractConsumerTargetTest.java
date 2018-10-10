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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
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

    private TestAbstractConsumerTarget _consumerTarget;
    private Consumer _consumer;
    private MessageSource _messageSource;
    private AMQPConnection<?> _connection = mock(AMQPConnection.class);
    private AMQPSession<?,TestAbstractConsumerTarget> _session = mock(AMQPSession.class);
    private MessageInstance _messageInstance;

    @Before
    public void setUp() throws Exception
    {
        when(_connection.getContextValue(eq(Long.class),
                                         eq(Consumer.SUSPEND_NOTIFICATION_PERIOD))).thenReturn(1000000L);

        _consumer = mock(Consumer.class);
        _messageSource = mock(MessageSource.class);
        when(_messageSource.getMessageConversionExceptionHandlingPolicy()).thenReturn(MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE);
        _messageInstance = mock(MessageInstance.class);
        when(_messageInstance.getOwningResource()).thenReturn(_messageSource);
        final MessageContainer messageContainer =
                new MessageContainer(_messageInstance, mock(MessageReference.class));
        when(_consumer.pullMessage()).thenReturn(messageContainer);
        _consumerTarget = new TestAbstractConsumerTarget();
        _consumerTarget.consumerAdded(_consumer);
    }

    @Test
    public void testClose() throws Exception
    {
        _consumerTarget = new TestAbstractConsumerTarget();
        assertEquals("Unexpected number of consumers", (long) 0, (long) _consumerTarget.getConsumers().size());

        _consumerTarget.consumerAdded(_consumer);
        assertEquals("Unexpected number of consumers after add",
                            (long) 1,
                            (long) _consumerTarget.getConsumers().size());


        _consumerTarget.close();
        assertEquals("Unexpected number of consumers after close",
                            (long) 0,
                            (long) _consumerTarget.getConsumers().size());

        verify(_consumer, times(1)).close();
    }

    @Test
    public void testNotifyWork() throws Exception
    {
        InOrder order = inOrder(_consumer);

        _consumerTarget = new TestAbstractConsumerTarget();
        assertEquals("Unexpected number of consumers", (long) 0, (long) _consumerTarget.getConsumers().size());

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

    @Test
    public void testConversionExceptionPolicyClose() throws Exception
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE);

        try
        {
            _consumerTarget.sendNextMessage();
            fail("exception not thrown");
        }
        catch (ConnectionScopedRuntimeException e)
        {
            final boolean condition = e.getCause() instanceof MessageConversionException;
            assertTrue(String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                                            e.getCause().getClass().getSimpleName()), condition);
        }
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @Test
    public void testConversionExceptionPolicyCloseForNonAcquiringConsumer() throws Exception
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.CLOSE);

        try
        {
            _consumerTarget.sendNextMessage();
            fail("exception not thrown");
        }
        catch (ConnectionScopedRuntimeException e)
        {
            final boolean condition = e.getCause() instanceof MessageConversionException;
            assertTrue(String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                                            e.getCause().getClass().getSimpleName()), condition);
        }
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @Test
    public void testConversionExceptionPolicyReroute() throws Exception
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.ROUTE_TO_ALTERNATE);

        _consumerTarget.sendNextMessage();
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance).routeToAlternate(null, null, null);
    }

    @Test
    public void testConversionExceptionPolicyRerouteForNonAcquiringConsumer() throws Exception
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.ROUTE_TO_ALTERNATE);

        _consumerTarget.sendNextMessage();
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    @Test
    public void testConversionExceptionPolicyReject() throws Exception
    {
        configureBehaviour(true, MessageSource.MessageConversionExceptionHandlingPolicy.REJECT);

        _consumerTarget.sendNextMessage();

        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance).reject(_consumer);
        verify(_messageInstance).release(_consumer);
    }

    @Test
    public void testConversionExceptionPolicyRejectForNonAcquiringConsumer() throws Exception
    {
        configureBehaviour(false, MessageSource.MessageConversionExceptionHandlingPolicy.REJECT);

        _consumerTarget.sendNextMessage();
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance).reject(_consumer);
        verify(_messageInstance).release(_consumer);
    }

    @Test
    public void testConversionExceptionPolicyWhenOwningResourceIsNotMessageSource() throws Exception
    {
        final TransactionLogResource owningResource = mock(TransactionLogResource.class);
        when(_messageInstance.getOwningResource()).thenReturn(owningResource);

        try
        {
            _consumerTarget.sendNextMessage();
            fail("exception not thrown");
        }
        catch (ConnectionScopedRuntimeException e)
        {
            final boolean condition = e.getCause() instanceof MessageConversionException;
            assertTrue(String.format("ConnectionScopedRuntimeException has unexpected cause '%s'",
                                            e.getCause().getClass().getSimpleName()), condition);
        }
        assertTrue("message credit was not restored", _consumerTarget.isCreditRestored());
        verify(_messageInstance, never()).routeToAlternate(any(Action.class), any(ServerTransaction.class), any());
    }

    private void configureBehaviour(final boolean acquires,
                                    final MessageSource.MessageConversionExceptionHandlingPolicy exceptionHandlingPolicy)
    {
        when(_consumer.acquires()).thenReturn(acquires);
        when(_messageSource.getMessageConversionExceptionHandlingPolicy()).thenReturn(exceptionHandlingPolicy);
    }

    private class TestAbstractConsumerTarget extends AbstractConsumerTarget<TestAbstractConsumerTarget>
    {
        private boolean _creditRestored;

        TestAbstractConsumerTarget()
        {
            super(false, _connection);
        }

        @Override
        protected void doSend(final MessageInstanceConsumer consumer, final MessageInstance entry, final boolean batch)
        {
            throw new MessageConversionException("testException");
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
