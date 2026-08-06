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
package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlowMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.test.utils.UnitTestBase;

class ConsumerTarget_0_10Test extends UnitTestBase
{
    private static final long MESSAGE_SIZE = 10L;

    private ServerSession _session;
    private MessageInstanceConsumer _consumer;
    private MessageInstance _entry;
    private List<MessageTransfer> _transfers;

    @BeforeEach
    void setUp()
    {
        _session = mock(ServerSession.class);
        final AMQPConnection_0_10 amqpConnection = mock(AMQPConnection_0_10.class);
        when(_session.getAMQPConnection()).thenReturn(amqpConnection);
        when(amqpConnection.getContextValue(eq(Long.class), eq(Consumer.SUSPEND_NOTIFICATION_PERIOD)))
                .thenReturn(0L);

        final ServerConnection connection = mock(ServerConnection.class);
        when(_session.getConnection()).thenReturn(connection);
        when(connection.getConnectionDelegate()).thenReturn(mock(ServerConnectionDelegate.class));
        when(_session.getModelObject()).thenReturn(mock(Session_0_10.class));

        final MessageTransferMessage message = mock(MessageTransferMessage.class);
        when(message.getSize()).thenReturn(MESSAGE_SIZE);
        _entry = mock(MessageInstance.class);
        when(_entry.getMessage()).thenReturn(message);
        _consumer = mock(MessageInstanceConsumer.class);

        _transfers = new ArrayList<>();
        doAnswer(invocation ->
        {
            _transfers.add(invocation.getArgument(0));
            return null;
        }).when(_session).sendMessage(any(MessageTransfer.class), any(Runnable.class));
    }

    @Test
    void completedTransferMarksTargetAndRestoresWindowOneCredit()
    {
        final WindowCreditManager creditManager = spy(new WindowCreditManager(-1L, 1L));
        final ConsumerTarget_0_10 target = createTarget(creditManager);
        target.updateNotifyWorkDesired();
        exhaustCredit(target, 1);

        completeTransfer(target);

        verify(_session).addConsumerTargetNeedingFlush(target);
        assertTrue(target.flushCreditState(false));
        verify(creditManager).restoreCredit(1L, MESSAGE_SIZE);
        assertFalse(target.isSuspended());
    }

    @Test
    void suspendedLargeWindowRetainsCreditUntilTargetBecomesActive()
    {
        final WindowCreditManager creditManager = spy(new WindowCreditManager(-1L, 400L));
        final ConsumerTarget_0_10 target = createTarget(creditManager);
        target.updateNotifyWorkDesired();
        exhaustCredit(target, 400);
        assertTrue(target.isSuspended());
        completeTransfer(target);

        assertFalse(target.flushCreditState(false));
        verify(creditManager, never()).restoreCredit(anyLong(), anyLong());

        target.addCredit(MessageCreditUnit.MESSAGE, 1);

        assertFalse(target.isSuspended());
        assertTrue(target.flushCreditState(false));
        verify(creditManager).restoreCredit(1L, MESSAGE_SIZE);
    }

    @Test
    void deferredCreditThresholdRemainsUnchanged()
    {
        final WindowCreditManager creditManager = spy(new WindowCreditManager(-1L, 400L));
        final ConsumerTarget_0_10 target = createTarget(creditManager);
        target.updateNotifyWorkDesired();
        exhaustCredit(target, 400);

        for (int i = 0; i < 199; i++)
        {
            completeTransfer(target);
        }
        assertFalse(target.flushCreditState(false));
        verify(creditManager, never()).restoreCredit(anyLong(), anyLong());

        completeTransfer(target);

        assertTrue(target.flushCreditState(false));
        verify(creditManager).restoreCredit(200L, 200L * MESSAGE_SIZE);
    }

    @Test
    void strictFlushRestoresSuspendedCredit()
    {
        final WindowCreditManager creditManager = spy(new WindowCreditManager(-1L, 400L));
        final ConsumerTarget_0_10 target = createTarget(creditManager);
        target.updateNotifyWorkDesired();
        exhaustCredit(target, 400);
        completeTransfer(target);

        assertTrue(target.flushCreditState(true));
        verify(creditManager).restoreCredit(1L, MESSAGE_SIZE);
    }

    @Test
    void noPendingCreditDoesNotRestoreZeroCredit()
    {
        final WindowCreditManager creditManager = spy(new WindowCreditManager(-1L, 1L));
        final ConsumerTarget_0_10 target = createTarget(creditManager);

        assertTrue(target.flushCreditState(false));
        verify(creditManager, never()).restoreCredit(anyLong(), anyLong());
    }

    private ConsumerTarget_0_10 createTarget(final WindowCreditManager creditManager)
    {
        return new ConsumerTarget_0_10(_session, "destination", MessageAcceptMode.EXPLICIT,
                MessageAcquireMode.PRE_ACQUIRED, MessageFlowMode.WINDOW, creditManager, Map.of(), false);
    }

    private void exhaustCredit(final ConsumerTarget_0_10 target, final int credit)
    {
        for (int i = 0; i < credit; i++)
        {
            assertTrue(target.allocateCredit(_entry.getMessage()));
        }
        target.updateNotifyWorkDesired();
    }

    private void completeTransfer(final ConsumerTarget_0_10 target)
    {
        final int transferCount = _transfers.size();
        target.doSend(_consumer, _entry, false);

        final MessageTransfer transfer = _transfers.get(transferCount);
        assertTrue(transfer.hasCompletionListener());
        transfer.complete();
    }
}
