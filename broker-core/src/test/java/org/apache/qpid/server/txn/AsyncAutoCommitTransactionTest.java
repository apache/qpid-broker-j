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
 */
package org.apache.qpid.server.txn;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction.FutureRecorder;
import org.apache.qpid.server.txn.ServerTransaction.Action;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("unchecked")
public class AsyncAutoCommitTransactionTest extends UnitTestBase
{
    private static final String STRICT_ORDER_SYSTEM_PROPERTY = AsyncAutoCommitTransaction.QPID_STRICT_ORDER_WITH_MIXED_DELIVERY_MODE;

    private final EnqueueableMessage<?> _message = mock(EnqueueableMessage.class);
    private final BaseQueue _queue = mock(BaseQueue.class);
    private final MessageStore _messageStore = mock(MessageStore.class);
    private final ServerTransaction.EnqueueAction _postTransactionAction = mock(ServerTransaction.EnqueueAction.class);
    private final ListenableFuture<Void> _future = mock(ListenableFuture.class);

    private Transaction _storeTransaction;
    private FutureRecorder _futureRecorder;

    @BeforeEach
    public void setUp() throws Exception
    {
        _futureRecorder = mock(FutureRecorder.class);
        _storeTransaction = mock(Transaction.class);
        when(_messageStore.newTransaction()).thenReturn(_storeTransaction);
        when(_storeTransaction.commitTranAsync((Void) null)).thenReturn(_future);
        when(_queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
    }

    @Test
    public void testEnqueuePersistentMessagePostCommitNotCalledWhenFutureAlreadyComplete()
    {
        setTestSystemProperty(STRICT_ORDER_SYSTEM_PROPERTY, "false");

        when(_message.isPersistent()).thenReturn(true);
        when(_future.isDone()).thenReturn(true);

        final AsyncAutoCommitTransaction asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(_messageStore, _futureRecorder);

        asyncAutoCommitTransaction.enqueue(_queue, _message, _postTransactionAction);

        verify(_storeTransaction).enqueueMessage(_queue, _message);
        verify(_futureRecorder).recordFuture(eq(_future), any(Action.class));
        verifyNoInteractions(_postTransactionAction);
    }

    @Test
    public void testEnqueuePersistentMessageOnMultipleQueuesPostCommitNotCalled()
    {
        setTestSystemProperty(STRICT_ORDER_SYSTEM_PROPERTY, "false");

        when(_message.isPersistent()).thenReturn(true);
        when(_future.isDone()).thenReturn(true);

        final AsyncAutoCommitTransaction asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(_messageStore, _futureRecorder);

        asyncAutoCommitTransaction.enqueue(Collections.singletonList(_queue), _message, _postTransactionAction);

        verify(_storeTransaction).enqueueMessage(_queue, _message);
        verify(_futureRecorder).recordFuture(eq(_future), any(Action.class));
        verifyNoInteractions(_postTransactionAction);
    }

    @Test
    public void testEnqueuePersistentMessagePostCommitNotCalledWhenFutureNotYetComplete()
    {
        setTestSystemProperty(STRICT_ORDER_SYSTEM_PROPERTY, "false");

        when(_message.isPersistent()).thenReturn(true);
        when(_future.isDone()).thenReturn(false);

        final AsyncAutoCommitTransaction asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(_messageStore, _futureRecorder);

        asyncAutoCommitTransaction.enqueue(_queue, _message, _postTransactionAction);

        verify(_storeTransaction).enqueueMessage(_queue, _message);
        verify(_futureRecorder).recordFuture(eq(_future), any(Action.class));
        verifyNoInteractions(_postTransactionAction);
    }

    @Test
    public void testEnqueueTransientMessagePostCommitIsCalledWhenNotBehavingStrictly()
    {
        setTestSystemProperty(STRICT_ORDER_SYSTEM_PROPERTY, "false");

        when(_message.isPersistent()).thenReturn(false);

        final AsyncAutoCommitTransaction asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(_messageStore, _futureRecorder);

        asyncAutoCommitTransaction.enqueue(_queue, _message, _postTransactionAction);

        verifyNoInteractions(_storeTransaction);
        verify(_postTransactionAction).postCommit((MessageEnqueueRecord)null);
        verifyNoInteractions(_futureRecorder);
    }

    @Test
    public void testEnqueueTransientMessagePostCommitIsCalledWhenBehavingStrictly()
    {
        setTestSystemProperty(STRICT_ORDER_SYSTEM_PROPERTY, "true");

        when(_message.isPersistent()).thenReturn(false);

        final AsyncAutoCommitTransaction asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(_messageStore, _futureRecorder);

        asyncAutoCommitTransaction.enqueue(_queue, _message, _postTransactionAction);

        verifyNoInteractions(_storeTransaction);
        verify(_futureRecorder).recordFuture(any(ListenableFuture.class), any(Action.class));
        verifyNoInteractions(_postTransactionAction);
    }
}
