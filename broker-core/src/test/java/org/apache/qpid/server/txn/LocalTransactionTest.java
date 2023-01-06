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
package org.apache.qpid.server.txn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.MockMessageInstance;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.MockStoreTransaction.TransactionState;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * A unit test ensuring that LocalTransactionTest creates a long-lived store transaction
 * that spans many dequeue/enqueue operations of enlistable messages.  Verifies
 * that the long-lived transaction is properly committed and rolled back, and that
 * post transaction actions are correctly fired.
 */
@SuppressWarnings("unchecked")
public class LocalTransactionTest extends UnitTestBase
{
    private ServerTransaction _transaction = null;  // Class under test
    
    private BaseQueue _queue;
    private List<BaseQueue> _queues;
    private Collection<MessageInstance> _queueEntries;
    private ServerMessage<?> _message;
    private MockAction _action1;
    private MockAction _action2;
    private MockStoreTransaction _storeTransaction;
    private MessageStore _transactionLog;

    @BeforeEach
    public void setUp() throws Exception
    {
        _storeTransaction = createTestStoreTransaction(false);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _action1 = new MockAction();
        _action2 = new MockAction();
        _transaction = new LocalTransaction(_transactionLog);
    }

    /**
     * Tests the enqueue of a non persistent message to a single non durable queue.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testEnqueueToNonDurableQueueOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queue = createQueue(false);
        _transaction.enqueue(_queue, _message, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of non-persistent message must not cause message to be enqueued");

        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the enqueue of a persistent message to a durable queue.
     * Asserts that a store transaction has been started.
     */
    @Test
    public void testEnqueueToDurableQueueOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        
        _transaction.enqueue(_queue, _message, _action1);

        assertEquals(1, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to durable queue must cause message to be enqueued");
        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted.
     */
    @Test
    public void testStoreEnqueueCausesException()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new LocalTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.enqueue(_queue, _message, _action1),
                "Exception not thrown");

        assertTrue(_action1.isRollbackActionFired(), "Rollback action must be fired");
        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired");
    }
    
    /**
     * Tests the enqueue of a non persistent message to a many non durable queues.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testEnqueueToManyNonDurableQueuesOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queues = createTestBaseQueues(new boolean[] {false, false, false});
        _transaction.enqueue(_queues, _message, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of non-persistent message must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertNotFired(_action1);
    }
    
    /**
     * Tests the enqueue of a persistent message to a many non durable queues.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testEnqueueToManyNonDurableQueuesOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {false, false, false});
        _transaction.enqueue(_queues, _message, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to non-durable queues must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the enqueue of a persistent message to many queues, some durable others not.
     * Asserts that a store transaction has been started.
     */
    @Test
    public void testEnqueueToDurableAndNonDurableQueuesOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {false, true, false, true});
        _transaction.enqueue(_queues, _message, _action1);

        assertEquals(2, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to durable/non-durable queues must cause messages to be enqueued");
        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted.
     */
    @Test
    public void testStoreEnqueuesCausesExceptions()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {true, true});
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new LocalTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.enqueue(_queues, _message, _action1),
                "Exception not thrown");

        assertTrue(_action1.isRollbackActionFired(), "Rollback action must be fired");
        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired");
    }

    /**
     * Tests the dequeue of a non persistent message from a single non durable queue.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testDequeueFromNonDurableQueueOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queue = createQueue(false);

        _transaction.dequeue((MessageEnqueueRecord)null, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Dequeue of non-persistent message must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);

    }

    /**
     * Tests the dequeue of a persistent message from a single non durable queue.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testDequeueFromDurableQueueOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action1);

        assertEquals(1, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of non-persistent message must cause message to be dequeued");
        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted.
     */
    @Test
    public void testStoreDequeueCausesException()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new LocalTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.dequeue(mock(MessageEnqueueRecord.class), _action1),
                "Exception not thrown");

        assertTrue(_action1.isRollbackActionFired(), "Rollback action must be fired");
        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired");
    }

    /**
     * Tests the dequeue of a non persistent message from many non durable queues.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testDequeueFromManyNonDurableQueuesOfNonPersistentMessage()
    {
        _queueEntries = createTestQueueEntries(new boolean[] {false, false, false}, new boolean[] {false, false, false});
        
        _transaction.dequeue(_queueEntries, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of non-persistent messages must not cause message to be dequeued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertNotFired(_action1);
    }
    
    /**
     * Tests the dequeue of a persistent message from a many non durable queues.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testDequeueFromManyNonDurableQueuesOfPersistentMessage()
    {
        _queueEntries = createTestQueueEntries(new boolean[] {false, false, false}, new boolean[] {true, true, true});
        _transaction.dequeue(_queueEntries, _action1);

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of persistent message from non-durable queues must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }

    /**
     * Tests the dequeue of a persistent message from many queues, some durable others not.
     * Asserts that a store transaction has not been started.
     */
    @Test
    public void testDequeueFromDurableAndNonDurableQueuesOfPersistentMessage()
    {
        // A transaction will exist owing to the 1st and 3rd.
        _queueEntries = createTestQueueEntries(new boolean[] {true, false, true, true}, new boolean[] {true, true, true, false});
        
        _transaction.dequeue(_queueEntries, _action1);

        assertEquals(2, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of persistent messages from durable/non-durable queues must cause messages to be dequeued");
        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertNotFired(_action1);
    }
    
    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted.
     */
    @Test
    public void testStoreDequeuesCauseExceptions()
    {
        // Transactions will exist owing to the 1st and 3rd queue entries in the collection
        _queueEntries = createTestQueueEntries(new boolean[] {true}, new boolean[] {true});
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new LocalTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.dequeue(_queueEntries, _action1),
                "Exception not thrown");

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertTrue(_action1.isRollbackActionFired(), "Rollback action must be fired");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired");
    }
    
    /** 
     * Tests the add of a post-commit action.  Unlike AutoCommitTransactions, the post transaction actions
     * is added to a list to be fired on commit or rollback.
     */
    @Test
    public void testAddingPostCommitActionNotFiredImmediately()
    {
        _transaction.addPostTransactionAction(_action1);
        assertNotFired(_action1);
    }

    /**
     * Tests committing a transaction without work accepted without error and without causing store
     * enqueues or dequeues.
     */
    @Test
    public void testCommitNoWork()
    {
        _transaction.commit();

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Unexpected number of store dequeues");
        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Unexpected number of store enqueues");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
    }
    
    /**
     * Tests rolling back a transaction without work accepted without error and without causing store
     * enqueues or dequeues.
     */
    @Test
    public void testRollbackNoWork()
    {
        _transaction.rollback();

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Unexpected number of store dequeues");
        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Unexpected number of store enqueues");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
    }
    
    /** 
     * Tests the dequeuing of a message with a commit.  Test ensures that the underlying store transaction is 
     * correctly controlled and the post commit action is fired.
     */
    @Test
    public void testCommitWork()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);

        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired yet");

        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action1);
        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isPostCommitActionFired(), "Post commit action must not be fired yet");

        _transaction.commit();

        assertEquals(TransactionState.COMMITTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertTrue(_action1.isPostCommitActionFired(), "Post commit action must be fired");
    }
    
    /** 
     * Tests the dequeuing of a message with a rollback.  Test ensures that the underlying store transaction is 
     * correctly controlled and the post rollback action is fired.
     */
    @Test
    public void testRollbackWork()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);

        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isRollbackActionFired(), "Rollback action must not be fired yet");

        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action1);

        assertEquals(TransactionState.STARTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertFalse(_action1.isRollbackActionFired(), "Rollback action must not be fired yet");

        _transaction.rollback();

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertTrue(_action1.isRollbackActionFired(), "Rollback action must be fired");
    }
    
    /**
     * Variation of testCommitWork with an additional post transaction action.
     * 
     */
    @Test
    public void testCommitWorkWithAdditionalPostAction()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        _transaction.addPostTransactionAction(_action1);
        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action2);
        _transaction.commit();

        assertEquals(TransactionState.COMMITTED, _storeTransaction.getState(), "Unexpected transaction state");

        assertTrue(_action1.isPostCommitActionFired(), "Post commit action1 must be fired");
        assertTrue(_action2.isPostCommitActionFired(), "Post commit action2 must be fired");

        assertFalse(_action1.isRollbackActionFired(), "Rollback action1 must not be fired");
        assertFalse(_action1.isRollbackActionFired(), "Rollback action2 must not be fired");
    }

    /**
     * Variation of testRollbackWork with an additional post transaction action.
     * 
     */
    @Test
    public void testRollbackWorkWithAdditionalPostAction()
    {
        _message = createTestMessage(true);
        _queue = createQueue(true);
        _transaction.addPostTransactionAction(_action1);
        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action2);
        _transaction.rollback();

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");

        assertFalse(_action1.isPostCommitActionFired(), "Post commit action1 must not be fired");
        assertFalse(_action2.isPostCommitActionFired(), "Post commit action2 must not be fired");

        assertTrue(_action1.isRollbackActionFired(), "Rollback action1 must be fired");
        assertTrue(_action1.isRollbackActionFired(), "Rollback action2 must be fired");
    }

    @Test
    public void testFirstEnqueueRecordsTransactionStartAndUpdateTime()
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        final long startTime = System.currentTimeMillis();
        _transaction.enqueue(_queue, _message, _action1);

        assertTrue(_transaction.getTransactionStartTime() >= startTime,
                "Transaction start time should have been recorded");
        assertEquals(_transaction.getTransactionStartTime(), _transaction.getTransactionUpdateTime(),
                "Transaction update time should be the same as transaction start time");
    }

    @Test
    public void testSubsequentEnqueueAdvancesTransactionUpdateTimeOnly() throws Exception
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        _transaction.enqueue(_queue, _message, _action1);

        final long transactionStartTimeAfterFirstEnqueue = _transaction.getTransactionStartTime();
        final long transactionUpdateTimeAfterFirstEnqueue = _transaction.getTransactionUpdateTime();

        Thread.sleep(1);
        _transaction.enqueue(_queue, _message, _action2);

        final long transactionStartTimeAfterSecondEnqueue = _transaction.getTransactionStartTime();
        final long transactionUpdateTimeAfterSecondEnqueue = _transaction.getTransactionUpdateTime();

        assertEquals(transactionStartTimeAfterFirstEnqueue, transactionStartTimeAfterSecondEnqueue,
                "Transaction start time after second enqueue should be unchanged");
        assertTrue(transactionUpdateTimeAfterSecondEnqueue > transactionUpdateTimeAfterFirstEnqueue,
                "Transaction update time after second enqueue should be greater than first update time");
    }

    @Test
    public void testFirstDequeueRecordsTransactionStartAndUpdateTime()
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        long startTime = System.currentTimeMillis();
        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action1);

        assertTrue(_transaction.getTransactionStartTime() >= startTime,
                "Transaction start time should have been recorded");
        assertEquals(_transaction.getTransactionStartTime(), _transaction.getTransactionUpdateTime(),
                "Transaction update time should be the same as transaction start time");
    }

    @Test
    public void testMixedEnqueuesAndDequeuesAdvancesTransactionUpdateTimeOnly() throws Exception
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        _transaction.enqueue(_queue, _message, _action1);

        final long transactionStartTimeAfterFirstEnqueue = _transaction.getTransactionStartTime();
        final long transactionUpdateTimeAfterFirstEnqueue = _transaction.getTransactionUpdateTime();

        Thread.sleep(1);
        _transaction.dequeue(mock(MessageEnqueueRecord.class), _action2);

        final long transactionStartTimeAfterFirstDequeue = _transaction.getTransactionStartTime();
        final long transactionUpdateTimeAfterFirstDequeue = _transaction.getTransactionUpdateTime();

        assertEquals(transactionStartTimeAfterFirstEnqueue, transactionStartTimeAfterFirstDequeue,
                "Transaction start time after first dequeue should be unchanged");
        assertTrue(transactionUpdateTimeAfterFirstDequeue > transactionUpdateTimeAfterFirstEnqueue,
                "Transaction update time after first dequeue should be greater than first update time");
    }

    @Test
    public void testCommitResetsTransactionStartAndUpdateTime()
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        long startTime = System.currentTimeMillis();
        _transaction.enqueue(_queue, _message, _action1);

        assertTrue(_transaction.getTransactionStartTime() >= startTime);
        assertTrue(_transaction.getTransactionUpdateTime() >= startTime);

        _transaction.commit();

        assertEquals(0, _transaction.getTransactionStartTime(),
                "Transaction start time should be reset after commit");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Transaction update time should be reset after commit");
    }

    @Test
    public void testRollbackResetsTransactionStartAndUpdateTime()
    {
        assertEquals(0, _transaction.getTransactionStartTime(),
                "Unexpected transaction start time before test");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Unexpected transaction update time before test");

        _message = createTestMessage(true);
        _queue = createQueue(true);

        long startTime = System.currentTimeMillis();
        _transaction.enqueue(_queue, _message, _action1);

        assertTrue(_transaction.getTransactionStartTime() >= startTime);
        assertTrue(_transaction.getTransactionUpdateTime() >= startTime);

        _transaction.rollback();

        assertEquals(0, _transaction.getTransactionStartTime(),
                "Transaction start time should be reset after rollback");
        assertEquals(0, _transaction.getTransactionUpdateTime(),
                "Transaction update time should be reset after rollback");
    }

    @Test
    public void testEnqueueInvokesTransactionObserver()
    {
        final TransactionObserver transactionObserver = mock(TransactionObserver.class);
        _transaction = new LocalTransaction(_transactionLog, transactionObserver);

        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {false, true, false, true});

        _transaction.enqueue(_queues, _message, null);

        verify(transactionObserver).onMessageEnqueue(_transaction, _message);

        ServerMessage<?> message2 = createTestMessage(true);
        _transaction.enqueue(createQueue(true), message2, null);

        verify(transactionObserver).onMessageEnqueue(_transaction, message2);
        verifyNoMoreInteractions(transactionObserver);
    }

    private Collection<MessageInstance> createTestQueueEntries(boolean[] queueDurableFlags, boolean[] messagePersistentFlags)
    {
        final Collection<MessageInstance> queueEntries = new ArrayList<>();

        assertEquals(queueDurableFlags.length, messagePersistentFlags.length, "Boolean arrays must be the same length");

        for (int i = 0; i < queueDurableFlags.length; i++)
        {
            final TransactionLogResource queue = createQueue(queueDurableFlags[i]);
            final ServerMessage<?> message = createTestMessage(messagePersistentFlags[i]);
            final boolean hasRecord = queueDurableFlags[i] && messagePersistentFlags[i];
            queueEntries.add(new MockMessageInstance()
            {

                @Override
                public ServerMessage<?> getMessage()
                {
                    return message;
                }

                @Override
                public TransactionLogResource getOwningResource()
                {
                    return queue;
                }

                @Override
                public MessageEnqueueRecord getEnqueueRecord()
                {
                    return hasRecord ? mock(MessageEnqueueRecord.class) : null;
                }
            });
        }
        
        return queueEntries;
    }

    private MockStoreTransaction createTestStoreTransaction(final boolean throwException)
    {
        return new MockStoreTransaction(throwException);
    }
    
    private List<BaseQueue> createTestBaseQueues(final boolean[] durableFlags)
    {
        return IntStream.range(0, durableFlags.length).mapToObj(idx -> durableFlags[idx])
                .map(this::createQueue).collect(Collectors.toList());
    }

    private BaseQueue createQueue(final boolean durable)
    {
        final BaseQueue queue = mock(BaseQueue.class);
        when(queue.getMessageDurability()).thenReturn(durable ? MessageDurability.DEFAULT : MessageDurability.NEVER);
        return queue;
    }

    private ServerMessage<?> createTestMessage(final boolean persistent)
    {
        return new MockServerMessage<>(persistent);
    }

    private void assertNotFired(final MockAction action)
    {
        assertFalse(action.isRollbackActionFired(), "Rollback action must not be fired");
        assertFalse(action.isPostCommitActionFired(), "Post commit action must not be fired");
    }
}
