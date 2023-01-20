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
 * A unit test ensuring that AutoCommitTransaction creates a separate transaction for
 * each dequeue/enqueue operation that involves enlistable messages. Verifies
 * that the transaction is properly committed (or rolled-back in the case of exception),
 * and that post transaction actions are correctly fired.
 */
public class AutoCommitTransactionTest extends UnitTestBase
{
    private ServerTransaction _transaction = null;  // Class under test
    
    private MessageStore _transactionLog;
    private BaseQueue _queue;
    private List<BaseQueue> _queues;
    private Collection<MessageInstance> _queueEntries;
    private ServerMessage<?> _message;
    private MockAction _action;
    private MockStoreTransaction _storeTransaction;

    @BeforeEach
    public void setUp() throws Exception
    {
        _storeTransaction = createTestStoreTransaction(false);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _action = new MockAction();
        _transaction = new AutoCommitTransaction(_transactionLog);
    }

    /**
     * Tests the enqueue of a non persistent message to a single non durable queue.
     * Asserts that a store transaction has not been started and commit action fired.
     */
    @Test
    public void testEnqueueToNonDurableQueueOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queue = createTestAMQQueue(false);
        
        _transaction.enqueue(_queue, _message, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of non-persistent message must not cause message to be enqueued");

        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the enqueue of a persistent message to a durable queue.
     * Asserts that a store transaction has been committed and commit action fired.
     */
    @Test
    public void testEnqueueToDurableQueueOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queue = createTestAMQQueue(true);
        
        _transaction.enqueue(_queue, _message, _action);

        assertEquals(1, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to durable queue must cause message to be enqueued");
        assertEquals(TransactionState.COMMITTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted and rollback action is fired.
     */
    @Test
    public void testStoreEnqueueCausesException()
    {
        _message = createTestMessage(true);
        _queue = createTestAMQQueue(true);
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new AutoCommitTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.enqueue(_queue, _message, _action),
                "Exception not thrown");

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertTrue(_action.isRollbackActionFired(), "Rollback action must be fired");
        assertFalse(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }
    
    /**
     * Tests the enqueue of a non persistent message to a many non durable queues.
     * Asserts that a store transaction has not been started and post commit action fired.
     */
    @Test
    public void testEnqueueToManyNonDurableQueuesOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queues = createTestBaseQueues(new boolean[] {false, false, false});
        
        _transaction.enqueue(_queues, _message, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of non-persistent message must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }
    
    
    /**
     * Tests the enqueue of a persistent message to a many non durable queues.
     * Asserts that a store transaction has not been started and post commit action
     * fired.
     */
    @Test
    public void testEnqueueToManyNonDurableQueuesOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {false, false, false});
        
        _transaction.enqueue(_queues, _message, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to non-durable queues must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the enqueue of a persistent message to many queues, some durable others not.
     * Asserts that a store transaction has been committed and post commit action fired.
     */
    @Test
    public void testEnqueueToDurableAndNonDurableQueuesOfPersistentMessage()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {false, true, false, true});
        
        _transaction.enqueue(_queues, _message, _action);

        assertEquals(2, (long) _storeTransaction.getNumberOfEnqueuedMessages(),
                "Enqueue of persistent message to durable/non-durable queues must cause messages to be enqueued");
        assertEquals(TransactionState.COMMITTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted and rollback action fired.
     */
    @Test
    public void testStoreEnqueuesCausesExceptions()
    {
        _message = createTestMessage(true);
        _queues = createTestBaseQueues(new boolean[] {true, true});
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new AutoCommitTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.enqueue(_queues, _message, _action),
                "Exception not thrown");

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");
        assertTrue(_action.isRollbackActionFired(), "Rollback action must be fired");
        assertFalse(_action.isPostCommitActionFired(), "Post commit action must not be fired");
    }
    
    /**
     * Tests the dequeue of a non persistent message from a single non durable queue.
     * Asserts that a store transaction has not been started and post commit action
     * fired.
     */
    @Test
    public void testDequeueFromNonDurableQueueOfNonPersistentMessage()
    {
        _message = createTestMessage(false);
        _queue = createTestAMQQueue(false);
        
        _transaction.dequeue((MessageEnqueueRecord)null, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of non-persistent message must not cause message to be dequeued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted and post rollback action
     * fired.
     */
    @Test
    public void testStoreDequeueCausesException()
    {
        _message = createTestMessage(true);
        _queue = createTestAMQQueue(true);
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new AutoCommitTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.dequeue(mock(MessageEnqueueRecord.class), _action),
                "Exception not thrown");

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");

        assertTrue(_action.isRollbackActionFired(), "Rollback action must be fired");
        assertFalse(_action.isPostCommitActionFired(), "Post commit action must not be fired");
    }

    /**
     * Tests the dequeue of a non persistent message from many non durable queues.
     * Asserts that a store transaction has not been started and post commit action
     * fired.
     */
    @Test
    public void testDequeueFromManyNonDurableQueuesOfNonPersistentMessage()
    {
        _queueEntries = createTestQueueEntries(new boolean[] {false, false, false}, new boolean[] {false, false, false});
        
        _transaction.dequeue(_queueEntries, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of non-persistent messages must not cause message to be dequeued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the dequeue of a persistent message from a many non durable queues.
     * Asserts that a store transaction has not been started and post commit action
     * fired.
     */
    @Test
    public void testDequeueFromManyNonDurableQueuesOfPersistentMessage()
    {
        _queueEntries = createTestQueueEntries(new boolean[] {false, false, false}, new boolean[] {true, true, true});
        
        _transaction.dequeue(_queueEntries, _action);

        assertEquals(0, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of persistent message from non-durable queues must not cause message to be enqueued");
        assertEquals(TransactionState.NOT_STARTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }

    /**
     * Tests the dequeue of a persistent message from many queues, some durable others not.
     * Asserts that a store transaction has not been started and post commit action fired.
     */
    @Test
    public void testDequeueFromDurableAndNonDurableQueuesOfPersistentMessage()
    {
        // A transaction will exist owing to the 1st and 3rd.
        _queueEntries = createTestQueueEntries(new boolean[] {true, false, true, true}, new boolean[] {true, true, true, false});
        
        _transaction.dequeue(_queueEntries, _action);

        assertEquals(2, (long) _storeTransaction.getNumberOfDequeuedMessages(),
                "Dequeue of persistent messages from durable/non-durable queues must cause messages to be dequeued");
        assertEquals(TransactionState.COMMITTED, _storeTransaction.getState(),
                "Unexpected transaction state");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must not be fired");
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
    }
    
    /**
     * Tests the case where the store operation throws an exception.
     * Asserts that the transaction is aborted and post rollback action fired.
     */
    @Test
    public void testStoreDequeuesCauseExceptions()
    {
        // Transactions will exist owing to the 1st and 3rd queue entries in the collection
        _queueEntries = createTestQueueEntries(new boolean[] {true}, new boolean[] {true});
        
        _storeTransaction = createTestStoreTransaction(true);
        _transactionLog = MockStoreTransaction.createTestTransactionLog(_storeTransaction);
        _transaction = new AutoCommitTransaction(_transactionLog);

        assertThrows(RuntimeException.class,
                () -> _transaction.dequeue(_queueEntries, _action),
                "Exception not thrown");

        assertEquals(TransactionState.ABORTED, _storeTransaction.getState(), "Unexpected transaction state");

        assertTrue(_action.isRollbackActionFired(), "Rollback action must be fired");
        assertFalse(_action.isPostCommitActionFired(), "Post commit action must not be fired");
    }
    
    /** 
     * Tests the add of a post-commit action.  Since AutoCommitTransactions
     * have no long lived transactions, the post commit action is fired immediately.
     */
    @Test
    public void testPostCommitActionFiredImmediately()
    {
        _transaction.addPostTransactionAction(_action);
        assertTrue(_action.isPostCommitActionFired(), "Post commit action must be fired");
        assertFalse(_action.isRollbackActionFired(), "Rollback action must be fired");
    }
    
    private Collection<MessageInstance> createTestQueueEntries(final boolean[] queueDurableFlags,
                                                               final boolean[] messagePersistentFlags)
    {
        final Collection<MessageInstance> queueEntries = new ArrayList<>();

        assertEquals(queueDurableFlags.length, messagePersistentFlags.length, "Boolean arrays must be the same length");

        for (int i = 0; i < queueDurableFlags.length; i++)
        {
            final BaseQueue queue = createTestAMQQueue(queueDurableFlags[i]);
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
                    if(hasRecord)
                    {
                        return mock(MessageEnqueueRecord.class);
                    }
                    else
                    {
                        return null;
                    }
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
               .map(this::createTestAMQQueue).collect(Collectors.toList());
    }

    private BaseQueue createTestAMQQueue(final boolean durable)
    {
        final BaseQueue queue = mock(BaseQueue.class);
        when(queue.getMessageDurability()).thenReturn(durable ? MessageDurability.DEFAULT : MessageDurability.NEVER);
        return queue;
    }

    private ServerMessage<?> createTestMessage(final boolean persistent)
    {
        return new MockServerMessage(persistent);
    }
}
