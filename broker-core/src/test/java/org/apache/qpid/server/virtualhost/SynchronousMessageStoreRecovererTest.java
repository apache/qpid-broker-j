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
package org.apache.qpid.server.virtualhost;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.NullMessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.Transaction.EnqueueRecord;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.txn.DtxBranch;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SynchronousMessageStoreRecovererTest extends UnitTestBase
{
    private QueueManagingVirtualHost<?> _virtualHost;

    @BeforeEach
    public void setUp() throws Exception
    {
        _virtualHost = mock(QueueManagingVirtualHost.class);
        when(_virtualHost.getEventLogger()).thenReturn(new EventLogger());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoveryOfSingleMessageOnSingleQueue()
    {
        final Queue<?> queue = createRegisteredMockQueue();
        final long messageId = 1;
        final StoredMessage<StorableMessageMetaData> storedMessage = createMockStoredMessage(messageId);

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                handler.handle(storedMessage);
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                handler.handle(new TestMessageEnqueueRecord(queue.getId(), messageId));
            }
        };

        when(_virtualHost.getMessageStore()).thenReturn(store);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        final ServerMessage<?> message = storedMessage.getMetaData().getType().createMessage(storedMessage);
        verify(queue, times(1)).recover(eq(message), any(MessageEnqueueRecord.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoveryOfMessageInstanceForNonExistingMessage()
    {
        final Queue<?> queue = createRegisteredMockQueue();
        final long messageId = 1;
        final Transaction transaction = mock(Transaction.class);

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                // no message to visit
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                handler.handle(new TestMessageEnqueueRecord(queue.getId(), messageId));
            }

            @Override
            public Transaction newTransaction()
            {
                return transaction;
            }
        };

        when(_virtualHost.getMessageStore()).thenReturn(store);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        verify(queue, never()).enqueue(any(ServerMessage.class), any(Action.class), any(MessageEnqueueRecord.class));
        verify(transaction).dequeueMessage(argThat(new MessageEnqueueRecordMatcher(queue.getId(), messageId)));
        verify(transaction, times(1)).commitTranAsync((Void) null);
    }

    @Test
    public void testRecoveryOfMessageInstanceForNonExistingQueue()
    {
        final UUID queueId = randomUUID();
        final Transaction transaction = mock(Transaction.class);
        final long messageId = 1;
        final StoredMessage<StorableMessageMetaData> storedMessage = createMockStoredMessage(messageId);

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                handler.handle(storedMessage);
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                handler.handle(new TestMessageEnqueueRecord(queueId, messageId));
            }

            @Override
            public Transaction newTransaction()
            {
                return transaction;
            }
        };

        when(_virtualHost.getMessageStore()).thenReturn(store);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        verify(transaction).dequeueMessage(argThat(new MessageEnqueueRecordMatcher(queueId, messageId)));
        verify(transaction, times(1)).commitTranAsync((Void) null);
    }

    @Test
    public void testRecoveryDeletesOrphanMessages()
    {
        final long messageId = 1;
        final StoredMessage<StorableMessageMetaData> storedMessage = createMockStoredMessage(messageId);

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                handler.handle(storedMessage);
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                // No messages instances
            }
        };

        when(_virtualHost.getMessageStore()).thenReturn(store);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        verify(storedMessage, times(1)).remove();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoveryOfSingleEnqueueWithDistributedTransaction()
    {
        final Queue<?> queue = createRegisteredMockQueue();
        final Transaction transaction = mock(Transaction.class);
        final StoredMessage<StorableMessageMetaData> storedMessage = createMockStoredMessage(1);
        final long messageId = storedMessage.getMessageNumber();

        final EnqueueableMessage<?> enqueueableMessage = createMockEnqueueableMessage(messageId, storedMessage);
        final EnqueueRecord enqueueRecord = createMockRecord(queue, enqueueableMessage);

        final long format = 1;
        final byte[] globalId = new byte[] {0};
        final byte[] branchId = new byte[] {0};
        final EnqueueRecord[] enqueues = { enqueueRecord };
        final Transaction.DequeueRecord[] dequeues = {};

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                handler.handle(storedMessage);
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                // No messages instances
            }

            @Override
            public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
            {
                handler.handle(new Transaction.StoredXidRecord()
                {
                    @Override
                    public long getFormat()
                    {
                        return format;
                    }

                    @Override
                    public byte[] getGlobalId()
                    {
                        return globalId;
                    }

                    @Override
                    public byte[] getBranchId()
                    {
                        return branchId;
                    }
                }, enqueues, dequeues);
            }

            @Override
            public Transaction newTransaction()
            {
                return transaction;
            }
        };

        final DtxRegistry dtxRegistry = new DtxRegistry(_virtualHost);

        when(_virtualHost.getMessageStore()).thenReturn(store);
        when(_virtualHost.getDtxRegistry()).thenReturn(dtxRegistry);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        final DtxBranch branch = dtxRegistry.getBranch(new Xid(format, globalId, branchId));
        assertNotNull(branch, "Expected dtx branch to be created");
        branch.commit();

        final ServerMessage<?> message = storedMessage.getMetaData().getType().createMessage(storedMessage);
        verify(queue, times(1)).enqueue(eq(message), isNull(), isNull());
        verify(transaction).commitTran();
    }

    @Test
    public void testRecoveryOfSingleDequeueWithDistributedTransaction()
    {
        final UUID queueId = randomUUID();
        final Queue<?> queue = createRegisteredMockQueue(queueId);
        final Transaction transaction = mock(Transaction.class);
        final StoredMessage<StorableMessageMetaData> storedMessage = createMockStoredMessage(1);
        final long messageId = storedMessage.getMessageNumber();

        final Transaction.DequeueRecord dequeueRecord = createMockDequeueRecord(queueId, messageId);

        final QueueEntry queueEntry = mock(QueueEntry.class);
        when(queueEntry.acquire()).thenReturn(true);
        when(queue.getMessageOnTheQueue(messageId)).thenReturn(queueEntry);

        final long format = 1;
        final byte[] globalId = new byte[] {0};
        final byte[] branchId = new byte[] {0};
        final EnqueueRecord[] enqueues = {};
        final Transaction.DequeueRecord[] dequeues = { dequeueRecord };

        final MessageStore store = new NullMessageStore()
        {
            @Override
            public void visitMessages(final MessageHandler handler) throws StoreException
            {
                handler.handle(storedMessage);
            }

            @Override
            public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
            {
                // We need the message to be enqueued onto the queue so that later the distributed transaction
                // can dequeue it.
                handler.handle(new TestMessageEnqueueRecord(queue.getId(), messageId));
            }

            @Override
            public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
            {
                handler.handle(new Transaction.StoredXidRecord()
                {
                    @Override
                    public long getFormat()
                    {
                        return format;
                    }

                    @Override
                    public byte[] getGlobalId()
                    {
                        return globalId;
                    }

                    @Override
                    public byte[] getBranchId()
                    {
                        return branchId;
                    }
                }, enqueues, dequeues);
            }

            @Override
            public Transaction newTransaction()
            {
                return transaction;
            }
        };

        final DtxRegistry dtxRegistry = new DtxRegistry(_virtualHost);

        when(_virtualHost.getMessageStore()).thenReturn(store);
        when(_virtualHost.getDtxRegistry()).thenReturn(dtxRegistry);

        final SynchronousMessageStoreRecoverer recoverer = new SynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);

        final DtxBranch branch = dtxRegistry.getBranch(new Xid(format, globalId, branchId));
        assertNotNull(branch, "Expected dtx branch to be created");
        branch.commit();

        verify(queueEntry, times(1)).delete();
        verify(transaction).commitTran();
    }

    protected EnqueueRecord createMockRecord(final Queue<?> queue, final EnqueueableMessage<?> enqueueableMessage)
    {
        final EnqueueRecord enqueueRecord = mock(EnqueueRecord.class);
        when(enqueueRecord.getMessage()).thenReturn(enqueueableMessage);
        when(enqueueRecord.getResource()).thenReturn(queue);
        return enqueueRecord;
    }

    protected Transaction.DequeueRecord createMockDequeueRecord(final UUID queueId, final long messageNumber)
    {
        final Transaction.DequeueRecord dequeueRecord = mock(Transaction.DequeueRecord.class);
        final MessageEnqueueRecord enqueueRecord = mock(MessageEnqueueRecord.class);
        when(enqueueRecord.getMessageNumber()).thenReturn(messageNumber);
        when(enqueueRecord.getQueueId()).thenReturn(queueId);
        when(dequeueRecord.getEnqueueRecord()).thenReturn(enqueueRecord);
        return dequeueRecord;
    }

    protected EnqueueableMessage<?> createMockEnqueueableMessage(final long messageId,
                                                                 final StoredMessage<StorableMessageMetaData> storedMessage)
    {
        final EnqueueableMessage enqueueableMessage = mock(EnqueueableMessage.class);
        when(enqueueableMessage.getMessageNumber()).thenReturn(messageId);
        when(enqueueableMessage.getStoredMessage()).thenReturn(storedMessage);
        return enqueueableMessage;
    }

    private StoredMessage<StorableMessageMetaData> createMockStoredMessage(final long messageId)
    {
        final TestMessageMetaData metaData = new TestMessageMetaData(messageId, 0);
        final StoredMessage<StorableMessageMetaData> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMessageNumber()).thenReturn(messageId);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return storedMessage;
    }

    private Queue<?> createRegisteredMockQueue()
    {
        return createRegisteredMockQueue(randomUUID());
    }

    private Queue<?> createRegisteredMockQueue(UUID queueId)
    {
        final Queue queue = mock(Queue.class);
        when(queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
        when(queue.getId()).thenReturn(queueId);
        when(queue.getName()).thenReturn("test-queue");
        when(_virtualHost.getAttainedQueue(queueId)).thenReturn(queue);
        when(_virtualHost.getAttainedChildFromAddress(Queue.class, "test-queue")).thenReturn(queue);
        return queue;
    }

    private static final class MessageEnqueueRecordMatcher implements ArgumentMatcher<MessageEnqueueRecord>
    {
        private final long _messageId;
        private final UUID _queueId;

        private MessageEnqueueRecordMatcher(final UUID queueId, final long messageId)
        {
            _messageId = messageId;
            _queueId = queueId;
        }

        @Override
        public boolean matches(final MessageEnqueueRecord argument)
        {
            return argument.getMessageNumber() == _messageId
                    && argument.getQueueId().equals(_queueId);
        }
    }

    private static class TestMessageEnqueueRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;
        private final long _messageId;

        TestMessageEnqueueRecord(final UUID queueId, final long messageId)
        {
            _queueId = queueId;
            _messageId = messageId;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }
    }
}
