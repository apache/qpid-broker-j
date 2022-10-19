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
package org.apache.qpid.server.store;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.Transaction.EnqueueRecord;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class MessageStoreTestCase extends UnitTestBase
{
    private MessageStore _store;
    private VirtualHost<?> _parent;
    private MessageStore.MessageStoreReader _storeReader;
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;


    @Before
    public void setUp() throws Exception
    {
        _parent = createVirtualHost();

        _store = createMessageStore();

        _store.openMessageStore(_parent);
        _storeReader = _store.newMessageStoreReader();

        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @After
    public void tearDown() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
    }

    protected VirtualHost<?> getVirtualHost()
    {
        return _parent;
    }

    protected abstract VirtualHost createVirtualHost();

    protected abstract MessageStore createMessageStore();

    protected abstract boolean flowToDiskSupported();

    protected MessageStore getStore()
    {
        return _store;
    }

    protected void reopenStore() throws Exception
    {
        _storeReader.close();
        _store.closeMessageStore();

        _store = createMessageStore();
        _store.openMessageStore(_parent);
        _storeReader = _store.newMessageStoreReader();

    }

    @Test
    public void testAddAndRemoveRecordXid() throws Exception
    {
        long format = 1L;
        EnqueueRecord enqueueRecord = getTestRecord(1);
        TestRecord dequeueRecord = getTestRecord(2);
        EnqueueRecord[] enqueues = { enqueueRecord };
        TestRecord[] dequeues = { dequeueRecord };
        byte[] globalId = new byte[] { 1 };
        byte[] branchId = new byte[] { 2 };

        Transaction transaction = _store.newTransaction();
        final Transaction.StoredXidRecord record =
                transaction.recordXid(format, globalId, branchId, enqueues, dequeues);
        transaction.commitTran();

        reopenStore();

        DistributedTransactionHandler handler = mock(DistributedTransactionHandler.class);
        _storeReader.visitDistributedTransactions(handler);
        verify(handler, times(1)).handle(eq(record), argThat(new RecordMatcher(enqueues)), argThat(new DequeueRecordMatcher(
                dequeues)));

        transaction = _store.newTransaction();
        transaction.removeXid(record);
        transaction.commitTran();

        reopenStore();

        handler = mock(DistributedTransactionHandler.class);
        _storeReader.visitDistributedTransactions(handler);
        verify(handler, never()).handle(eq(record), argThat(new RecordMatcher(enqueues)), argThat(new DequeueRecordMatcher(
                dequeues)));
    }

    @Test
    public void testVisitMessages() throws Exception
    {
        long messageId = 1;
        int contentSize = 0;
        final MessageHandle<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(messageId, contentSize));
        enqueueMessage(message.allContentAdded(), "dummyQ");

        MessageHandler handler = mock(MessageHandler.class);
        _storeReader.visitMessages(handler);

        verify(handler, times(1)).handle(argThat(new MessageMetaDataMatcher(messageId)));

    }

    private void enqueueMessage(final StoredMessage<TestMessageMetaData> message, final String queueName)
    {
        Transaction txn = _store.newTransaction();
        txn.enqueueMessage(new TransactionLogResource()
        {
            private final UUID _id = UUID.nameUUIDFromBytes(queueName.getBytes());

            @Override
            public String getName()
            {
                return queueName;
            }

            @Override
            public UUID getId()
            {
                return _id;
            }

            @Override
            public MessageDurability getMessageDurability()
            {
                return MessageDurability.DEFAULT;
            }
        }, new EnqueueableMessage()
        {
            @Override
            public long getMessageNumber()
            {
                return message.getMessageNumber();
            }

            @Override
            public boolean isPersistent()
            {
                return true;
            }

            @Override
            public StoredMessage getStoredMessage()
            {
                return message;
            }
        });
        txn.commitTran();
    }

    @Test
    public void testVisitMessagesAborted() throws Exception
    {
        int contentSize = 0;
        for (int i = 0; i < 3; i++)
        {
            final MessageHandle<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(i + 1, contentSize));
            enqueueMessage(message.allContentAdded(), "dummyQ");
        }

        MessageHandler handler = mock(MessageHandler.class);
        when(handler.handle(any(StoredMessage.class))).thenReturn(true, false);

        _storeReader.visitMessages(handler);

        verify(handler, times(2)).handle(any(StoredMessage.class));
    }

    @Test
    public void testReopenedMessageStoreUsesLastMessageId() throws Exception
    {
        int contentSize = 0;
        for (int i = 0; i < 3; i++)
        {
            final StoredMessage<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(i + 1, contentSize)).allContentAdded();
            enqueueMessage(message, "dummyQ");

        }

        reopenStore();

        final StoredMessage<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(4, contentSize)).allContentAdded();

        enqueueMessage(message, "dummyQ");


        assertTrue("Unexpected message id " + message.getMessageNumber(), message.getMessageNumber() >= 4);
    }

    @Test
    public void testVisitMessageInstances() throws Exception
    {
        long messageId = 1;
        int contentSize = 0;
        final StoredMessage<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(messageId, contentSize)).allContentAdded();

        EnqueueableMessage enqueueableMessage = createMockEnqueueableMessage(messageId, message);

        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId);

        Transaction transaction = _store.newTransaction();
        transaction.enqueueMessage(queue, enqueueableMessage);
        transaction.commitTran();

        MessageInstanceHandler handler = mock(MessageInstanceHandler.class);
        _storeReader.visitMessageInstances(handler);
        verify(handler, times(1)).handle(argThat(new EnqueueRecordMatcher(queueId, messageId)));
    }

    @Test
    public void testVisitDistributedTransactions() throws Exception
    {
        long format = 1L;
        byte[] branchId = new byte[] { 2 };
        byte[] globalId = new byte[] { 1 };
        EnqueueRecord enqueueRecord = getTestRecord(1);
        TestRecord dequeueRecord = getTestRecord(2);
        EnqueueRecord[] enqueues = { enqueueRecord };
        TestRecord[] dequeues = { dequeueRecord };

        Transaction transaction = _store.newTransaction();
        final Transaction.StoredXidRecord record =
                transaction.recordXid(format, globalId, branchId, enqueues, dequeues);
        transaction.commitTran();

        DistributedTransactionHandler handler = mock(DistributedTransactionHandler.class);
        _storeReader.visitDistributedTransactions(handler);

        verify(handler, times(1)).handle(eq(record),
                                         argThat(new RecordMatcher(enqueues)),
                                         argThat(new DequeueRecordMatcher(dequeues)));

    }

    @Test
    public void testCommitTransaction() throws Exception
    {
        final UUID mockQueueId = UUIDGenerator.generateRandomUUID();
        TransactionLogResource mockQueue = createTransactionLogResource(mockQueueId);

        Transaction txn = getStore().newTransaction();

        long messageId1 = 1L;
        long messageId2 = 5L;
        final EnqueueableMessage enqueueableMessage1 = createEnqueueableMessage(messageId1);
        final EnqueueableMessage enqueueableMessage2 = createEnqueueableMessage(messageId2);

        txn.enqueueMessage(mockQueue, enqueueableMessage1);
        txn.enqueueMessage(mockQueue, enqueueableMessage2);
        txn.commitTran();

        QueueFilteringMessageInstanceHandler filter = new QueueFilteringMessageInstanceHandler(mockQueueId);
        _storeReader.visitMessageInstances(filter);
        Set<Long> enqueuedIds = filter.getEnqueuedIds();

        assertEquals("Number of enqueued messages is incorrect", 2, enqueuedIds.size());
        assertTrue("Message with id " + messageId1 + " is not found", enqueuedIds.contains(messageId1));
        assertTrue("Message with id " + messageId2 + " is not found", enqueuedIds.contains(messageId2));
    }

    @Test
    public void testRollbackTransactionBeforeCommit() throws Exception
    {
        final UUID mockQueueId = UUIDGenerator.generateRandomUUID();
        TransactionLogResource mockQueue = createTransactionLogResource(mockQueueId);

        long messageId1 = 21L;
        long messageId2 = 22L;
        long messageId3 = 23L;
        final EnqueueableMessage enqueueableMessage1 = createEnqueueableMessage(messageId1);
        final EnqueueableMessage enqueueableMessage2 = createEnqueueableMessage(messageId2);
        final EnqueueableMessage enqueueableMessage3 = createEnqueueableMessage(messageId3);

        Transaction txn = getStore().newTransaction();

        txn.enqueueMessage(mockQueue, enqueueableMessage1);
        txn.abortTran();

        txn = getStore().newTransaction();
        txn.enqueueMessage(mockQueue, enqueueableMessage2);
        txn.enqueueMessage(mockQueue, enqueueableMessage3);
        txn.commitTran();

        QueueFilteringMessageInstanceHandler filter = new QueueFilteringMessageInstanceHandler(mockQueueId);
        _storeReader.visitMessageInstances(filter);
        Set<Long> enqueuedIds = filter.getEnqueuedIds();

        assertEquals("Number of enqueued messages is incorrect", 2, enqueuedIds.size());
        assertTrue("Message with id " + messageId2 + " is not found", enqueuedIds.contains(messageId2));
        assertTrue("Message with id " + messageId3 + " is not found", enqueuedIds.contains(messageId3));
    }

    @Test
    public void testRollbackTransactionAfterCommit() throws Exception
    {
        final UUID mockQueueId = UUIDGenerator.generateRandomUUID();
        TransactionLogResource mockQueue = createTransactionLogResource(mockQueueId);

        long messageId1 = 30L;
        long messageId2 = 31L;
        long messageId3 = 32L;

        final EnqueueableMessage enqueueableMessage1 = createEnqueueableMessage(messageId1);
        final EnqueueableMessage enqueueableMessage2 = createEnqueueableMessage(messageId2);
        final EnqueueableMessage enqueueableMessage3 = createEnqueueableMessage(messageId3);

        Transaction txn = getStore().newTransaction();

        txn.enqueueMessage(mockQueue, enqueueableMessage1);
        txn.commitTran();

        txn = getStore().newTransaction();
        txn.enqueueMessage(mockQueue, enqueueableMessage2);
        txn.abortTran();

        txn = getStore().newTransaction();
        txn.enqueueMessage(mockQueue, enqueueableMessage3);
        txn.commitTran();

        QueueFilteringMessageInstanceHandler filter = new QueueFilteringMessageInstanceHandler(mockQueueId);
        _storeReader.visitMessageInstances(filter);
        Set<Long> enqueuedIds = filter.getEnqueuedIds();

        assertEquals("Number of enqueued messages is incorrect", 2, enqueuedIds.size());
        assertTrue("Message with id " + messageId1 + " is not found", enqueuedIds.contains(messageId1));
        assertTrue("Message with id " + messageId3 + " is not found", enqueuedIds.contains(messageId3));
    }

    @Test
    public void testAddAndRemoveMessageWithoutContent() throws Exception
    {
        long messageId = 1;
        int contentSize = 0;
        final StoredMessage<TestMessageMetaData> message = _store.addMessage(new TestMessageMetaData(messageId, contentSize)).allContentAdded();
        enqueueMessage(message, "dummyQ");

        final AtomicReference<StoredMessage<?>> retrievedMessageRef = new AtomicReference<StoredMessage<?>>();
        _storeReader.visitMessages(new MessageHandler()
        {

            @Override
            public boolean handle(StoredMessage<?> storedMessage)
            {
                retrievedMessageRef.set(storedMessage);
                return true;
            }
        });

        StoredMessage<?> retrievedMessage = retrievedMessageRef.get();
        assertNotNull("Message was not found", retrievedMessageRef);
        assertEquals("Unexpected retrieved message", message.getMessageNumber(), retrievedMessage.getMessageNumber());

        retrievedMessage.remove();

        retrievedMessageRef.set(null);
        _storeReader.visitMessages(new MessageHandler()
        {

            @Override
            public boolean handle(StoredMessage<?> storedMessage)
            {
                retrievedMessageRef.set(storedMessage);
                return true;
            }
        });
        assertNull(retrievedMessageRef.get());
    }

    @Test
    public void testMessageDeleted() throws Exception
    {
        MessageStore.MessageDeleteListener listener = mock(MessageStore.MessageDeleteListener.class);
        _store.addMessageDeleteListener(listener);

        long messageId = 1;
        int contentSize = 0;
        final MessageHandle<TestMessageMetaData> messageHandle = _store.addMessage(new TestMessageMetaData(messageId, contentSize));
        StoredMessage<TestMessageMetaData> message = messageHandle.allContentAdded();
        message.remove();

        verify(listener, times(1)).messageDeleted(message);
    }

    @Test
    public void testFlowToDisk() throws Exception
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();

        assertEquals(storedMessage.getContentSize() + storedMessage.getMetadataSize(), storedMessage.getInMemorySize());
        assertTrue(storedMessage.flowToDisk());
        assertEquals(0, storedMessage.getInMemorySize());
    }

    @Test
    public void testFlowToDiskAfterMetadataReload()
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();

        assertTrue(storedMessage.flowToDisk());
        assertNotNull(storedMessage.getMetaData());
        assertEquals(storedMessage.getMetadataSize(), storedMessage.getInMemorySize());

        assertTrue(storedMessage.flowToDisk());
        assertEquals(0, storedMessage.getInMemorySize());
    }

    @Test
    public void testFlowToDiskAfterContentReload()
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();

        assertTrue(storedMessage.flowToDisk());
        assertNotNull(storedMessage.getContent(0, storedMessage.getContentSize()));
        assertEquals(storedMessage.getContentSize(), storedMessage.getInMemorySize());

        assertTrue(storedMessage.flowToDisk());
        assertEquals(0, storedMessage.getInMemorySize());
    }


    @Test
    public void testIsInContentInMemoryBeforeFlowControl()
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();

        assertTrue(storedMessage.isInContentInMemory());
    }

    @Test
    public void testIsInContentInMemoryAfterFlowControl()
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();
        assertTrue(storedMessage.flowToDisk());
        assertFalse(storedMessage.isInContentInMemory());
    }

    @Test
    public void testIsInContentInMemoryAfterReload()
    {
        assumeThat(flowToDiskSupported(), is(equalTo(true)));

        final StoredMessage<?> storedMessage = createStoredMessage();
        assertTrue(storedMessage.flowToDisk());
        assertFalse(storedMessage.isInContentInMemory());
        assertNotNull(storedMessage.getContent(0, storedMessage.getContentSize()));
        assertTrue(storedMessage.isInContentInMemory());
    }

    private StoredMessage<?> createStoredMessage()
    {
        return createStoredMessage(Collections.singletonMap("test", "testValue"), "testContent", "testQueue");
    }

    private StoredMessage<?> createStoredMessage(final Map<String, String> headers,
                                                 final String content,
                                                 final String queueName)
    {
        return createInternalTestMessage(headers, content, queueName).getStoredMessage();
    }

    private InternalMessage createInternalTestMessage(final Map<String, String> headers,
                                                      final String content,
                                                      final String queueName)
    {
        final AMQMessageHeader messageHeader = mock(AMQMessageHeader.class);
        if (headers != null)
        {
            headers.forEach((k,v) -> when(messageHeader.getHeader(k)).thenReturn(v));
            when(messageHeader.getHeaderNames()).thenReturn(headers.keySet());
        }

        return InternalMessage.createMessage(_store, messageHeader, content, true, queueName);
    }

    private TransactionLogResource createTransactionLogResource(UUID queueId)
    {
        TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(queueId);
        when(queue.getName()).thenReturn("testQueue");
        when(queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
        return queue;
    }

    private EnqueueableMessage createMockEnqueueableMessage(long messageId, final StoredMessage<TestMessageMetaData> message)
    {
        EnqueueableMessage enqueueableMessage = mock(EnqueueableMessage.class);
        when(enqueueableMessage.isPersistent()).thenReturn(true);
        when(enqueueableMessage.getMessageNumber()).thenReturn(messageId);
        when(enqueueableMessage.getStoredMessage()).thenReturn(message);
        return enqueueableMessage;
    }

    private TestRecord getTestRecord(long messageNumber)
    {
        UUID queueId1 = UUIDGenerator.generateRandomUUID();
        TransactionLogResource queue1 = mock(TransactionLogResource.class);
        when(queue1.getId()).thenReturn(queueId1);
        EnqueueableMessage message1 = mock(EnqueueableMessage.class);
        when(message1.isPersistent()).thenReturn(true);
        when(message1.getMessageNumber()).thenReturn(messageNumber);
        final StoredMessage<?> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMessageNumber()).thenReturn(messageNumber);
        when(message1.getStoredMessage()).thenReturn(storedMessage);
        TestRecord enqueueRecord = new TestRecord(queue1, message1);
        return enqueueRecord;
    }

    private EnqueueableMessage createEnqueueableMessage(long messageId1)
    {
        final StoredMessage<TestMessageMetaData> message1 = _store.addMessage(new TestMessageMetaData(messageId1, 0)).allContentAdded();
        EnqueueableMessage enqueueableMessage1 = createMockEnqueueableMessage(messageId1, message1);
        return enqueueableMessage1;
    }

    private static class MessageMetaDataMatcher implements ArgumentMatcher<StoredMessage<?>>
    {
        private final long _messageNumber;

        MessageMetaDataMatcher(long messageNumber)
        {
            super();
            _messageNumber = messageNumber;
        }

        @Override
        public boolean matches(StoredMessage<?> obj)
        {
            return obj.getMessageNumber() == _messageNumber;
        }
    }

    private static class QueueFilteringMessageInstanceHandler implements MessageInstanceHandler
    {
        private final UUID _queueId;
        private final Set<Long> _enqueuedIds = new HashSet<>();

        QueueFilteringMessageInstanceHandler(UUID queueId)
        {
            _queueId = queueId;
        }

        @Override
        public boolean handle(final MessageEnqueueRecord record)
        {
            long messageId = record.getMessageNumber();
            if (record.getQueueId().equals(_queueId))
            {
                if (_enqueuedIds.contains(messageId))
                {
                    fail("Queue with id " + _queueId + " contains duplicate message ids");
                }
                _enqueuedIds.add(messageId);
            }
            return true;
        }

        Set<Long> getEnqueuedIds()
        {
            return _enqueuedIds;
        }
    }

    private static class EnqueueRecordMatcher implements ArgumentMatcher<MessageEnqueueRecord>
    {
        private final UUID _queueId;
        private final long _messageId;

        EnqueueRecordMatcher(final UUID queueId, final long messageId)
        {
            _queueId = queueId;
            _messageId = messageId;
        }

        @Override
        public boolean matches(final MessageEnqueueRecord record)
        {
            return record.getQueueId().equals(_queueId) && record.getMessageNumber() == _messageId;
        }
    }


    private static class RecordMatcher implements ArgumentMatcher<Transaction.EnqueueRecord[]>
    {

        private final EnqueueRecord[] _expect;

        RecordMatcher(Transaction.EnqueueRecord[] expect)
        {
            _expect = expect;
        }

        @Override
        public boolean matches(final Transaction.EnqueueRecord[] actual)
        {
            if(actual.length == _expect.length)
            {
                for(int i = 0; i < actual.length; i++)
                {
                    if(!actual[i].getResource().getId().equals(_expect[i].getResource().getId())
                       || actual[i].getMessage().getMessageNumber() != _expect[i].getMessage().getMessageNumber())
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                return false;
            }

        }
    }

    private static class DequeueRecordMatcher implements ArgumentMatcher<Transaction.DequeueRecord[]>
    {

        private final Transaction.DequeueRecord[] _expect;

        DequeueRecordMatcher(Transaction.DequeueRecord[] expect)
        {
            _expect = expect;
        }

        @Override
        public boolean matches(final Transaction.DequeueRecord[] actual)
        {
            if(actual.length == _expect.length)
            {
                for(int i = 0; i < actual.length; i++)
                {
                    if(!actual[i].getEnqueueRecord().getQueueId().equals(_expect[i].getEnqueueRecord().getQueueId())
                       || actual[i].getEnqueueRecord().getMessageNumber() != _expect[i].getEnqueueRecord().getMessageNumber())
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
