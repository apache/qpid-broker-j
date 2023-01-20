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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("rawtypes")
public class AsynchronousMessageStoreRecovererTest extends UnitTestBase
{
    private QueueManagingVirtualHost<?> _virtualHost;
    private MessageStore _store;
    private MessageStore.MessageStoreReader _storeReader;

    @BeforeEach
    public void setUp() throws Exception
    {
        _virtualHost = mock(QueueManagingVirtualHost.class);
        _store = mock(MessageStore.class);
        _storeReader = mock(MessageStore.MessageStoreReader.class);

        when(_virtualHost.getEventLogger()).thenReturn(new EventLogger());
        when(_virtualHost.getMessageStore()).thenReturn(_store);
        when(_store.newMessageStoreReader()).thenReturn(_storeReader);
    }

    @Test
    public void testExceptionOnRecovery()
    {
        final ServerScopedRuntimeException exception = new ServerScopedRuntimeException("test");
        doThrow(exception).when(_storeReader).visitMessageInstances(any(TransactionLogResource.class),
                any(MessageInstanceHandler.class));
        final Queue<?> queue = mock(Queue.class);
        when(_virtualHost.getChildren(eq(Queue.class))).thenReturn(Set.of(queue));

        final AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        final ListenableFuture<Void> result = recoverer.recover(_virtualHost);
        final ExecutionException thrown = assertThrows(ExecutionException.class,
                result::get,
                "ServerScopedRuntimeException should be rethrown");
        assertEquals(exception, thrown.getCause(), "Unexpected cause");
    }

    @Test
    public void testRecoveryEmptyQueue() throws Exception
    {
        final Queue<?> queue = mock(Queue.class);
        when(_virtualHost.getChildren(eq(Queue.class))).thenReturn(Set.of(queue));

        final AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        final ListenableFuture<Void> result = recoverer.recover(_virtualHost);
        assertNull(result.get());
    }

    @Test
    public void testRecoveryWhenLastRecoveryMessageIsConsumedBeforeRecoveryCompleted() throws Exception
    {
        final Queue<?> queue = mock(Queue.class);
        when(queue.getId()).thenReturn(randomUUID());
        when(_virtualHost.getChildren(eq(Queue.class))).thenReturn(Set.of(queue));
        when(_store.getNextMessageId()).thenReturn(3L);
        when(_store.newTransaction()).thenReturn(mock(Transaction.class));

        final List<StoredMessage<?>> testMessages = new ArrayList<>();
        final StoredMessage<?> storedMessage = createTestMessage(1L);
        testMessages.add(storedMessage);
        final StoredMessage<?> orphanedMessage = createTestMessage(2L);
        testMessages.add(orphanedMessage);

        final StoredMessage<?> newMessage = createTestMessage(4L);
        testMessages.add(newMessage);

        final MessageEnqueueRecord messageEnqueueRecord = mock(MessageEnqueueRecord.class);
        final UUID id = queue.getId();
        when(messageEnqueueRecord.getQueueId()).thenReturn(id);
        when(messageEnqueueRecord.getMessageNumber()).thenReturn(1L);

        final MockStoreReader storeReader = new MockStoreReader(Collections.singletonList(messageEnqueueRecord), testMessages);
        when(_store.newMessageStoreReader()).thenReturn(storeReader);

        final AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        final ListenableFuture<Void> result = recoverer.recover(_virtualHost);
        assertNull(result.get());

        verify(orphanedMessage, times(1)).remove();
        verify(newMessage, times(0)).remove();
        verify(queue).recover(argThat(serverMessage ->
                serverMessage.getMessageNumber() == storedMessage.getMessageNumber()), same(messageEnqueueRecord));
    }

    private StoredMessage<?> createTestMessage(final long messageNumber)
    {
        final StorableMessageMetaData metaData = new TestMessageMetaData(messageNumber, 0);
        final StoredMessage storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMessageNumber()).thenReturn(messageNumber);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return storedMessage;
    }

    private static class MockStoreReader implements MessageStore.MessageStoreReader
    {
        private final List<MessageEnqueueRecord> _messageEnqueueRecords;
        private final List<StoredMessage<?>> _messages;

        private MockStoreReader(final List<MessageEnqueueRecord> messageEnqueueRecords, final List<StoredMessage<?>> messages)
        {
            _messageEnqueueRecords = messageEnqueueRecords;
            _messages = messages;
        }

        @Override
        public void visitMessages(final MessageHandler handler) throws StoreException
        {
            for (StoredMessage<?> message: _messages)
            {
                handler.handle(message);
            }
        }

        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
        {
            for (MessageEnqueueRecord record: _messageEnqueueRecords)
            {
                handler.handle(record);
            }
        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue,
                                          final MessageInstanceHandler handler) throws StoreException
        {
            visitMessageInstances(handler);
        }

        @Override
        public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
        {

        }

        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            for (final StoredMessage<?> message: _messages)
            {
                if (message.getMessageNumber() == messageId)
                {
                    return message;
                }
            }
            return null;
        }

        @Override
        public void close()
        {

        }
    }
}
