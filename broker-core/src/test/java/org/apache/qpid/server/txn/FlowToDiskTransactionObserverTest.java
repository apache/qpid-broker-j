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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class FlowToDiskTransactionObserverTest extends UnitTestBase
{
    private static final int MAX_UNCOMMITTED_IN_MEMORY_SIZE = 100;
    private FlowToDiskTransactionObserver _flowToDiskMessageObserver;
    private EventLogger _eventLogger;
    private LogSubject _logSubject;
    private ServerTransaction _transaction;

    @BeforeEach
    public void setUp() throws Exception
    {
        _eventLogger = mock(EventLogger.class);
        _logSubject = mock(LogSubject.class);
        _flowToDiskMessageObserver = new FlowToDiskTransactionObserver(MAX_UNCOMMITTED_IN_MEMORY_SIZE,
                                                                       _logSubject,
                                                                       _eventLogger);
        _transaction = mock(ServerTransaction.class);
    }

    @Test
    public void testOnMessageEnqueue()
    {
        final EnqueueableMessage<?> message1 = createMessage(MAX_UNCOMMITTED_IN_MEMORY_SIZE);
        final EnqueueableMessage<?> message2 = createMessage(1);
        final EnqueueableMessage<?> message3 = createMessage(1);

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message1);

        final StoredMessage<?> handle1 = message1.getStoredMessage();
        verify(handle1, never()).flowToDisk();
        verify(_eventLogger, never()).message(same(_logSubject), any(LogMessage.class));

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message2);

        final StoredMessage<?> handle2 = message2.getStoredMessage();
        verify(handle1).flowToDisk();
        verify(handle2).flowToDisk();
        verify(_eventLogger).message(same(_logSubject), any(LogMessage.class));

        final ServerTransaction transaction2 = mock(ServerTransaction.class);
        _flowToDiskMessageObserver.onMessageEnqueue(transaction2, message3);

        final StoredMessage<?> handle3 = message2.getStoredMessage();
        verify(handle1).flowToDisk();
        verify(handle2).flowToDisk();
        verify(handle3).flowToDisk();
        verify(_eventLogger).message(same(_logSubject), any(LogMessage.class));
    }

    @Test
    public void testOnDischarge()
    {
        final EnqueueableMessage<?> message1 = createMessage(MAX_UNCOMMITTED_IN_MEMORY_SIZE - 1);
        final EnqueueableMessage<?> message2 = createMessage(1);
        final EnqueueableMessage<?> message3 = createMessage(1);

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message1);
        final ServerTransaction transaction2 = mock(ServerTransaction.class);
        _flowToDiskMessageObserver.onMessageEnqueue(transaction2, message2);
        _flowToDiskMessageObserver.onDischarge(_transaction);
        _flowToDiskMessageObserver.onMessageEnqueue(transaction2, message3);

        final StoredMessage<?> handle1 = message1.getStoredMessage();
        final StoredMessage<?> handle2 = message2.getStoredMessage();
        final StoredMessage<?> handle3 = message2.getStoredMessage();
        verify(handle1, never()).flowToDisk();
        verify(handle2, never()).flowToDisk();
        verify(handle3, never()).flowToDisk();
        verify(_eventLogger, never()).message(same(_logSubject), any(LogMessage.class));
    }

    @Test
    public void testBreachLimitTwice()
    {
        final EnqueueableMessage<?> message1 = createMessage(MAX_UNCOMMITTED_IN_MEMORY_SIZE + 1);

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message1);

        final StoredMessage<?> handle1 = message1.getStoredMessage();
        verify(handle1).flowToDisk();
        verify(_eventLogger, times(1)).message(same(_logSubject), any(LogMessage.class));

        _flowToDiskMessageObserver.onDischarge(_transaction);

        final EnqueueableMessage<?> message2 = createMessage(MAX_UNCOMMITTED_IN_MEMORY_SIZE / 2);
        final EnqueueableMessage<?> message3 = createMessage((MAX_UNCOMMITTED_IN_MEMORY_SIZE / 2) + 1);

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message2);

        final StoredMessage<?> handle2 = message2.getStoredMessage();
        verify(handle2, never()).flowToDisk();

        _flowToDiskMessageObserver.onMessageEnqueue(_transaction, message3);

        final StoredMessage<?> handle3 = message3.getStoredMessage();
        verify(handle2).flowToDisk();
        verify(handle3).flowToDisk();

        verify(_eventLogger, times(2)).message(same(_logSubject), any(LogMessage.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private EnqueueableMessage<?> createMessage(int size)
    {
        final EnqueueableMessage<?> message = mock(EnqueueableMessage.class);
        final StoredMessage handle = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(handle);
        when(handle.getContentSize()).thenReturn(size);
        final StorableMessageMetaData metadata = mock(StorableMessageMetaData.class);
        when(metadata.getStorableSize()).thenReturn(0);
        when(metadata.getContentSize()).thenReturn(size);
        when(handle.getMetaData()).thenReturn(metadata);
        return message;
    }
}
