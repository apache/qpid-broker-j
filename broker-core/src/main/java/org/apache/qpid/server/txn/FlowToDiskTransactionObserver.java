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

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;

public class FlowToDiskTransactionObserver implements TransactionObserver
{
    private final AtomicLong _uncommittedMessageSize;
    private final ConcurrentMap<ServerTransaction, TransactionDetails> _uncommittedMessages;
    private final LogSubject _logSubject;
    private final EventLogger _eventLogger;
    private final long _maxUncommittedInMemorySize;
    private volatile boolean _reported;

    public FlowToDiskTransactionObserver(final long maxUncommittedInMemorySize,
                                         final LogSubject logSubject,
                                         final EventLogger eventLogger)
    {
        _uncommittedMessageSize = new AtomicLong();
        _uncommittedMessages = new ConcurrentHashMap<>();
        _logSubject = logSubject;
        _eventLogger = eventLogger;
        _maxUncommittedInMemorySize = maxUncommittedInMemorySize;
    }

    @Override
    public void onMessageEnqueue(final ServerTransaction transaction,
                                 final EnqueueableMessage<? extends StorableMessageMetaData> message)
    {
        StoredMessage<? extends StorableMessageMetaData> handle = message.getStoredMessage();
        long messageSize = handle.getContentSize() + handle.getMetadataSize();
        long newUncommittedSize = _uncommittedMessageSize.addAndGet(messageSize);
        TransactionDetails details = _uncommittedMessages.computeIfAbsent(transaction, key -> new TransactionDetails());
        details.messageEnqueued(handle);
        if (newUncommittedSize > _maxUncommittedInMemorySize)
        {
            // flow to disk only current transaction messages
            // in order to handle malformed messages on correct channel
            try
            {
                details.flowToDisk();
            }
            finally
            {
                if (!_reported)
                {
                    _eventLogger.message(_logSubject, ConnectionMessages.LARGE_TRANSACTION_WARN(newUncommittedSize, _maxUncommittedInMemorySize));
                    _reported = true;
                }
            }
        }
    }

    @Override
    public void onDischarge(final ServerTransaction transaction)
    {
        TransactionDetails transactionDetails = _uncommittedMessages.remove(transaction);
        if (transactionDetails != null)
        {
            _uncommittedMessageSize.addAndGet(-transactionDetails.getUncommittedMessageSize());

        }
        if (_maxUncommittedInMemorySize > _uncommittedMessageSize.get())
        {
            _reported = false;
        }
    }

    @Override
    public void reset()
    {
        _uncommittedMessages.clear();
        _uncommittedMessageSize.set(0);
    }

    private static class TransactionDetails
    {
        private final AtomicLong _uncommittedMessageSize;
        private final Queue<StoredMessage<? extends StorableMessageMetaData>> _uncommittedMessages;

        private TransactionDetails()
        {
            _uncommittedMessageSize = new AtomicLong();
            _uncommittedMessages = new ConcurrentLinkedQueue<>();
        }

        private void messageEnqueued(StoredMessage<? extends StorableMessageMetaData> handle)
        {
            long size = handle.getContentSize() + handle.getMetadataSize();
            _uncommittedMessageSize.addAndGet(size);
            _uncommittedMessages.add(handle);
        }

        private void flowToDisk()
        {
            for (StoredMessage<? extends StorableMessageMetaData> uncommittedHandle: _uncommittedMessages )
            {
                uncommittedHandle.flowToDisk();
            }
            _uncommittedMessages.clear();
        }

        private long getUncommittedMessageSize()
        {
            return _uncommittedMessageSize.get();
        }
    }

}
