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
import org.apache.qpid.server.logging.messages.ChannelMessages;
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
        long size = handle.getContentSize() + handle.getMetaData().getStorableSize();
        long uncommittedMessages = _uncommittedMessageSize.accumulateAndGet(size,
                                                                            (left, right) -> left + right);
        if (uncommittedMessages > _maxUncommittedInMemorySize)
        {
            handle.flowToDisk();
            if(!_uncommittedMessages.isEmpty() || uncommittedMessages == size)
            {
                _eventLogger.message(_logSubject, ChannelMessages.LARGE_TRANSACTION_WARN(uncommittedMessages));
            }

            if(!_uncommittedMessages.isEmpty())
            {
                for (TransactionDetails transactionDetails : _uncommittedMessages.values())
                {
                    transactionDetails.flowToDisk();
                }
                _uncommittedMessages.clear();
            }
        }
        else
        {
            TransactionDetails newDetails = new TransactionDetails();
            TransactionDetails details = _uncommittedMessages.putIfAbsent(transaction, newDetails);
            if (details == null)
            {
                details = newDetails;
            }
            details.messageEnqueued(handle);
        }
    }

    @Override
    public void onDischarge(final ServerTransaction transaction)
    {
        TransactionDetails transactionDetails = _uncommittedMessages.remove(transaction);
        if (transactionDetails != null)
        {
            _uncommittedMessageSize.accumulateAndGet(transactionDetails.getUncommittedMessageSize(),
                                                     (left, right) -> left - right);

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
            long size = handle.getContentSize() + handle.getMetaData().getStorableSize();
            _uncommittedMessageSize.accumulateAndGet(size, (left, right) -> left + right);
            _uncommittedMessages.add(handle);
        }

        private void flowToDisk()
        {
            for (StoredMessage<? extends StorableMessageMetaData> uncommittedHandle: _uncommittedMessages )
            {
                uncommittedHandle.flowToDisk();
            }
        }

        private long getUncommittedMessageSize()
        {
            return _uncommittedMessageSize.get();
        }
    }

}
