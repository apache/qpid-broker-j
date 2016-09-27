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
package org.apache.qpid.server.store.serializer.v1;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;

class DTXRecord implements Record
{
    private final Transaction.StoredXidRecord _xid;
    private final Transaction.EnqueueRecord[] _enqueues;
    private final Transaction.DequeueRecord[] _dequeues;

    public DTXRecord(final Transaction.StoredXidRecord storedXid,
                     final Transaction.EnqueueRecord[] enqueues,
                     final Transaction.DequeueRecord[] dequeues)
    {
        _xid = storedXid;
        _enqueues = enqueues;
        _dequeues = dequeues;
    }

    @Override
    public RecordType getType()
    {
        return RecordType.DTX;
    }

    public Transaction.StoredXidRecord getXid()
    {
        return _xid;
    }

    public Transaction.EnqueueRecord[] getEnqueues()
    {
        return _enqueues;
    }

    public Transaction.DequeueRecord[] getDequeues()
    {
        return _dequeues;
    }

    @Override
    public void writeData(final Serializer output) throws IOException
    {
        output.writeLong(_xid.getFormat());
        output.writeInt(_xid.getGlobalId().length);
        output.write(_xid.getGlobalId());
        output.writeInt(_xid.getBranchId().length);
        output.write(_xid.getBranchId());

        output.writeInt(_enqueues.length);
        for(Transaction.EnqueueRecord record : _enqueues)
        {
            output.writeLong(record.getMessage().getMessageNumber());
            output.writeLong(record.getResource().getId().getMostSignificantBits());
            output.writeLong(record.getResource().getId().getLeastSignificantBits());
        }

        output.writeInt(_dequeues.length);
        for(Transaction.DequeueRecord record : _dequeues)
        {
            output.writeLong(record.getEnqueueRecord().getMessageNumber());
            output.writeLong(record.getEnqueueRecord().getQueueId().getMostSignificantBits());
            output.writeLong(record.getEnqueueRecord().getQueueId().getLeastSignificantBits());
        }


    }

    public static DTXRecord read(final Deserializer deserializer) throws IOException
    {
        final long format = deserializer.readLong();
        final byte[] globalId = deserializer.readBytes(deserializer.readInt());
        final byte[] branchId = deserializer.readBytes(deserializer.readInt());
        Transaction.StoredXidRecord xid = new Transaction.StoredXidRecord()
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
                                            };

        Transaction.EnqueueRecord[] enqueues = new Transaction.EnqueueRecord[deserializer.readInt()];
        for(int i = 0; i < enqueues.length; i++)
        {
            enqueues[i] = new EnqueueRecordImpl(deserializer.readLong(), deserializer.readUUID());
        }
        Transaction.DequeueRecord[] dequeues = new Transaction.DequeueRecord[deserializer.readInt()];
        for(int i = 0; i < dequeues.length; i++)
        {
            dequeues[i] = new DequeueRecordImpl(deserializer.readLong(), deserializer.readUUID());
        }
        return new DTXRecord(xid, enqueues, dequeues);
    }

    static class EnqueueRecordImpl implements Transaction.EnqueueRecord, TransactionLogResource, EnqueueableMessage
    {
        private final long _messageNumber;
        private final UUID _queueId;

        public EnqueueRecordImpl(final long messageNumber,
                                 final UUID queueId)
        {
            _messageNumber = messageNumber;
            _queueId = queueId;
        }

        @Override
        public TransactionLogResource getResource()
        {
            return this;
        }

        @Override
        public EnqueueableMessage getMessage()
        {
            return this;
        }

        @Override
        public String getName()
        {
            return _queueId.toString();
        }

        @Override
        public UUID getId()
        {
            return _queueId;
        }

        @Override
        public MessageDurability getMessageDurability()
        {
            return MessageDurability.DEFAULT;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }

        @Override
        public boolean isPersistent()
        {
            return true;
        }

        @Override
        public StoredMessage getStoredMessage()
        {
            throw new UnsupportedOperationException();
        }
    }

    static class DequeueRecordImpl implements Transaction.DequeueRecord, MessageEnqueueRecord
    {
        private UUID _queueId;
        private long _messageNumber;

        public DequeueRecordImpl(final long messageNumber,
                                 final UUID queueId)
        {
            _messageNumber = messageNumber;
            _queueId = queueId;
        }

        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return this;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }
    }
}
