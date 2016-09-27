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

import org.apache.qpid.server.store.MessageEnqueueRecord;

class MessageInstanceRecord implements Record
{

    private final long _messageNumber;
    private final UUID _queueId;

    MessageInstanceRecord(final MessageEnqueueRecord record)
    {
        this(record.getMessageNumber(), record.getQueueId());
    }

    private MessageInstanceRecord(final long messageNumber, final UUID queueId)
    {
        _messageNumber = messageNumber;
        _queueId = queueId;
    }

    public long getMessageNumber()
    {
        return _messageNumber;
    }

    public UUID getQueueId()
    {
        return _queueId;
    }

    @Override
    public RecordType getType()
    {
        return RecordType.MESSAGE_INSTANCE;
    }

    @Override
    public void writeData(final Serializer output) throws IOException
    {
        output.writeLong(_messageNumber);
        output.writeLong(_queueId.getMostSignificantBits());
        output.writeLong(_queueId.getLeastSignificantBits());
    }

    public static MessageInstanceRecord read(final Deserializer deserializer) throws IOException
    {
        long messageNumber = deserializer.readLong();
        UUID queueId = deserializer.readUUID();
        return new MessageInstanceRecord(messageNumber, queueId);
    }
}
