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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.StoredMessage;

class MessageRecord implements Record
{

    private final long _messageNumber;
    private final byte[] _metaData;
    private final byte[] _content;

    public MessageRecord(final StoredMessage<?> storedMessage)
    {
        _messageNumber = storedMessage.getMessageNumber();
        _metaData = new byte[1 + storedMessage.getMetadataSize()];
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(_metaData))
        {
            buf.put((byte) storedMessage.getMetaData().getType().ordinal());
            storedMessage.getMetaData().writeToBuffer(buf);
        }

        _content = new byte[storedMessage.getContentSize()];
        try (QpidByteBuffer content = storedMessage.getContent(0, storedMessage.getContentSize()))
        {
            content.get(_content);
        }
    }

    MessageRecord(long messageNumber, byte[] metaData, byte[] content)
    {
        _messageNumber = messageNumber;
        _metaData = metaData;
        _content = content;
    }

    @Override
    public RecordType getType()
    {
        return RecordType.MESSAGE;
    }

    public int getLength()
    {
        return _metaData.length + _content.length + 16;
    }

    @Override
    public void writeData(final Serializer output) throws IOException
    {
        output.writeLong(_messageNumber);
        output.writeInt(_metaData.length);
        output.write(_metaData);
        output.writeInt(_content.length);
        output.write(_content);
    }

    public long getMessageNumber()
    {
        return _messageNumber;
    }

    public byte[] getMetaData()
    {
        return _metaData;
    }

    public byte[] getContent()
    {
        return _content;
    }

    public static MessageRecord read(final Deserializer deserializer) throws IOException
    {
        long messageNumber = deserializer.readLong();
        int storableSize = deserializer.readInt();
        byte[] metaDataContent = deserializer.readBytes(storableSize);
        int messageSize = deserializer.readInt();
        byte[] content = deserializer.readBytes(messageSize);
        return new MessageRecord(messageNumber, metaDataContent, content);
    }
}
