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
package org.apache.qpid.server.store.berkeleydb.tuple;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;

/**
 * Handles the mapping to and from message meta data
 */
public class MessageMetaDataBinding implements EntryBinding<StorableMessageMetaData>
{

    private static final MessageMetaDataBinding INSTANCE = new MessageMetaDataBinding();

    public static MessageMetaDataBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private MessageMetaDataBinding() { }

    @Override
    public StorableMessageMetaData entryToObject(DatabaseEntry entry)
    {
        try(DataInputStream stream = new DataInputStream(new ByteArrayInputStream(entry.getData(),
                                                                                  entry.getOffset(),
                                                                                  entry.getSize())))
        {
            final int bodySize = stream.readInt() ^ 0x80000000;
            final int metaDataType = stream.readByte() & 0xff;
            MessageMetaDataType type = MessageMetaDataTypeRegistry.fromOrdinal(metaDataType);

            try (QpidByteBuffer buf = QpidByteBuffer.asQpidByteBuffer(stream))
            {
                return type.createMetaData(buf);
            }
        }
        catch (IOException | RuntimeException e)
        {
            throw new StoreException(String.format("Unable to convert entry %s to metadata", entry));
        }
    }

    @Override
    public void objectToEntry(StorableMessageMetaData metaData, DatabaseEntry entry)
    {
        final int bodySize = 1 + metaData.getStorableSize();
        byte[] underlying = new byte[4+bodySize];
        underlying[4] = (byte) metaData.getType().ordinal();
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(underlying))
        {
            buf.putInt(bodySize ^ 0x80000000);
            buf.position(5);
            try (QpidByteBuffer bufSlice = buf.slice())
            {
                metaData.writeToBuffer(bufSlice);
            }
        }
        entry.setData(underlying);
    }
}
