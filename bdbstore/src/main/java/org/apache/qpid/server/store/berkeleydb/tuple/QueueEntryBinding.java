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

import java.util.UUID;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.store.berkeleydb.entry.QueueEntryKey;

public class QueueEntryBinding implements EntryBinding<QueueEntryKey>
{

    private static final QueueEntryBinding INSTANCE = new QueueEntryBinding();

    public static QueueEntryBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private QueueEntryBinding() { }

    public QueueEntryKey entryToObject(DatabaseEntry entry)
    {
        byte[] data = entry.getData();
        int offset = entry.getOffset();

        UUID queueId = new UUID(readUnsignedLong(data,offset)^ 0x8000000000000000L, readUnsignedLong(data,offset+8)^ 0x8000000000000000L);
        long messageId = readUnsignedLong(data,offset+16)^ 0x8000000000000000L;

        return new QueueEntryKey(queueId, messageId);
    }

    public void objectToEntry(QueueEntryKey mk, DatabaseEntry entry)
    {
        byte[] output = new byte[24];
        UUID uuid = mk.getQueueId();
        writeUnsignedLong(uuid.getMostSignificantBits() ^ 0x8000000000000000L, output, 0);
        writeUnsignedLong(uuid.getLeastSignificantBits() ^ 0x8000000000000000L, output, 8);
        writeUnsignedLong(mk.getMessageId() ^ 0x8000000000000000L, output, 16);
        entry.setData(output);
    }

    private void writeUnsignedLong(long val, byte[] data, int offset)
    {
        data[offset++] = (byte) (val >>> 56);
        data[offset++] = (byte) (val >>> 48);
        data[offset++] = (byte) (val >>> 40);
        data[offset++] = (byte) (val >>> 32);
        data[offset++] = (byte) (val >>> 24);
        data[offset++] = (byte) (val >>> 16);
        data[offset++] = (byte) (val >>> 8);
        data[offset] = (byte) val;
    }

    private long readUnsignedLong(final byte[] data, int offset)
    {
        return (((long)data[offset++] & 0xffl) << 56)
               | (((long)data[offset++] & 0xffl) << 48)
               | (((long)data[offset++] & 0xffl) << 40)
               | (((long)data[offset++] & 0xffl) << 32)
               | (((long)data[offset++] & 0xffl) << 24)
               | (((long)data[offset++] & 0xffl) << 16)
               | (((long)data[offset++] & 0xffl) << 8)
               | ((long)data[offset] & 0xffl) ;
    }




}
