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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

class Deserializer
{
    private final MessageDigest _digest;
    private final InputStream _inputStream;

    Deserializer(InputStream inputStream)
    {
        _inputStream = inputStream;
        try
        {
            _digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalArgumentException("The required message digest algorithm SHA-256 is not supported in this JVM");
        }
    }


    Record readRecord() throws IOException
    {
        int recordOrdinal = _inputStream.read();
        RecordType recordType = RecordType.values()[recordOrdinal];
        _digest.update((byte)recordOrdinal);
        return recordType.read(this);
    }

    byte[] readBytes(final int size) throws IOException
    {
        byte[] bytes = new byte[size];
        int pos = 0;
        while(pos < size)
        {
            int read;

            read = _inputStream.read(bytes, pos, size - pos);
            if (read == -1)
            {
                throw new EOFException("Unexpected end of input");
            }
            else
            {
                pos += read;
            }
        }
        _digest.update(bytes);
        return bytes;
    }

    long readLong() throws IOException
    {
        byte[] data = readBytes(8);
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(data))
        {
            return buf.getLong();
        }
    }


    int readInt() throws IOException
    {
        byte[] data = readBytes(4);
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(data))
        {
            return buf.getInt();
        }
    }


    UUID readUUID() throws IOException
    {
        byte[] data = readBytes(16);
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(data))
        {
            long msb = buf.getLong();
            long lsb = buf.getLong();
            return new UUID(msb, lsb);
        }
    }

    Record validateDigest() throws IOException
    {
        byte[] calculatedDigest = _digest.digest();
        final byte[] fileDigest = readBytes(calculatedDigest.length);
        if(!Arrays.equals(calculatedDigest, fileDigest))
        {
            throw new IllegalArgumentException("Calculated checksum does not agree with that in the input");
        }
        if(_inputStream.read() != -1)
        {
            throw new IllegalArgumentException("The import contains extra data after the digest");
        }

        return new Record()
        {
            @Override
            public RecordType getType()
            {
                return RecordType.DIGEST;
            }

            @Override
            public void writeData(final Serializer output) throws IOException
            {
                output.write(fileDigest);
            }
        };
    }
}
