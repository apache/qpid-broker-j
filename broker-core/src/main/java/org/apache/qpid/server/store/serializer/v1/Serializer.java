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
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class Serializer
{
    private final OutputStream _outputStream;
    private final MessageDigest _digest;
    private final byte[] _tmpBuf = new byte[8];

    Serializer(final OutputStream outputStream)
            throws IOException
    {
        _outputStream = outputStream;

        try
        {
            _digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalArgumentException("The required message digest algorithm SHA-256 is not supported in this JVM");
        }

        add(new VersionRecord());
    }

    void add(Record record) throws IOException
    {
        write((byte) record.getType().ordinal());
        record.writeData(this);
    }

    public final void writeInt(long val) throws IOException
    {

        _tmpBuf[0] = (byte)(val >>> 24);
        _tmpBuf[1] = (byte)(val >>> 16);
        _tmpBuf[2] = (byte)(val >>>  8);
        _tmpBuf[3] = (byte)val;
        write(_tmpBuf, 0, 4);
    }


    public final void writeLong(long val) throws IOException
    {
        _tmpBuf[0] = (byte)(val >>> 56);
        _tmpBuf[1] = (byte)(val >>> 48);
        _tmpBuf[2] = (byte)(val >>> 40);
        _tmpBuf[3] = (byte)(val >>> 32);
        _tmpBuf[4] = (byte)(val >>> 24);
        _tmpBuf[5] = (byte)(val >>> 16);
        _tmpBuf[6] = (byte)(val >>>  8);
        _tmpBuf[7] = (byte)val;
        write(_tmpBuf, 0, 8);
    }

    void complete() throws IOException
    {
        write((byte)RecordType.DIGEST.ordinal());
        _outputStream.write(_digest.digest());
        _outputStream.flush();
    }


    void write(final int b) throws IOException
    {
        _digest.update((byte)b);
        _outputStream.write(b);
    }

    void write(final byte[] b) throws IOException
    {
        _digest.update(b);
        _outputStream.write(b);
    }

    void write(final byte[] input, final int off, final int len) throws IOException
    {
        _digest.update(input, off, len);
        _outputStream.write(input, off, len);
    }

}
