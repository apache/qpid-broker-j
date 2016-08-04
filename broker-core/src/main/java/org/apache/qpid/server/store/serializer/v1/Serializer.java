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
        add((byte) record.getType().ordinal());
        add(record.getData());
    }

    void add(final int value) throws IOException
    {
        add(new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value});
    }

    void add(byte data) throws IOException
    {
        _digest.update(data);
        _outputStream.write(data);
    }

    private void add(byte[] data) throws IOException
    {
        _digest.update(data);
        _outputStream.write(data);
    }


    void complete() throws IOException
    {
        add((byte)RecordType.DIGEST.ordinal());
        _outputStream.write(_digest.digest());
        _outputStream.flush();
    }

}
