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
package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ByteBufferInputStreamTest extends UnitTestBase
{
    private final byte[] _data = {2, 1, 5, 3, 4};
    private ByteBufferInputStream _inputStream;

    @BeforeEach
    public void setUp() throws Exception
    {
        _inputStream = new ByteBufferInputStream(ByteBuffer.wrap(_data));
    }

    @Test
    public void testRead() throws IOException
    {
        for (int i = 0; i < _data.length; i++)
        {
            assertEquals(_data[i], (long) _inputStream.read(), "Unexpected byte at position " + i);
        }
        assertEquals(-1, (long) _inputStream.read(), "EOF not reached");
    }

    @Test
    public void testReadByteArray() throws IOException
    {
        final byte[] readBytes = new byte[_data.length];
        int length = _inputStream.read(readBytes, 0, 2);

        final byte[] expected = new byte[_data.length];
        System.arraycopy(_data, 0, expected, 0, 2);

        assertArrayEquals(expected, readBytes, "Unexpected data");
        assertEquals(2, (long) length, "Unexpected length");

        length = _inputStream.read(readBytes, 2, 3);

        assertArrayEquals(_data, readBytes, "Unexpected data");
        assertEquals(3, (long) length, "Unexpected length");

        length = _inputStream.read(readBytes);
        assertEquals(-1, (long) length, "EOF not reached");
    }

    @Test
    public void testSkip() throws IOException
    {
        _inputStream.skip(3);
        final byte[] readBytes = new byte[_data.length - 3];
        final int length = _inputStream.read(readBytes);

        final byte[] expected = new byte[_data.length - 3];
        System.arraycopy(_data, 3, expected, 0, _data.length - 3);

        assertArrayEquals(expected, readBytes, "Unexpected data");
        assertEquals(_data.length - 3, (long) length, "Unexpected length");
    }

    @Test
    public void testAvailable() throws IOException
    {
        int available = _inputStream.available();
        assertEquals(_data.length, (long) available, "Unexpected number of available bytes");
        final byte[] readBytes = new byte[_data.length];
        _inputStream.read(readBytes);
        available = _inputStream.available();
        assertEquals(0, (long) available, "Unexpected number of available bytes");
    }

    @Test
    public void testMarkReset() throws IOException
    {
        _inputStream.mark(0);
        byte[] readBytes = new byte[_data.length];
        int length = _inputStream.read(readBytes);
        assertEquals(_data.length, (long) length, "Unexpected length");
        assertEquals(0, (long) _inputStream.available(), "Unexpected number of available bytes");

        _inputStream.reset();
        readBytes = new byte[_data.length];
        length = _inputStream.read(readBytes);
        assertEquals(_data.length, (long) length, "Unexpected length");
        assertEquals(0, (long) _inputStream.available(), "Unexpected number of available bytes");
    }

    @Test
    public void testMarkSupported()
    {
        assertTrue(_inputStream.markSupported(), "Unexpected mark supported");
    }
}
