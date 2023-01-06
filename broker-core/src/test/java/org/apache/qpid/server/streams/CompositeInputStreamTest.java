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

package org.apache.qpid.server.streams;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CompositeInputStreamTest extends UnitTestBase
{

    @Test
    public void testReadByteByByte_MultipleStreams() throws Exception
    {
        final InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        final InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        try (final CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2)))
        {

            assertEquals('a', cis.read(), "1st read byte unexpected");
            assertEquals('b', cis.read(), "2nd read byte unexpected");
            assertEquals('c', cis.read(), "3rd read byte unexpected");
            assertEquals('d', cis.read(), "4th read byte unexpected");
            assertEquals(-1, cis.read(), "Expecting EOF");
        }
    }

    @Test
    public void testReadByteArray_MultipleStreams() throws Exception
    {
        final InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        final InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        try (final CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2)))
        {
            final byte[] buf = new byte[3];

            final int read1 = cis.read(buf);
            assertEquals(3, read1, "Unexpected return value from 1st array read");
            assertArrayEquals("abc".getBytes(), buf, "Unexpected bytes from 1st array read");

            final int read2 = cis.read(buf);
            assertEquals(1, read2, "Unexpected return value from 2nd array read");
            assertArrayEquals("d".getBytes(), Arrays.copyOf(buf, 1), "Unexpected bytes from 1st array read");

            final int read3 = cis.read(buf);
            assertEquals(-1, read3, "Expecting EOF");
        }
    }

    @Test
    public void testReadsMixed_SingleStream() throws Exception
    {
        final InputStream bis = new ByteArrayInputStream("abcd".getBytes());

        try (final CompositeInputStream cis = new CompositeInputStream(List.of(bis)))
        {

            final byte[] buf = new byte[3];

            final int read1 = cis.read(buf);
            assertEquals(3, read1, "Unexpected return value from 1st array read");
            assertArrayEquals("abc".getBytes(), buf, "Unexpected bytes from 1st array read");

            assertEquals('d', cis.read(), "1st read byte unexpected");

            assertEquals(-1, cis.read(buf), "Expecting EOF");
        }
    }

    @Test
    public void testAvailable_MultipleStreams() throws Exception
    {
        final InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        final InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        try (final CompositeInputStream cis = new CompositeInputStream(List.of(bis1, bis2)))
        {
            assertEquals(4, cis.available(), "Unexpected number of available bytes before read");
            cis.read();
            assertEquals(3, cis.available(), "Unexpected number of available bytes after 1st read");
            cis.read();
            cis.read();
            assertEquals(1, cis.available(), "Unexpected number of available bytes after 3rd read");
            cis.read();
            assertEquals(0, cis.available(), "Unexpected number of available bytes after last byte read");
        }
    }

    @Test
    public void testClose() throws Exception
    {
        final InputStream bis1 = mock(InputStream.class);
        final InputStream bis2 = mock(InputStream.class);
        final CompositeInputStream cis = new CompositeInputStream(List.of(bis1, bis2));

        cis.close();
        verify(bis1).close();
        verify(bis1).close();
        when(bis1.read()).thenThrow(new IOException("mocked stream closed"));

        assertThrows(IOException.class, cis::read, "Exception not thrown");
    }
}