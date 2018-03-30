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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CompositeInputStreamTest extends UnitTestBase
{

    @Test
    public void testReadByteByByte_MultipleStreams() throws Exception
    {
        InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2));

        assertEquals("1st read byte unexpected", 'a', cis.read());
        assertEquals("2nd read byte unexpected", 'b', cis.read());
        assertEquals("3rd read byte unexpected", 'c', cis.read());
        assertEquals("4th read byte unexpected", 'd', cis.read());

        assertEquals("Expecting EOF", -1, cis.read());
    }

    @Test
    public void testReadByteArray_MultipleStreams() throws Exception
    {
        InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2));

        byte[] buf = new byte[3];

        int read1 = cis.read(buf);
        assertEquals("Unexpected return value from 1st array read", 3, read1);
        assertArrayEquals("Unexpected bytes from 1st array read", "abc".getBytes(), buf);

        int read2 = cis.read(buf);
        assertEquals("Unexpected return value from 2nd array read", 1, read2);
        assertArrayEquals("Unexpected bytes from 1st array read", "d".getBytes(), Arrays.copyOf(buf, 1));

        int read3 = cis.read(buf);
        assertEquals("Expecting EOF", -1, read3);
    }

    @Test
    public void testReadsMixed_SingleStream() throws Exception
    {
        InputStream bis = new ByteArrayInputStream("abcd".getBytes());

        CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis));

        byte[] buf = new byte[3];

        int read1 = cis.read(buf);
        assertEquals("Unexpected return value from 1st array read", 3, read1);
        assertArrayEquals("Unexpected bytes from 1st array read", "abc".getBytes(), buf);

        assertEquals("1st read byte unexpected", 'd', cis.read());

        assertEquals("Expecting EOF", -1, cis.read(buf));
    }

    @Test
    public void testAvailable_MultipleStreams() throws Exception
    {
        InputStream bis1 = new ByteArrayInputStream("ab".getBytes());
        InputStream bis2 = new ByteArrayInputStream("cd".getBytes());

        CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2));

        assertEquals("Unexpected number of available bytes before read", 4, cis.available());
        cis.read();
        assertEquals("Unexpected number of available bytes after 1st read", 3, cis.available());
        cis.read();
        cis.read();
        assertEquals("Unexpected number of available bytes after 3rd read", 1, cis.available());
        cis.read();
        assertEquals("Unexpected number of available bytes after last byte read", 0, cis.available());
    }

    @Test
    public void testClose() throws Exception
    {
        InputStream bis1 = mock(InputStream.class);
        InputStream bis2 = mock(InputStream.class);

        CompositeInputStream cis = new CompositeInputStream(Arrays.asList(bis1, bis2));

        cis.close();
        verify(bis1).close();
        verify(bis1).close();
        when(bis1.read()).thenThrow(new IOException("mocked stream closed"));

        try
        {
            cis.read();
            fail("Excetion not thrown");
        }
        catch(IOException ioe)
        {
            // PASS
        }
    }

}