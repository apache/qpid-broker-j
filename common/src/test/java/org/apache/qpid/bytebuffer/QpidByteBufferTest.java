/*
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

package org.apache.qpid.bytebuffer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Assert;

import org.apache.qpid.test.utils.QpidTestCase;

public class QpidByteBufferTest extends QpidTestCase
{

    public static final int BUFFER_SIZE = 10;
    public static final int POOL_SIZE = 20;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE);
    }

    public void testPooledBufferIsZeroedLoan() throws Exception
    {
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE);

        buffer.put((byte) 0xFF);
        buffer.dispose();

        buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        buffer.limit(1);
        assertEquals("Pooled QpidByteBuffer is not zeroed.", (byte) 0x0, buffer.get());
    }

    public void testAllocateDirectOfSameSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE;
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize);
        assertEquals("Unexpected buffer size", bufferSize, buffer.capacity());
        assertEquals("Unexpected position on newly created buffer", 0, buffer.position());
        assertEquals("Unexpected limit on newly created buffer", bufferSize, buffer.limit());
    }

    public void testAllocateDirectOfSmallerSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE - 1;
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize);
        assertEquals("Unexpected buffer size", bufferSize, buffer.capacity());
        assertEquals("Unexpected position on newly created buffer", 0, buffer.position());
        assertEquals("Unexpected limit on newly created buffer", bufferSize, buffer.limit());
    }

    public void testAllocateDirectOfLargerSize() throws Exception
    {
        int bufferSize = BUFFER_SIZE + 1;
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(bufferSize);
        assertEquals("Unexpected buffer size", bufferSize, buffer.capacity());
        assertEquals("Unexpected position on newly created buffer", 0, buffer.position());
        assertEquals("Unexpected limit on newly created buffer", bufferSize, buffer.limit());
    }

    public void testAllocateDirectWithNegativeSize() throws Exception
    {
        try
        {
            QpidByteBuffer.allocateDirect(-1);
            fail("It is not legal to create buffer with negative size.");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testSettingUpPoolTwice() throws Exception
    {
        try
        {
            QpidByteBuffer.initialisePool(BUFFER_SIZE + 1, POOL_SIZE + 1);
            fail("It is not legal to initialize buffer twice with different settings.");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    public void testDeflateInflate() throws Exception
    {
        byte[] input = "aaabbbcccddddeeeffff".getBytes();
        QpidByteBuffer original = QpidByteBuffer.wrap(input);

        Collection<QpidByteBuffer> deflated = QpidByteBuffer.deflate(Collections.singleton(original));
        assertNotNull(deflated);

        Collection<QpidByteBuffer> inflated = QpidByteBuffer.inflate(deflated);
        assertNotNull(inflated);
        assertEquals("Inflated to an unexpected number of inflated buffers", 2, inflated.size());

        Iterator<QpidByteBuffer> bufItr = inflated.iterator();
        QpidByteBuffer buf1 = bufItr.next();
        QpidByteBuffer buf2 = bufItr.next();

        assertEquals("Unexpected total remaining", input.length, buf1.remaining() + buf2.remaining());

        byte[] bytes1 = new byte[buf1.remaining()];
        buf1.get(bytes1);
        Assert.assertArrayEquals("Inflated buf1 has unexpected content", Arrays.copyOf(input, bytes1.length), bytes1);

        byte[] bytes2 = new byte[buf2.remaining()];
        buf2.get(bytes2);
        Assert.assertArrayEquals("Inflated buf2 has unexpected content", Arrays.copyOfRange(input,
                                                                                            bytes1.length,
                                                                                            input.length), bytes2);
    }

    public void testInflatingUncompressedBytes_ThrowsZipException() throws Exception
    {
        byte[] input = "not_a_compressed_stream".getBytes();
        QpidByteBuffer original = QpidByteBuffer.wrap(input);

        try
        {
            QpidByteBuffer.inflate(Collections.singleton(original));
            fail("Exception not thrown");
        }
        catch(java.util.zip.ZipException ze)
        {
            // PASS
        }
    }

}
