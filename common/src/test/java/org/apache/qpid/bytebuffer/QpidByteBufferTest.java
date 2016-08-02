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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;

import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.util.ByteBufferUtils;

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

    public void testDeflateInflateDirect() throws Exception
    {
        byte[] input = "aaabbbcccddddeeeffff".getBytes();
        Collection<QpidByteBuffer> inputBufs = QpidByteBuffer.allocateDirectCollection(input.length);

        int offset = 0;
        for (QpidByteBuffer buf : inputBufs)
        {
            int len = buf.remaining();
            buf.put(input, offset, len);
            buf.flip();
            offset += len;
        }
        assertEquals(input.length, ByteBufferUtils.remaining(inputBufs));

        doDeflateInflate(input, inputBufs, true);
    }

    public void testDeflateInflateHeap() throws Exception
    {
        byte[] input = "aaabbbcccddddeeeffff".getBytes();
        Collection<QpidByteBuffer> inputBufs = Collections.singleton(QpidByteBuffer.wrap(input));

        doDeflateInflate(input, inputBufs, false);
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

    private void doDeflateInflate(byte[] input,
                                  Collection<QpidByteBuffer> inputBufs,
                                  boolean direct) throws IOException
    {
        Collection<QpidByteBuffer> deflatedBufs = QpidByteBuffer.deflate(inputBufs);
        assertNotNull(deflatedBufs);

        Collection<QpidByteBuffer> inflatedBufs = QpidByteBuffer.inflate(deflatedBufs);
        assertNotNull(inflatedBufs);
        assertTrue("Expected at least on buffer", inflatedBufs.size() >= 1);

        int bufNum = 1;
        int inputOffset = 0;
        int inflatedBytesTotal = 0;
        for(QpidByteBuffer inflatedBuf : inflatedBufs)
        {
            assertEquals("Inflated buf " + bufNum + " is of wrong type", direct, inflatedBuf.isDirect());

            int inflatedBytesCount = inflatedBuf.remaining();
            inflatedBytesTotal += inflatedBytesCount;

            byte[] inflatedBytes = new byte[inflatedBytesCount];
            inflatedBuf.get(inflatedBytes);
            byte[] expectedBytes = Arrays.copyOfRange(input, inputOffset, inputOffset + inflatedBytes.length);
            Assert.assertArrayEquals("Inflated buf" + bufNum + " has unexpected content", expectedBytes, inflatedBytes);

            inputOffset += inflatedBytes.length;
            bufNum++;
        }

        assertEquals("Unexpected number of inflated bytes", input.length, inflatedBytesTotal);
    }

}
