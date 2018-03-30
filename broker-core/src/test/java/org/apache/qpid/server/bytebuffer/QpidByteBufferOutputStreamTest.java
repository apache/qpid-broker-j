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

package org.apache.qpid.server.bytebuffer;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class QpidByteBufferOutputStreamTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 0.5;

    @Before
    public void setUp() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
        }
    }

    @Test
    public void testWriteByteByByte() throws Exception
    {
        boolean direct = false;
        try (QpidByteBufferOutputStream stream = new QpidByteBufferOutputStream(direct, 3))
        {
            stream.write('a');
            stream.write('b');

            assertBufferContent(false, "ab".getBytes(StandardCharsets.UTF_8), stream.fetchAccumulatedBuffer());
        }
    }

    @Test
    public void testWriteByteArrays() throws Exception
    {
        boolean direct = false;
        try (QpidByteBufferOutputStream stream = new QpidByteBufferOutputStream(direct, 8))
        {
            stream.write("abcd".getBytes(), 0, 4);
            stream.write("_ef_".getBytes(), 1, 2);

            assertBufferContent(direct, "abcdef".getBytes(StandardCharsets.UTF_8), stream.fetchAccumulatedBuffer());
        }
    }

    @Test
    public void testWriteMixed() throws Exception
    {
        boolean direct = true;
        try (QpidByteBufferOutputStream stream = new QpidByteBufferOutputStream(direct, 3))
        {
            stream.write('a');
            stream.write("bcd".getBytes());

            assertBufferContent(direct, "abcd".getBytes(StandardCharsets.UTF_8), stream.fetchAccumulatedBuffer());
        }
    }


    @Test
    public void testWriteByteArrays_ArrayTooLargeForSingleBuffer() throws Exception
    {
        boolean direct = false;
        try (QpidByteBufferOutputStream stream = new QpidByteBufferOutputStream(direct, 8))
        {
            stream.write("abcdefghi".getBytes());

            assertBufferContent(direct, "abcdefghi".getBytes(StandardCharsets.UTF_8), stream.fetchAccumulatedBuffer());
        }
    }

    private void assertBufferContent(final boolean isDirect, final byte[] expected, final QpidByteBuffer buffer)
    {
        assertEquals("Unexpected buffer type", isDirect, buffer.isDirect());
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        buffer.dispose();
        Assert.assertArrayEquals("Unexpected buffer content", expected, buf);
    }
}
