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

import org.apache.qpid.test.utils.QpidTestCase;

public class QpidByteBufferTest extends QpidTestCase
{

    public static final int BUFFER_SIZE = 37;
    public static final int POOL_SIZE = 1;

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
}
