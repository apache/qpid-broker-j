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

package org.apache.qpid.server.protocol.v0_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

public class EncodingUtilsTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    private QpidByteBuffer _buffer;

    @Before
    public void setUp() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
        _buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            _buffer.dispose();
            QpidByteBuffer.deinitialisePool();
        }
        finally
        {
        }
    }

    @Test
    public void testReadLongAsShortStringWhenDigitsAreSpecified() throws Exception
    {
        _buffer.putUnsignedByte((short)3);
        _buffer.put((byte)'9');
        _buffer.put((byte)'2');
        _buffer.put((byte)'0');
        _buffer.flip();
        assertEquals("Unexpected result", 920L, EncodingUtils.readLongAsShortString(_buffer));
    }

    @Test
    public void testReadLongAsShortStringWhenNonDigitCharacterIsSpecified() throws Exception
    {
        _buffer.putUnsignedByte((short)2);
        _buffer.put((byte)'1');
        _buffer.put((byte)'a');
        _buffer.flip();
        try
        {
            EncodingUtils.readLongAsShortString(_buffer);
            fail("Exception is expected");
        }
        catch(AMQFrameDecodingException e)
        {
            // pass
        }
    }
}