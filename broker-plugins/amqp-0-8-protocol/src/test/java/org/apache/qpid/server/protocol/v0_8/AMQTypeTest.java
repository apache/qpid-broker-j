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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQTypeTest extends UnitTestBase
{
    private static final int SIZE = 1024;

    @Test
    public void testUnsignedShort()
    {
        doTest(AMQType.UNSIGNED_SHORT, 0);
        doTest(AMQType.UNSIGNED_SHORT, 65535);
    }

    @Test
    public void testUnsignedByte()
    {
        doTest(AMQType.UNSIGNED_BYTE, (short)0);
        doTest(AMQType.UNSIGNED_BYTE, (short)255);
    }

    private <T> void doTest(final AMQType type, final T value)
    {
        final QpidByteBuffer buf = QpidByteBuffer.allocate(false, SIZE);
        AMQTypedValue.createAMQTypedValue(type, value).writeToBuffer(buf);
        buf.flip();

        T read = (T) AMQTypedValue.readFromBuffer(buf).getValue();
        assertEquals("Unexpected round trip value", value, read);
        buf.dispose();
    }
}