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
package org.apache.qpid.server.protocol.v0_8.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

class ConnectionStartOkBodyTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 64;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    @BeforeEach
    void setUp()
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @AfterEach
    void tearDown()
    {
        QpidByteBuffer.deinitialisePool();
    }

    @Test
    void processDisposesClientPropertiesWhenFieldTableDecodeFails()
    {
        final ServerMethodProcessor dispatcher = mock(ServerMethodProcessor.class);

        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();

        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        // client-properties field table (length 7): entry key "s", value type 'S', long-string length 0x80000000
        in.putInt(7);
        in.putUnsignedByte((short) 1);
        in.put((byte) 's');
        in.put((byte) 'S');
        in.putInt(0x80000000);
        // mechanism "PLAIN" (short string)
        in.putUnsignedByte((short) 5);
        in.put("PLAIN".getBytes(StandardCharsets.US_ASCII));
        // response "guest" (long bytes)
        in.putInt(5);
        in.put("guest".getBytes(StandardCharsets.US_ASCII));
        // locale "en_US" (short string)
        in.putUnsignedByte((short) 5);
        in.put("en_US".getBytes(StandardCharsets.US_ASCII));
        in.flip();

        assertThrows(IllegalArgumentException.class, () -> ConnectionStartOkBody.process(in, dispatcher));

        in.dispose();

        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Malformed start-ok leaked a pooled direct buffer");
    }

    @Test
    void processDisposesNestedClientPropertyBeforeLaterDecodeFailure()
    {
        final ServerMethodProcessor dispatcher = mock(ServerMethodProcessor.class);
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);

        // client-properties field table: a valid nested table followed by an invalid long-string length
        in.putInt(17);
        in.putUnsignedByte((short) 1);
        in.put((byte) 'n');
        in.put((byte) 'F');
        in.putInt(3);
        in.putUnsignedByte((short) 1);
        in.put((byte) 'x');
        in.put((byte) 'V');
        in.putUnsignedByte((short) 1);
        in.put((byte) 's');
        in.put((byte) 'S');
        in.putInt(0x80000000);
        // mechanism "PLAIN" (short string)
        in.putUnsignedByte((short) 5);
        in.put("PLAIN".getBytes(StandardCharsets.US_ASCII));
        // response "guest" (long bytes)
        in.putInt(5);
        in.put("guest".getBytes(StandardCharsets.US_ASCII));
        // locale "en_US" (short string)
        in.putUnsignedByte((short) 5);
        in.put("en_US".getBytes(StandardCharsets.US_ASCII));
        in.flip();

        assertThrows(IllegalArgumentException.class, () -> ConnectionStartOkBody.process(in, dispatcher));

        in.dispose();

        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Malformed start-ok leaked a nested client-properties buffer");
    }
}
