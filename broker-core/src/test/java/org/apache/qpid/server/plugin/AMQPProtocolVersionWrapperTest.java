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
package org.apache.qpid.server.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQPProtocolVersionWrapperTest extends UnitTestBase
{
    @Test
    public void testAMQPProtocolVersionWrapper()
    {
        final AMQPProtocolVersionWrapper wrapper0_8 = new AMQPProtocolVersionWrapper(Protocol.AMQP_0_8);
        assertEquals(0, (long) wrapper0_8.getMajor());
        assertEquals(8, (long) wrapper0_8.getMinor());
        assertEquals(0, (long) wrapper0_8.getPatch());

        final AMQPProtocolVersionWrapper wrapper0_9 = new AMQPProtocolVersionWrapper(Protocol.AMQP_0_9);
        assertEquals(0, (long) wrapper0_9.getMajor());
        assertEquals(9, (long) wrapper0_9.getMinor());
        assertEquals(0, (long) wrapper0_9.getPatch());

        final AMQPProtocolVersionWrapper wrapper0_9_1 = new AMQPProtocolVersionWrapper(Protocol.AMQP_0_9_1);
        assertEquals(0, (long) wrapper0_9_1.getMajor());
        assertEquals(9, (long) wrapper0_9_1.getMinor());
        assertEquals(1, (long) wrapper0_9_1.getPatch());

        final AMQPProtocolVersionWrapper wrapper0_10 = new AMQPProtocolVersionWrapper(Protocol.AMQP_0_10);
        assertEquals(0, (long) wrapper0_10.getMajor());
        assertEquals(10, (long) wrapper0_10.getMinor());
        assertEquals(0, (long) wrapper0_10.getPatch());

        final AMQPProtocolVersionWrapper wrapper1_0 = new AMQPProtocolVersionWrapper(Protocol.AMQP_1_0);
        assertEquals(1, (long) wrapper1_0.getMajor());
        assertEquals(0, (long) wrapper1_0.getMinor());
        assertEquals(0, (long) wrapper1_0.getPatch());
    }

    @Test
    public void testAMQPProtocolVersionWrapperGetProtocol()
    {
        Arrays.stream(Protocol.values()).filter(Protocol::isAMQP)
                .forEach(protocol -> assertEquals(protocol, new AMQPProtocolVersionWrapper(protocol).getProtocol()));
    }

    @Test
    public void testWrappingNonAMQPProtocol()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new AMQPProtocolVersionWrapper(Protocol.HTTP),
                "IllegalArgumentException exception expected when Protocol is not AMQP based");
    }
}
