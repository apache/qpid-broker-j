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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.model.Protocol;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProtocolEngineCreator_1_0_0_SASLTest
{
    private static final byte[] ORIGINAL_SASL_HEADER = { (byte) 'A', (byte) 'M', (byte) 'Q', (byte) 'P',
            (byte) 3, (byte) 1, (byte) 0, (byte) 0 };

    @Test
    void getHeaderIdentifier()
    {
        assertArrayEquals(ORIGINAL_SASL_HEADER, ProtocolEngineCreator_1_0_0_SASL.getInstance().getHeaderIdentifier(),
                "AMQP header should not be changed");
    }

    @Test
    void getSuggestedAlternativeHeader()
    {
        assertNull(ProtocolEngineCreator_1_0_0_SASL.getInstance().getSuggestedAlternativeHeader(),
                "No suggested alternative header expected");
    }

    @Test
    void getVersion()
    {
        assertEquals(Protocol.AMQP_1_0, ProtocolEngineCreator_1_0_0_SASL.getInstance().getVersion(),
                "Protocol version should be AMQP_1_0");
    }

    @Test
    void getType()
    {
        assertEquals("AMQP_1_0", ProtocolEngineCreator_1_0_0_SASL.getInstance().getType(),
                "Protocol version should be AMQP_1_0");
    }
}
