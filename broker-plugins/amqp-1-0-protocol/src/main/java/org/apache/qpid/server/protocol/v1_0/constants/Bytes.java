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

package org.apache.qpid.server.protocol.v1_0.constants;

/** Utility class holding frequently used byte arrays */
public final class Bytes
{
    private Bytes()
    {
        // constructor is private for utility class
    }

    /** AMQP header byte array representation  */
    private static final byte[] AMQP_HEADER_BYTES = { (byte) 'A', (byte) 'M', (byte) 'Q', (byte) 'P',
            (byte) 0, (byte) 1, (byte) 0, (byte) 0 };

    /** Empty byte array  */
    public static final byte[] EMPTY_BYTE_ARRAY = { };

    /** SASL header byte array representation  */
    private static final byte[] SASL_HEADER_BYTES = { (byte) 'A', (byte) 'M', (byte) 'Q', (byte) 'P',
            (byte) 3, (byte) 1, (byte) 0, (byte) 0 };

    /**
     * Returns a AMQP header shared array instance to avoid per-call allocation.
     * The returned array must be treated as immutable and must not be modified by callers.
     * @return AMQP header byte array
     */
    public static byte[] amqpHeader()
    {
        return AMQP_HEADER_BYTES;
    }

    /**
     * Returns a SASL header shared array instance to avoid per-call allocation.
     * The returned array must be treated as immutable and must not be modified by callers.
     * @return SASL header byte array
     */
    public static byte[] saslHeader()
    {
        return SASL_HEADER_BYTES;
    }
}
