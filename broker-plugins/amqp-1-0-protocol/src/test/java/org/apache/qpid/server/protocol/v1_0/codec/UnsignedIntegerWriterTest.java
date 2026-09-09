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

package org.apache.qpid.server.protocol.v1_0.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;

class UnsignedIntegerWriterTest
{
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance();

    @Test
    void encodingChangesAtSmallUnsignedIntegerBoundary()
    {
        assertEquals(2, getEncodedSize(255));
        assertEquals(5, getEncodedSize(256));
    }

    private static int getEncodedSize(final int value)
    {
        final ValueWriter<UnsignedInteger> writer = TYPE_REGISTRY.getValueWriter(UnsignedInteger.valueOf(value));
        return writer.getEncodedSize();
    }
}
