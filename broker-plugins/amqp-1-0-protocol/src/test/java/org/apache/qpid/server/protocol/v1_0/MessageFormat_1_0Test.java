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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorRuntimeException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class MessageFormat_1_0Test extends UnitTestBase
{
    @Test
    public void createMessageUsesSuppliedSectionDecoder()
    {
        final int customLimit = 5;
        final SectionDecoder sectionDecoder =
                new SectionDecoderImpl(MessageConverter_v1_0_to_Internal.TYPE_REGISTRY.getSectionDecoderRegistry(),
                                       customLimit);
        final MessageStore messageStore = new TestMemoryMessageStore();

        try
        {
            final byte[] payload = buildAmqpValueSection(buildNestedList32(customLimit + 1));
            try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(payload))
            {
                final AmqpErrorRuntimeException thrown = assertThrows(AmqpErrorRuntimeException.class, () ->
                        new MessageFormat_1_0().createMessage(buffer, messageStore, null, sectionDecoder));

                assertEquals(AmqpError.DECODE_ERROR, thrown.getCause().getError().getCondition());
            }
        }
        finally
        {
            messageStore.closeMessageStore();
        }
    }

    private byte[] buildAmqpValueSection(final byte[] valueBytes)
    {
        final byte[] bytes = new byte[valueBytes.length + 3];
        bytes[0] = ValueHandler.DESCRIBED_TYPE;
        bytes[1] = 0x53;
        bytes[2] = 0x77;
        System.arraycopy(valueBytes, 0, bytes, 3, valueBytes.length);
        return bytes;
    }

    private byte[] buildNestedList32(final int levels)
    {
        byte[] inner = {0x40};

        for (int i = 0; i < levels; i++)
        {
            final int size = 4 + inner.length;
            final byte[] outer = new byte[1 + 4 + 4 + inner.length];
            outer[0] = (byte) 0xD0;
            outer[1] = (byte) (size >> 24);
            outer[2] = (byte) (size >> 16);
            outer[3] = (byte) (size >> 8);
            outer[4] = (byte) size;
            outer[8] = 1;
            System.arraycopy(inner, 0, outer, 9, inner.length);
            inner = outer;
        }
        return inner;
    }
}
