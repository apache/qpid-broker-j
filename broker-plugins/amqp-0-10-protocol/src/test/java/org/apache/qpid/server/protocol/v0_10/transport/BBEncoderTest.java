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

package org.apache.qpid.server.protocol.v0_10.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class BBEncoderTest extends UnitTestBase
{
    @Test
    void grow()
    {
        final BBEncoder enc = new BBEncoder(4);
        enc.writeInt32(0xDEADBEEF);
        ByteBuffer buf = enc.buffer();
        assertEquals(0xDEADBEEF, (long) buf.getInt(0));
        enc.writeInt32(0xBEEFDEAD);
        buf = enc.buffer();
        assertEquals(0xDEADBEEF, (long) buf.getInt(0));
        assertEquals(0xBEEFDEAD, (long) buf.getInt(4));
    }

    @Test
    void readWriteStruct()
    {
        final BBEncoder encoder = new BBEncoder(4);

        final ReplyTo replyTo = new ReplyTo("amq.direct", "test");
        encoder.writeStruct(ReplyTo.TYPE, replyTo);

        final ByteBuffer buffer = encoder.buffer();

        assertEquals(EncoderUtils.getStructLength(ReplyTo.TYPE, replyTo), (long) buffer.remaining(), "Unexpected size");

        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        final ReplyTo decoded = (ReplyTo)decoder.readStruct(ReplyTo.TYPE);

        assertEquals(replyTo.getExchange(), decoded.getExchange(), "Unexpected exchange");
        assertEquals(replyTo.getRoutingKey(), decoded.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void readWriteStruct32()
    {
        final BBEncoder encoder = new BBEncoder(4);
        final Map<String, Object> applicationHeaders = Map.of("testProperty", "testValue",
                "list", List.of("a", 1, 2.0),
                "map", Map.of("mapKey", "mapValue"));
        final MessageProperties messageProperties = new MessageProperties(10,
                UUID.randomUUID(),
                "abc".getBytes(UTF_8),
                new ReplyTo("amq.direct", "test"),
                "text/plain",
                "identity",
                "cba".getBytes(UTF_8),
                "app".getBytes(UTF_8),
                applicationHeaders);

        encoder.writeStruct32(messageProperties);

        final ByteBuffer buffer = encoder.buffer();

        assertEquals(EncoderUtils.getStruct32Length(messageProperties), (long) buffer.remaining(), "Unexpected size");

        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        final MessageProperties decoded = (MessageProperties)decoder.readStruct32();

        assertEquals(messageProperties.getContentLength(), decoded.getContentLength(), "Unexpected content length");
        assertEquals(messageProperties.getMessageId(), decoded.getMessageId(), "Unexpected message id");
        assertArrayEquals(messageProperties.getCorrelationId(), decoded.getCorrelationId(),
                "Unexpected correlation id");
        assertEquals(messageProperties.getReplyTo(), decoded.getReplyTo(), "Unexpected reply to");
        assertEquals(messageProperties.getContentType(), decoded.getContentType(), "Unexpected content type");
        assertEquals(messageProperties.getContentEncoding(), decoded.getContentEncoding(), "Unexpected content encoding");
        assertArrayEquals(messageProperties.getUserId(), decoded.getUserId(), "Unexpected user id");
        assertArrayEquals(messageProperties.getAppId(), decoded.getAppId(), "Unexpected application id");
        assertEquals(messageProperties.getApplicationHeaders(), decoded.getApplicationHeaders(), "Unexpected application headers");
    }

    @Test
    void encodedStr8Caching()
    {
        final String testString = "Test";
        final Cache< String, byte[]> original = BBEncoder.getEncodedStringCache();
        final Cache< String, byte[]> cache = Caffeine.newBuilder().maximumSize(2).build();
        try
        {
            final BBEncoder encoder = new BBEncoder(64);
            BBEncoder.setEncodedStringCache(cache);
            encoder.writeStr8(testString);
            encoder.writeStr8(testString);

            assertThat(cache.estimatedSize(), is(equalTo(1L)));
        }
        finally
        {
            cache.cleanUp();
            BBEncoder.setEncodedStringCache(original);
        }
    }
}
