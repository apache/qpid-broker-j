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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class BBEncoderTest extends UnitTestBase
{
    @Test
    public void testGrow()
    {
        BBEncoder enc = new BBEncoder(4);
        enc.writeInt32(0xDEADBEEF);
        ByteBuffer buf = enc.buffer();
        assertEquals(0xDEADBEEF, (long) buf.getInt(0));
        enc.writeInt32(0xBEEFDEAD);
        buf = enc.buffer();
        assertEquals(0xDEADBEEF, (long) buf.getInt(0));
        assertEquals(0xBEEFDEAD, (long) buf.getInt(4));
    }

    @Test
    public void testReadWriteStruct()
    {
        BBEncoder encoder = new BBEncoder(4);

        ReplyTo replyTo = new ReplyTo("amq.direct", "test");
        encoder.writeStruct(ReplyTo.TYPE, replyTo);

        ByteBuffer buffer = encoder.buffer();

        assertEquals(EncoderUtils.getStructLength(ReplyTo.TYPE, replyTo), (long) buffer.remaining(), "Unexpected size");

        BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        ReplyTo decoded = (ReplyTo)decoder.readStruct(ReplyTo.TYPE);

        assertEquals(replyTo.getExchange(), decoded.getExchange(), "Unexpected exchange");
        assertEquals(replyTo.getRoutingKey(), decoded.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    public void testReadWriteStruct32()
    {
        BBEncoder encoder = new BBEncoder(4);
        Map<String, Object> applicationHeaders = new HashMap<>();
        applicationHeaders.put("testProperty", "testValue");
        applicationHeaders.put("list", Arrays.asList("a", 1, 2.0));
        applicationHeaders.put("map", Collections.singletonMap("mapKey", "mapValue"));
        MessageProperties messageProperties = new MessageProperties(10,
                                                                    UUID.randomUUID(),
                                                                    "abc".getBytes(UTF_8),
                                                                    new ReplyTo("amq.direct", "test"),
                                                                    "text/plain",
                                                                    "identity",
                                                                    "cba".getBytes(UTF_8),
                                                                    "app".getBytes(UTF_8),
                                                                    applicationHeaders);

        encoder.writeStruct32(messageProperties);

        ByteBuffer buffer = encoder.buffer();

        assertEquals(EncoderUtils.getStruct32Length(messageProperties), (long) buffer.remaining(), "Unexpected size");

        BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        MessageProperties decoded = (MessageProperties)decoder.readStruct32();

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
    public void encodedStr8Caching()
    {
        String testString = "Test";
        Cache< String, byte[]> original = BBEncoder.getEncodedStringCache();
        Cache< String, byte[]> cache = CacheBuilder.newBuilder().maximumSize(2).build();
        try
        {
            BBEncoder encoder = new BBEncoder(64);
            BBEncoder.setEncodedStringCache(cache);
            encoder.writeStr8(testString);
            encoder.writeStr8(testString);

            assertThat(cache.size(), is(equalTo(1L)));
        }
        finally
        {
            cache.cleanUp();
            BBEncoder.setEncodedStringCache(original);
        }
    }
}
