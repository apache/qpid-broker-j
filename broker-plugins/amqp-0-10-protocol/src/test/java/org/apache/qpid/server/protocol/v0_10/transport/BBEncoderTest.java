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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class BBEncoderTest extends UnitTestBase
{

    @Test
    public void testGrow()
    {
        BBEncoder enc = new BBEncoder(4);
        enc.writeInt32(0xDEADBEEF);
        ByteBuffer buf = enc.buffer();
        assertEquals((long) 0xDEADBEEF, (long) buf.getInt(0));
        enc.writeInt32(0xBEEFDEAD);
        buf = enc.buffer();
        assertEquals((long) 0xDEADBEEF, (long) buf.getInt(0));
        assertEquals((long) 0xBEEFDEAD, (long) buf.getInt(4));
    }


    @Test
    public void testReadWriteStruct()
    {
        BBEncoder encoder = new BBEncoder(4);

        ReplyTo replyTo = new ReplyTo("amq.direct", "test");
        encoder.writeStruct(ReplyTo.TYPE, replyTo);

        ByteBuffer buffer = encoder.buffer();

        assertEquals("Unexpected size",
                            (long) EncoderUtils.getStructLength(ReplyTo.TYPE, replyTo),
                            (long) buffer.remaining());

        BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        ReplyTo decoded = (ReplyTo)decoder.readStruct(ReplyTo.TYPE);

        assertEquals("Unexpected exchange", replyTo.getExchange(), decoded.getExchange());
        assertEquals("Unexpected routing key", replyTo.getRoutingKey(), decoded.getRoutingKey());
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

        assertEquals("Unexpected size",
                            (long) EncoderUtils.getStruct32Length(messageProperties),
                            (long) buffer.remaining());

        BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);

        MessageProperties decoded = (MessageProperties)decoder.readStruct32();

        assertEquals("Unexpected content length",
                            messageProperties.getContentLength(),
                            decoded.getContentLength());
        assertEquals("Unexpected message id", messageProperties.getMessageId(), decoded.getMessageId());
        assertArrayEquals("Unexpected correlation id", messageProperties.getCorrelationId(), decoded.getCorrelationId
                ());
        assertEquals("Unexpected reply to", messageProperties.getReplyTo(), decoded.getReplyTo());
        assertEquals("Unexpected content type", messageProperties.getContentType(), decoded.getContentType());
        assertEquals("Unexpected content encoding",
                            messageProperties.getContentEncoding(),
                            decoded.getContentEncoding());
        assertArrayEquals("Unexpected user id", messageProperties.getUserId(), decoded.getUserId());
        assertArrayEquals("Unexpected application id", messageProperties.getAppId(), decoded.getAppId());
        assertEquals("Unexpected application headers",
                            messageProperties.getApplicationHeaders(),
                            decoded.getApplicationHeaders());
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
