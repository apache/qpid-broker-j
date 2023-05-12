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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class BBDecoderTest extends UnitTestBase
{
    @Test
    void str8Caching()
    {
        final String testString = "Test";
        final BBEncoder encoder = new BBEncoder(64);
        encoder.writeStr8(testString);
        encoder.writeStr8(testString);
        final ByteBuffer buffer = encoder.buffer();

        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);
        final Cache<Binary, String> original  = BBDecoder.getStringCache();
        final Cache<Binary, String> cache = CacheBuilder.newBuilder().maximumSize(2).build();
        try
        {
            BBDecoder.setStringCache(cache);

            final String decodedString1 = decoder.readStr8();
            final String decodedString2 = decoder.readStr8();

            assertThat(testString, is(equalTo(decodedString1)));
            assertThat(testString, is(equalTo(decodedString2)));
            assertSame(decodedString1, decodedString2);
        }
        finally
        {
            cache.cleanUp();
            BBDecoder.setStringCache(original);
        }
    }
}
