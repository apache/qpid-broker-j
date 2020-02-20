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
import static org.junit.Assert.assertSame;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class BBDecoderTest extends UnitTestBase
{
    @Test
    public void str8Caching()
    {
        String testString = "Test";
        BBEncoder encoder = new BBEncoder(64);
        encoder.writeStr8(testString);
        encoder.writeStr8(testString);
        ByteBuffer buffer = encoder.buffer();

        BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);
        Cache<Binary, String> original  = BBDecoder.getStringCache();
        Cache<Binary, String> cache = CacheBuilder.newBuilder().maximumSize(2).build();
        try
        {
            BBDecoder.setStringCache(cache);

            String decodedString1 = decoder.readStr8();
            String decodedString2 = decoder.readStr8();

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
