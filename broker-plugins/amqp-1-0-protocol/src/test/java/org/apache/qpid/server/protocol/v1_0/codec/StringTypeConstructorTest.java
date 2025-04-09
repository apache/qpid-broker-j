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
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

class StringTypeConstructorTest
{
    @Test
    void construct() throws Exception
    {
        final StringTypeConstructor constructor = StringTypeConstructor.getInstance(1);
        final Cache<ByteBuffer, String> original = StringTypeConstructor.getCache();
        final Cache<ByteBuffer, String> cache = Caffeine.newBuilder().maximumSize(2).build();
        StringTypeConstructor.setCache(cache);
        try
        {
            final String string1 = constructor.construct(QpidByteBuffer.wrap(new byte[]{4, 't', 'e', 's', 't'}), null);
            final String string2 = constructor.construct(QpidByteBuffer.wrap(new byte[]{4, 't', 'e', 's', 't'}), null);
            assertEquals(string1, string2);
            assertSame(string1, string2);
        }
        finally
        {
            cache.cleanUp();
            StringTypeConstructor.setCache(original);
        }
    }
}
