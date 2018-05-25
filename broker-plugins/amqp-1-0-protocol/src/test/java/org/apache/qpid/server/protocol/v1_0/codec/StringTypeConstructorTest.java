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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class StringTypeConstructorTest
{

    @Test
    public void construct() throws Exception
    {
        StringTypeConstructor constructor = StringTypeConstructor.getInstance(1);
        Cache<ByteBuffer, String> original = StringTypeConstructor.getCache();
        Cache<ByteBuffer, String> cache = CacheBuilder.newBuilder().maximumSize(2).build();
        StringTypeConstructor.setCache(cache);
        try
        {
            String string1 = constructor.construct(QpidByteBuffer.wrap(new byte[]{4, 't', 'e', 's', 't'}), null);
            String string2 = constructor.construct(QpidByteBuffer.wrap(new byte[]{4, 't', 'e', 's', 't'}), null);
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
