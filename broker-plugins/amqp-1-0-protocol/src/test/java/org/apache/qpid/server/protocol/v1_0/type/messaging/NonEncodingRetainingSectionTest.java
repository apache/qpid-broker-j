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
package org.apache.qpid.server.protocol.v1_0.type.messaging;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.qpid.test.utils.QpidTestCase;

public class NonEncodingRetainingSectionTest extends QpidTestCase
{
    public void testGetCached()
    {
        Cache<String, String> original = NonEncodingRetainingSection.getStringCache();
        Cache<String, String> cache = CacheBuilder.newBuilder().maximumSize(1).build();
        NonEncodingRetainingSection.setCache(cache);

        try
        {
            String string1 = NonEncodingRetainingSection.getCached(new String(new char[]{'t', 'e', 's', 't'}));
            String string2 = NonEncodingRetainingSection.getCached(new String(new char[]{'t', 'e', 's', 't'}));

            assertEquals(string1, string2);
            assertSame(string1, string2);
        }
        finally
        {
            cache.cleanUp();
            NonEncodingRetainingSection.setCache(original);
        }
    }
}
