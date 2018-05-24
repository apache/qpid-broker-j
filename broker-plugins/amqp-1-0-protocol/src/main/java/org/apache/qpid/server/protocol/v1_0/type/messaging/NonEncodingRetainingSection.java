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

import org.apache.qpid.server.protocol.v1_0.type.Section;
import org.apache.qpid.server.virtualhost.CacheFactory;
import org.apache.qpid.server.virtualhost.NullCache;

public interface NonEncodingRetainingSection<T> extends Section<T>
{
    NullCache<String, String> NULL_CACHE = new NullCache<>();
    ThreadLocal<Cache<String, String>> CACHE =
            ThreadLocal.withInitial(() -> CacheFactory.getCache("stringCache", NULL_CACHE));

    static Cache<String, String> getStringCache()
    {
        return CACHE.get();
    }

    /** Unit testing only */
    static void setCache(final Cache<String, String> cache)
    {
        CACHE.set(cache);
    }

    static String getCached(final String value)
    {
        String cached = getStringCache().getIfPresent(value);
        if (cached == null)
        {
            cached = value;
            getStringCache().put(cached, cached);
        }
        return cached;
    }

    EncodingRetainingSection<T> createEncodingRetainingSection();
}
