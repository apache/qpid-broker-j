/*
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

package org.apache.qpid.server.virtualhost;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;

public class NullCache<K, V> implements Cache<K, V>
{
    @Override
    public V getIfPresent(final Object key)
    {
        return null;
    }

    @Override
    public V get(final K key, final Callable<? extends V> loader) throws ExecutionException
    {
        try
        {
            return loader.call();
        }
        catch (Exception e)
        {
            throw new ExecutionException(e);
        }
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(final Iterable<?> keys)
    {
        return ImmutableMap.of();
    }

    @Override
    public void put(final K key, final V value)
    {
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m)
    {
    }

    @Override
    public void invalidate(final Object key)
    {
    }

    @Override
    public void invalidateAll(final Iterable<?> keys)
    {
    }

    @Override
    public void invalidateAll()
    {
    }

    @Override
    public long size()
    {
        return 0;
    }

    @Override
    public CacheStats stats()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<K, V> asMap()
    {
        return new ConcurrentHashMap<>();
    }

    @Override
    public void cleanUp()
    {
    }
}
