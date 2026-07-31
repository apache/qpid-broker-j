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

package org.apache.qpid.server.protocol.v1_0.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class SymbolCacheTest extends UnitTestBase
{
    @BeforeEach
    void beforeEach() throws Exception
    {
        getNormalSymbolCache().clear();
        getSaslSymbolCache().clear();
    }

    @Test
    void smallSymbolsReuseCachedInstancesWithinEachScope()
    {
        final Symbol normal1 = Symbol.valueOf("shared-symbol");
        final Symbol normal2 = Symbol.valueOf("shared-symbol");
        final Symbol sasl1 = Symbol.getSymbol("shared-symbol", true);
        final Symbol sasl2 = Symbol.getSymbol("shared-symbol", true);

        assertSame(normal1, normal2);
        assertSame(sasl1, sasl2);
        assertNotSame(normal1, sasl1);
    }

    @Test
    void nullInputReturnsNull()
    {
        assertNull(Symbol.valueOf(null));
        assertNull(Symbol.getSymbol(null));
        assertNull(Symbol.getSymbol(null, true));
    }

    @Test
    void normalCacheSizeIsBounded() throws Exception
    {
        final int symbolsToCreate = Symbol.MAX_CACHE_SIZE + 1000;

        for (int i = 0; i < symbolsToCreate; i++)
        {
            Symbol.valueOf("normal-cache-" + i);
        }

        assertTrue(getNormalCacheSize() <= Symbol.MAX_CACHE_SIZE,
                   "Normal symbol cache size should not exceed the configured bound");
    }

    @Test
    void saslCacheSizeIsBounded() throws Exception
    {
        final int symbolsToCreate = Symbol.MAX_SASL_CACHE_SIZE + 1000;

        for (int i = 0; i < symbolsToCreate; i++)
        {
            Symbol.getSymbol("sasl-cache-" + i, true);
        }

        assertTrue(getSaslCacheSize() <= Symbol.MAX_SASL_CACHE_SIZE,
                   "SASL symbol cache size should not exceed the configured bound");
    }

    @Test
    void normalCacheSizeIsBoundedUnderConcurrentInsertion() throws Exception
    {
        createConcurrently(16, 320, (thread, index) -> Symbol.valueOf("normal-c-" + thread + "-" + index));

        assertEquals(Symbol.MAX_CACHE_SIZE, getNormalCacheSize());
    }

    @Test
    void saslCacheSizeIsBoundedUnderConcurrentInsertion() throws Exception
    {
        createConcurrently(16, 64, (thread, index) -> Symbol.getSymbol("sasl-c-" + thread + "-" + index, true));

        assertEquals(Symbol.MAX_SASL_CACHE_SIZE, getSaslCacheSize());
    }

    @Test
    void longSymbolsAreNotCachedInEitherScope() throws Exception
    {
        final String normalLong = "n".repeat(Symbol.MAX_CACHEABLE_LENGTH + 1);
        final String saslLong = "s".repeat(Symbol.MAX_SASL_CACHEABLE_LENGTH + 1);

        final Symbol normal1 = Symbol.valueOf(normalLong);
        final Symbol normal2 = Symbol.valueOf(normalLong);
        final Symbol sasl1 = Symbol.getSymbol(saslLong, true);
        final Symbol sasl2 = Symbol.getSymbol(saslLong, true);

        assertEquals(normal1, normal2);
        assertEquals(sasl1, sasl2);
        assertNotSame(normal1, normal2);
        assertNotSame(sasl1, sasl2);
        assertEquals(0, getNormalCacheSize());
        assertEquals(0, getSaslCacheSize());
    }

    @Test
    void cacheFullSymbolsRemainCorrectWithoutGrowingCache() throws Exception
    {
        for (int i = 0; i < Symbol.MAX_CACHE_SIZE; i++)
        {
            Symbol.valueOf("fill-cache-" + i);
        }

        final Symbol overflow1 = Symbol.valueOf("overflow-symbol");
        final Symbol overflow2 = Symbol.valueOf("overflow-symbol");

        assertEquals(overflow1, overflow2);
        assertNotSame(overflow1, overflow2);
        assertEquals(Symbol.MAX_CACHE_SIZE, getNormalCacheSize());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Symbol> getNormalSymbolCache() throws Exception
    {
        final Field field = Symbol.class.getDeclaredField("SYMBOLS");
        field.setAccessible(true);
        return (Map<String, Symbol>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Symbol> getSaslSymbolCache() throws Exception
    {
        final Field field = Symbol.class.getDeclaredField("SASL_SYMBOLS");
        field.setAccessible(true);
        return (Map<String, Symbol>) field.get(null);
    }

    private static int getNormalCacheSize() throws Exception
    {
        return getNormalSymbolCache().size();
    }

    private static int getSaslCacheSize() throws Exception
    {
        return getSaslSymbolCache().size();
    }

    private static void createConcurrently(final int threadCount,
                                           final int symbolsPerThread,
                                           final SymbolCreator creator) throws Exception
    {
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        try
        {
            for (int thread = 0; thread < threadCount; thread++)
            {
                final int threadId = thread;
                futures.add(executor.submit(() ->
                {
                    startLatch.await();
                    for (int index = 0; index < symbolsPerThread; index++)
                    {
                        creator.create(threadId, index);
                    }
                    return null;
                }));
            }
            startLatch.countDown();
            for (final Future<?> future : futures)
            {
                future.get();
            }
        }
        finally
        {
            executor.shutdown();
            if (!executor.awaitTermination(10L, TimeUnit.SECONDS))
            {
                executor.shutdownNow();
            }
        }
    }

    @FunctionalInterface
    private interface SymbolCreator
    {
        void create(final int thread, final int index);
    }
}
