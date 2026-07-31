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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.test.utils.UnitTestBase;

class SymbolTypeConstructorCacheTest extends UnitTestBase
{
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();

    private ValueHandler _valueHandler;
    private ValueHandler _saslValueHandler;

    @BeforeEach
    void beforeEach() throws Exception
    {
        clearWireCaches();
        clearSymbolCaches();
        _valueHandler = new ValueHandler(TYPE_REGISTRY);
        _saslValueHandler = new ValueHandler(TYPE_REGISTRY, true);
    }

    @Test
    void normalWireCacheSizeIsBounded() throws Exception
    {
        final int symbolsToCreate = Symbol.MAX_CACHE_SIZE + 500;

        for (int i = 0; i < symbolsToCreate; i++)
        {
            _valueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8("wire-normal-" + i)));
        }

        assertTrue(getAmqpWireCacheSize() <= Symbol.MAX_CACHE_SIZE,
                   "AMQP wire symbol cache size should not exceed the configured bound");
    }

    @Test
    void saslWireCacheSizeIsBounded() throws Exception
    {
        final int symbolsToCreate = Symbol.MAX_SASL_CACHE_SIZE + 500;

        for (int i = 0; i < symbolsToCreate; i++)
        {
            _saslValueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8("wire-sasl-" + i)));
        }

        assertTrue(getSaslWireCacheSize() <= Symbol.MAX_SASL_CACHE_SIZE,
                   "SASL wire symbol cache size should not exceed the configured bound");
    }

    @Test
    void normalWireCacheSizeIsBoundedUnderConcurrentInsertion() throws Exception
    {
        parseSymbolsConcurrently(_valueHandler, "wn", 16, 320);

        assertEquals(Symbol.MAX_CACHE_SIZE, getAmqpWireCacheSize());
    }

    @Test
    void saslWireCacheSizeIsBoundedUnderConcurrentInsertion() throws Exception
    {
        parseSymbolsConcurrently(_saslValueHandler, "ws", 16, 64);

        assertEquals(Symbol.MAX_SASL_CACHE_SIZE, getSaslWireCacheSize());
    }

    @Test
    void longSymbolsDecodeWithoutPopulatingWireCaches() throws Exception
    {
        final Symbol sym8Symbol = (Symbol) _valueHandler.parse(
                QpidByteBuffer.wrap(encodeSymbol8("a".repeat(Symbol.MAX_CACHEABLE_LENGTH + 1))));
        final Symbol sym32Symbol = (Symbol) _valueHandler.parse(
                QpidByteBuffer.wrap(encodeSymbol32("b".repeat(300))));

        assertEquals(Symbol.MAX_CACHEABLE_LENGTH + 1, sym8Symbol.length());
        assertEquals(300, sym32Symbol.length());
        assertEquals(0, getAmqpWireCacheSize());
        assertEquals(0, getSaslWireCacheSize());
    }

    @Test
    void repeatedSmallSymbolsReuseCachedValues() throws Exception
    {
        final Symbol first = (Symbol) _valueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8("repeat-me")));
        final Symbol second = (Symbol) _valueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8("repeat-me")));

        assertSame(first, second);
        assertEquals(1, getAmqpWireCacheSize());
    }

    @Test
    void saslDecodingPopulatesOnlySaslWireCache() throws Exception
    {
        _saslValueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8("sasl-only")));

        assertEquals(0, getAmqpWireCacheSize());
        assertEquals(1, getSaslWireCacheSize());
    }

    @Test
    void saslSymbolicDescriptorPopulatesOnlySaslCaches() throws Exception
    {
        final AmqpErrorException thrown = assertThrows(AmqpErrorException.class,
                () -> _saslValueHandler.parse(QpidByteBuffer.wrap(encodeDescribedSymbol8Descriptor("sasl-desc"))));

        assertEquals(AmqpError.DECODE_ERROR, thrown.getError().getCondition());
        assertEquals(0, getAmqpWireCacheSize());
        assertEquals(1, getSaslWireCacheSize());
        assertEquals(0, getNormalSymbolCache().size());
        assertEquals(1, getSaslSymbolCache().size());
    }

    private static byte[] encodeSymbol8(final String value)
    {
        final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
        final byte[] payload = new byte[ascii.length + 2];
        payload[0] = (byte) 0xA3;
        payload[1] = (byte) ascii.length;
        System.arraycopy(ascii, 0, payload, 2, ascii.length);
        return payload;
    }

    private static byte[] encodeSymbol32(final String value)
    {
        final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
        final byte[] payload = new byte[ascii.length + 5];
        payload[0] = (byte) 0xB3;
        payload[1] = (byte) (ascii.length >> 24);
        payload[2] = (byte) (ascii.length >> 16);
        payload[3] = (byte) (ascii.length >> 8);
        payload[4] = (byte) ascii.length;
        System.arraycopy(ascii, 0, payload, 5, ascii.length);
        return payload;
    }

    private static byte[] encodeDescribedSymbol8Descriptor(final String descriptor)
    {
        final byte[] symbol = encodeSymbol8(descriptor);
        final byte[] payload = new byte[symbol.length + 2];
        payload[0] = 0x00;
        System.arraycopy(symbol, 0, payload, 1, symbol.length);
        payload[payload.length - 1] = 0x40;
        return payload;
    }

    @SuppressWarnings("unchecked")
    private static Map<BinaryString, Symbol> getAmqpWireSymbolMap() throws Exception
    {
        final Field field = SymbolTypeConstructor.class.getDeclaredField("AMQP_SYMBOL_MAP");
        field.setAccessible(true);
        return (Map<BinaryString, Symbol>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<BinaryString, Symbol> getSaslWireSymbolMap() throws Exception
    {
        final Field field = SymbolTypeConstructor.class.getDeclaredField("SASL_SYMBOL_MAP");
        field.setAccessible(true);
        return (Map<BinaryString, Symbol>) field.get(null);
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

    private static void clearWireCaches() throws Exception
    {
        getAmqpWireSymbolMap().clear();
        getSaslWireSymbolMap().clear();
    }

    private static void clearSymbolCaches() throws Exception
    {
        getNormalSymbolCache().clear();
        getSaslSymbolCache().clear();
    }

    private static int getAmqpWireCacheSize() throws Exception
    {
        return getAmqpWireSymbolMap().size();
    }

    private static int getSaslWireCacheSize() throws Exception
    {
        return getSaslWireSymbolMap().size();
    }

    private static void parseSymbolsConcurrently(final ValueHandler valueHandler,
                                                 final String prefix,
                                                 final int threadCount,
                                                 final int symbolsPerThread) throws Exception
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
                        valueHandler.parse(QpidByteBuffer.wrap(encodeSymbol8(prefix + "-" + threadId + "-" + index)));
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
}
