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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class Symbol implements Comparable<Symbol>, CharSequence
{
    public static final int MAX_CACHE_SIZE = 4096;
    public static final int MAX_CACHEABLE_LENGTH = 64;
    public static final int MAX_SASL_CACHE_SIZE = 256;
    public static final int MAX_SASL_CACHEABLE_LENGTH = 32;

    private static final ConcurrentMap<String, Symbol> SYMBOLS = new ConcurrentHashMap<>(MAX_CACHE_SIZE);
    private static final ConcurrentMap<String, Symbol> SASL_SYMBOLS = new ConcurrentHashMap<>(MAX_SASL_CACHE_SIZE);

    private final String _underlying;

    private Symbol(String underlying)
    {
        _underlying = underlying;
    }

    @Override
    public int length()
    {
        return _underlying.length();
    }

    @Override
    public int compareTo(Symbol o)
    {
        return _underlying.compareTo(o._underlying);
    }

    @Override
    public char charAt(int index)
    {
        return _underlying.charAt(index);
    }

    @Override
    public CharSequence subSequence(int beginIndex, int endIndex)
    {
        return _underlying.subSequence(beginIndex, endIndex);
    }

    @Override
    public String toString()
    {
        return _underlying;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final Symbol symbol = (Symbol) o;

        return _underlying.equals(symbol._underlying);
    }

    @Override
    public int hashCode()
    {
        return _underlying.hashCode();
    }

    public static Symbol valueOf(final String symbolVal)
    {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(final String symbolVal)
    {
        return getSymbol(symbolVal, false);
    }

    public static Symbol getSymbol(final String symbolVal, final boolean sasl)
    {
        if (symbolVal == null)
        {
            return null;
        }

        final int maxCacheableLength = sasl ? MAX_SASL_CACHEABLE_LENGTH : MAX_CACHEABLE_LENGTH;
        if (symbolVal.length() > maxCacheableLength)
        {
            return new Symbol(symbolVal);
        }

        final ConcurrentMap<String, Symbol> cache = sasl ? SASL_SYMBOLS : SYMBOLS;
        final int maxCacheSize = sasl ? MAX_SASL_CACHE_SIZE : MAX_CACHE_SIZE;

        Symbol symbol = cache.get(symbolVal);
        if (symbol == null)
        {
            symbol = cacheSymbol(symbolVal, cache, maxCacheSize);
        }
        return symbol;
    }

    private static Symbol cacheSymbol(final String symbolVal,
                                      final ConcurrentMap<String, Symbol> cache,
                                      final int maxCacheSize)
    {
        final Symbol symbol = new Symbol(symbolVal);
        synchronized (cache)
        {
            final Symbol existing = cache.get(symbolVal);
            if (existing != null)
            {
                return existing;
            }
            if (cache.size() < maxCacheSize)
            {
                cache.put(symbolVal, symbol);
            }
        }
        return symbol;
    }
}
