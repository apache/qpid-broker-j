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

import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class SymbolTypeConstructor extends VariableWidthTypeConstructor<Symbol>
{
    private static final Charset ASCII = Charset.forName("US-ASCII");

    private static final ConcurrentMap<BinaryString, Symbol> AMQP_SYMBOL_MAP =
            new ConcurrentHashMap<>(Symbol.MAX_CACHE_SIZE);
    private static final ConcurrentMap<BinaryString, Symbol> SASL_SYMBOL_MAP =
            new ConcurrentHashMap<>(Symbol.MAX_SASL_CACHE_SIZE);

    public static SymbolTypeConstructor getInstance(int i)
    {
        return new SymbolTypeConstructor(i);
    }


    private SymbolTypeConstructor(int size)
    {
        super(size);
    }

    @Override
    public Symbol construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        final int size;

        if (!in.hasRemaining(getSize()))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct symbol: insufficient input data");
        }

        if (getSize() == 1)
        {
            size = in.getUnsignedByte();
        }
        else
        {
            size = in.getInt();
        }

        if (!in.hasRemaining(size))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct symbol: insufficient input data");
        }

        final byte[] data = new byte[size];
        in.get(data);

        final boolean sasl = handler.isSasl();
        final int maxCacheableLength = sasl ? Symbol.MAX_SASL_CACHEABLE_LENGTH : Symbol.MAX_CACHEABLE_LENGTH;
        if (size > maxCacheableLength)
        {
            return Symbol.getSymbol(new String(data, ASCII), sasl);
        }

        final ConcurrentMap<BinaryString, Symbol> symbolMap = sasl ? SASL_SYMBOL_MAP : AMQP_SYMBOL_MAP;
        final int maxCacheSize = sasl ? Symbol.MAX_SASL_CACHE_SIZE : Symbol.MAX_CACHE_SIZE;
        final BinaryString binaryStr = new BinaryString(data);

        Symbol symbolVal = symbolMap.get(binaryStr);
        if (symbolVal == null)
        {
            symbolVal = Symbol.getSymbol(new String(data, ASCII), sasl);
            synchronized (symbolMap)
            {
                final Symbol existing = symbolMap.get(binaryStr);
                if (existing != null)
                {
                    return existing;
                }
                if (symbolMap.size() < maxCacheSize)
                {
                    symbolMap.put(binaryStr, symbolVal);
                }
            }
        }

        return symbolVal;
    }
}
