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

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;

public class SymbolTypeConstructor extends VariableWidthTypeConstructor<Symbol>
{
    private static final Charset ASCII = Charset.forName("US-ASCII");

    private static final ConcurrentMap<BinaryString, Symbol> SYMBOL_MAP =
            new ConcurrentHashMap<BinaryString, Symbol>(2048);

    public static SymbolTypeConstructor getInstance(int i)
    {
        return new SymbolTypeConstructor(i);
    }


    private SymbolTypeConstructor(int size)
    {
        super(size);
    }

    private Symbol constructFromSingleBuffer(final QpidByteBuffer in, final int size)
    {
        BinaryString binaryStr;
        if (in.hasArray())
        {
            binaryStr = new BinaryString(in.array(), in.arrayOffset()+in.position(), size);
        }
        else
        {
            byte[] b = new byte[in.remaining()];
            QpidByteBuffer dup = in.duplicate();
            dup.get(b);
            dup.dispose();
            binaryStr = new BinaryString(b, 0, b.length);
        }

        Symbol symbolVal = SYMBOL_MAP.get(binaryStr);
        if(symbolVal == null)
        {
            QpidByteBuffer dup = in.duplicate();
            dup.limit(in.position()+size);
            CharBuffer charBuf = dup.decode(ASCII);
            dup.dispose();

            symbolVal = Symbol.getSymbol(charBuf.toString());

            byte[] data = new byte[size];
            in.get(data);
            binaryStr = new BinaryString(data, 0, size);
            SYMBOL_MAP.putIfAbsent(binaryStr, symbolVal);
        }
        else
        {
            in.position(in.position()+size);
        }

        return symbolVal;
    }

    @Override
    public Symbol construct(final List<QpidByteBuffer> in, final ValueHandler handler) throws AmqpErrorException
    {

        int size;

        if(getSize() == 1)
        {
            size = QpidByteBufferUtils.get(in) & 0xFF;
        }
        else
        {
            size = QpidByteBufferUtils.getInt(in);
        }

        if(!QpidByteBufferUtils.hasRemaining(in, size))
        {
            org.apache.qpid.server.protocol.v1_0.type.transport.Error error = new org.apache.qpid.server.protocol.v1_0.type.transport.Error();
            error.setCondition(ConnectionError.FRAMING_ERROR);
            error.setDescription("Cannot construct symbol: insufficient input data");
            throw new AmqpErrorException(error);
        }

        for(int i = 0; i<in.size(); i++)
        {
            QpidByteBuffer buf = in.get(i);
            if(buf.hasRemaining())
            {
                if(buf.remaining() >= size)
                {
                    return constructFromSingleBuffer(buf, size);
                }
                break;
            }
        }

        byte[] data = new byte[size];
        QpidByteBufferUtils.get(in, data);
        final BinaryString binaryStr = new BinaryString(data);

        Symbol symbolVal = SYMBOL_MAP.get(binaryStr);
        if(symbolVal == null)
        {
            symbolVal = Symbol.valueOf(new String(data, ASCII));
            SYMBOL_MAP.putIfAbsent(binaryStr, symbolVal);
        }

        return symbolVal;
    }
}
