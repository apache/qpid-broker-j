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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class SymbolWriter extends VariableWidthWriter<Symbol>
{
    private static final Charset ENCODING_CHARSET = Charset.forName("US-ASCII");
    public static final byte LARGE_ENCODING_CODE = (byte) 0xb3;
    public static final byte SMALL_ENCODING_CODE = (byte) 0xa3;
    private final Symbol _value;

    public SymbolWriter(final Symbol object)
    {
        super(object.length());
        _value = object;
    }

    @Override
    protected byte getFourOctetEncodingCode()
    {
        return LARGE_ENCODING_CODE;
    }

    @Override
    protected byte getSingleOctetEncodingCode()
    {
        return SMALL_ENCODING_CODE;
    }

    @Override
    protected void writeBytes(QpidByteBuffer buf, int offset, int length)
    {
        int end = offset + length;
        for(int i = offset; i < end; i++)
        {
            buf.put((byte)_value.charAt(i));
        }
    }

    private static final Factory<Symbol> FACTORY = new Factory<Symbol>()
                                            {

                                                @Override
                                                public ValueWriter<Symbol> newInstance(final Registry registry,
                                                                                       final Symbol object)
                                                {
                                                    return new SymbolWriter(object);
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Symbol.class, FACTORY);
    }
}
