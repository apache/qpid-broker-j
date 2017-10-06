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
 */

package org.apache.qpid.server.protocol.v1_0.codec;

import java.nio.ByteBuffer;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class SymbolArrayWriter extends SimpleVariableWidthWriter<Symbol[]>
{
    private SymbolArrayWriter(final Symbol[] object)
    {
        super(getEncodedValue(object));
    }

    @Override
    protected byte getFourOctetEncodingCode()
    {
        return (byte) 0xf0;
    }

    @Override
    protected byte getSingleOctetEncodingCode()
    {
        return (byte) 0xe0;
    }

    private static byte[] getEncodedValue(final Symbol[] value)
    {
        byte[] encodedVal;
        int length;
        boolean useSmallConstructor = useSmallConstructor(value);
        boolean isSmall = useSmallConstructor && canFitInSmall(value);
        if(isSmall)
        {
            length = 2;
        }
        else
        {
            length = 5;
        }
        for(Symbol symbol : value)
        {
            length += symbol.length() ;
        }
        length += value.length * (useSmallConstructor ? 1 : 4);

        encodedVal = new byte[length];

        ByteBuffer buf = ByteBuffer.wrap(encodedVal);
        if(isSmall)
        {
            buf.put((byte)value.length);
            buf.put(SymbolWriter.SMALL_ENCODING_CODE);
        }
        else
        {
            buf.putInt(value.length);
            buf.put(SymbolWriter.LARGE_ENCODING_CODE);
        }

        for(Symbol symbol : value)
        {
                if(isSmall)
                {
                    buf.put((byte)symbol.length());
                }
                else
                {
                    buf.putInt(symbol.length());
                }


            for(int i = 0; i < symbol.length(); i++)
            {
                buf.put((byte)symbol.charAt(i));
            }
        }
        return encodedVal;
    }

    private static boolean useSmallConstructor(final Symbol[] value)
    {
        for(Symbol sym : value)
        {
            if(sym.length()>255)
            {
                return false;
            }
        }
        return true;
    }

    private static boolean canFitInSmall(final Symbol[] value)
    {
        if(value.length>=127)
        {
            return false;
        }

        int remaining = 253 - value.length;
        for(Symbol symbol : value)
        {

            if((remaining -= symbol.length()) < 0)
            {
                return false;
            }
        }

        return true;
    }


    private static final Factory<Symbol[]> FACTORY = new Factory<Symbol[]>()
                                            {

                                                @Override
                                                public ValueWriter<Symbol[]> newInstance(final Registry registry,
                                                                                         final Symbol[] object)
                                                {
                                                    return new SymbolArrayWriter(object);
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Symbol[].class, FACTORY);
    }
}
