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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

public class UnsignedIntegerWriter
{
    private static final byte EIGHT_BYTE_FORMAT_CODE = (byte)0x70;
    private static final byte ONE_BYTE_FORMAT_CODE = (byte) 0x52;
    private static final byte ZERO_BYTE_FORMAT_CODE = (byte) 0x43;


    private static final ValueWriter<UnsignedInteger> ZERO_BYTE_WRITER = new ValueWriter<UnsignedInteger>()
        {

            @Override
            public int getEncodedSize()
            {
                return 1;
            }

            @Override
            public void writeToBuffer(QpidByteBuffer buffer)
            {
                buffer.put(ZERO_BYTE_FORMAT_CODE);
            }
        };





    private static final ValueWriter.Factory<UnsignedInteger> FACTORY = new ValueWriter.Factory<UnsignedInteger>()
                                            {

                                                @Override
                                                public ValueWriter<UnsignedInteger> newInstance(final ValueWriter.Registry registry,
                                                                                                final UnsignedInteger uint)
                                                {
                                                    if(uint.equals(UnsignedInteger.ZERO))
                                                    {
                                                        return ZERO_BYTE_WRITER;
                                                    }
                                                    else if(uint.compareTo(UnsignedInteger.valueOf(256))<0)
                                                    {
                                                        return new UnsignedIntegerFixedOneWriter(uint);
                                                    }
                                                    else
                                                    {
                                                        return new UnsignedIntegerFixedFourWriter(uint);
                                                    }
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(UnsignedInteger.class, FACTORY);
    }

    private static class UnsignedIntegerFixedFourWriter extends FixedFourWriter<UnsignedInteger>
    {

        UnsignedIntegerFixedFourWriter(final UnsignedInteger object)
        {
            super(object.intValue());
        }

        @Override
        byte getFormatCode()
        {
            return EIGHT_BYTE_FORMAT_CODE;
        }

    }

    private static class UnsignedIntegerFixedOneWriter extends FixedOneWriter<UnsignedInteger>
    {

        UnsignedIntegerFixedOneWriter(final UnsignedInteger value)
        {
            super(value.byteValue());
        }

        @Override
        protected byte getFormatCode()
        {
            return ONE_BYTE_FORMAT_CODE;
        }
    }
}
