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
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;

public class UnsignedLongWriter
{
    private static final byte EIGHT_BYTE_FORMAT_CODE = (byte) 0x80;
    private static final byte ONE_BYTE_FORMAT_CODE = (byte) 0x53;
    private static final byte ZERO_BYTE_FORMAT_CODE = (byte) 0x44;


    private static final ValueWriter<UnsignedLong> ZERO_BYTE_WRITER = new ValueWriter<UnsignedLong>()
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



    private static final ValueWriter.Factory<UnsignedLong> FACTORY =
            new ValueWriter.Factory<UnsignedLong>()
            {

                @Override
                public ValueWriter<UnsignedLong> newInstance(final ValueWriter.Registry registry,
                                                             final UnsignedLong object)
                {
                    if (object.equals(UnsignedLong.ZERO))
                    {
                        return ZERO_BYTE_WRITER;
                    }
                    else if ((object.longValue() & 0xffL) == object.longValue())
                    {
                        return new UnsignedLongFixedOneWriter(object);
                    }
                    else
                    {
                        return new UnsignedLongFixedEightWriter(object);
                    }
                }
            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(UnsignedLong.class, FACTORY);
    }

    private static class UnsignedLongFixedOneWriter extends FixedOneWriter<UnsignedLong>
    {
        UnsignedLongFixedOneWriter(final UnsignedLong value)
        {
            super(value.byteValue());
        }

        UnsignedLongFixedOneWriter(byte value)
        {
            super(value);
        }

        @Override
        protected byte getFormatCode()
        {
            return ONE_BYTE_FORMAT_CODE;
        }
    }

    private static class UnsignedLongFixedEightWriter extends FixedEightWriter<UnsignedLong>
    {
        public UnsignedLongFixedEightWriter(final UnsignedLong object)
        {
            super(object.longValue());
        }
        public UnsignedLongFixedEightWriter(final long value)
        {
            super(value);
        }

        @Override
        byte getFormatCode()
        {
            return EIGHT_BYTE_FORMAT_CODE;
        }
    }

    public static ValueWriter<UnsignedLong> getWriter(byte value)
    {
        return new UnsignedLongFixedOneWriter(value);
    }


    public static ValueWriter<UnsignedLong> getWriter(long value)
    {
        return new UnsignedLongFixedEightWriter(value);
    }
}
