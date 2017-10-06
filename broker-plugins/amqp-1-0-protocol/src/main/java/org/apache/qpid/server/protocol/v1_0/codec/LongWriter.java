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

public class LongWriter
{
    private static final byte EIGHT_BYTE_FORMAT_CODE = (byte) 0x81;


    private static final byte ONE_BYTE_FORMAT_CODE = (byte) 0x55;

    private static final ValueWriter.Factory<Long> FACTORY =
            new ValueWriter.Factory<Long>()
            {
                @Override
                public ValueWriter<Long> newInstance(final ValueWriter.Registry registry,
                                                     final Long l)
                {
                    if (l >= -128 && l <= 127)
                    {
                        return new LongFixedOneWriter(l);
                    }
                    else
                    {
                        return new LongFixedEightWriter(l);
                    }
                }
            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Long.class, FACTORY);
    }

    private static class LongFixedEightWriter extends FixedEightWriter<Long>
    {
        public LongFixedEightWriter(final Long object)
        {
            super(object);
        }

        @Override
        byte getFormatCode()
        {
            return EIGHT_BYTE_FORMAT_CODE;
        }
    }

    private static class LongFixedOneWriter extends FixedOneWriter<Long>
    {
        public LongFixedOneWriter(final Long value)
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
