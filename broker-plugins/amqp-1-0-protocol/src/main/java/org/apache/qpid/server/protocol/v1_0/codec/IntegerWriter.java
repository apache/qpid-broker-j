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

public class IntegerWriter
{
    private static final byte EIGHT_BYTE_FORMAT_CODE = (byte)0x71;
    private static final byte ONE_BYTE_FORMAT_CODE = (byte) 0x54;

    private static final ValueWriter.Factory<Integer> FACTORY = new ValueWriter.Factory<Integer>()
                                            {

                                                @Override
                                                public ValueWriter<Integer> newInstance(final ValueWriter.Registry registry,
                                                                                        final Integer i)
                                                {
                                                    if(i >= -128 && i <= 127)
                                                    {
                                                        return new IntegerFixedOneWriter(i);
                                                    }
                                                    else
                                                    {
                                                        return new IntegerFixedFourWriter(i);
                                                    }
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Integer.class, FACTORY);
    }

    private static class IntegerFixedFourWriter extends FixedFourWriter<Integer>
    {
        public IntegerFixedFourWriter(final Integer object)
        {
            super(object);
        }

        @Override
        byte getFormatCode()
        {
            return EIGHT_BYTE_FORMAT_CODE;
        }

    }

    private static class IntegerFixedOneWriter extends FixedOneWriter<Integer>
    {

        public IntegerFixedOneWriter(final Integer value)
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
