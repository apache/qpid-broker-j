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

public class CharWriter extends FixedFourWriter<Character>
{
    private static final byte FORMAT_CODE = (byte)0x73;

    public CharWriter(final Character object)
    {
        super((int)object.charValue());
    }

    @Override
    byte getFormatCode()
    {
        return FORMAT_CODE;
    }

    private static final Factory<Character> FACTORY = new Factory<Character>()
                                            {

                                                @Override
                                                public ValueWriter<Character> newInstance(final Registry registry,
                                                                                          final Character object)
                                                {
                                                    return new CharWriter(object);
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Character.class, FACTORY);
    }
}
