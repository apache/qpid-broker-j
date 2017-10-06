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

public class ByteWriter implements ValueWriter<Byte>
{
    private byte _value;

    public ByteWriter()
    {
    }

    public ByteWriter(final Byte object)
    {
        setValue(object);
    }

    @Override
    public int getEncodedSize()
    {
        return 2;
    }

    @Override
    public void writeToBuffer(QpidByteBuffer buffer)
    {

        buffer.put((byte)0x51);
        buffer.put(_value);
    }

    public void setValue(Byte value)
    {
        _value = value.byteValue();
    }

    private static final Factory<Byte> FACTORY = new Factory<Byte>()
                                            {

                                                @Override
                                                public ValueWriter<Byte> newInstance(final Registry registry,
                                                                                     final Byte object)
                                                {
                                                    return new ByteWriter(object);
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Byte.class, FACTORY);
    }
}
