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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class StringTypeConstructor extends VariableWidthTypeConstructor<String>
{


    public static StringTypeConstructor getInstance(int i)
    {
        return new StringTypeConstructor(i);
    }


    private StringTypeConstructor(int size)
    {
        super(size);
    }

    @Override
    public String construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        int size;

        if (!in.hasRemaining(getSize()))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct string: insufficient input data");
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
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct string: insufficient input data");
        }

        byte[] data = new byte[size];
        in.get(data);
        return new String(data, UTF_8);
    }
}
