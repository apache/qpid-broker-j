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
 *
 */

package org.apache.qpid.server.protocol.v1_0.codec;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class ListConstructor extends VariableWidthTypeConstructor<List<Object>>
{
    private ListConstructor(final int size)
    {
        super(size);
    }

    @Override
    public List<Object> construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        int size;
        int count;
        long remaining = (long) in.remaining();
        if (remaining < getSize() * 2)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         String.format("Not sufficient data for deserialization of 'list'."
                                                       + " Expected at least %d bytes. Got %d bytes.",
                                                       getSize(),
                                                       remaining));
        }

        if (getSize() == 1)
        {
            size = in.getUnsignedByte();
            count = in.getUnsignedByte();
        }
        else
        {
            size = in.getInt();
            count = in.getInt();
        }
        remaining -= getSize();
        if (remaining < size)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         String.format("Not sufficient data for deserialization of 'list'."
                                                       + " Expected at least %d bytes. Got %d bytes.",
                                                       size,
                                                       remaining));
        }
        return construct(in, handler, size, count);
    }

    protected List<Object> construct(final QpidByteBuffer in,
                                     final ValueHandler handler,
                                     final int size,
                                     final int count)
            throws AmqpErrorException
    {
        List<Object> list = new ArrayList<>(count);

        for (int i = 0; i < count; i++)
        {
            list.add(handler.parse(in));
        }

        return list;
    }

    public static TypeConstructor getInstance(final int size)
    {
        return new ListConstructor(size);
    }
}
