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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public abstract class ArrayTypeConstructor implements TypeConstructor<Object[]>
{


    public Object[] construct(final List<QpidByteBuffer> in, final ValueHandler handler) throws AmqpErrorException
    {
        int size = read(in);
        long remaining = QpidByteBufferUtils.remaining(in);
        if(remaining < (long) size)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         "Insufficient data to decode array - requires %d octects, only %d remaining.",
                                         size, remaining);
        }

        List rval;
        int count = read(in);
        TypeConstructor t = handler.readConstructor(in);
        rval = new ArrayList(count);
        for(int i = 0; i < count; i++)
        {
            rval.add(t.construct(in, handler));
        }
        long unconsumedBytes = remaining - (QpidByteBufferUtils.remaining(in) + (long) size);

        if(unconsumedBytes > 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         "Array incorrectly encoded, %d bytes remaining after decoding %d elements",
                                         unconsumedBytes, count);
        }
        else if (unconsumedBytes < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         "Array incorrectly encoded, %d bytes beyond provided size consumed after decoding %d elements",
                                         -unconsumedBytes, count);
        }
        if(rval.size() == 0)
        {
            return null;
        }
        else
        {
            return rval.toArray((Object[])Array.newInstance(rval.get(0).getClass(), rval.size()));
        }
    }


    abstract int read(QpidByteBuffer in) throws AmqpErrorException;
    abstract int read(List<QpidByteBuffer> in) throws AmqpErrorException;


    private static final ArrayTypeConstructor ONE_BYTE_SIZE_ARRAY = new ArrayTypeConstructor()
    {

        @Override int read(final QpidByteBuffer in) throws AmqpErrorException
        {
            if(!in.hasRemaining())
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode array");
            }
            return ((int)in.get()) & 0xff;
        }

        @Override
        int read(final List<QpidByteBuffer> in) throws AmqpErrorException
        {
            if(!QpidByteBufferUtils.hasRemaining(in))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode array");
            }
            return ((int)QpidByteBufferUtils.get(in)) & 0xff;
        }
    };

    private static final ArrayTypeConstructor FOUR_BYTE_SIZE_ARRAY = new ArrayTypeConstructor()
    {

        @Override int read(final QpidByteBuffer in) throws AmqpErrorException
        {
            if(in.remaining()<4)
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode array");
            }
            return in.getInt();
        }
        @Override
        int read(final List<QpidByteBuffer> in) throws AmqpErrorException
        {
            if(!QpidByteBufferUtils.hasRemaining(in,4))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode array");
            }
            return QpidByteBufferUtils.getInt(in);
        }
    };

    public static ArrayTypeConstructor getOneByteSizeTypeConstructor()
    {
        return ONE_BYTE_SIZE_ARRAY;
    }

    public static ArrayTypeConstructor getFourByteSizeTypeConstructor()
    {
        return FOUR_BYTE_SIZE_ARRAY;
    }

}
