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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class MapConstructor extends VariableWidthTypeConstructor<Map<Object,Object>>
{
    private MapConstructor(final int size)
    {
        super(size);
    }

    @Override
    public Map<Object, Object> construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        return construct(in, handler, Object.class, Object.class);
    }

    public <T, S> Map<T, S> construct(final QpidByteBuffer in,
                                      final ValueHandler handler,
                                      Class<T> keyType,
                                      Class<S> valueType) throws AmqpErrorException
    {
        int size;
        int count;

        long remaining = (long) in.remaining();
        if (remaining < getSize() * 2)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         String.format("Not sufficient data for deserialization of 'map'."
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
                                         String.format("Not sufficient data for deserialization of 'map'."
                                                       + " Expected at least %d bytes. Got %d bytes.",
                                                       size,
                                                       remaining));
        }

        return construct(in, handler, size, count, keyType, valueType);
    }


    private <T, S> Map<T, S> construct(final QpidByteBuffer in,
                                       final ValueHandler handler,
                                       final int size,
                                       final int count,
                                       Class<T> keyType,
                                       Class<S> valueType)
            throws AmqpErrorException
    {

        // Can't have an odd number of elements in a map
        if ((count & 0x1) == 1)
        {
            String message = String.format("Map cannot have odd number of elements: %d", count);
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
        }

        Map<T, S> map = new LinkedHashMap<>(count);

        final int mapSize = count / 2;
        for (int i = 0; i < mapSize; i++)
        {
            Object key = handler.parse(in);
            if (key != null && !keyType.isAssignableFrom(key.getClass()))
            {
                String message = String.format("Expected key type is '%s' but got '%s'",
                                               keyType.getSimpleName(),
                                               key.getClass().getSimpleName());
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
            }

            Object value = handler.parse(in);
            if (value instanceof DescribedType
                && SpecializedDescribedType.class.isAssignableFrom(valueType)
                && SpecializedDescribedType.hasInvalidValue((Class<SpecializedDescribedType>)valueType))
            {
                value = SpecializedDescribedType.getInvalidValue((Class<SpecializedDescribedType>)valueType, (DescribedType) value);
            }
            else if (value != null && !valueType.isAssignableFrom(value.getClass()))
            {
                String message = String.format("Expected value type is '%s' but got '%s'",
                                               valueType.getSimpleName(),
                                               value.getClass().getSimpleName());
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
            }

            Object oldValue;
            if ((oldValue = map.put((T) key, (S) value)) != null)
            {
                String message = String.format("Map cannot have duplicate keys: %s has values (%s, %s)",
                                               key,
                                               oldValue,
                                               value);
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
            }
        }
        return map;
    }


    public static MapConstructor getInstance(int size)
    {
        return new MapConstructor(size);
    }
}
