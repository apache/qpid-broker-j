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


import java.lang.reflect.Array;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;

public abstract class AbstractCompositeTypeConstructor<T> implements DescribedTypeConstructor<T>
{
    @Override
    public TypeConstructor<T> construct(final Object descriptor,
                                        final QpidByteBuffer in,
                                        final int originalPosition,
                                        final ValueHandler valueHandler) throws AmqpErrorException
    {
        return new FieldValueReader();
    }

    protected abstract String getTypeName();

    protected abstract T construct(FieldValueReader x) throws AmqpErrorException;

    protected class FieldValueReader implements TypeConstructor<T>
    {
        private QpidByteBuffer _in;
        private ValueHandler _valueHandler;
        private int _count;

        @Override
        public T construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {
            _in = in;
            _valueHandler = handler;
            return constructType();
        }

        private T constructType() throws AmqpErrorException
        {
            int size;
            final TypeConstructor typeConstructor = _valueHandler.readConstructor(_in);
            long remaining = _in.remaining();
            if (typeConstructor instanceof ListConstructor)
            {
                ListConstructor listConstructor = (ListConstructor) typeConstructor;
                if (remaining < listConstructor.getSize() * 2)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Not sufficient data for deserialization of '%s'."
                                                               + " Expected at least %d bytes. Got %d bytes.",
                                                               getTypeName(),
                                                               listConstructor.getSize(),
                                                               remaining));
                }

                if (listConstructor.getSize() == 1)
                {
                    size = _in.getUnsignedByte();
                    _count = _in.getUnsignedByte();
                }
                else
                {
                    size = _in.getInt();
                    _count = _in.getInt();
                }

                remaining -= listConstructor.getSize();
                if (remaining < size)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Not sufficient data for deserialization of '%s'."
                                                               + " Expected at least %d bytes. Got %d bytes.",
                                                               getTypeName(),
                                                               size,
                                                               remaining));
                }
            }
            else if (typeConstructor instanceof ZeroListConstructor)
            {
                size = 0;
                _count = 0;
            }
            else
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format("Unexpected format when deserializing of '%s'",
                                                           getTypeName()));
            }

            final T constructedObject = AbstractCompositeTypeConstructor.this.construct(this);

            long expectedRemaining = remaining - size;
            long unconsumedBytes = _in.remaining() - expectedRemaining;
            if(unconsumedBytes > 0)
            {
                final String msg =
                        String.format("%s incorrectly encoded, %d bytes remaining after decoding %d elements",
                                      getTypeName(), unconsumedBytes, _count);
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, msg);
            }
            else if (unconsumedBytes < 0)
            {
                final String msg = String.format(
                        "%s incorrectly encoded, %d bytes beyond provided size consumed after decoding %d elements",
                        getTypeName(),
                        -unconsumedBytes,
                        _count);
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, msg);
            }
            return constructedObject;
        }


        public <F> F readValue(final int fieldIndex,
                               final String fieldName,
                               final boolean mandatory,
                               final Class<F> expectedType) throws AmqpErrorException
        {
            if (fieldIndex >= _count)
            {
                if (mandatory)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Mandatory field '%s' of '%s' was not provided",
                                                               fieldName,
                                                               getTypeName()));
                }
                return null;
            }

            Object value = _valueHandler.parse(_in);


            if (value == null && mandatory)
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format("Mandatory field '%s' of '%s' was not provided",
                                                           fieldName,
                                                           getTypeName()));
            }

            if (value != null && !expectedType.isAssignableFrom(value.getClass()))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format(
                                                     "Wrong type for field '%s' of '%s'. Expected '%s' but got '%s'.",
                                                     fieldName,
                                                     getTypeName(),
                                                     expectedType.getSimpleName(),
                                                     value.getClass().getSimpleName()));
            }

            return (F) value;
        }

        public <K, V> Map<K, V> readMapValue(final int fieldIndex,
                                             final String fieldName,
                                             final boolean mandatory,
                                             final Class<K> expectedKeyType,
                                             final Class<V> expectedValueType)
                throws AmqpErrorException
        {
            if (fieldIndex >= _count)
            {
                if (mandatory)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Mandatory field '%s' of '%s' was not provided",
                                                               fieldName,
                                                               getTypeName()));
                }
                return null;
            }

            TypeConstructor typeConstructor = _valueHandler.readConstructor(_in);
            if (typeConstructor instanceof MapConstructor)
            {
                MapConstructor mapConstructor = ((MapConstructor) typeConstructor);

                return mapConstructor.construct(_in,
                                                _valueHandler,
                                                expectedKeyType,
                                                expectedValueType);
            }
            else if (typeConstructor instanceof NullTypeConstructor)
            {
                if (mandatory)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 "Mandatory field '%s' of '%s' was not provided",
                                                 fieldName,
                                                 getTypeName());
                }
            }
            else
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format("Could not decode value field '%s' of '%s'",
                                                           fieldName,
                                                           getTypeName()));
            }

            return null;
        }

        public <F> F[] readArrayValue(final int fieldIndex,
                                      final String fieldName,
                                      final boolean mandatory,
                                      final Class<F> expectedType,
                                      final Converter<F> converter) throws AmqpErrorException
        {
            if (fieldIndex >= _count)
            {
                if (mandatory)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Mandatory field '%s' of '%s' was not provided",
                                                               fieldName,
                                                               getTypeName()));
                }
                return null;
            }

            Object value = _valueHandler.parse(_in);

            if (mandatory && value == null)
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format("Mandatory field '%s' of '%s' was not provided",
                                                           fieldName,
                                                           getTypeName()));
            }

            if (value != null)
            {
                if (value.getClass().isArray())
                {
                    if (expectedType.isAssignableFrom(value.getClass().getComponentType()))
                    {
                        return (F[]) value;
                    }
                    else
                    {
                        final Object[] objects = (Object[]) value;
                        F[] array = (F[]) Array.newInstance(expectedType, objects.length);
                        try
                        {
                            for (int i = 0; i < objects.length; ++i)
                            {
                                array[i] = converter.convert(objects[i]);
                            }
                        }
                        catch (RuntimeException e)
                        {
                            Error error = new Error(AmqpError.DECODE_ERROR,
                                                    String.format("Could not decode value field '%s' of '%s'", fieldName, getTypeName()));
                            throw new AmqpErrorException(error, e);
                        }
                        return array;
                    }
                }
                else if (expectedType.isAssignableFrom(value.getClass()))
                {
                    F[] array = (F[]) Array.newInstance(expectedType, 1);
                    array[0] = (F) value;
                    return array;
                }
                else
                {
                    try
                    {
                        final F convertedValue = converter.convert(value);
                        F[] array = (F[]) Array.newInstance(expectedType, 1);
                        array[0] = convertedValue;
                        return array;
                    }
                    catch (RuntimeException e)
                    {
                        Error error = new Error(AmqpError.DECODE_ERROR,
                                                String.format("Could not decode value field '%s' of '%s'", fieldName, getTypeName()));
                        throw new AmqpErrorException(error, e);
                    }
                }
            }

            return null;
        }
    }

    public interface Converter<T>
    {
        T convert(Object o) throws AmqpErrorException;
    }
}
