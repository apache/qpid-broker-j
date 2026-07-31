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

import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;

/** {@link ValueHandler} is thread safe */
public class ValueHandler implements DescribedTypeConstructorRegistry.Source
{
    public static final byte DESCRIBED_TYPE = (byte)0;
    static final int DEFAULT_MAX_NESTED_OBJECTS = 50;
    public static final int DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS = 0;

    private static final ThreadLocal<RecursionState> RECURSION_STATE = new ThreadLocal<>();

    private final DescribedTypeConstructorRegistry _describedTypeConstructorRegistry;
    private final int _maxNestedObjects;
    private final int _maxZeroWidthArrayElements;
    private final boolean _sasl;


    private static final TypeConstructor[][] TYPE_CONSTRUCTORS =
            {
                    {},
                    {},
                    {},
                    {},
                    { NullTypeConstructor.getInstance(),   BooleanConstructor.getTrueInstance(),
                      BooleanConstructor.getFalseInstance(), ZeroUIntConstructor.getInstance(),
                      ZeroULongConstructor.getInstance(),  ZeroListConstructor.getInstance()       },
                    { UByteTypeConstructor.getInstance(),  ByteTypeConstructor.getInstance(),
                      SmallUIntConstructor.getInstance(),  SmallULongConstructor.getInstance(),
                      SmallIntConstructor.getInstance(),   SmallLongConstructor.getInstance(),
                      BooleanConstructor.getByteInstance()},
                    { UShortTypeConstructor.getInstance(), ShortTypeConstructor.getInstance()      },
                    { UIntTypeConstructor.getInstance(),   IntTypeConstructor.getInstance(),
                      FloatTypeConstructor.getInstance(),  CharTypeConstructor.getInstance(),
                      DecimalConstructor.getDecimal32Instance()},
                    { ULongTypeConstructor.getInstance(),  LongTypeConstructor.getInstance(),
                      DoubleTypeConstructor.getInstance(), TimestampTypeConstructor.getInstance(),
                      DecimalConstructor.getDecimal64Instance()},
                    { null,                                null,
                      null,                                null,
                      DecimalConstructor.getDecimal128Instance(), null,
                      null,                                null,
                      UUIDTypeConstructor.getInstance()                                            },
                    { BinaryTypeConstructor.getInstance(1),
                      StringTypeConstructor.getInstance(1),
                      null,
                      SymbolTypeConstructor.getInstance(1)                                         },
                    { BinaryTypeConstructor.getInstance(4),
                      StringTypeConstructor.getInstance(4),
                      null,
                      SymbolTypeConstructor.getInstance(4)                                         },
                    { ListConstructor.getInstance(1), MapConstructor.getInstance(1)  },
                    { ListConstructor.getInstance(4), MapConstructor.getInstance(4)  },
                    {
                      ArrayTypeConstructor.getOneByteSizeTypeConstructor()
                    },
                    {
                      ArrayTypeConstructor.getFourByteSizeTypeConstructor()
                    }
            };


    public ValueHandler(final DescribedTypeConstructorRegistry registry)
    {
        this(registry, DEFAULT_MAX_NESTED_OBJECTS, DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, false);
    }

    public ValueHandler(final DescribedTypeConstructorRegistry registry, final boolean sasl)
    {
        this(registry, DEFAULT_MAX_NESTED_OBJECTS, DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, sasl);
    }

    public ValueHandler(final DescribedTypeConstructorRegistry registry, final int maxNestedObjects)
    {
        this(registry, maxNestedObjects, DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, false);
    }

    public ValueHandler(final DescribedTypeConstructorRegistry registry,
                        final int maxNestedObjects,
                        final int maxZeroWidthArrayElements)
    {
        this(registry, maxNestedObjects, maxZeroWidthArrayElements, false);
    }

    public ValueHandler(final DescribedTypeConstructorRegistry registry,
                        final int maxNestedObjects,
                        final int maxZeroWidthArrayElements,
                        final boolean sasl)
    {
        _describedTypeConstructorRegistry = registry;
        _maxNestedObjects = maxNestedObjects;
        _maxZeroWidthArrayElements = maxZeroWidthArrayElements;
        _sasl = sasl;
    }


    public Object parse(final QpidByteBuffer in) throws AmqpErrorException
    {
        final boolean topLevelDecode = isTopLevelDecode();
        try
        {
            final TypeConstructor constructor = readConstructor(in);
            return construct(in, constructor);
        }
        finally
        {
            clearTopLevelDecode(topLevelDecode);
        }
    }

    public void skipValue(final QpidByteBuffer in) throws AmqpErrorException
    {
        final boolean topLevelDecode = isTopLevelDecode();
        try
        {
            final SkipConstructor constructor = readSkipConstructor(in);
            constructor.skip(in);
        }
        finally
        {
            clearTopLevelDecode(topLevelDecode);
        }
    }

    public void skipValueAllowingDescribedTypeValue(final QpidByteBuffer in) throws AmqpErrorException
    {
        final boolean topLevelDecode = isTopLevelDecode();
        try
        {
            final SkipConstructor constructor = readSkipConstructorAllowingDescribedTypeValue(in);
            constructor.skip(in);
        }
        finally
        {
            clearTopLevelDecode(topLevelDecode);
        }
    }

    public void skipValue(final QpidByteBuffer in, final byte formatCode) throws AmqpErrorException
    {
        final boolean topLevelDecode = isTopLevelDecode();
        try
        {
            final SkipConstructor constructor = readSkipConstructor(in, formatCode);
            constructor.skip(in);
        }
        finally
        {
            clearTopLevelDecode(topLevelDecode);
        }
    }

    /**
     * Parses a value using a pre-read type constructor, with recursion depth checking.
     * Used by {@link ArrayTypeConstructor} to decode array elements: arrays share a single element constructor read
     * once from the stream, so {@link #parse(QpidByteBuffer)} cannot be used (it would read a new constructor for
     * each element).
     */
    Object parseWithConstructor(final QpidByteBuffer in, final TypeConstructor constructor)
            throws AmqpErrorException
    {
        return construct(in, constructor);
    }

    Object parseWithConstructorAllowingDescribedTypeValue(final QpidByteBuffer in, final TypeConstructor constructor)
            throws AmqpErrorException
    {
        return construct(in, constructor);
    }

    <T, S> Map<T, S> parseMapWithConstructor(final QpidByteBuffer in,
                                             final MapConstructor constructor,
                                             final Class<T> keyType,
                                             final Class<S> valueType)
            throws AmqpErrorException
    {
        final RecursionState recursionState = enterNestedDecode();
        try
        {
            return constructor.construct(in, this, keyType, valueType);
        }
        finally
        {
            exitNestedDecode(recursionState);
        }
    }

    public TypeConstructor readConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        return readConstructor(in, true, false);
    }

    TypeConstructor readNonDescribedConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        return readConstructor(in, false, false);
    }

    TypeConstructor readConstructorAllowingDescribedTypeValue(final QpidByteBuffer in) throws AmqpErrorException
    {
        return readConstructor(in, true, true);
    }

    private TypeConstructor readConstructor(final QpidByteBuffer in,
                                            final boolean allowDescribedType,
                                            final boolean countDescribedType)
            throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data - expected type, no data remaining");
        }
        final byte formatCode = in.get();

        if (formatCode == DESCRIBED_TYPE)
        {
            if (!allowDescribedType)
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                        "Described type values cannot directly contain described type values.");
            }
            if (countDescribedType)
            {
                final NestedObjectScope nestedObjectScope = enterNestedObject();
                try
                {
                    return readDescribedTypeConstructor(in);
                }
                finally
                {
                    nestedObjectScope.close();
                }
            }
            return readDescribedTypeConstructor(in);
        }
        else
        {
            return readNonDescribedConstructor(formatCode);
        }
    }

    private TypeConstructor readDescribedTypeConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        final int originalPositions = in.position() - 1;
        final Object descriptor = readDescribedTypeDescriptor(in);
        final DescribedTypeConstructor describedTypeConstructor = resolveDescribedTypeConstructor(descriptor);

        return new DescribedTypeConstructorWrapper(
                describedTypeConstructor.construct(descriptor, in, originalPositions, this));
    }

    private DescribedTypeConstructor resolveDescribedTypeConstructor(final Object descriptor)
            throws AmqpErrorException
    {
        final DescribedTypeConstructor describedTypeConstructor =
                _describedTypeConstructorRegistry.getConstructor(descriptor);
        if (describedTypeConstructor != null)
        {
            return describedTypeConstructor;
        }
        if (_sasl)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         "Unknown SASL described type descriptor '%s'",
                                         descriptor);
        }
        return new DefaultDescribedTypeConstructor(descriptor);
    }

    private Object readDescribedTypeDescriptor(final QpidByteBuffer in) throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                    "Insufficient data - expected described type descriptor, no data remaining");
        }
        final byte formatCode = in.get();
        if (!isDescribedTypeDescriptorFormatCode(formatCode))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                    "Described type descriptor must be encoded as ulong or symbol.");
        }

        final Object descriptor = readNonDescribedConstructor(formatCode).construct(in, this);
        if (!(descriptor instanceof UnsignedLong) && !(descriptor instanceof Symbol))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                    "Described type descriptor must be encoded as ulong or symbol.");
        }
        return descriptor;
    }

    private boolean isDescribedTypeDescriptorFormatCode(final byte formatCode)
    {
        return formatCode == 0x44 ||
                formatCode == 0x53 ||
                formatCode == (byte) 0x80 ||
                formatCode == (byte) 0xA3 ||
                formatCode == (byte) 0xB3;
    }

    private TypeConstructor readNonDescribedConstructor(final byte formatCode) throws AmqpErrorException
    {
        final int subCategory = (formatCode >> 4) & 0x0F;
        final int subtype = formatCode & 0x0F;
        final TypeConstructor tc = getTypeConstructor(subCategory, subtype);

        if (tc == null)
        {
            throw new AmqpErrorException(ConnectionError.FRAMING_ERROR,
                    "Unknown type format-code 0x%02x",
                    formatCode);
        }

        return tc;
    }

    private TypeConstructor getTypeConstructor(final int subCategory, final int subtype)
    {
        try
        {
            return TYPE_CONSTRUCTORS[subCategory][subtype];
        }
        catch (final IndexOutOfBoundsException e)
        {
            return null;
        }
    }

    private SkipConstructor readSkipConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data - expected type, no data remaining");
        }
        return readSkipConstructor(in, in.get(), true, false);
    }

    private SkipConstructor readSkipConstructor(final QpidByteBuffer in, final byte formatCode)
            throws AmqpErrorException
    {
        return readSkipConstructor(in, formatCode, true, false);
    }

    private SkipConstructor readSkipConstructorAllowingDescribedTypeValue(final QpidByteBuffer in)
            throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data - expected type, no data remaining");
        }
        return readSkipConstructor(in, in.get(), true, true);
    }

    private SkipConstructor readNonDescribedSkipConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data - expected type, no data remaining");
        }
        return readSkipConstructor(in, in.get(), false, false);
    }

    private SkipConstructor readSkipConstructor(final QpidByteBuffer in,
                                               final byte formatCode,
                                               final boolean allowDescribedType,
                                               final boolean countDescribedType)
            throws AmqpErrorException
    {
        if (formatCode == DESCRIBED_TYPE)
        {
            if (!allowDescribedType)
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                        "Described type values cannot directly contain described type values.");
            }

            if (countDescribedType)
            {
                final NestedObjectScope nestedObjectScope = enterNestedObject();
                try
                {
                    return readDescribedSkipConstructor(in);
                }
                finally
                {
                    nestedObjectScope.close();
                }
            }
            return readDescribedSkipConstructor(in);
        }

        final TypeConstructor tc = readNonDescribedConstructor(formatCode);

        if (tc instanceof ZeroListConstructor)
        {
            return new ZeroListSkipConstructor();
        }
        else if (tc instanceof ListConstructor)
        {
            return new ListSkipConstructor(getCompoundSizeBytes(formatCode));
        }
        else if (tc instanceof MapConstructor)
        {
            return new MapSkipConstructor(getCompoundSizeBytes(formatCode));
        }
        else if (tc instanceof ArrayTypeConstructor)
        {
            return new ArraySkipConstructor(getCompoundSizeBytes(formatCode));
        }
        else
        {
            return new SimpleSkipConstructor(getSimpleSkipBytes(formatCode));
        }
    }

    private SkipConstructor readDescribedSkipConstructor(final QpidByteBuffer in) throws AmqpErrorException
    {
        final Object descriptor = readDescribedTypeDescriptor(in);
        final DescribedTypeConstructor describedTypeConstructor = resolveDescribedTypeConstructor(descriptor);
        final SkipConstructor underlyingConstructor = describedTypeConstructor.allowsDescribedTypeValue()
                ? readSkipConstructorAllowingDescribedTypeValue(in)
                : readNonDescribedSkipConstructor(in);

        return new DescribedSkipConstructor(underlyingConstructor);
    }

    private int getCompoundSizeBytes(final byte formatCode)
    {
        return ((formatCode >> 4) & 0x0F) == 0x0C || ((formatCode >> 4) & 0x0F) == 0x0E ? 1 : 4;
    }

    private int getSimpleSkipBytes(final byte formatCode) throws AmqpErrorException
    {
        final int category = (formatCode >> 4) & 0x0F;
        switch (category)
        {
            case 0x04:
                return 0;
            case 0x05:
                return 1;
            case 0x06:
                return 2;
            case 0x07:
                return 4;
            case 0x08:
                return 8;
            case 0x09:
                return 16;
            case 0x0A:
                return -1;
            case 0x0B:
                return -4;
            default:
                throw new AmqpErrorException(ConnectionError.FRAMING_ERROR, "Unknown type format-code 0x%02x",
                        formatCode);
        }
    }

    private void skipBytes(final QpidByteBuffer in, final int bytes, final String typeName)
            throws AmqpErrorException
    {
        if (bytes < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "%s size cannot be negative: %d",
                    typeName, bytes);
        }
        if (!in.hasRemaining(bytes))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode %s.", typeName);
        }
        in.position(in.position() + bytes);
    }

    private int readSize(final QpidByteBuffer in, final int sizeBytes, final String typeName)
            throws AmqpErrorException
    {
        if (!in.hasRemaining(sizeBytes))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode %s.", typeName);
        }
        final int size = sizeBytes == 1 ? in.getUnsignedByte() : in.getInt();
        if (size < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "%s size cannot be negative: %d", typeName, size);
        }
        return size;
    }

    private int readCount(final QpidByteBuffer in, final int sizeBytes, final String typeName)
            throws AmqpErrorException
    {
        if (!in.hasRemaining(sizeBytes))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode %s.", typeName);
        }
        final int count = sizeBytes == 1 ? in.getUnsignedByte() : in.getInt();
        if (count < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "%s count cannot be negative: %d", typeName, count);
        }
        return count;
    }

    private void validateCompoundSize(final QpidByteBuffer in,
                                      final int size,
                                      final int sizeBytes,
                                      final String typeName)
            throws AmqpErrorException
    {
        if (size < sizeBytes)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "%s size is too small for count field: %d",
                    typeName, size);
        }
        if (!in.hasRemaining(size))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode %s.", typeName);
        }
    }

    private void validateCompoundConsumption(final QpidByteBuffer in,
                                             final int expectedPosition,
                                             final int count,
                                             final String typeName)
            throws AmqpErrorException
    {
        final int unconsumedBytes = expectedPosition - in.position();
        if (unconsumedBytes > 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "%s incorrectly encoded, %d bytes remaining after decoding %d elements",
                    typeName, unconsumedBytes, count);
        }
        else if (unconsumedBytes < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                    "%s incorrectly encoded, %d bytes beyond provided size consumed after decoding %d elements",
                    typeName, -unconsumedBytes, count);
        }
    }

    @Override
    public String toString()
    {
        return "ValueHandler{" +
                ", _describedTypeConstructorRegistry=" + _describedTypeConstructorRegistry +
                '}';
    }


    @Override
    public DescribedTypeConstructorRegistry getDescribedTypeRegistry()
    {
        return _describedTypeConstructorRegistry;
    }

    public int getMaxNestedObjects()
    {
        return _maxNestedObjects;
    }

    public int getMaxZeroWidthArrayElements()
    {
        return _maxZeroWidthArrayElements;
    }

    boolean isSasl()
    {
        return _sasl;
    }

    private Object construct(final QpidByteBuffer in, final TypeConstructor constructor)
            throws AmqpErrorException
    {
        if (isNestedObjectConstructor(constructor))
        {
            return constructNested(in, constructor);
        }
        else
        {
            return constructor.construct(in, this);
        }
    }

    private Object constructNested(final QpidByteBuffer in, final TypeConstructor constructor)
            throws AmqpErrorException
    {
        final NestedObjectScope nestedObjectScope = enterNestedObject();
        try
        {
            return constructor.construct(in, this);
        }
        finally
        {
            nestedObjectScope.close();
        }
    }

    private boolean isNestedObjectConstructor(final TypeConstructor constructor)
    {
        return constructor instanceof ListConstructor ||
                constructor instanceof MapConstructor ||
                constructor instanceof ArrayTypeConstructor ||
                constructor instanceof ZeroListConstructor;
    }

    NestedObjectScope enterNestedObject() throws AmqpErrorException
    {
        return new NestedObjectScope(enterNestedDecode());
    }

    private boolean isTopLevelDecode()
    {
        return RECURSION_STATE.get() == null;
    }

    private void clearTopLevelDecode(final boolean topLevelDecode)
    {
        if (topLevelDecode)
        {
            RECURSION_STATE.remove();
        }
    }

    private RecursionState enterNestedDecode() throws AmqpErrorException
    {
        RecursionState recursionState = RECURSION_STATE.get();
        if (recursionState == null)
        {
            recursionState = new RecursionState();
            RECURSION_STATE.set(recursionState);
        }

        final int nextDepth = recursionState._depth + 1;
        if (nextDepth > _maxNestedObjects)
        {
            if (recursionState._depth == 0)
            {
                RECURSION_STATE.remove();
            }
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Maximum type nesting depth (%d) exceeded",
                    _maxNestedObjects);
        }

        recursionState._depth = nextDepth;
        return recursionState;
    }

    private void exitNestedDecode(final RecursionState recursionState)
    {
        recursionState._depth--;
        if (recursionState._depth == 0)
        {
            RECURSION_STATE.remove();
        }
    }

    final class NestedObjectScope
    {
        private final RecursionState _recursionState;

        private NestedObjectScope(final RecursionState recursionState)
        {
            _recursionState = recursionState;
        }

        void close()
        {
            exitNestedDecode(_recursionState);
        }
    }

    private final class DescribedTypeConstructorWrapper implements TypeConstructor
    {
        private final TypeConstructor _typeConstructor;

        private DescribedTypeConstructorWrapper(final TypeConstructor typeConstructor)
        {
            _typeConstructor = typeConstructor;
        }

        @Override
        public Object construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {
            return _typeConstructor.construct(in, handler);
        }

        @Override
        public boolean isZeroWidthArrayElement()
        {
            return _typeConstructor.isZeroWidthArrayElement();
        }
    }

    private interface SkipConstructor
    {
        void skip(final QpidByteBuffer in) throws AmqpErrorException;

        default int getFixedWidth()
        {
            return -1;
        }
    }

    private final class DescribedSkipConstructor implements SkipConstructor
    {
        private final SkipConstructor _underlyingConstructor;

        private DescribedSkipConstructor(final SkipConstructor underlyingConstructor)
        {
            _underlyingConstructor = underlyingConstructor;
        }

        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            _underlyingConstructor.skip(in);
        }

        @Override
        public int getFixedWidth()
        {
            return _underlyingConstructor.getFixedWidth();
        }
    }

    private final class SimpleSkipConstructor implements SkipConstructor
    {
        private final int _skipBytes;

        private SimpleSkipConstructor(final int skipBytes)
        {
            _skipBytes = skipBytes;
        }

        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (_skipBytes < 0)
            {
                final int sizeBytes = -_skipBytes;
                final int size = readSize(in, sizeBytes, "AMQP value");
                skipBytes(in, size, "AMQP value");
            }
            else
            {
                skipBytes(in, _skipBytes, "AMQP value");
            }
        }

        @Override
        public int getFixedWidth()
        {
            return _skipBytes;
        }
    }

    private final class ZeroListSkipConstructor implements SkipConstructor
    {
        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            final RecursionState recursionState = enterNestedDecode();
            exitNestedDecode(recursionState);
        }

        @Override
        public int getFixedWidth()
        {
            return 0;
        }
    }

    private final class ListSkipConstructor implements SkipConstructor
    {
        private final int _sizeBytes;

        private ListSkipConstructor(final int sizeBytes)
        {
            _sizeBytes = sizeBytes;
        }

        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            final RecursionState recursionState = enterNestedDecode();
            try
            {
                final int size = readSize(in, _sizeBytes, "list");
                validateCompoundSize(in, size, _sizeBytes, "list");
                final int expectedPosition = in.position() + size;
                final int count = readCount(in, _sizeBytes, "list");

                for (int i = 0; i < count; i++)
                {
                    skipValue(in);
                }

                validateCompoundConsumption(in, expectedPosition, count, "List");
            }
            finally
            {
                exitNestedDecode(recursionState);
            }
        }
    }

    private final class MapSkipConstructor implements SkipConstructor
    {
        private final int _sizeBytes;

        private MapSkipConstructor(final int sizeBytes)
        {
            _sizeBytes = sizeBytes;
        }

        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            final RecursionState recursionState = enterNestedDecode();
            try
            {
                final int size = readSize(in, _sizeBytes, "map");
                validateCompoundSize(in, size, _sizeBytes, "map");
                final int expectedPosition = in.position() + size;
                final int count = readCount(in, _sizeBytes, "map");

                if ((count & 0x1) == 1)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Map cannot have odd number of elements: %d",
                            count);
                }

                for (int i = 0; i < count; i++)
                {
                    skipValue(in);
                }

                validateCompoundConsumption(in, expectedPosition, count, "Map");
            }
            finally
            {
                exitNestedDecode(recursionState);
            }
        }
    }

    private final class ArraySkipConstructor implements SkipConstructor
    {
        private final int _sizeBytes;

        private ArraySkipConstructor(final int sizeBytes)
        {
            _sizeBytes = sizeBytes;
        }

        @Override
        public void skip(final QpidByteBuffer in) throws AmqpErrorException
        {
            final RecursionState recursionState = enterNestedDecode();
            try
            {
                final int size = readSize(in, _sizeBytes, "array");
                validateCompoundSize(in, size, _sizeBytes, "array");
                final int expectedPosition = in.position() + size;
                final int count = readCount(in, _sizeBytes, "array");
                if (in.position() >= expectedPosition)
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode array.");
                }
                final SkipConstructor elementConstructor = readSkipConstructor(in);
                final int elementDataBytes = expectedPosition - in.position();
                final int fixedWidth = elementConstructor.getFixedWidth();

                if (fixedWidth == 0)
                {
                    if (count > _maxZeroWidthArrayElements)
                    {
                        throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                "Array element count %d exceeds configured zero-width element limit %d",
                                count, _maxZeroWidthArrayElements);
                    }
                    if (count > 0)
                    {
                        elementConstructor.skip(in);
                    }
                }
                else if (fixedWidth > 0)
                {
                    final long requiredElementDataBytes = (long) count * fixedWidth;
                    if (requiredElementDataBytes != elementDataBytes)
                    {
                        throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                "Array incorrectly encoded, %d bytes required for %d elements of width %d, "
                                        + "but %d bytes provided",
                                requiredElementDataBytes, count, fixedWidth, elementDataBytes);
                    }
                    skipBytes(in, elementDataBytes, "array");
                }
                else
                {
                    if (count > elementDataBytes)
                    {
                        throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                "Array element count %d exceeds available element data (%d bytes)",
                                count, elementDataBytes);
                    }
                    for (int i = 0; i < count; i++)
                    {
                        elementConstructor.skip(in);
                    }
                }

                validateCompoundConsumption(in, expectedPosition, count, "Array");
            }
            finally
            {
                exitNestedDecode(recursionState);
            }
        }
    }

    private static final class RecursionState
    {
        private int _depth;
    }

}
