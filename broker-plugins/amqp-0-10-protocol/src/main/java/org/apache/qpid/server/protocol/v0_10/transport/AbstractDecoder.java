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
package org.apache.qpid.server.protocol.v0_10.transport;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.benmanes.caffeine.cache.Cache;

import org.apache.qpid.server.virtualhost.CacheFactory;
import org.apache.qpid.server.virtualhost.NullCache;


/**
 * AbstractDecoder
 *
 * @author Rafael H. Schloming
 */

public abstract class AbstractDecoder implements Decoder
{
    public static final int DEFAULT_MAX_NESTED_OBJECTS = 50;
    public static final int DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS = 0;

    private static final int NO_COMPOUND = -1;
    private static final NullCache<Binary, String> NULL_CACHE = new NullCache<>();
    private static final ThreadLocal<Cache<Binary, String>> CACHE =
            ThreadLocal.withInitial(() -> CacheFactory.getCache("str8Cache", NULL_CACHE));

    private final int _maxNestedObjects;
    private final int _maxZeroWidthArrayElements;

    private int _compoundRemaining = NO_COMPOUND;
    private int _nestedObjectDepth;

    protected AbstractDecoder()
    {
        this(DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, DEFAULT_MAX_NESTED_OBJECTS);
    }

    protected AbstractDecoder(final int maxZeroWidthArrayElements, final int maxNestedObjects)
    {
        if (maxZeroWidthArrayElements < 0)
        {
            throw new IllegalArgumentException("Maximum zero-width array element count must not be negative");
        }
        if (maxNestedObjects < 0)
        {
            throw new IllegalArgumentException("Maximum nested objects must not be negative: " + maxNestedObjects);
        }
        _maxNestedObjects = maxNestedObjects;
        _maxZeroWidthArrayElements = maxZeroWidthArrayElements;
    }

    protected abstract byte doGet();

    protected abstract void doGet(final byte[] bytes);

    protected abstract int underlyingRemaining();

    protected final int remaining()
    {
        final int underlyingRemaining = underlyingRemaining();
        return _compoundRemaining == NO_COMPOUND
                ? underlyingRemaining
                : Math.min(underlyingRemaining, _compoundRemaining);
    }

    protected final void resetDecoderState()
    {
        _compoundRemaining = NO_COMPOUND;
    }

    protected final void checkAvailable(final int length)
    {
        if (length < 0 || (_compoundRemaining != NO_COMPOUND && length > _compoundRemaining))
        {
            throw new IllegalArgumentException("Cannot read " + length +
                    " byte(s) beyond the declared compound boundary");
        }
    }

    protected final void recordBytesRead(final int length)
    {
        if (_compoundRemaining != NO_COMPOUND)
        {
            _compoundRemaining -= length;
        }
    }

    protected byte get()
    {
        checkAvailable(1);
        final byte value = doGet();
        recordBytesRead(1);
        return value;
    }

    protected void get(final byte[] bytes)
    {
        checkAvailable(bytes.length);
        doGet(bytes);
        recordBytesRead(bytes.length);
    }

    protected Binary get(final int size)
    {
        final byte[] bytes = new byte[size];
        get(bytes);
        return new Binary(bytes);
    }

    protected final int validateLength(final long length)
    {
        final int remaining = remaining();
        if (length < 0 || length > remaining)
        {
            throw new IllegalArgumentException("Declared field length " + length + " is invalid; decoder has " +
                    remaining + " byte(s) remaining");
        }
        return (int) length;
    }

    protected final byte[] readByteArray(final long length)
    {
        final byte[] bytes = new byte[validateLength(length)];
        get(bytes);
        return bytes;
    }

    protected short uget()
    {
        return (short) (0xFF & get());
    }

    @Override
    public short readUint8()
    {
        return uget();
    }

    @Override
    public int readUint16()
    {
        int i = uget() << 8;
        i |= uget();
        return i;
    }

    @Override
    public long readUint32()
    {
        return ((long) uget() << 24) |
                ((long) uget() << 16) |
                ((long) uget() << 8) |
                uget();
    }

    @Override
    public int readSequenceNo()
    {
        return (int) readUint32();
    }

    @Override
    public long readUint64()
    {
        long l = 0;
        for (int i = 0; i < 8; i++)
        {
            l |= ((long) (0xFF & get())) << (56 - i*8);
        }
        return l;
    }

    @Override
    public long readDatetime()
    {
        return readUint64();
    }

    @Override
    public String readStr8()
    {
        final int size = validateLength(readUint8());
        Binary bin = get(size);
        String str = getStringCache().getIfPresent(bin);

        if (str == null)
        {
            str = new String(bin.array(), bin.offset(), bin.size(), StandardCharsets.UTF_8);
            if (bin.hasExcessCapacity())
            {
                bin = bin.copy();
            }
            getStringCache().put(bin, str);
        }
        return str;
    }

    @Override
    public String readStr16()
    {
        return new String(readByteArray(readUint16()), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] readVbin8()
    {
        return readByteArray(readUint8());
    }

    @Override
    public byte[] readVbin16()
    {
        return readByteArray(readUint16());
    }

    @Override
    public byte[] readVbin32()
    {
        return readByteArray(readUint32());
    }

    @Override
    public RangeSet readSequenceSet()
    {
        int count = readUint16()/8;
        switch(count)
        {
            case 0:
                return null;
            case 1:
                return Range.newInstance(readSequenceNo(), readSequenceNo());
            default:
                RangeSet ranges = RangeSetFactory.createRangeSet(count);
                for (int i = 0; i < count; i++)
                {
                    ranges.add(readSequenceNo(), readSequenceNo());
                }
                return ranges;
        }
    }

    @Override
    public RangeSet readByteRanges()
    {
        throw new Error("not implemented");
    }

    @Override
    public UUID readUuid()
    {
        long msb = readUint64();
        long lsb = readUint64();
        return new UUID(msb, lsb);
    }

    @Override
    public Struct readStruct(final int type)
    {
        final Struct st = Struct.create(type);
        final int width = st.getSizeWidth();
        if (width > 0)
        {
            final long size = readSize(width);
            if (size == 0)
            {
                return null;
            }

            enterNestedObject();
            try
            {
                final int originalCompoundRemaining = beginCompound(size, type > 0 ? 2 : 0, "struct");
                try
                {
                    if (type > 0)
                    {
                        final int code = readUint16();
                        assert code == type;
                    }
                    st.read(this);
                    validateCompoundConsumed("struct");
                    return st;
                }
                finally
                {
                    restoreCompound(originalCompoundRemaining, (int) size);
                }
            }
            finally
            {
                exitNestedObject();
            }
        }

        enterNestedObject();
        try
        {
            if (type > 0)
            {
                final int code = readUint16();
                assert code == type;
            }
            st.read(this);
            return st;
        }
        finally
        {
            exitNestedObject();
        }
    }

    @Override
    public Struct readStruct32()
    {
        final long size = readUint32();
        if (size == 0)
        {
            return null;
        }

        enterNestedObject();
        try
        {
            final int originalCompoundRemaining = beginCompound(size, 2, "struct32");
            try
            {
                final int type = readUint16();
                final Struct result = Struct.create(type);
                result.read(this);
                validateCompoundConsumed("struct32");
                return result;
            }
            finally
            {
                restoreCompound(originalCompoundRemaining, (int) size);
            }
        }
        finally
        {
            exitNestedObject();
        }
    }

    @Override
    public Map<String, Object> readMap()
    {
        final long size = readUint32();

        if (size == 0)
        {
            return null;
        }

        enterNestedObject();
        try
        {
            final int originalCompoundRemaining = beginCompound(size, 4, "map");
            try
            {
                final int count = validateCount(readUint32(), remaining() / 2, "map entry");
                final Map<String, Object> result;

                if (count == 0)
                {
                    result = Collections.emptyMap();
                }
                else
                {
                    result = new LinkedHashMap<>();
                    for (int i = 0; i < count; i++)
                    {
                        final String key = readStr8();
                        final byte code = get();
                        final Type t = getType(code);
                        final Object value = read(t);
                        result.put(key, value);
                    }
                }

                validateCompoundConsumed("map");
                return result;
            }
            finally
            {
                restoreCompound(originalCompoundRemaining, (int) size);
            }
        }
        finally
        {
            exitNestedObject();
        }
    }

    @Override
    public List<Object> readList()
    {
        final long size = readUint32();

        if (size == 0)
        {
            return null;
        }

        enterNestedObject();
        try
        {
            final int originalCompoundRemaining = beginCompound(size, 4, "list");
            try
            {
                final int count = validateCount(readUint32(), remaining(), "list item");
                final List<Object> result;

                if (count == 0)
                {
                    result = Collections.emptyList();
                }
                else
                {
                    result = new ArrayList<>();
                    for (int i = 0; i < count; i++)
                    {
                        final byte code = get();
                        final Type t = getType(code);
                        final Object value = read(t);
                        result.add(value);
                    }
                }

                validateCompoundConsumed("list");
                return result;
            }
            finally
            {
                restoreCompound(originalCompoundRemaining, (int) size);
            }
        }
        finally
        {
            exitNestedObject();
        }
    }

    @Override
    public List<Object> readArray()
    {
        final long size = readUint32();

        if (size == 0)
        {
            return null;
        }

        enterNestedObject();
        try
        {
            final int originalCompoundRemaining = beginCompound(size, 5, "array");
            try
            {
                final byte code = get();
                final Type t = getType(code);
                final long encodedCount = readUint32();
                final int elementWidth = t.getWidth();
                final int maximumCount = elementWidth == 0
                        ? _maxZeroWidthArrayElements
                        : remaining() / elementWidth;
                final int count = validateCount(encodedCount, maximumCount, "array element");
                final List<Object> result;

                if (count == 0)
                {
                    result = Collections.emptyList();
                }
                else
                {
                    result = new ArrayList<>();
                    for (int i = 0; i < count; i++)
                    {
                        result.add(read(t));
                    }
                }

                validateCompoundConsumed("array");
                return result;
            }
            finally
            {
                restoreCompound(originalCompoundRemaining, (int) size);
            }
        }
        finally
        {
            exitNestedObject();
        }
    }

    private void enterNestedObject()
    {
        if (_nestedObjectDepth >= _maxNestedObjects)
        {
            throw new IllegalArgumentException("Maximum type nesting depth (" + _maxNestedObjects + ") exceeded");
        }
        _nestedObjectDepth++;
    }

    private void exitNestedObject()
    {
        _nestedObjectDepth--;
    }

    private int beginCompound(final long size, final int minimumSize, final String type)
    {
        final int available = remaining();
        if (size < minimumSize || size > available)
        {
            throw new IllegalArgumentException("Declared " + type + " size " + size +
                    " is invalid; decoder has " + available + " byte(s) remaining");
        }

        final int originalCompoundRemaining = _compoundRemaining;
        _compoundRemaining = (int) size;
        return originalCompoundRemaining;
    }

    private void restoreCompound(final int originalCompoundRemaining, final int declaredSize)
    {
        if (originalCompoundRemaining == NO_COMPOUND)
        {
            _compoundRemaining = NO_COMPOUND;
        }
        else
        {
            final int consumed = declaredSize - _compoundRemaining;
            _compoundRemaining = originalCompoundRemaining - consumed;
        }
    }

    private int validateCount(final long count, final int maximumCount, final String valueDescription)
    {
        if (count < 0 || count > maximumCount)
        {
            throw new IllegalArgumentException("Declared " + valueDescription + " count " + count +
                    " exceeds maximum " + maximumCount);
        }
        return (int) count;
    }

    private void validateCompoundConsumed(final String type)
    {
        if (_compoundRemaining != 0)
        {
            throw new IllegalArgumentException("Declared " + type + " has " + _compoundRemaining +
                    " unconsumed byte(s)");
        }
    }

    private Type getType(final byte code)
    {
        final Type type = Type.get(code);
        if (type == null)
        {
            throw new IllegalArgumentException("unknown code: " + code);
        }
        else
        {
            return type;
        }
    }

    private long readSize(final Type t)
    {
        if (t.isFixed())
        {
            return t.getWidth();
        }
        else
        {
            return readSize(t.getWidth());
        }
    }

    private long readSize(final int width)
    {
        switch (width)
        {
        case 1:
            return readUint8();
        case 2:
            return readUint16();
        case 4:
            return readUint32();
        default:
            throw new IllegalStateException("illegal width: " + width);
        }
    }

    private byte[] readBytes(final Type t)
    {
        return readByteArray(readSize(t));
    }

    private Object read(final Type t)
    {
        switch (t)
        {
        case BIN8:
        case UINT8:
            return readUint8();
        case INT8:
            return get();
        case CHAR:
            return (char) get();
        case BOOLEAN:
            return get() > 0;

        case BIN16:
        case UINT16:
            return readUint16();

        case INT16:
            return (short) readUint16();

        case BIN32:
        case UINT32:
            return readUint32();

        case CHAR_UTF32:
        case INT32:
            return (int) readUint32();

        case FLOAT:
            return Float.intBitsToFloat((int) readUint32());

        case BIN64:
        case UINT64:
        case INT64:
        case DATETIME:
            return readUint64();

        case DOUBLE:
            return Double.longBitsToDouble(readUint64());

        case UUID:
            return readUuid();

        case STR8:
            return readStr8();

        case STR16:
            return readStr16();

        case STR8_LATIN:
        case STR16_LATIN:
            Charset charset;
            try
            {
                charset = Charset.forName("ISO-8859-15");
            }
            catch (UnsupportedCharsetException e)
            {
                // We do not want to start throwing execptions from here so we fall back to ISO_8859_1
                charset = StandardCharsets.ISO_8859_1;
            }
            return new String(readBytes(t), charset);

        case STR8_UTF16:
        case STR16_UTF16:
            return new String(readBytes(t), StandardCharsets.UTF_16);

        case MAP:
            return readMap();
        case LIST:
            return readList();
        case ARRAY:
            return readArray();
        case STRUCT32:
            return readStruct32();

        case BIN40:
        case DEC32:
        case BIN72:
        case DEC64:
            // XXX: what types are we supposed to use here?
            return readBytes(t);

        case VOID:
            return null;

        default:
            return readBytes(t);
        }
    }

    static Cache<Binary, String> getStringCache()
    {
        return CACHE.get();
    }

    /** Unit testing only */
    static void setStringCache(final Cache<Binary, String> cache)
    {
        CACHE.set(cache);
    }
}
