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

import static org.apache.qpid.server.transport.util.Functions.lsb;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.cache.Cache;

import org.apache.qpid.server.virtualhost.CacheFactory;
import org.apache.qpid.server.virtualhost.NullCache;


/**
 * AbstractEncoder
 *
 * @author Rafael H. Schloming
 */

public abstract class AbstractEncoder implements Encoder
{

    private static final NullCache<String, byte[]> NULL_CACHE = new NullCache<>();
    private static final ThreadLocal<Cache<String, byte[]>> CACHE =
            ThreadLocal.withInitial(() -> CacheFactory.getCache("encodedStr8BytesCache", NULL_CACHE));

    protected abstract void doPut(byte b);

    protected abstract void doPut(ByteBuffer src);

    protected void put(byte b)
    {
        doPut(b);
    }

    protected void put(ByteBuffer src)
    {
        doPut(src);
    }

    protected void put(byte[] bytes)
    {
        put(ByteBuffer.wrap(bytes));
    }

    protected abstract int beginSize8();
    protected abstract void endSize8(int pos);

    protected abstract int beginSize16();
    protected abstract void endSize16(int pos);

    protected abstract int beginSize32();
    protected abstract void endSize32(int pos);

    @Override
    public void writeUint8(short b)
    {
        assert b < 0x100;

        put((byte) b);
    }

    @Override
    public void writeUint16(int s)
    {
        assert s < 0x10000;

        put(lsb(s >>> 8));
        put(lsb(s));
    }

    @Override
    public void writeUint32(long i)
    {
        assert i < 0x100000000L;

        put(lsb(i >>> 24));
        put(lsb(i >>> 16));
        put(lsb(i >>> 8));
        put(lsb(i));
    }

    @Override
    public void writeSequenceNo(int i)
    {
        writeUint32(i);
    }

    @Override
    public void writeUint64(long l)
    {
        for (int i = 0; i < 8; i++)
        {
            put(lsb(l >> (56 - i*8)));
        }
    }


    @Override
    public void writeDatetime(long l)
    {
        writeUint64(l);
    }

    @Override
    public void writeStr8(String s)
    {
        if (s == null)
        {
            s = "";
        }

        byte[] bytes = getEncodedStringCache().getIfPresent(s);
        if (bytes == null)
        {
            bytes = s.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 255)
            {
                throw new IllegalArgumentException(String.format("String too long (%d) for str8", bytes.length));
            }
            getEncodedStringCache().put(s, bytes);
        }
        writeUint8((short) bytes.length);
        put(bytes);
    }

    @Override
    public void writeStr16(String s)
    {
        if (s == null)
        {
            s = "";
        }

        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 65535)
        {
            throw new IllegalArgumentException(String.format("String too long (%d) for str16", bytes.length));
        }

        writeUint16(bytes.length);
        put(bytes);
    }

    @Override
    public void writeVbin8(byte[] bytes)
    {
        if (bytes == null) { bytes = new byte[0]; }
        if (bytes.length > 255)
        {
            throw new IllegalArgumentException("array too long: " + bytes.length);
        }
        writeUint8((short) bytes.length);
        put(ByteBuffer.wrap(bytes));
    }

    @Override
    public void writeVbin16(byte[] bytes)
    {
        if (bytes == null) { bytes = new byte[0]; }
        writeUint16(bytes.length);
        put(ByteBuffer.wrap(bytes));
    }

    @Override
    public void writeVbin32(byte[] bytes)
    {
        if (bytes == null) { bytes = new byte[0]; }
        writeUint32(bytes.length);
        put(ByteBuffer.wrap(bytes));
    }

    @Override
    public void writeSequenceSet(RangeSet ranges)
    {
        if (ranges == null)
        {
            writeUint16((short) 0);
        }
        else
        {
            writeUint16(ranges.size() * 8);
            for (Range range : ranges)
            {
                writeSequenceNo(range.getLower());
                writeSequenceNo(range.getUpper());
            }
        }
    }

    @Override
    public void writeByteRanges(RangeSet ranges)
    {
        throw new Error("not implemented");
    }

    @Override
    public void writeUuid(UUID uuid)
    {
        long msb = 0;
        long lsb = 0;
        if (uuid != null)
        {
            msb = uuid.getMostSignificantBits();
            lsb = uuid.getLeastSignificantBits();
        }
        writeUint64(msb);
        writeUint64(lsb);
    }

    @Override
    public void writeStruct(int type, Struct s)
    {
        boolean empty = false;
        if (s == null)
        {
            s = Struct.create(type);
            empty = true;
        }

        int width = s.getSizeWidth();
        int pos = -1;
        if (width > 0)
        {
            pos = beginSize(width);
        }

        if (type > 0)
        {
            writeUint16(type);
        }

        s.write(this);

        if (width > 0)
        {
            endSize(width, pos);
        }
    }

    @Override
    public void writeStruct32(Struct s)
    {
        if (s == null)
        {
            writeUint32(0);
        }
        else
        {
            int pos = beginSize32();
            writeUint16(s.getEncodedType());
            s.write(this);
            endSize32(pos);
        }
    }

    @Override
    public void writeMap(Map<String,Object> map)
    {
        int pos = beginSize32();
        if (map != null)
        {
            writeUint32(map.size());
            writeMapEntries(map);
        }
        endSize32(pos);
    }

    protected void writeMapEntries(Map<String,Object> map)
    {
        for (Map.Entry<String,Object> entry : map.entrySet())
        {
            String key = entry.getKey();
            Object value = entry.getValue();
            Type type = EncoderUtils.getEncodingType(value);
            writeStr8(key);
            put(type.getCode());
            write(type, value);
        }
    }

    @Override
    public void writeList(List<Object> list)
    {
        int pos = beginSize32();
        if (list != null)
        {
            writeUint32(list.size());
            writeListEntries(list);
        }
        endSize32(pos);
    }

    protected void writeListEntries(List<Object> list)
    {
        for (Object value : list)
        {
            Type type = EncoderUtils.getEncodingType(value);
            put(type.getCode());
            write(type, value);
        }
    }

    @Override
    public void writeArray(List<Object> array)
    {
        int pos = beginSize32();
        if (array != null)
        {
            writeArrayEntries(array);
        }
        endSize32(pos);
    }

    protected void writeArrayEntries(List<Object> array)
    {
        Type type;

        if (array.isEmpty())
        {
            return;
        }
        else
        {
            type = EncoderUtils.getEncodingType(array.get(0));
        }

        put(type.getCode());

        writeUint32(array.size());

        for (Object value : array)
        {
            write(type, value);
        }
    }

    private void writeSize(Type t, int size)
    {
        if (t.isFixed())
        {
            if (size != t.getWidth())
            {
                throw new IllegalArgumentException
                    ("size does not match fixed width " + t.getWidth() + ": " +
                     size);
            }
        }
        else
        {
            writeSize(t.getWidth(), size);
        }
    }

    private void writeSize(int width, int size)
    {
        // XXX: should check lengths
        switch (width)
        {
        case 1:
            writeUint8((short) size);
            break;
        case 2:
            writeUint16(size);
            break;
        case 4:
            writeUint32(size);
            break;
        default:
            throw new IllegalStateException("illegal width: " + width);
        }
    }

    private int beginSize(int width)
    {
        switch (width)
        {
        case 1:
            return beginSize8();
        case 2:
            return beginSize16();
        case 4:
            return beginSize32();
        default:
            throw new IllegalStateException("illegal width: " + width);
        }
    }

    private void endSize(int width, int pos)
    {
        switch (width)
        {
        case 1:
            endSize8(pos);
            break;
        case 2:
            endSize16(pos);
            break;
        case 4:
            endSize32(pos);
            break;
        default:
            throw new IllegalStateException("illegal width: " + width);
        }
    }

    private void writeBytes(Type t, byte[] bytes)
    {
        writeSize(t, bytes.length);
        put(bytes);
    }

    private <T> T coerce(Class<T> klass, Object value)
    {
        if (klass.isInstance(value))
        {
            return klass.cast(value);
        }
        else
        {
            throw new IllegalArgumentException("" + value);
        }
    }

    private void write(Type t, Object value)
    {
        switch (t)
        {
        case BIN8:
        case UINT8:
            writeUint8(coerce(Short.class, value));
            break;
        case INT8:
            put(coerce(Byte.class, value));
            break;
        case CHAR:
            put((byte) ((char)coerce(Character.class, value)));
            break;
        case BOOLEAN:
            if (coerce(Boolean.class, value))
            {
                put((byte) 1);
            }
            else
            {
                put((byte) 0);
            }
            break;

        case BIN16:
        case UINT16:
            writeUint16(coerce(Integer.class, value));
            break;

        case INT16:
            writeUint16(coerce(Short.class, value));
            break;

        case BIN32:
        case UINT32:
            writeUint32(coerce(Long.class, value));
            break;

        case CHAR_UTF32:
        case INT32:
            writeUint32(coerce(Integer.class, value));
            break;

        case FLOAT:
            writeUint32(Float.floatToIntBits(coerce(Float.class, value)));
            break;

        case BIN64:
        case UINT64:
        case INT64:
        case DATETIME:
            writeUint64(coerce(Long.class, value));
            break;

        case DOUBLE:
            long bits = Double.doubleToLongBits(coerce(Double.class, value));
            writeUint64(bits);
            break;

        case UUID:
            writeUuid(coerce(UUID.class, value));
            break;

        case STR8:
            writeStr8(coerce(String.class, value));
            break;

        case STR16:
            writeStr16(coerce(String.class, value));
            break;

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
            writeBytes(t, coerce(String.class, value).getBytes(charset));
            break;

        case STR8_UTF16:
        case STR16_UTF16:
            writeBytes(t, coerce(String.class, value).getBytes(StandardCharsets.UTF_16));
            break;

        case MAP:
            writeMap((Map<String,Object>) coerce(Map.class, value));
            break;
        case LIST:
            writeList(coerce(List.class, value));
            break;
        case ARRAY:
            writeArray(coerce(List.class, value));
            break;
        case STRUCT32:
            writeStruct32(coerce(Struct.class, value));
            break;

        case BIN40:
        case DEC32:
        case BIN72:
        case DEC64:
            // XXX: what types are we supposed to use here?
            writeBytes(t, coerce(byte[].class, value));
            break;

        case VOID:
            break;

        default:
            writeBytes(t, coerce(byte[].class, value));
            break;
        }
    }

    static Cache<String, byte[]> getEncodedStringCache()
    {
        return CACHE.get();
    }

    /** Unit testing only */
    static void setEncodedStringCache(final Cache<String, byte[]> cache)
    {
        CACHE.set(cache);
    }
}
