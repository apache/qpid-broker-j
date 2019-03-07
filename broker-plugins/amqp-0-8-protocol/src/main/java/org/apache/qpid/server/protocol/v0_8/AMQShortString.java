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

package org.apache.qpid.server.protocol.v0_8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.virtualhost.CacheFactory;
import org.apache.qpid.server.virtualhost.NullCache;

/**
 * A short string is a representation of an AMQ Short String
 * Short strings differ from the Java String class by being limited to on ASCII characters (0-127)
 * and thus can be held more effectively in a byte buffer.
 *
 */
public final class AMQShortString implements Comparable<AMQShortString>
{
    /**
     * The maximum number of octets in AMQ short string as defined in AMQP specification
     */
    public static final int MAX_LENGTH = 255;

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQShortString.class);
    private static final NullCache<ByteBuffer, AMQShortString> NULL_CACHE = new NullCache<>();

    // Unfortunately CacheBuilder does not yet support keyEquivalence, so we have to wrap the keys in ByteBuffers
    // rather than using the byte arrays as keys.
    private static final ThreadLocal<Cache<ByteBuffer, AMQShortString>> CACHE =
            ThreadLocal.withInitial(() -> CacheFactory.getCache("amqShortStringCache", NULL_CACHE));

    private final byte[] _data;
    private int _hashCode;
    private String _asString = null;

    public static final AMQShortString EMPTY_STRING = createAMQShortString((String)null);


    private AMQShortString(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException("Cannot create AMQShortString with null data[]");
        }
        if (data.length > MAX_LENGTH)
        {
            throw new IllegalArgumentException("Cannot create AMQShortString with number of octets over 255!");
        }
        _data = data;
    }

    static byte[] readAMQShortStringAsBytes(QpidByteBuffer buffer)
    {
        int length = buffer.getUnsignedByte();
        if(length == 0)
        {
            return null;
        }
        else
        {
            if (length > MAX_LENGTH)
            {
                throw new IllegalArgumentException("Cannot create AMQShortString with number of octets over 255!");
            }
            if(length > buffer.remaining())
            {
                throw new IllegalArgumentException("Cannot create AMQShortString with length "
                                                   + length + " from a ByteBuffer with only "
                                                   + buffer.remaining()
                                                   + " bytes.");

            }
            byte[] data = new byte[length];
            buffer.get(data,0, length);

            return data;
        }
    }

    public static String readAMQShortStringAsString(QpidByteBuffer buffer)
    {
        byte[] data = readAMQShortStringAsBytes(buffer);
        if (data == null)
        {
            return null;
        }
        else
        {
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    public static AMQShortString readAMQShortString(QpidByteBuffer buffer)
    {
        byte[] data = readAMQShortStringAsBytes(buffer);
        if (data == null)
        {
            return null;
        }
        else
        {
            ByteBuffer stringBuffer = ByteBuffer.wrap(data);
            AMQShortString cached = getShortStringCache().getIfPresent(stringBuffer);
            if (cached == null)
            {
                cached = new AMQShortString(data);
                getShortStringCache().put(stringBuffer, cached);
            }
            return cached;
        }
    }

    public static AMQShortString createAMQShortString(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException("Cannot create AMQShortString with null data[]");
        }

        final AMQShortString cached = getShortStringCache().getIfPresent(ByteBuffer.wrap(data));
        return cached != null ? cached : new AMQShortString(data);
    }

    public static AMQShortString createAMQShortString(String string)
    {
        final byte[] data = EncodingUtils.asUTF8Bytes(string);

        final AMQShortString cached = getShortStringCache().getIfPresent(ByteBuffer.wrap(data));
        if (cached != null)
        {
            return cached;
        }
        else
        {
            final AMQShortString shortString = new AMQShortString(data);

            int hash = 0;
            for (int i = 0; i < data.length; i++)
            {
                data[i] = (byte) (0xFF & data[i]);
                hash = (31 * hash) + data[i];
            }
            shortString._hashCode = hash;
            shortString._asString = string;
            return  shortString;
        }
    }

    /**
     * Get the length of the short string
     * @return length of the underlying byte array
     */
    public int length()
    {
        return _data.length;
    }

    public char charAt(int index)
    {
        return (char) _data[index];

    }

    public byte[] getBytes()
    {
        return _data.clone();
    }

    public void writeToBuffer(QpidByteBuffer buffer)
    {
        writeShortStringBytes(buffer, _data);
    }

    public static void writeShortString(final QpidByteBuffer buffer, final String data)
    {
        writeShortStringBytes(buffer, data.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeShortStringBytes(final QpidByteBuffer buffer, final byte[] data)
    {
        final short size = (short) data.length;
        buffer.putUnsignedByte(size);
        buffer.put(data, 0, size);
    }

    @Override
    public boolean equals(Object o)
    {

        if (o == this)
        {
            return true;
        }

        if(o instanceof AMQShortString)
        {
            return equals((AMQShortString) o);
        }

        return false;

    }

    public boolean equals(final AMQShortString otherString)
    {
        if (otherString == this)
        {
            return true;
        }

        if (otherString == null)
        {
            return false;
        }

        final int hashCode = _hashCode;

        final int otherHashCode = otherString._hashCode;

        if ((hashCode != 0) && (otherHashCode != 0) && (hashCode != otherHashCode))
        {
            return false;
        }

        final int length = _data.length;

        if(length != otherString._data.length)
        {
            return false;
        }

        return Arrays.equals(_data, otherString._data);

    }

    @Override
    public int hashCode()
    {
        int hash = _hashCode;
        if (hash == 0)
        {
            final int size = length();

            for (int i = 0; i < size; i++)
            {
                hash = (31 * hash) + _data[i];
            }

            _hashCode = hash;
        }

        return hash;
    }

    @Override
    public String toString()
    {
        if (_asString == null)
        {
            _asString = new String(_data, StandardCharsets.UTF_8);
        }
        return _asString;
    }

    @Override
    public int compareTo(AMQShortString name)
    {
        if(name == this)
        {
            return 0;
        }
        else if (name == null)
        {
            return 1;
        }
        else
        {

            if (name.length() < length())
            {
                return -name.compareTo(this);
            }

            for (int i = 0; i < length(); i++)
            {
                final byte d = _data[i];
                final byte n = name._data[i];
                if (d < n)
                {
                    return -1;
                }

                if (d > n)
                {
                    return 1;
                }
            }

            return (length() == name.length()) ? 0 : -1;
        }
    }

    public boolean contains(final byte b)
    {
        final int end = _data.length;
        for(int i = 0; i < end; i++)
        {
            if(_data[i] == b)
            {
                return true;
            }
        }
        return false;
    }

    public static AMQShortString validValueOf(Object obj)
    {
        return valueOf(obj,true,true);
    }

    static AMQShortString valueOf(Object obj, boolean truncate, boolean nullAsEmptyString)
    {
        if (obj == null)
        {
            if (nullAsEmptyString)
            {
                return EMPTY_STRING;
            }
            return null;
        }
        else
        {
            String value = String.valueOf(obj);
            int strLength = Math.min(value.length(), AMQShortString.MAX_LENGTH);

            byte[] bytes = EncodingUtils.asUTF8Bytes(value);
            if(truncate)
            {
                while (bytes.length > AMQShortString.MAX_LENGTH)
                {
                    value = value.substring(0, strLength-- - 3) + "...";
                    bytes  = EncodingUtils.asUTF8Bytes(value);
                }

            }
            return createAMQShortString(bytes);
        }
    }

    public static AMQShortString valueOf(Object obj)
    {
        return valueOf(obj, false, false);
    }

    public static AMQShortString valueOf(String obj)
    {
        if(obj == null)
        {
            return null;
        }
        else
        {
            return createAMQShortString(obj);
        }

    }

    public static String toString(AMQShortString amqShortString)
    {
        return amqShortString == null ? null : amqShortString.toString();
    }

    static Cache<ByteBuffer, AMQShortString> getShortStringCache()
    {
        return CACHE.get();
    }

    /** Unit testing only */
    static void setShortStringCache(final Cache<ByteBuffer, AMQShortString> cache)
    {
        CACHE.set(cache);
    }
}
