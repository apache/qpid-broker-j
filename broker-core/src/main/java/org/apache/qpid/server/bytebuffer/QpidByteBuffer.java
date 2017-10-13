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

package org.apache.qpid.server.bytebuffer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public class QpidByteBuffer implements AutoCloseable
{
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];
    private static final QpidByteBuffer EMPTY_QPID_BYTE_BUFFER = QpidByteBuffer.wrap(new byte[0]);
    private static final ThreadLocal<QpidByteBufferFragment> _cachedBuffer = new ThreadLocal<>();
    private volatile static boolean _isPoolInitialized;
    private volatile static BufferPool _bufferPool;
    private volatile static int _pooledBufferSize;
    private volatile static double _sparsityFraction;
    private volatile static ByteBuffer _zeroed;
    private volatile List<QpidByteBufferFragment> _fragments = new ArrayList<>();
    private volatile int _resetFragmentIndex = -1;

    //////////////////
    // Absolute puts
    //////////////////

    public QpidByteBuffer put(final int index, final byte b)
    {
        return put(index, new byte[]{b});
    }

    public QpidByteBuffer putShort(final int index, final short value)
    {
        byte[] valueArray = Shorts.toByteArray(value);
        return put(index, valueArray);
    }

    public QpidByteBuffer putChar(final int index, final char value)
    {
        byte[] valueArray = Chars.toByteArray(value);
        return put(index, valueArray);
    }

    public QpidByteBuffer putInt(final int index, final int value)
    {
        byte[] valueArray = Ints.toByteArray(value);
        return put(index, valueArray);
    }

    public QpidByteBuffer putLong(final int index, final long value)
    {
        byte[] valueArray = Longs.toByteArray(value);
        return put(index, valueArray);
    }

    public QpidByteBuffer putFloat(final int index, final float value)
    {
        int intValue = Float.floatToRawIntBits(value);
        return putInt(index, intValue);
    }

    public QpidByteBuffer putDouble(final int index, final double value)
    {
        long longValue = Double.doubleToRawLongBits(value);
        return putLong(index, longValue);
    }

    public final QpidByteBuffer put(final int index, final byte[] src)
    {
        final int valueWidth = src.length;
        if (index < 0 || index + valueWidth > limit())
        {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds [%d, %d)", index, 0, limit()));
        }
        boolean bytewise = false;
        int written = 0;
        int bytesToSkip = index;
        for (int i = 0, size = _fragments.size(); i < size; i++)
        {
            final QpidByteBufferFragment buffer = _fragments.get(i);
            final int limit = buffer.limit();
            boolean isLastFragmentToConsider = valueWidth + bytesToSkip - written <= limit;
            if (!isLastFragmentToConsider && limit != buffer.capacity())
            {
                throw new IllegalStateException(String.format("Unexpected limit %d on fragment %d", limit, i));
            }

            if (bytewise)
            {
                int offset = 0;
                while (limit > offset && written < valueWidth)
                {
                    buffer.put(offset, src[written]);
                    offset++;
                    written++;
                }
                if (written == valueWidth)
                {
                    break;
                }
            }
            else
            {
                if (limit >= bytesToSkip + valueWidth)
                {
                    for (int i1 = 0; i1 < valueWidth; i1++)
                    {
                        buffer.put(bytesToSkip + i1, src[i1]);
                        written++;
                    }
                    break;
                }
                else if (limit > bytesToSkip)
                {
                    bytewise = true;
                    while (limit > bytesToSkip + written)
                    {
                        buffer.put(bytesToSkip + written, src[written]);
                        written++;
                    }
                    bytesToSkip = 0;
                }
                else
                {
                    bytesToSkip -= limit;
                }
            }
        }
        if (valueWidth != written)
        {
            throw new BufferOverflowException();
        }
        return this;
    }

    ////////////////
    // Relative Puts
    ////////////////

    public final QpidByteBuffer put(final byte b)
    {
        return put(new byte[]{b});
    }

    public final QpidByteBuffer putUnsignedByte(final short s)
    {
        put((byte) s);
        return this;
    }

    public final QpidByteBuffer putShort(final short value)
    {
        byte[] valueArray = Shorts.toByteArray(value);
        return put(valueArray);
    }

    public final QpidByteBuffer putUnsignedShort(final int i)
    {
        putShort((short) i);
        return this;
    }

    public final QpidByteBuffer putChar(final char value)
    {
        byte[] valueArray = Chars.toByteArray(value);
        return put(valueArray);
    }

    public final QpidByteBuffer putInt(final int value)
    {
        byte[] valueArray = Ints.toByteArray(value);
        return put(valueArray);
    }

    public final QpidByteBuffer putUnsignedInt(final long value)
    {
        putInt((int) value);
        return this;
    }

    public final QpidByteBuffer putLong(final long value)
    {
        byte[] valueArray = Longs.toByteArray(value);
        return put(valueArray);
    }

    public final QpidByteBuffer putFloat(final float value)
    {
        int intValue = Float.floatToRawIntBits(value);
        return putInt(intValue);
    }

    public final QpidByteBuffer putDouble(final double value)
    {
        long longValue = Double.doubleToRawLongBits(value);
        return putLong(longValue);
    }

    public final QpidByteBuffer put(byte[] src)
    {
        return put(src, 0, src.length);
    }

    public final QpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        final int valueWidth = length;
        if (valueWidth > remaining())
        {
            throw new BufferOverflowException();
        }
        boolean bytewise = false;
        int written = 0;
        for (int i = 0, size = _fragments.size(); i < size; i++)
        {
            final QpidByteBufferFragment buffer = _fragments.get(i);
            if (bytewise)
            {
                while (buffer.remaining() > 0 && written < valueWidth)
                {
                    buffer.put(src[offset + written]);
                    written++;
                }
                if (written == valueWidth)
                {
                    break;
                }
            }
            else
            {
                final int remaining = buffer.remaining();
                if (remaining >= valueWidth)
                {
                    buffer.put(src, offset, length);
                    written += length;
                    break;
                }
                else
                {
                    bytewise = true;
                    buffer.put(src, offset, remaining);
                    written = remaining;
                }
            }
        }
        if (written != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.", written, length));
        }
        return this;
    }

    public final QpidByteBuffer put(final ByteBuffer src)
    {
        final int valueWidth = src.remaining();
        if (valueWidth > remaining())
        {
            throw new BufferOverflowException();
        }
        boolean bytewise = false;
        int written = 0;
        for (int i = 0, size = _fragments.size(); i < size; i++)
        {
            final QpidByteBufferFragment buffer = _fragments.get(i);
            if (bytewise)
            {
                while (src.remaining() > 0 && buffer.remaining() > 0)
                {
                    buffer.put(src.get());
                    written++;
                }
                if (written == valueWidth)
                {
                    break;
                }
            }
            else
            {
                final int remaining = buffer.remaining();
                if (remaining >= valueWidth)
                {
                    buffer.put(src);
                    written += valueWidth;
                    break;
                }
                else
                {
                    bytewise = true;
                    while (remaining > written)
                    {
                        buffer.put(src.get());
                        written++;
                    }
                }
            }
        }
        if (written != valueWidth)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.", written, valueWidth));
        }
        return this;
    }

    public final QpidByteBuffer put(final QpidByteBuffer src)
    {
        final int valueWidth = src.remaining();
        if (valueWidth > remaining())
        {
            throw new BufferOverflowException();
        }
        int i = 0;
        boolean bytewise = false;
        int written = 0;
        int size = _fragments.size();
        final List<QpidByteBufferFragment> fragments = src._fragments;
        for (int i1 = 0, fragmentsSize = fragments.size(); i1 < fragmentsSize; i1++)
        {
            final QpidByteBufferFragment srcFragment = fragments.get(i1);
            for (; i < size; i++)
            {
                final QpidByteBufferFragment dstFragment = _fragments.get(i);
                if (dstFragment.hasRemaining())
                {
                    final int srcFragmentRemaining = srcFragment.remaining();
                    if (bytewise)
                    {
                        while (srcFragmentRemaining > 0 && dstFragment.remaining() > 0)
                        {
                            dstFragment.put(srcFragment.get());
                            written++;
                        }
                        if (!srcFragment.hasRemaining())
                        {
                            break;
                        }
                    }
                    else
                    {
                        final int remaining = dstFragment.remaining();
                        if (remaining >= srcFragmentRemaining)
                        {
                            dstFragment.put(srcFragment);
                            written += srcFragmentRemaining;
                            break;
                        }
                        else
                        {
                            bytewise = true;
                            while (remaining > written)
                            {
                                dstFragment.put(srcFragment.get());
                                written++;
                            }
                        }
                    }
                }
            }
        }
        if (written != valueWidth)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.", written, valueWidth));
        }
        return this;
    }

    ///////////////////
    // Absolute Gets
    ///////////////////

    public byte get(final int index)
    {
        final byte[] byteArray = getByteArray(index, 1);
        return byteArray[0];
    }

    public short getShort(final int index)
    {
        final byte[] byteArray = getByteArray(index, 2);
        return Shorts.fromByteArray(byteArray);
    }

    public final int getUnsignedShort(int index)
    {
        return ((int) getShort(index)) & 0xFFFF;
    }

    public char getChar(final int index)
    {
        final byte[] byteArray = getByteArray(index, 2);
        return Chars.fromByteArray(byteArray);
    }

    public int getInt(final int index)
    {
        final byte[] byteArray = getByteArray(index, 4);
        return Ints.fromByteArray(byteArray);
    }

    public long getLong(final int index)
    {
        final byte[] byteArray = getByteArray(index, 8);
        return Longs.fromByteArray(byteArray);
    }

    public float getFloat(final int index)
    {
        final int intValue = getInt(index);
        return Float.intBitsToFloat(intValue);
    }

    public double getDouble(final int index)
    {
        final long longValue = getLong(index);
        return Double.longBitsToDouble(longValue);
    }

    private byte[] getByteArray(final int index, final int length)
    {
        if (index + length > limit())
        {
            throw new IndexOutOfBoundsException(String.format("%d bytes at index %d do not fit into bounds [%d, %d)", length, index, 0, limit()));
        }

        byte[] value = new byte[length];
        boolean bytewise = false;
        int consumed = 0;
        int bytesToSkip = index;
        for (int i = 0, size = _fragments.size(); i < size; i++)
        {
            final QpidByteBufferFragment buffer = _fragments.get(i);
            final int limit = buffer.limit();
            boolean isLastFragmentToConsider = length + bytesToSkip - consumed <= limit;
            if (!isLastFragmentToConsider && limit != buffer.capacity())
            {
                throw new IllegalStateException(String.format("Unexpectedly limit %d on fragment %d.", limit, i));
            }
            if (bytewise)
            {
                int offset = 0;
                while (limit > offset && consumed < length)
                {
                    value[consumed] = buffer.get(offset);
                    offset++;
                    consumed++;
                }
                if (consumed == length)
                {
                    break;
                }
            }
            else
            {
                if (limit >= bytesToSkip + length)
                {
                    while (consumed < length)
                    {
                        value[consumed] = buffer.get(bytesToSkip + consumed);
                        consumed++;
                    }
                    break;
                }
                else if (limit > bytesToSkip)
                {
                    bytewise = true;
                    while (limit > bytesToSkip + consumed)
                    {
                        value[consumed] = buffer.get(bytesToSkip + consumed);
                        consumed++;
                    }
                    bytesToSkip = 0;
                }
                else
                {
                    bytesToSkip -= limit;
                }
            }
        }
        if (consumed != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only consumed %d of %d bytes.", consumed, length));
        }
        return value;
    }

    //////////////////
    // Relative Gets
    //////////////////

    public final byte get()
    {
        byte[] value = new byte[1];
        get(value, 0, 1);
        return value[0];
    }

    public final short getUnsignedByte()
    {
        return (short) (get() & 0xFF);
    }

    public final short getShort()
    {
        byte[] value = new byte[2];
        get(value, 0, value.length);
        return Shorts.fromByteArray(value);
    }

    public final int getUnsignedShort()
    {
        return ((int) getShort()) & 0xFFFF;
    }

    public final char getChar()
    {
        byte[] value = new byte[2];
        get(value, 0, value.length);
        return Chars.fromByteArray(value);
    }

    public final int getInt()
    {
        byte[] value = new byte[4];
        get(value, 0, value.length);
        return Ints.fromByteArray(value);
    }

    public final long getUnsignedInt()
    {
        return ((long) getInt()) & 0xFFFFFFFFL;
    }

    public final long getLong()
    {
        byte[] value = new byte[8];
        get(value, 0, value.length);
        return Longs.fromByteArray(value);
    }

    public final float getFloat()
    {
        final int intValue = getInt();
        return Float.intBitsToFloat(intValue);
    }

    public final double getDouble()
    {
        final long longValue = getLong();
        return Double.longBitsToDouble(longValue);
    }

    public final QpidByteBuffer get(final byte[] dst)
    {
        return get(dst, 0, dst.length);
    }

    public final QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        if (remaining() < length)
        {
            throw new BufferUnderflowException();
        }
        boolean bytewise = false;
        int consumed = 0;
        for (int i = 0, size = _fragments.size(); i < size; i++)
        {
            final QpidByteBufferFragment buffer = _fragments.get(i);
            if (bytewise)
            {
                while (buffer.hasRemaining() && consumed < length)
                {
                    dst[offset + consumed] = buffer.get();
                    consumed++;
                }
                if (consumed == length)
                {
                    return this;
                }
            }
            else
            {
                final int remaining = buffer.remaining();
                if (remaining >= length)
                {
                    buffer.get(dst, offset, length);
                    return this;
                }
                else if (remaining > 0)
                {
                    bytewise = true;
                    while (remaining > consumed)
                    {
                        dst[offset + consumed] = buffer.get();
                        consumed++;
                    }
                }
            }
        }
        if (consumed != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only consumed %d of %d bytes.", consumed, length));
        }
        return this;
    }

    ///////////////
    // Other stuff
    ////////////////

    public final void copyTo(final byte[] dst)
    {
        if (remaining() < dst.length)
        {
            throw new BufferUnderflowException();
        }
        if (remaining() > dst.length)
        {
            throw new BufferOverflowException();
        }
        int offset = 0;
        for (QpidByteBufferFragment fragment : _fragments)
        {
            final int length = Math.min(fragment.remaining(), dst.length - offset);
            fragment._buffer.duplicate().get(dst, offset, length);
            offset += length;
        }
    }

    public final void copyTo(final ByteBuffer dst)
    {
        if (dst.remaining() < remaining())
        {
            throw new BufferOverflowException();
        }
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            dst.put(fragment._buffer.duplicate());
        }
    }

    public final void putCopyOf(final QpidByteBuffer source)
    {
        int remaining = remaining();
        int sourceRemaining = source.remaining();
        if (sourceRemaining > remaining)
        {
            throw new BufferOverflowException();
        }
        for (int i = 0, fragmentsSize = source._fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment srcFragment = source._fragments.get(i);
            put(srcFragment._buffer.duplicate());
        }
    }

    public final boolean isDirect()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            if (!fragment.isDirect())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public final void close()
    {
        dispose();
    }

    public final void dispose()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            fragment.dispose();
        }
    }

    public final InputStream asInputStream()
    {
        return new QpidByteBuffer.BufferInputStream(this);
    }

    public final long read(ScatteringByteChannel channel) throws IOException
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[_fragments.size()];
        for (int i = 0; i < byteBuffers.length; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            byteBuffers[i] = fragment.getUnderlyingBuffer();
        }
        return channel.read(byteBuffers);
    }

    @Override
    public String toString()
    {
        return "QpidByteBuffer{" + _fragments.size() + " fragments}";
    }

    public QpidByteBuffer reset()
    {
        if (_resetFragmentIndex < 0)
        {
            throw new InvalidMarkException();
        }
        final QpidByteBufferFragment fragment = _fragments.get(_resetFragmentIndex);
        fragment.reset();
        for (int i = _resetFragmentIndex + 1, size = _fragments.size(); i < size; ++i)
        {
            _fragments.get(i).position(0);
        }
        return this;
    }

    public QpidByteBuffer rewind()
    {
        _resetFragmentIndex = -1;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            fragment.rewind();
        }
        return this;
    }

    public final boolean hasArray()
    {
        return _fragments.size() == 1 && _fragments.get(0).hasArray();
    }

    public byte[] array()
    {
        if (!hasArray())
        {
            throw new UnsupportedOperationException("This QpidByteBuffer is not backed by an array.");
        }
        return _fragments.get(0).array();
    }

    public QpidByteBuffer clear()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            _fragments.get(i).clear();
        }
        return this;
    }

    public QpidByteBuffer compact()
    {
        if (_fragments.size() == 1)
        {
            _fragments.get(0).compact();
        }
        else
        {
            int position = position();
            int limit = limit();
            if (position != 0)
            {
                int dstPos = 0;
                for (int srcPos = position; srcPos < limit; ++srcPos, ++dstPos)
                {
                    put(dstPos, get(srcPos));
                }
                position(dstPos);
                limit(capacity());
            }
        }
        _resetFragmentIndex = -1;
        return this;
    }

    public int position()
    {
        int totalPosition = 0;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            totalPosition += fragment.position();
            if (fragment.position() != fragment.limit())
            {
                break;
            }
        }
        return totalPosition;
    }

    public QpidByteBuffer position(int newPosition)
    {
        if (newPosition < 0 || newPosition > limit())
        {
            throw new IllegalArgumentException(String.format("new position %d is out of bounds [%d, %d)", newPosition, 0, limit()));
        }
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            final int fragmentLimit = fragment.limit();
            if (newPosition <= fragmentLimit)
            {
                fragment.position(newPosition);
                newPosition = 0;
            }
            else
            {
                if (fragmentLimit != fragment.capacity())
                {
                    throw new IllegalStateException(String.format("QBB Fragment %d has limit %d != capacity %d",
                                                                  i,
                                                                  fragmentLimit,
                                                                  fragment.capacity()));
                }
                fragment.position(fragmentLimit);
                newPosition -= fragmentLimit;
            }
        }
        return this;
    }

    public int limit()
    {
        int totalLimit = 0;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            final int fragmentLimit = fragment.limit();
            totalLimit += fragmentLimit;
            if (fragmentLimit != fragment.capacity())
            {
                break;
            }
        }

        return totalLimit;
    }

    public QpidByteBuffer limit(int newLimit)
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            final int fragmentCapacity = fragment.capacity();
            final int fragmentLimit = Math.min(newLimit, fragmentCapacity);
            fragment.limit(fragmentLimit);
            newLimit -= fragmentLimit;
        }
        return this;
    }

    public final QpidByteBuffer mark()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            if (fragment.position() != fragment.limit())
            {
                fragment.mark();
                _resetFragmentIndex = i;
                return this;
            }
        }
        _resetFragmentIndex = _fragments.size() - 1;
        _fragments.get(_resetFragmentIndex).mark();
        return this;
    }

    public final int remaining()
    {
        int remaining = 0;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            remaining += fragment.remaining();
        }
        return remaining;
    }

    public final boolean hasRemaining()
    {
        return hasRemaining(1);
    }

    public final boolean hasRemaining(int atLeast)
    {
        int remaining = 0;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            remaining += fragment.remaining();
            if (remaining >= atLeast)
            {
                return true;
            }
        }
        return false;
    }

    public QpidByteBuffer flip()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            fragment.flip();
        }
        return this;
    }

    public int capacity()
    {
        int totalCapacity = 0;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            totalCapacity += _fragments.get(i).capacity();
        }
        return totalCapacity;
    }

    /**
     * Method does not respect mark.
     *
     * @return QpidByteBuffer
     */
    public QpidByteBuffer duplicate()
    {
        final QpidByteBuffer newQpidByteBuffer = new QpidByteBuffer();
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            newQpidByteBuffer._fragments.add(_fragments.get(i).duplicate());
        }
        return newQpidByteBuffer;
    }

    public QpidByteBuffer slice()
    {
        return view(0, remaining());
    }

    public QpidByteBuffer view(int offset, int length)
    {
        if (offset + length > remaining())
        {
            throw new IllegalArgumentException(String.format("offset: %d, length: %d, remaining: %d", offset, length, remaining()));
        }
        final QpidByteBuffer newQpidByteBuffer = new QpidByteBuffer();
        boolean firstFragmentToBeConsidered = true;
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize && length > 0; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            if (fragment.hasRemaining())
            {
                if (!firstFragmentToBeConsidered && fragment.position() != 0)
                {
                    throw new IllegalStateException(String.format("Unexpectedly position %d on fragment %d.", fragment.position(), i));
                }
                firstFragmentToBeConsidered = false;
                if (fragment.remaining() > offset)
                {
                    final int fragmentViewLength = Math.min(fragment.remaining() - offset, length);
                    newQpidByteBuffer._fragments.add(fragment.view(offset, fragmentViewLength));
                    length -= fragmentViewLength;
                    offset = 0;
                }
                else
                {
                    offset -= fragment.remaining();
                }
            }
        }

        return newQpidByteBuffer;
    }

    List<ByteBuffer> getUnderlyingBuffers()
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            byteBuffers.add(_fragments.get(i).getUnderlyingBuffer());
        }
        return byteBuffers;

    }

    public static QpidByteBuffer allocate(boolean direct, int size)
    {
        return direct ? allocateDirect(size) : allocate(size);
    }

    public static QpidByteBuffer allocate(int size)
    {
        final QpidByteBuffer qpidByteBuffer = new QpidByteBuffer();
        qpidByteBuffer._fragments.add(QpidByteBufferFragment.allocate(size));
        return qpidByteBuffer;
    }

    public static QpidByteBuffer allocateDirect(int size)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Cannot allocate QpidByteBufferFragment with size "
                                               + size
                                               + " which is negative.");
        }

        if (_isPoolInitialized)
        {
            QpidByteBuffer qpidByteBuffer = new QpidByteBuffer();
            int allocatedSize = 0;
            while (size - allocatedSize >= _pooledBufferSize)
            {
                qpidByteBuffer._fragments.add(QpidByteBufferFragment.allocateDirect(_pooledBufferSize));
                allocatedSize += _pooledBufferSize;
            }
            if (allocatedSize != size)
            {
                qpidByteBuffer._fragments.add(QpidByteBufferFragment.allocateDirect(size - allocatedSize));
            }
            return qpidByteBuffer;
        }
        else
        {
            return allocate(size);
        }
    }

    private static QpidByteBuffer asQpidByteBuffer(final byte[] data, final int offset, final int length)
    {
        try (QpidByteBufferOutputStream outputStream = new QpidByteBufferOutputStream(true, getPooledBufferSize()))
        {
            outputStream.write(data, offset, length);
            return outputStream.fetchAccumulatedBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException("unexpected Error converting array to QpidByteBuffers", e);
        }
    }

    public static QpidByteBuffer asQpidByteBuffer(final InputStream stream) throws IOException
    {
        final QpidByteBuffer qpidByteBuffer = new QpidByteBuffer();
        final int pooledBufferSize = QpidByteBuffer.getPooledBufferSize();
        byte[] transferBuf = new byte[pooledBufferSize];
        int readFragment = 0;
        int read = stream.read(transferBuf, readFragment, pooledBufferSize - readFragment);
        while (read > 0)
        {
            readFragment += read;
            if (readFragment == pooledBufferSize)
            {
                QpidByteBufferFragment fragment = QpidByteBufferFragment.allocateDirect(pooledBufferSize);
                fragment.put(transferBuf, 0, pooledBufferSize);
                fragment.flip();
                qpidByteBuffer._fragments.add(fragment);
                readFragment = 0;
            }
            read = stream.read(transferBuf, readFragment, pooledBufferSize - readFragment);
        }
        if (readFragment != 0)
        {
            QpidByteBufferFragment fragment = QpidByteBufferFragment.allocateDirect(readFragment);
            fragment.put(transferBuf, 0, readFragment);
            fragment.flip();
            qpidByteBuffer._fragments.add(fragment);
        }
        return qpidByteBuffer;
    }

    public final SSLEngineResult decryptSSL(SSLEngine engine, QpidByteBuffer dst) throws SSLException
    {
        final List<ByteBuffer> dstUnderlyingBuffers = dst.getUnderlyingBuffers();
        final List<ByteBuffer> underlyingBuffers = getUnderlyingBuffers();
        if (underlyingBuffers.size() != 1)
        {
            throw new IllegalStateException("Expected single fragment buffer");
        }
        return engine.unwrap(underlyingBuffers.get(0),
                             dstUnderlyingBuffers.toArray(new ByteBuffer[dstUnderlyingBuffers.size()]));
    }

    public static SSLEngineResult encryptSSL(SSLEngine engine,
                                             final Collection<QpidByteBuffer> buffers,
                                             QpidByteBuffer dest) throws SSLException
    {
        final ByteBuffer[] src;
        // QPID-7447: prevent unnecessary allocations
        if (buffers.isEmpty())
        {
            src = EMPTY_BYTE_BUFFER_ARRAY;
        }
        else
        {
            List<ByteBuffer> buffers_ = new LinkedList<>();
            for (QpidByteBuffer buffer : buffers)
            {
                buffers_.addAll(buffer.getUnderlyingBuffers());
            }
            src = buffers_.toArray(new ByteBuffer[buffers_.size()]);
        }
        final List<ByteBuffer> dstUnderlyingBuffers = dest.getUnderlyingBuffers();
        if (dstUnderlyingBuffers.size() != 1)
        {
            throw new IllegalStateException("Expected a single fragment output buffer");
        }
        return engine.wrap(src, dstUnderlyingBuffers.get(0));
    }

    public static QpidByteBuffer inflate(QpidByteBuffer compressedBuffer) throws IOException
    {
        if (compressedBuffer == null)
        {
            throw new IllegalArgumentException("compressedBuffer cannot be null");
        }

        boolean isDirect = compressedBuffer.isDirect();
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        List<QpidByteBuffer> uncompressedBuffers = new ArrayList<>();
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(compressedBuffer.asInputStream()))
        {
            byte[] buf = new byte[bufferSize];
            int read;
            while ((read = gzipInputStream.read(buf)) != -1)
            {
                uncompressedBuffers.add(QpidByteBuffer.asQpidByteBuffer(buf, 0, read));
            }
            return QpidByteBuffer.concatenate(uncompressedBuffers);
        }
        finally
        {
            uncompressedBuffers.forEach(QpidByteBuffer::dispose);
        }
    }

    public static QpidByteBuffer deflate(QpidByteBuffer uncompressedBuffer) throws IOException
    {
        if (uncompressedBuffer == null)
        {
            throw new IllegalArgumentException("uncompressedBuffer cannot be null");
        }

        boolean isDirect = uncompressedBuffer.isDirect();
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        try (QpidByteBufferOutputStream compressedOutput = new QpidByteBufferOutputStream(isDirect, bufferSize);
             InputStream compressedInput = uncompressedBuffer.asInputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(new BufferedOutputStream(compressedOutput,
                                                                                         bufferSize)))
        {
            byte[] buf = new byte[16384];
            int read;
            while ((read = compressedInput.read(buf)) > -1)
            {
                gzipStream.write(buf, 0, read);
            }
            gzipStream.finish();
            gzipStream.flush();
            return compressedOutput.fetchAccumulatedBuffer();
        }
    }

    public static long write(GatheringByteChannel channel, Collection<QpidByteBuffer> qpidByteBuffers)
            throws IOException
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        for (QpidByteBuffer qpidByteBuffer : qpidByteBuffers)
        {
            for (QpidByteBufferFragment fragment : qpidByteBuffer._fragments)
            {
                byteBuffers.add(fragment.getUnderlyingBuffer());
            }
        }
        return channel.write(byteBuffers.toArray(new ByteBuffer[byteBuffers.size()]));
    }

    public static QpidByteBuffer wrap(final ByteBuffer wrap)
    {
        final QpidByteBuffer qpidByteBuffer = new QpidByteBuffer();
        qpidByteBuffer._fragments.add(new QpidByteBufferFragment(new NonPooledByteBufferRef(wrap)));
        return qpidByteBuffer;
    }

    public static QpidByteBuffer wrap(final byte[] data)
    {
        return wrap(ByteBuffer.wrap(data));
    }

    public static QpidByteBuffer wrap(final byte[] data, int offset, int length)
    {
        return wrap(ByteBuffer.wrap(data, offset, length));
    }

    static void returnToPool(final ByteBuffer buffer)
    {
        buffer.clear();
        if (_isPoolInitialized)
        {
            final ByteBuffer duplicate = _zeroed.duplicate();
            duplicate.limit(buffer.capacity());
            buffer.put(duplicate);

            _bufferPool.returnBuffer(buffer);
        }
    }

    public synchronized static void initialisePool(int bufferSize, int maxPoolSize, final double sparsityFraction)
    {
        if (_isPoolInitialized && (bufferSize != _pooledBufferSize
                                   || maxPoolSize != _bufferPool.getMaxSize()
                                   || sparsityFraction != _sparsityFraction))
        {
            final String errorMessage = String.format(
                    "QpidByteBuffer pool has already been initialised with bufferSize=%d, maxPoolSize=%d, and sparsityFraction=%f."
                    +
                    "Re-initialisation with different bufferSize=%d and maxPoolSize=%d is not allowed.",
                    _pooledBufferSize,
                    _bufferPool.getMaxSize(),
                    _sparsityFraction,
                    bufferSize,
                    maxPoolSize);
            throw new IllegalStateException(errorMessage);
        }
        if (bufferSize <= 0)
        {
            throw new IllegalArgumentException("Negative or zero bufferSize illegal : " + bufferSize);
        }

        _bufferPool = new BufferPool(maxPoolSize);
        _pooledBufferSize = bufferSize;
        _zeroed = ByteBuffer.allocateDirect(_pooledBufferSize);
        _sparsityFraction = sparsityFraction;
        _isPoolInitialized = true;
    }

    /**
     * Test use only
     */
    public synchronized static void deinitialisePool()
    {
        if (_isPoolInitialized)
        {
            _bufferPool = null;
            _pooledBufferSize = -1;
            _zeroed = null;
            _isPoolInitialized = false;
            _sparsityFraction = 1.0;
            final QpidByteBufferFragment cachedBuffer = _cachedBuffer.get();
            if (cachedBuffer != null)
            {
                cachedBuffer.dispose();
                _cachedBuffer.remove();
            }
        }
    }

    public static int getPooledBufferSize()
    {
        return _pooledBufferSize;
    }

    public static long getAllocatedDirectMemorySize()
    {
        return _pooledBufferSize * getNumberOfActivePooledBuffers();
    }

    public static int getNumberOfActivePooledBuffers()
    {
        return PooledByteBufferRef.getActiveBufferCount();
    }

    public static int getNumberOfPooledBuffers()
    {
        return _bufferPool.size();
    }

    public static long getPooledBufferDisposalCounter()
    {
        return PooledByteBufferRef.getDisposalCounter();
    }

    public static QpidByteBuffer reallocateIfNecessary(final QpidByteBuffer data)
    {
        if (data != null && data.isDirect() && data.isSparse())
        {
            QpidByteBuffer newBuf = allocateDirect(data.remaining());
            newBuf.put(data);
            newBuf.flip();
            data.dispose();
            return newBuf;
        }
        else
        {
            return data;
        }
    }

    boolean isSparse()
    {
        for (int i = 0, fragmentsSize = _fragments.size(); i < fragmentsSize; i++)
        {
            final QpidByteBufferFragment fragment = _fragments.get(i);
            if (fragment.isSparse())
            {
                return true;
            }
        }
        return false;
    }

    public static QpidByteBuffer concatenate(final List<QpidByteBuffer> buffers)
    {
        final QpidByteBuffer qpidByteBuffer = new QpidByteBuffer();
        for (QpidByteBuffer buffer : buffers)
        {
            for (QpidByteBufferFragment fragment : buffer._fragments)
            {
                qpidByteBuffer._fragments.add(fragment.slice());
            }
        }
        return qpidByteBuffer;
    }

    public static QpidByteBuffer concatenate(QpidByteBuffer... buffers)
    {
        return concatenate(Arrays.asList(buffers));
    }

    public static QpidByteBuffer emptyQpidByteBuffer()
    {
        return EMPTY_QPID_BYTE_BUFFER.duplicate();
    }

    public static ThreadFactory createQpidByteBufferTrackingThreadFactory(final ThreadFactory factory)
    {
        return r -> factory.newThread(() -> {
            try
            {
                r.run();
            }
            finally
            {
                final QpidByteBufferFragment cachedThreadLocalBuffer = _cachedBuffer.get();
                if (cachedThreadLocalBuffer != null)
                {
                    cachedThreadLocalBuffer.dispose();
                    _cachedBuffer.remove();
                }
            }
        });
    }

    private static final class BufferInputStream extends InputStream
    {
        private final QpidByteBuffer _qpidByteBuffer;

        private BufferInputStream(final QpidByteBuffer buffer)
        {
            _qpidByteBuffer = buffer.duplicate();
        }

        @Override
        public int read() throws IOException
        {
            if (_qpidByteBuffer.hasRemaining())
            {
                return _qpidByteBuffer.getUnsignedByte();
            }
            return -1;
        }


        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            if (!_qpidByteBuffer.hasRemaining())
            {
                return -1;
            }
            if (_qpidByteBuffer.remaining() < len)
            {
                len = _qpidByteBuffer.remaining();
            }
            _qpidByteBuffer.get(b, off, len);

            return len;
        }

        @Override
        public void mark(int readlimit)
        {
            _qpidByteBuffer.mark();
        }

        @Override
        public void reset() throws IOException
        {
            _qpidByteBuffer.reset();
        }

        @Override
        public boolean markSupported()
        {
            return true;
        }

        @Override
        public long skip(long n) throws IOException
        {
            _qpidByteBuffer.position(_qpidByteBuffer.position() + (int) n);
            return n;
        }

        @Override
        public int available() throws IOException
        {
            return _qpidByteBuffer.remaining();
        }

        @Override
        public void close()
        {
            _qpidByteBuffer.dispose();
        }
    }


    static class QpidByteBufferFragment implements AutoCloseable
    {

        private static final AtomicIntegerFieldUpdater<QpidByteBufferFragment>
                DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
                QpidByteBufferFragment.class,
                "_disposed");

        private final int _offset;

        final ByteBufferRef _ref;
        volatile ByteBuffer _buffer;
        @SuppressWarnings("unused")
        private volatile int _disposed;


        QpidByteBufferFragment(ByteBufferRef ref)
        {
            this(ref, ref.getBuffer(), 0);
        }

        private QpidByteBufferFragment(ByteBufferRef ref, ByteBuffer buffer, int offset)
        {
            _ref = ref;
            _buffer = buffer;
            _offset = offset;
            _ref.incrementRef(capacity());
        }

        public final boolean isDirect()
        {
            return _buffer.isDirect();
        }

        @Override
        public final void close()
        {
            dispose();
        }

        public final void dispose()
        {
            if (DISPOSED_UPDATER.compareAndSet(this, 0, 1))
            {
                _ref.decrementRef(capacity());
                _buffer = null;
            }
        }

        public final CharBuffer decode(Charset charset)
        {
            return charset.decode(getUnderlyingBuffer());
        }

        @Override
        public String toString()
        {
            return "QpidByteBufferFragment{" +
                   "_buffer=" + _buffer +
                   ", _disposed=" + _disposed +
                   '}';
        }

        public final boolean hasRemaining()
        {
            return _buffer.hasRemaining();
        }

        public final QpidByteBufferFragment put(final byte b)
        {
            _buffer.put(b);
            return this;
        }

        public QpidByteBufferFragment put(final int index, final byte b)
        {
            _buffer.put(index, b);
            return this;
        }

        public final QpidByteBufferFragment mark()
        {
            _buffer.mark();
            return this;
        }

        public final boolean hasArray()
        {
            return _buffer.hasArray();
        }

        public byte[] array()
        {
            return _buffer.array();
        }

        public final int remaining()
        {
            return _buffer.remaining();
        }


        public final QpidByteBufferFragment put(final ByteBuffer src)
        {
            _buffer.put(src);
            return this;
        }

        public final QpidByteBufferFragment put(final QpidByteBufferFragment src)
        {
            int sourceRemaining = src.remaining();
            if (sourceRemaining > remaining())
            {
                throw new BufferOverflowException();
            }

            _buffer.put(src.getUnderlyingBuffer());
            return this;
        }

        public final QpidByteBufferFragment get(final byte[] dst, final int offset, final int length)
        {
            _buffer.get(dst, offset, length);
            return this;
        }

        public QpidByteBufferFragment rewind()
        {
            _buffer.rewind();
            return this;
        }

        public QpidByteBufferFragment clear()
        {
            _buffer.clear();
            return this;
        }

        public QpidByteBufferFragment compact()
        {
            _buffer.compact();
            return this;
        }

        public int limit()
        {
            return _buffer.limit();
        }

        public QpidByteBufferFragment reset()
        {
            _buffer.reset();
            return this;
        }

        public QpidByteBufferFragment flip()
        {
            _buffer.flip();
            return this;
        }

        public QpidByteBufferFragment limit(final int newLimit)
        {
            _buffer.limit(newLimit);
            return this;
        }

        /**
         * Method does not respect mark.
         *
         * @return QpidByteBufferFragment
         */
        public QpidByteBufferFragment duplicate()
        {
            ByteBuffer buffer = _ref.getBuffer();
            if (!(_ref instanceof PooledByteBufferRef))
            {
                buffer = buffer.duplicate();
            }

            buffer.position(_offset);
            buffer.limit(_offset + _buffer.capacity());

            buffer = buffer.slice();

            buffer.limit(_buffer.limit());
            buffer.position(_buffer.position());
            return new QpidByteBufferFragment(_ref, buffer, _offset);
        }

        public final QpidByteBufferFragment put(final byte[] src, final int offset, final int length)
        {
            _buffer.put(src, offset, length);
            return this;
        }


        public int capacity()
        {
            return _buffer.capacity();
        }


        public final byte get()
        {
            return _buffer.get();
        }

        public byte get(final int index)
        {
            return _buffer.get(index);
        }

        public QpidByteBufferFragment position(final int newPosition)
        {
            _buffer.position(newPosition);
            return this;
        }

        public QpidByteBufferFragment slice()
        {
            return view(0, _buffer.remaining());
        }

        public QpidByteBufferFragment view(int offset, int length)
        {
            ByteBuffer buffer = _ref.getBuffer();
            if (!(_ref instanceof PooledByteBufferRef))
            {
                buffer = buffer.duplicate();
            }

            int newRemaining = Math.min(_buffer.remaining() - offset, length);

            int newPosition = _offset + _buffer.position() + offset;
            buffer.limit(newPosition + newRemaining);
            buffer.position(newPosition);

            buffer = buffer.slice();

            return new QpidByteBufferFragment(_ref, buffer, newPosition);
        }

        public int position()
        {
            return _buffer.position();
        }

        ByteBuffer getUnderlyingBuffer()
        {
            return _buffer;
        }

        public static QpidByteBufferFragment allocate(boolean direct, int size)
        {
            return direct ? allocateDirect(size) : allocate(size);
        }

        public static QpidByteBufferFragment allocate(int size)
        {
            return new QpidByteBufferFragment(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
        }

        public static QpidByteBufferFragment allocateDirect(int size)
        {
            if (size < 0)
            {
                throw new IllegalArgumentException("Cannot allocate QpidByteBufferFragment with size "
                                                   + size
                                                   + " which is negative.");
            }

            final ByteBufferRef ref;
            if (_isPoolInitialized && _pooledBufferSize >= size)
            {
                if (_pooledBufferSize == size)
                {
                    ByteBuffer buf = _bufferPool.getBuffer();
                    if (buf == null)
                    {
                        buf = ByteBuffer.allocateDirect(size);
                    }
                    ref = new PooledByteBufferRef(buf);
                }
                else
                {
                    QpidByteBufferFragment buf = _cachedBuffer.get();
                    if (buf == null || buf.remaining() < size)
                    {
                        if (buf != null)
                        {
                            buf.dispose();
                        }
                        buf = allocateDirect(_pooledBufferSize);
                        _cachedBuffer.set(buf);
                    }
                    QpidByteBufferFragment rVal = buf.view(0, size);
                    buf.position(buf.position() + size);

                    return rVal;
                }
            }
            else
            {
                ref = new NonPooledByteBufferRef(ByteBuffer.allocateDirect(size));
            }
            return new QpidByteBufferFragment(ref);
        }

        boolean isSparse()
        {
            return _ref.isSparse(_sparsityFraction);
        }

    }
}
