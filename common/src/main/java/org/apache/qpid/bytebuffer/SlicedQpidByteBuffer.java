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

package org.apache.qpid.bytebuffer;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

final class SlicedQpidByteBuffer extends QpidByteBuffer
{
    private static final int SIZE_DOUBLE = 8;
    private static final int SIZE_FLOAT = 4;
    private static final int SIZE_LONG = 8;
    private static final int SIZE_INT = 4;
    private static final int SIZE_CHAR = 2;
    private static final int SIZE_SHORT = 2;
    private static final int SIZE_BYTE = 1;

    private final int _capacity;
    private final int _offset;

    private int _mark = -1;
    private int _position = 0;
    private int _limit;
    private ByteBuffer _lastUnderlyingBuffer;

    SlicedQpidByteBuffer(final int position,
                         final int limit,
                         final int capacity,
                         final int offset,
                         final ByteBufferRef ref)
    {
        super(ref, ref.getBuffer());

        if (capacity < 0)
        {
            throw new IllegalArgumentException("Capacity cannot be negative");
        }

        if (limit > capacity || limit < 0)
        {
            throw new IllegalArgumentException("Limit cannot be greater than capacity or negative");
        }

        if (position > limit || position < 0)
        {
            throw new IllegalArgumentException("Position cannot be greater than limit or negative");
        }

        if (offset < 0)
        {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        _capacity = capacity;
        _position = position;
        _limit = limit;
        _offset = offset;
        _ref.incrementRef();
    }

    @Override
    public boolean hasRemaining()
    {
        return _position < _limit;
    }

    @Override
    public QpidByteBuffer putInt(final int index, final int value)
    {
        checkIndexBounds(index, SIZE_INT);
        _buffer.putInt(_offset + index, value);
        return this;
    }

    @Override
    public QpidByteBuffer putShort(final int index, final short value)
    {
        checkIndexBounds(index, SIZE_SHORT);
        _buffer.putShort(_offset + index, value);
        return this;
    }

    @Override
    public QpidByteBuffer putChar(final int index, final char value)
    {
        checkIndexBounds(index, SIZE_CHAR);
        _buffer.putChar(_offset + index, value);
        return this;
    }

    @Override
    public QpidByteBuffer put(final int index, final byte b)
    {
        checkIndexBounds(index, SIZE_BYTE);
        _buffer.put(_offset + index, b);
        return this;
    }

    @Override
    public QpidByteBuffer put(final byte b)
    {
        checkOverflow(SIZE_BYTE);
        put(_position, b);
        _position++;
        return this;
    }

    @Override
    public QpidByteBuffer putDouble(final int index, final double value)
    {
        checkIndexBounds(index, SIZE_DOUBLE);

        _buffer.putDouble(_offset + index, value);
        return this;
    }


    @Override
    public short getShort(final int index)
    {
        checkIndexBounds(index, SIZE_SHORT);
        return _buffer.getShort(index + _offset);
    }

    @Override
    public QpidByteBuffer mark()
    {
        _mark = _position;
        return this;
    }

    @Override
    public long getLong()
    {
        checkUnderflow(SIZE_LONG);

        long value = getLong(_position);
        _position += SIZE_LONG;
        return value;
    }

    @Override
    public QpidByteBuffer putFloat(final int index, final float value)
    {
        checkIndexBounds(index, SIZE_FLOAT);

        _buffer.putFloat(_offset + index, value);
        return this;
    }

    @Override
    public double getDouble(final int index)
    {
        checkIndexBounds(index, SIZE_DOUBLE);
        return _buffer.getDouble(index + _offset);
    }

    @Override
    public boolean hasArray()
    {
        return _buffer.hasArray();
    }

    @Override
    public double getDouble()
    {
        checkUnderflow(SIZE_DOUBLE);

        double value = getDouble(_position);
        _position += SIZE_DOUBLE;
        return value;
    }

    @Override
    public QpidByteBuffer putFloat(final float value)
    {
        checkOverflow(SIZE_FLOAT);

        putFloat(position(), value);
        _position += SIZE_FLOAT;
        return this;
    }

    @Override
    public QpidByteBuffer putInt(final int value)
    {
        checkOverflow(SIZE_INT);

        putInt(position(), value);
        _position += SIZE_INT;
        return this;
    }

    @Override
    public byte[] array()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QpidByteBuffer putShort(final short value)
    {
        checkOverflow(SIZE_SHORT);

        putShort(position(), value);
        _position += SIZE_SHORT;
        return this;
    }

    @Override
    public int getInt(final int index)
    {
        checkIndexBounds(index, SIZE_INT);
        return _buffer.getInt(index + _offset);
    }

    @Override
    public int remaining()
    {
        return _limit - _position;
    }

    @Override
    public QpidByteBuffer put(final byte[] src)
    {
        return put(src, 0, src.length);
    }

    @Override
    public QpidByteBuffer put(final ByteBuffer src)
    {
        int sourceRemaining = src.remaining();
        if (sourceRemaining > remaining())
        {
            throw new BufferOverflowException();
        }

        final int length = src.remaining();
        ByteBuffer dup = getDuplicateForBulk();
        dup.put(src);
        _position += length;

        return this;
    }

    @Override
    public QpidByteBuffer put(final QpidByteBuffer src)
    {
        if (src == this)
        {
            throw new IllegalArgumentException();
        }

        int sourceRemaining = src.remaining();
        if (sourceRemaining > remaining())
        {
            throw new BufferOverflowException();
        }

        put(src.getUnderlyingBuffer());
        src.updateFromLastUnderlying();

        return this;
    }

    @Override
    public QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        checkBounds(dst, offset, length);

        if (length > remaining())
        {
            throw new BufferUnderflowException();
        }

        ByteBuffer dup = getDuplicateForBulk();
        dup.get(dst, offset, length);
        _position += length;
        return this;
    }

    @Override
    public QpidByteBuffer get(final ByteBuffer dst)
    {
        int remaining = remaining();
        copyTo(dst);
        _position += remaining;
        return this;
    }

    @Override
    public void copyTo(final ByteBuffer dst)
    {
        int destinationRemaining = dst.remaining();
        int remaining = remaining();
        if (destinationRemaining < remaining)
        {
            throw new BufferUnderflowException();
        }

        ByteBuffer dup = getDuplicateForBulk();
        dst.put(dup);
    }

    @Override
    public void putCopyOf(final QpidByteBuffer source)
    {
        int remaining = remaining();
        int sourceRemaining = source.remaining();
        if (sourceRemaining > remaining)
        {
            throw new BufferOverflowException();
        }

        put(source.getUnderlyingBuffer().duplicate());
    }

    @Override
    public QpidByteBuffer rewind()
    {
        _position = 0;
        _mark = -1;
        return this;
    }

    @Override
    public QpidByteBuffer clear()
    {
        _position = 0;
        _limit = _capacity;
        _mark = -1;
        return this;
    }

    @Override
    public QpidByteBuffer putLong(final int index, final long value)
    {
        checkIndexBounds(index, SIZE_LONG);

        _buffer.putLong(_offset + index, value);
        return this;
    }

    @Override
    public QpidByteBuffer compact()
    {
        int remaining = remaining();
        if (_position > 0 && _position < _limit)
        {
            getUnderlyingBuffer().compact();
            _lastUnderlyingBuffer = null;
        }
        _position = remaining;
        _limit = _capacity;
        _mark = -1;
        return this;
    }

    @Override
    public QpidByteBuffer putDouble(final double value)
    {
        checkOverflow(SIZE_DOUBLE);

        putDouble(position(), value);
        _position += SIZE_DOUBLE;
        return this;
    }

    @Override
    public int limit()
    {
        return _limit;
    }

    @Override
    public QpidByteBuffer reset()
    {
        if (_mark < 0)
        {
            throw new InvalidMarkException();
        }
        _position = _mark;
        return this;
    }

    @Override
    public QpidByteBuffer flip()
    {
        _limit = _position;
        _position = 0;
        _mark = -1;
        return this;
    }

    @Override
    public short getShort()
    {
        checkUnderflow(SIZE_SHORT);

        short value = getShort(_position);
        _position += SIZE_SHORT;
        return value;
    }

    @Override
    public float getFloat()
    {
        checkUnderflow(SIZE_FLOAT);

        float value = getFloat(_position);
        _position += SIZE_FLOAT;
        return value;
    }

    @Override
    public QpidByteBuffer limit(final int newLimit)
    {
        if (newLimit > _capacity || newLimit < 0)
        {
            throw new IllegalArgumentException();
        }
        _limit = newLimit;
        if (_position > _limit)
        {
            _position = _limit;
        }
        if (_mark > _limit)
        {
            _mark = -1;
        }
        return this;
    }

    @Override
    public QpidByteBuffer duplicate()
    {
        SlicedQpidByteBuffer duplicate = new SlicedQpidByteBuffer(_position, _limit, _capacity, _offset, _ref);
        duplicate._mark = _mark;
        return duplicate;
    }

    @Override
    public QpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        checkBounds(src, offset, length);

        if (length > remaining())
        {
            throw new BufferOverflowException();
        }

        ByteBuffer dup = getDuplicateForBulk();
        dup.put(src, offset, length);
        _position += length;

        return this;
    }

    @Override
    public long getLong(final int index)
    {
        checkIndexBounds(index, SIZE_LONG);
        return _buffer.getLong(index + _offset);
    }

    @Override
    public int capacity()
    {
        return _capacity;
    }

    @Override
    public char getChar(final int index)
    {
        checkIndexBounds(index, SIZE_CHAR);
        return _buffer.getChar(index + _offset);
    }

    @Override
    public byte get()
    {
        checkUnderflow(SIZE_BYTE);

        byte value = get(_position);
        _position += SIZE_BYTE;
        return value;
    }

    @Override
    public byte get(final int index)
    {
        checkIndexBounds(index, SIZE_BYTE);
        return _buffer.get(index + _offset);
    }

    @Override
    public QpidByteBuffer get(final byte[] dst)
    {
        return get(dst, 0, dst.length);
    }

    @Override
    public void copyTo(final byte[] dst)
    {
        if (remaining() < dst.length)
        {
            throw new BufferUnderflowException();
        }

        ByteBuffer dup = getDuplicateForBulk();
        dup.get(dst);
    }

    @Override
    public QpidByteBuffer putChar(final char value)
    {
        checkOverflow(SIZE_CHAR);

        putChar(position(), value);
        _position += SIZE_CHAR;
        return this;
    }

    @Override
    public QpidByteBuffer position(final int newPosition)
    {
        if (newPosition > _limit || newPosition < 0)
        {
            throw new IllegalArgumentException();
        }
        _position = newPosition;
        if (_mark > _position)
        {
            _mark = -1;
        }
        return this;
    }

    @Override
    public int arrayOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar()
    {
        checkUnderflow(SIZE_CHAR);

        char value = getChar(_position);
        _position += SIZE_CHAR;
        return value;
    }

    @Override
    public int getInt()
    {
        checkUnderflow(SIZE_INT);

        int value = getInt(_position);
        _position += SIZE_INT;
        return value;
    }

    @Override
    public QpidByteBuffer putLong(final long value)
    {
        checkOverflow(SIZE_LONG);

        putLong(position(), value);
        _position += SIZE_LONG;
        return this;
    }

    @Override
    public float getFloat(final int index)
    {
        checkIndexBounds(index, SIZE_FLOAT);
        return _buffer.getFloat(index + _offset);
    }

    @Override
    public QpidByteBuffer slice()
    {
        return new SlicedQpidByteBuffer(0, remaining(), remaining(), _offset + _position, _ref);
    }


    @Override
    public QpidByteBuffer view(final int offset, final int length)
    {
        int newCapacity = Math.min(length, remaining() - offset);
        return new SlicedQpidByteBuffer(0, newCapacity, newCapacity, _offset + _position + offset, _ref);
    }

    @Override
    public int position()
    {
        return _position;
    }

    @Override
    public String toString()
    {
        return "SlicedQpidByteBuffer{" +
               "_capacity=" + _capacity +
               ", _offset=" + _offset +
               ", _mark=" + _mark +
               ", _position=" + _position +
               ", _limit=" + _limit +
               '}';
    }

    @Override
    ByteBuffer getUnderlyingBuffer()
    {
        ByteBuffer buffer = _buffer.duplicate();
        buffer.position(_offset);
        buffer.limit(_offset + _capacity);

        buffer = buffer.slice();
        buffer.position(_position);
        buffer.limit(_limit);
        _lastUnderlyingBuffer = buffer;
        return buffer;
    }

    @Override
    void updateFromLastUnderlying()
    {
        if (_lastUnderlyingBuffer == null)
        {
            throw new IllegalStateException("No last underlying ByteBuffer recorded for " + this);
        }
        _position = _lastUnderlyingBuffer.position();
        _limit = _lastUnderlyingBuffer.limit();
        _lastUnderlyingBuffer = null;
    }

    void clearLastUnderlyingBuffer()
    {
        _lastUnderlyingBuffer = null;
    }

    private ByteBuffer getDuplicateForBulk()
    {
        ByteBuffer dup = _buffer.duplicate();
        dup.position(_offset + _position);
        dup.limit(_offset + _limit);
        return dup;
    }

    private void checkBounds(final byte[] array, final int offset, final int length)
    {
        if (offset < 0 || (offset > 0 && offset > array.length - 1) || length < 0 || length > array.length)
        {
            throw new IndexOutOfBoundsException();
        }
    }

    private void checkIndexBounds(int index, int size)
    {
        if (index < 0 || size > _limit - index)
        {
            throw new IndexOutOfBoundsException();
        }
    }

    private void checkOverflow(final int size)
    {
        if (_limit - _position < size)
        {
            throw new BufferOverflowException();
        }
    }

    private void checkUnderflow(final int size)
    {
        if (_limit - _position < size)
        {
            throw new BufferUnderflowException();
        }
    }

}
