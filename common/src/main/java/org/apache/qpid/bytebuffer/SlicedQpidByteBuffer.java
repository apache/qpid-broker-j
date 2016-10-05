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

import java.nio.ByteBuffer;

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

    SlicedQpidByteBuffer(final int position,
                         final int limit,
                         final int capacity,
                         final int offset,
                         final ByteBufferRef ref)
    {
        super(ref, ref instanceof PooledByteBufferRef ? ref.getBuffer() : ref.getBuffer().duplicate());

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

        if (offset >= _buffer.capacity())
        {
            throw new IllegalArgumentException("Offset exceeds capacity");
        }

        _capacity = capacity;
        _offset = offset;
        _ref.incrementRef();

        _buffer.limit(offset + limit);
        _buffer.position(offset + position);
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
    public long getLong(final int index)
    {
        checkIndexBounds(index, SIZE_LONG);
        return _buffer.getLong(index + _offset);
    }

    @Override
    public int getInt(final int index)
    {
        checkIndexBounds(index, SIZE_INT);
        return _buffer.getInt(index + _offset);
    }

    @Override
    public QpidByteBuffer putLong(final int index, final long value)
    {
        checkIndexBounds(index, SIZE_LONG);
        _buffer.putLong(_offset + index, value);
        return this;
    }

    @Override
    public char getChar(final int index)
    {
        checkIndexBounds(index, SIZE_CHAR);
        return _buffer.getChar(index + _offset);
    }

    @Override
    public byte get(final int index)
    {
        checkIndexBounds(index, SIZE_BYTE);
        return _buffer.get(index + _offset);
    }

    @Override
    public float getFloat(final int index)
    {
        checkIndexBounds(index, SIZE_FLOAT);
        return _buffer.getFloat(index + _offset);
    }

    @Override
    public byte[] array()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QpidByteBuffer rewind()
    {
        _buffer.position(_offset);
        return this;
    }

    @Override
    public QpidByteBuffer clear()
    {
        _buffer.position(_offset);
        _buffer.limit(_offset + _capacity);
        return this;
    }

    @Override
    public QpidByteBuffer compact()
    {
        int remaining = remaining();
        if (_buffer.position() > _offset)
        {
            ByteBuffer buffer = _buffer.duplicate();
            buffer.position(_offset);
            buffer.limit(_offset + _capacity);

            buffer = buffer.slice();
            buffer.position(position());
            buffer.limit(limit());

            buffer.compact();
        }

        _buffer.limit(_offset + _capacity);
        _buffer.position(_offset + remaining);
        return this;
    }

    @Override
    public int limit()
    {
        return _buffer.limit() - _offset;
    }

    @Override
    public QpidByteBuffer reset()
    {
        _buffer.reset();
        return this;
    }

    @Override
    public QpidByteBuffer flip()
    {
        final int position = _buffer.position();
        _buffer.position(_offset);
        _buffer.limit(position);
        return this;
    }

    @Override
    public QpidByteBuffer limit(final int newLimit)
    {
        if (newLimit > _capacity || newLimit < 0)
        {
            throw new IllegalArgumentException();
        }
        _buffer.limit(_offset + newLimit);
        return this;
    }

    @Override
    public QpidByteBuffer duplicate()
    {
        return new SlicedQpidByteBuffer(position(), limit(), _capacity, _offset, _ref);
    }


    @Override
    public int capacity()
    {
        return _capacity;
    }

    @Override
    public QpidByteBuffer position(final int newPosition)
    {
        if (newPosition > limit() || newPosition < 0)
        {
            throw new IllegalArgumentException();
        }
        _buffer.position(_offset + newPosition);
        return this;
    }

    @Override
    public int arrayOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QpidByteBuffer slice()
    {
        return new SlicedQpidByteBuffer(0, remaining(), remaining(), _buffer.position(), _ref);
    }


    @Override
    public QpidByteBuffer view(final int offset, final int length)
    {
        int newCapacity = Math.min(length, remaining() - offset);
        return new SlicedQpidByteBuffer(0, newCapacity, newCapacity, _buffer.position() + offset, _ref);
    }

    @Override
    public int position()
    {
        return _buffer.position() - _offset;
    }

    @Override
    public String toString()
    {
        return "SlicedQpidByteBuffer{" +
               "_capacity=" + _capacity +
               ", _offset=" + _offset +
               ", _buffer=" + _buffer +
               '}';
    }

    @Override
    ByteBuffer getUnderlyingBuffer()
    {
        return _buffer;
    }

    private void checkIndexBounds(int index, int size)
    {
        if (index < 0 || size > limit() - index)
        {
            throw new IndexOutOfBoundsException();
        }
    }
}
