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
package org.apache.qpid.bytebuffer;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

final class QpidByteBufferImpl extends QpidByteBuffer
{

    QpidByteBufferImpl(ByteBufferRef ref)
    {
        this(ref.getBuffer(), ref);
    }

    private QpidByteBufferImpl(ByteBuffer buf, ByteBufferRef ref)
    {
        super(ref, buf);
        ref.incrementRef();
    }

    @Override
    public boolean hasRemaining()
    {
        return _buffer.hasRemaining();
    }

    @Override
    public QpidByteBuffer putInt(final int index, final int value)
    {
        _buffer.putInt(index, value);
        return this;
    }

    @Override
    public QpidByteBuffer putShort(final int index, final short value)
    {
        _buffer.putShort(index, value);
        return this;
    }

    @Override
    public QpidByteBuffer putChar(final int index, final char value)
    {
        _buffer.putChar(index, value);
        return this;
    }

    @Override
    public QpidByteBuffer put(final byte b)
    {
        _buffer.put(b);
        return this;
    }

    @Override
    public QpidByteBuffer put(final int index, final byte b)
    {
        _buffer.put(index, b);
        return this;
    }

    @Override
    public short getShort(final int index)
    {
        return _buffer.getShort(index);
    }


    @Override
    public QpidByteBuffer mark()
    {
        _buffer.mark();
        return this;
    }

    @Override
    public long getLong()
    {
        return _buffer.getLong();
    }

    @Override
    public QpidByteBuffer putFloat(final int index, final float value)
    {
        _buffer.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble(final int index)
    {
        return _buffer.getDouble(index);
    }

    @Override
    public boolean hasArray()
    {
        return _buffer.hasArray();
    }

    @Override
    public double getDouble()
    {
        return _buffer.getDouble();
    }

    @Override
    public QpidByteBuffer putFloat(final float value)
    {
        _buffer.putFloat(value);
        return this;
    }

    @Override
    public QpidByteBuffer putInt(final int value)
    {
        _buffer.putInt(value);
        return this;
    }

    @Override
    public byte[] array()
    {
        return _buffer.array();
    }

    @Override
    public QpidByteBuffer putShort(final short value)
    {
        _buffer.putShort(value);
        return this;
    }

    @Override
    public int getInt(final int index)
    {
        return _buffer.getInt(index);
    }

    @Override
    public int remaining()
    {
        return _buffer.remaining();
    }

    @Override
    public QpidByteBuffer put(final byte[] src)
    {
        _buffer.put(src);
        return this;
    }

    @Override
    public QpidByteBuffer put(final ByteBuffer src)
    {
        _buffer.put(src);
        return this;
    }

    @Override
    public QpidByteBuffer put(final QpidByteBuffer src)
    {
        ByteBuffer underlyingBuffer = src.getUnderlyingBuffer();
        _buffer.put(underlyingBuffer);
        src.updateFromLastUnderlying();
        return this;
    }

    @Override
    public QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        _buffer.get(dst, offset, length);
        return this;
    }

    @Override
    public QpidByteBuffer get(final ByteBuffer dst)
    {
        int destinationRemaining = dst.remaining();
        int remaining = remaining();
        if (destinationRemaining < remaining)
        {
            throw new BufferUnderflowException();
        }
        dst.put(_buffer);
        return this;
    }

    @Override
    public void copyTo(final ByteBuffer dst)
    {
        dst.put(_buffer.duplicate());
    }

    @Override
    public void putCopyOf(final QpidByteBuffer buf)
    {
        _buffer.put(buf.getUnderlyingBuffer().duplicate());
        if (buf instanceof SlicedQpidByteBuffer)
        {
            ((SlicedQpidByteBuffer)buf).clearLastUnderlyingBuffer();
        }
    }

    @Override
    public QpidByteBuffer rewind()
    {
        _buffer.rewind();
        return this;
    }

    @Override
    public QpidByteBuffer clear()
    {
        _buffer.clear();
        return this;
    }

    @Override
    public QpidByteBuffer putLong(final int index, final long value)
    {
        _buffer.putLong(index, value);
        return this;
    }

    @Override
    public QpidByteBuffer compact()
    {
        _buffer.compact();
        return this;
    }

    @Override
    public QpidByteBuffer putDouble(final double value)
    {
        _buffer.putDouble(value);
        return this;
    }

    @Override
    public int limit()
    {
        return _buffer.limit();
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
        _buffer.flip();
        return this;
    }

    @Override
    public short getShort()
    {
        return _buffer.getShort();
    }

    @Override
    public float getFloat()
    {
        return _buffer.getFloat();
    }

    @Override
    public QpidByteBuffer limit(final int newLimit)
    {
        _buffer.limit(newLimit);
        return this;
    }

    @Override
    public QpidByteBufferImpl duplicate()
    {
        return new QpidByteBufferImpl(_buffer.duplicate(), _ref);
    }

    @Override
    public QpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        _buffer.put(src, offset, length);
        return this;
    }

    @Override
    public long getLong(final int index)
    {
        return _buffer.getLong(index);
    }

    @Override
    public int capacity()
    {
        return _buffer.capacity();
    }

    @Override
    public char getChar(final int index)
    {
        return _buffer.getChar(index);
    }

    @Override
    public byte get()
    {
        return _buffer.get();
    }

    @Override
    public byte get(final int index)
    {
        return _buffer.get(index);
    }

    @Override
    public QpidByteBuffer get(final byte[] dst)
    {
        _buffer.get(dst);
        return this;
    }

    @Override
    public void copyTo(final byte[] dst)
    {
        _buffer.duplicate().get(dst);
    }

    @Override
    public QpidByteBuffer putChar(final char value)
    {
        _buffer.putChar(value);
        return this;
    }

    @Override
    public QpidByteBuffer position(final int newPosition)
    {
        _buffer.position(newPosition);
        return this;
    }

    @Override
    public int arrayOffset()
    {
        return _buffer.arrayOffset();
    }

    @Override
    public char getChar()
    {
        return _buffer.getChar();
    }

    @Override
    public int getInt()
    {
        return _buffer.getInt();
    }

    @Override
    public QpidByteBuffer putLong(final long value)
    {
        _buffer.putLong(value);
        return this;
    }

    @Override
    public float getFloat(final int index)
    {
        return _buffer.getFloat(index);
    }


    @Override
    public QpidByteBuffer slice()
    {
        if (isDirect())
        {
            return new SlicedQpidByteBuffer(0, remaining(), remaining(), position(), _ref);
        }
        else
        {
            return new QpidByteBufferImpl(_buffer.slice(), _ref);
        }
    }

    @Override
    public QpidByteBuffer view(int offset, int length)
    {
        if (isDirect())
        {
            int capacity = Math.min(_buffer.remaining() - offset, length);
            return new SlicedQpidByteBuffer(0, capacity, capacity, offset + position(), _ref);
        }
        else
        {
            ByteBuffer buf = _buffer.slice();
            buf.position(offset);
            buf.limit(offset + Math.min(length, buf.remaining()));
            buf = buf.slice();
            return new QpidByteBufferImpl(buf, _ref);
        }
    }

    @Override
    public int position()
    {
        return _buffer.position();
    }

    @Override
    public QpidByteBuffer putDouble(final int index, final double value)
    {
        _buffer.putDouble(index, value);
        return this;
    }

    ByteBuffer getUnderlyingBuffer()
    {
        return _buffer;
    }

    @Override
    void updateFromLastUnderlying()
    {
        // noop
    }
}
