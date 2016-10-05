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
    public byte[] array()
    {
        return _buffer.array();
    }

    @Override
    public int getInt(final int index)
    {
        return _buffer.getInt(index);
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
    public byte get(final int index)
    {
        return _buffer.get(index);
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

}
