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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.codec.MarkableDataInput;
import org.apache.qpid.framing.AMQShortString;

public final class QpidByteBuffer
{
    private static final AtomicIntegerFieldUpdater<QpidByteBuffer> DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(QpidByteBuffer.class, "_disposed");

    private final ByteBuffer _buffer;
    private final ByteBufferRef _ref;
    private volatile int _disposed;

    QpidByteBuffer(ByteBufferRef ref)
    {
        this(ref.getBuffer(), ref);
    }

    private QpidByteBuffer(ByteBuffer buf, ByteBufferRef ref)
    {
        _buffer = buf;
        _ref = ref;
        ref.incrementRef();
    }


    public boolean hasRemaining()
    {
        return _buffer.hasRemaining();
    }

    public QpidByteBuffer putInt(final int index, final int value)
    {
        _buffer.putInt(index, value);
        return this;
    }

    public boolean isDirect()
    {
        return _buffer.isDirect();
    }

    public QpidByteBuffer putShort(final int index, final short value)
    {
        _buffer.putShort(index, value);
        return this;
    }

    public QpidByteBuffer putChar(final int index, final char value)
    {
        _buffer.putChar(index, value);
        return this;

    }

    public QpidByteBuffer put(final byte b)
    {
        _buffer.put(b);
        return this;
    }

    public QpidByteBuffer put(final int index, final byte b)
    {
        _buffer.put(index, b);
        return this;
    }

    public short getShort(final int index)
    {
        return _buffer.getShort(index);
    }


    public QpidByteBuffer mark()
    {
        _buffer.mark();
        return this;
    }

    public long getLong()
    {
        return _buffer.getLong();
    }

    public QpidByteBuffer putFloat(final int index, final float value)
    {
        _buffer.putFloat(index, value);
        return this;
    }

    public double getDouble(final int index)
    {
        return _buffer.getDouble(index);
    }

    public boolean hasArray()
    {
        return _buffer.hasArray();
    }

    public QpidByteBuffer asReadOnlyBuffer()
    {
        return new QpidByteBuffer(_buffer.asReadOnlyBuffer(), _ref);
    }

    public double getDouble()
    {
        return _buffer.getDouble();
    }

    public QpidByteBuffer putFloat(final float value)
    {
        _buffer.putFloat(value);
        return this;
    }

    public QpidByteBuffer putInt(final int value)
    {
        _buffer.putInt(value);
        return this;
    }

    public byte[] array()
    {
        return _buffer.array();
    }

    public QpidByteBuffer putShort(final short value)
    {
        _buffer.putShort(value);
        return this;
    }

    public int getInt(final int index)
    {
        return _buffer.getInt(index);
    }

    public int remaining()
    {
        return _buffer.remaining();
    }

    public QpidByteBuffer put(final byte[] src)
    {
        _buffer.put(src);
        return this;
    }

    public QpidByteBuffer put(final ByteBuffer src)
    {
        _buffer.put(src);
        return this;
    }

    public QpidByteBuffer put(final QpidByteBuffer src)
    {
        _buffer.put(src._buffer);
        return this;
    }



    public QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        _buffer.get(dst, offset, length);
        return this;
    }

    public QpidByteBuffer get(final ByteBuffer src)
    {
        src.put(_buffer);
        return this;
    }


    public QpidByteBuffer rewind()
    {
        _buffer.rewind();
        return this;
    }

    public QpidByteBuffer clear()
    {
        _buffer.clear();
        return this;
    }

    public QpidByteBuffer putLong(final int index, final long value)
    {
        _buffer.putLong(index, value);
        return this;
    }
    public QpidByteBuffer compact()
    {
        _buffer.compact();
        return this;
    }

    public QpidByteBuffer putDouble(final double value)
    {
        _buffer.putDouble(value);
        return this;
    }

    public int limit()
    {
        return _buffer.limit();
    }

    public QpidByteBuffer reset()
    {
        _buffer.reset();
        return this;
    }

    public QpidByteBuffer flip()
    {
        _buffer.flip();
        return this;
    }

    public short getShort()
    {
        return _buffer.getShort();
    }

    public float getFloat()
    {
        return _buffer.getFloat();
    }

    public QpidByteBuffer limit(final int newLimit)
    {
        _buffer.limit(newLimit);
        return this;
    }

    public QpidByteBuffer duplicate()
    {
        return new QpidByteBuffer(_buffer.duplicate(), _ref);
    }

    public QpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        _buffer.put(src, offset, length);
        return this;
    }

    public long getLong(final int index)
    {
        return _buffer.getLong(index);
    }

    public int capacity()
    {
        return _buffer.capacity();
    }

    public boolean isReadOnly()
    {
        return _buffer.isReadOnly();
    }

    public char getChar(final int index)
    {
        return _buffer.getChar(index);
    }

    public byte get()
    {
        return _buffer.get();
    }

    public byte get(final int index)
    {
        return _buffer.get(index);
    }

    public QpidByteBuffer get(final byte[] dst)
    {
        _buffer.get(dst);
        return this;
    }

    public QpidByteBuffer putChar(final char value)
    {
        _buffer.putChar(value);
        return this;
    }

    public QpidByteBuffer position(final int newPosition)
    {
        _buffer.position(newPosition);
        return this;
    }

    public int arrayOffset()
    {
        return _buffer.arrayOffset();
    }

    public char getChar()
    {
        return _buffer.getChar();
    }

    public int getInt()
    {
        return _buffer.getInt();
    }

    public QpidByteBuffer putLong(final long value)
    {
        _buffer.putLong(value);
        return this;
    }

    public float getFloat(final int index)
    {
        return _buffer.getFloat(index);
    }

    public QpidByteBuffer slice()
    {
        return new QpidByteBuffer(_buffer.slice(), _ref);
    }

    public QpidByteBuffer view(int offset, int length)
    {
        ByteBuffer buf = _buffer.slice();
        buf.position(offset);
        buf = buf.slice();
        buf.limit(length);
        return new QpidByteBuffer(buf, _ref);
    }

    public int position()
    {
        return _buffer.position();
    }

    public QpidByteBuffer putDouble(final int index, final double value)
    {
        _buffer.putDouble(index, value);
        return this;
    }

    @Override
    protected void finalize() throws Throwable
    {
        dispose();
        super.finalize();
    }

    public void dispose()
    {
        if(DISPOSED_UPDATER.compareAndSet(this,0,1))
        {
            _ref.decrementRef();
        }
    }

    public InputStream asInputStream()
    {
        return new BufferInputStream();
    }

    public MarkableDataInput asDataInput()
    {
        return new BufferDataInput();
    }


    public DataOutput asDataOutput()
    {
        return new BufferDataOutput();
    }

    public static QpidByteBuffer allocate(int size)
    {
        return new QpidByteBuffer(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
    }


    public static QpidByteBuffer allocateDirect(int size)
    {
        return new QpidByteBuffer(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
    }


    public ByteBuffer getNativeBuffer()
    {
        return _buffer;
    }

    public CharBuffer decode(Charset charset)
    {
        return charset.decode(_buffer);
    }

    public static QpidByteBuffer wrap(final ByteBuffer wrap)
    {
        return new QpidByteBuffer(new NonPooledByteBufferRef(wrap));
    }

    public static QpidByteBuffer wrap(final byte[] data)
    {
        return wrap(ByteBuffer.wrap(data));
    }

    public static QpidByteBuffer wrap(final byte[] data, int offset, int length)
    {
        return wrap(ByteBuffer.wrap(data, offset, length));
    }

    private final class BufferInputStream extends InputStream
    {

        @Override
        public int read() throws IOException
        {
            if (_buffer.hasRemaining())
            {
                return _buffer.get() & 0xFF;
            }
            return -1;
        }


        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            if (!_buffer.hasRemaining())
            {
                return -1;
            }
            if(_buffer.remaining() < len)
            {
                len = _buffer.remaining();
            }
            _buffer.get(b, off, len);

            return len;
        }

        @Override
        public void mark(int readlimit)
        {
            _buffer.mark();
        }

        @Override
        public void reset() throws IOException
        {
            _buffer.reset();
        }

        @Override
        public boolean markSupported()
        {
            return true;
        }

        @Override
        public long skip(long n) throws IOException
        {
            _buffer.position(_buffer.position()+(int)n);
            return n;
        }

        @Override
        public int available() throws IOException
        {
            return _buffer.remaining();
        }

        @Override
        public void close()
        {
        }
    }

    private final class BufferDataInput implements MarkableDataInput
    {
        private int _mark;
        private final int _offset;

        public BufferDataInput()
        {
            _offset = _buffer.position();
        }

        public void readFully(byte[] b)
        {
            _buffer.get(b);
        }

        public void readFully(byte[] b, int off, int len)
        {
            _buffer.get(b, 0, len);
        }

        public QpidByteBuffer readAsByteBuffer(int len)
        {
            final QpidByteBuffer view = view(0, len);
            skipBytes(len);
            return view;
        }

        public int skipBytes(int n)
        {
            _buffer.position(_buffer.position()+n);
            return _buffer.position()-_offset;
        }

        public boolean readBoolean()
        {
            return _buffer.get() != 0;
        }

        public byte readByte()
        {
            return _buffer.get();
        }

        public int readUnsignedByte()
        {
            return ((int) _buffer.get()) & 0xFF;
        }

        public short readShort()
        {
            return _buffer.getShort();
        }

        public int readUnsignedShort()
        {
            return ((int) _buffer.getShort()) & 0xffff;
        }

        public char readChar()
        {
            return (char) _buffer.getChar();
        }

        public int readInt()
        {
            return _buffer.getInt();
        }

        public long readLong()
        {
            return _buffer.getLong();
        }

        public float readFloat()
        {
            return _buffer.getFloat();
        }

        public double readDouble()
        {
            return _buffer.getDouble();
        }

        public AMQShortString readAMQShortString()
        {
            return AMQShortString.readAMQShortString(_buffer);
        }

        public String readLine()
        {
            throw new UnsupportedOperationException();
        }

        public String readUTF()
        {
            throw new UnsupportedOperationException();
        }

        public int available()
        {
            return _buffer.remaining();
        }


        public long skip(long i)
        {
            _buffer.position(_buffer.position()+(int)i);
            return i;
        }

        public int read(byte[] b)
        {
            readFully(b);
            return b.length;
        }

        public int position()
        {
            return _buffer.position()-_offset;
        }

        public void position(int position)
        {
            _buffer.position(position + _offset);
        }

        public int length()
        {
            return _buffer.limit();
        }


        public void mark(int readAhead)
        {
            _mark = position();
        }

        public void reset()
        {
            _buffer.position(_mark);
        }
    }

    private final class BufferDataOutput implements DataOutput
    {
        public void write(int b)
        {
            _buffer.put((byte) b);
        }

        public void write(byte[] b)
        {
            _buffer.put(b);
        }


        public void write(byte[] b, int off, int len)
        {
            _buffer.put(b, off, len);

        }

        public void writeBoolean(boolean v)
        {
            _buffer.put(v ? (byte) 1 : (byte) 0);
        }

        public void writeByte(int v)
        {
            _buffer.put((byte) v);
        }

        public void writeShort(int v)
        {
            _buffer.putShort((short) v);
        }

        public void writeChar(int v)
        {
            _buffer.put((byte) (v >>> 8));
            _buffer.put((byte) v);
        }

        public void writeInt(int v)
        {
            _buffer.putInt(v);
        }

        public void writeLong(long v)
        {
            _buffer.putLong(v);
        }

        public void writeFloat(float v)
        {
            writeInt(Float.floatToIntBits(v));
        }

        public void writeDouble(double v)
        {
            writeLong(Double.doubleToLongBits(v));
        }

        public void writeBytes(String s)
        {
            throw new UnsupportedOperationException("writeBytes(String s) not supported");
        }

        public void writeChars(String s)
        {
            int len = s.length();
            for (int i = 0 ; i < len ; i++)
            {
                int v = s.charAt(i);
                _buffer.put((byte) (v >>> 8));
                _buffer.put((byte) v);
            }
        }

        public void writeUTF(String s)
        {
            int strlen = s.length();

            int pos = _buffer.position();
            _buffer.position(pos + 2);


            for (int i = 0; i < strlen; i++)
            {
                int c = s.charAt(i);
                if ((c >= 0x0001) && (c <= 0x007F))
                {
                    c = s.charAt(i);
                    _buffer.put((byte) c);

                }
                else if (c > 0x07FF)
                {
                    _buffer.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
                    _buffer.put((byte) (0x80 | ((c >> 6) & 0x3F)));
                    _buffer.put((byte) (0x80 | (c & 0x3F)));
                }
                else
                {
                    _buffer.put((byte) (0xC0 | ((c >> 6) & 0x1F)));
                    _buffer.put((byte) (0x80 | (c & 0x3F)));
                }
            }

            int len = _buffer.position() - (pos + 2);

            _buffer.put(pos++, (byte) (len >>> 8));
            _buffer.put(pos, (byte) len);
        }

    }

}
