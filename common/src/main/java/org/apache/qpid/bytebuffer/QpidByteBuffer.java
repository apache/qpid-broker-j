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

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.codec.MarkableDataInput;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.streams.CompositeInputStream;

public final class QpidByteBuffer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidByteBuffer.class);

    private static final AtomicIntegerFieldUpdater<QpidByteBuffer> DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
            QpidByteBuffer.class,
            "_disposed");

    private static final ThreadLocal<QpidByteBuffer> _cachedBuffer = new ThreadLocal<>();
    private volatile ByteBuffer _buffer;
    private final ByteBufferRef _ref;

    @SuppressWarnings("unused")
    private volatile int _disposed;

    private volatile static boolean _isPoolInitialized;
    private volatile static BufferPool _bufferPool;
    private volatile static int _pooledBufferSize;
    private volatile static ByteBuffer _zeroed;

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

    public QpidByteBuffer get(final ByteBuffer dst)
    {
        dst.put(_buffer);
        return this;
    }

    public void copyTo(final ByteBuffer dst)
    {
        dst.put(_buffer.duplicate());
    }

    public void putCopyOf(final QpidByteBuffer buf)
    {
        _buffer.put(buf._buffer.duplicate());
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


    public void copyTo(final byte[] dst)
    {
        _buffer.duplicate().get(dst);
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
        buf.limit(offset+Math.min(length, buf.remaining()));
        buf = buf.slice();

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

    public void dispose()
    {
        if(DISPOSED_UPDATER.compareAndSet(this,0,1))
        {
            _ref.decrementRef();
            _buffer = null;
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
        if (size < 0)
        {
            throw new IllegalArgumentException("Cannot allocate QpidByteBuffer with size " + size + " which is negative.");
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
                QpidByteBuffer buf = _cachedBuffer.get();
                if (buf == null || buf.remaining() < size)
                {
                    if (buf != null)
                    {
                        buf.dispose();
                    }
                    buf = allocateDirect(_pooledBufferSize);
                }
                QpidByteBuffer rVal = buf.view(0, size);
                buf.position(buf.position() + size);

                _cachedBuffer.set(buf);
                return rVal;
            }
        }
        else
        {
            ref = new NonPooledByteBufferRef(ByteBuffer.allocateDirect(size));
        }
        return new QpidByteBuffer(ref);
    }

    public static Collection<QpidByteBuffer> allocateDirectCollection(int size)
    {
        if(_pooledBufferSize == 0)
        {
            return Collections.singleton(allocateDirect(size));
        }
        else
        {
            List<QpidByteBuffer> buffers = new ArrayList<>((size / _pooledBufferSize)+2);
            int remaining = size;

            QpidByteBuffer buf = _cachedBuffer.get();
            if(buf == null)
            {
                buf = allocateDirect(_pooledBufferSize);
            }
            while(remaining > buf.remaining())
            {
                int bufRemaining = buf.remaining();
                if (buf  == _cachedBuffer.get())
                {
                    buffers.add(buf.view(0, bufRemaining));
                    buf.dispose();
                }
                else
                {
                    buffers.add(buf);
                }
                remaining -= bufRemaining;
                buf = allocateDirect(_pooledBufferSize);
            }
            buffers.add(buf.view(0, remaining));
            buf.position(buf.position() + remaining);

            if (buf.hasRemaining())
            {
                _cachedBuffer.set(buf);
            }
            else
            {
                _cachedBuffer.set(allocateDirect(_pooledBufferSize));
                buf.dispose();
            }
            return buffers;
        }
    }

    public ByteBuffer asByteBuffer()
    {
        _ref.removeFromPool();
        return _buffer;
    }

    public CharBuffer decode(Charset charset)
    {
        return charset.decode(_buffer);
    }

    public int read(ReadableByteChannel channel) throws IOException
    {
        return channel.read(_buffer);
    }


    public SSLEngineResult decryptSSL(SSLEngine engine, QpidByteBuffer dest) throws SSLException
    {
        return engine.unwrap(_buffer, dest._buffer);
    }


    public static SSLEngineResult encryptSSL(SSLEngine engine,
                                             final Collection<QpidByteBuffer> buffers,
                                             QpidByteBuffer dest) throws SSLException
    {
        final ByteBuffer[] src = new ByteBuffer[buffers.size()];
        Iterator<QpidByteBuffer> iter = buffers.iterator();
        for(int i = 0; i<src.length; i++)
        {
            src[i] = iter.next()._buffer;
        }
        return engine.wrap(src, dest._buffer);
    }

    public static Collection<QpidByteBuffer> inflate(Collection<QpidByteBuffer> compressedBuffers) throws IOException
    {
        if (compressedBuffers == null)
        {
            throw new IllegalArgumentException("compressedBuffers cannot be null");
        }

        boolean isDirect = false;
        Collection<InputStream> streams = new ArrayList<>(compressedBuffers.size());
        for (QpidByteBuffer buffer : compressedBuffers)
        {
            isDirect = isDirect || buffer.isDirect();
            streams.add(buffer.asInputStream());
        }
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        Collection<QpidByteBuffer> uncompressedBuffers = new ArrayList<>();
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new CompositeInputStream(streams)))
        {
            byte[] buf = new byte[bufferSize];
            int read;
            while ((read = gzipInputStream.read(buf)) != -1)
            {
                QpidByteBuffer output = isDirect ? allocateDirect(read) : allocate(read);
                output.put(buf, 0, read);
                output.flip();
                uncompressedBuffers.add(output);
            }
            return uncompressedBuffers;
        }
    }

    public static Collection<QpidByteBuffer> deflate(Collection<QpidByteBuffer> uncompressedBuffers) throws IOException
    {
        if (uncompressedBuffers == null)
        {
            throw new IllegalArgumentException("uncompressedBuffers cannot be null");
        }

        boolean isDirect = false;
        Collection<InputStream> streams = new ArrayList<>(uncompressedBuffers.size());
        for (QpidByteBuffer buffer : uncompressedBuffers)
        {
            isDirect = isDirect || buffer.isDirect();
            streams.add(buffer.asInputStream());
        }
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        QpidByteBufferOutputStream compressedOutput = new QpidByteBufferOutputStream(isDirect, bufferSize);

        try(InputStream compressedInput = new CompositeInputStream(streams);
            GZIPOutputStream gzipStream = new GZIPOutputStream(new BufferedOutputStream(compressedOutput, bufferSize)))
        {
            byte[] buf = new byte[16384];
            int read;
            while ((read = compressedInput.read(buf)) > -1)
            {
                gzipStream.write(buf, 0, read);
            }
        }

        // output pipeline will be already flushed and closed

        return compressedOutput.fetchAccumulatedBuffers();
    }

    public static long write(GatheringByteChannel channel, Collection<QpidByteBuffer> buffers) throws IOException
    {
        ByteBuffer[] bufs = new ByteBuffer[buffers.size()];
        Iterator<QpidByteBuffer> bufIter = buffers.iterator();
        for(int i = 0; i < bufs.length; i++)
        {
            bufs[i] = bufIter.next()._buffer;
        }
        return channel.write(bufs);
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

    static void returnToPool(final ByteBuffer buffer)
    {
        buffer.clear();
        final ByteBuffer duplicate = _zeroed.duplicate();
        duplicate.limit(buffer.capacity());
        buffer.put(duplicate);

        _bufferPool.returnBuffer(buffer);
    }

    public synchronized static void initialisePool(int bufferSize, int maxPoolSize)
    {
        if (_isPoolInitialized && (bufferSize != _pooledBufferSize || maxPoolSize != _bufferPool.getMaxSize()))
        {
            final String errorMessage = String.format("QpidByteBuffer pool has already been initialised with bufferSize=%d and maxPoolSize=%d." +
                            "Re-initialisation with different bufferSize=%d and maxPoolSize=%d is not allowed.",
                            _pooledBufferSize, _bufferPool.getMaxSize(), bufferSize, maxPoolSize);
            throw new IllegalStateException(errorMessage);
        }
        if (bufferSize <= 0)
        {
            throw new IllegalArgumentException("Negative or zero bufferSize illegal : " + bufferSize);
        }


        _bufferPool = new BufferPool(maxPoolSize);
        _pooledBufferSize = bufferSize;
        _zeroed = ByteBuffer.allocateDirect(_pooledBufferSize);
        _isPoolInitialized = true;
    }

    public static int getPooledBufferSize()
    {
        return _pooledBufferSize;
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
            position(_mark);
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
