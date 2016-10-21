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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
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

import org.apache.qpid.streams.CompositeInputStream;

public class QpidByteBuffer
{
    private static final AtomicIntegerFieldUpdater<QpidByteBuffer>
            DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
            QpidByteBuffer.class,
            "_disposed");
    private static final ThreadLocal<QpidByteBuffer> _cachedBuffer = new ThreadLocal<>();
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];
    private volatile static boolean _isPoolInitialized;
    private volatile static BufferPool _bufferPool;
    private volatile static int _pooledBufferSize;
    private volatile static ByteBuffer _zeroed;
    private final int _offset;

    final ByteBufferRef _ref;
    volatile ByteBuffer _buffer;
    @SuppressWarnings("unused")
    private volatile int _disposed;


    QpidByteBuffer(ByteBufferRef ref)
    {
        this(ref, ref.getBuffer(), 0);
    }

    private QpidByteBuffer(ByteBufferRef ref, ByteBuffer buffer, int offset)
    {
        _ref = ref;
        _buffer = buffer;
        _offset = offset;
        _ref.incrementRef();
    }

    public final boolean isDirect()
    {
        return _buffer.isDirect();
    }

    public final short getUnsignedByte()
    {
        return (short) (((short) get()) & 0xFF);
    }

    public final int getUnsignedShort()
    {
        return ((int) getShort()) & 0xffff;
    }

    public final long getUnsignedInt()
    {
        return ((long) getInt()) & 0xffffffffL;
    }

    public final QpidByteBuffer putUnsignedByte(final short s)
    {
        put((byte) s);
        return this;
    }

    public final QpidByteBuffer putUnsignedShort(final int i)
    {
        putShort((short) i);
        return this;
    }

    public final QpidByteBuffer putUnsignedInt(final long value)
    {
        putInt((int) value);
        return this;
    }

    public final void dispose()
    {
        if (DISPOSED_UPDATER.compareAndSet(this, 0, 1))
        {
            _ref.decrementRef();
            _buffer = null;
        }
    }

    public final InputStream asInputStream()
    {
        return new BufferInputStream(this);
    }

    public final ByteBuffer asByteBuffer()
    {
        try
        {
            return getUnderlyingBuffer();
        }
        finally
        {
            dispose();
        }
    }

    public final CharBuffer decode(Charset charset)
    {
        return charset.decode(getUnderlyingBuffer());
    }

    public final int read(ReadableByteChannel channel) throws IOException
    {
        return channel.read(getUnderlyingBuffer());
    }

    public final SSLEngineResult decryptSSL(SSLEngine engine, QpidByteBuffer dest) throws SSLException
    {
        return engine.unwrap(getUnderlyingBuffer(), dest.getUnderlyingBuffer());
    }

    @Override
    public String toString()
    {
        return "QpidByteBuffer{" +
               "_buffer=" + _buffer +
               ", _disposed=" + _disposed +
               '}';
    }

    public final boolean hasRemaining()
    {
        return _buffer.hasRemaining();
    }

    public QpidByteBuffer putInt(final int index, final int value)
    {
        _buffer.putInt(index, value);
        return this;
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

    public final QpidByteBuffer put(final byte b)
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

    public final QpidByteBuffer mark()
    {
        _buffer.mark();
        return this;
    }

    public final long getLong()
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

    public final boolean hasArray()
    {
        return _buffer.hasArray();
    }

    public final double getDouble()
    {
        return _buffer.getDouble();
    }

    public final QpidByteBuffer putFloat(final float value)
    {
        _buffer.putFloat(value);
        return this;
    }

    public final QpidByteBuffer putInt(final int value)
    {
        _buffer.putInt(value);
        return this;
    }

    public byte[] array()
    {
        return _buffer.array();
    }

    public final QpidByteBuffer putShort(final short value)
    {
        _buffer.putShort(value);
        return this;
    }

    public int getInt(final int index)
    {
        return _buffer.getInt(index);
    }

    public final int remaining()
    {
        return _buffer.remaining();
    }

    public final QpidByteBuffer put(final byte[] src)
    {
        _buffer.put(src);
        return this;
    }

    public final QpidByteBuffer put(final ByteBuffer src)
    {
        _buffer.put(src);
        return this;
    }

    public final QpidByteBuffer put(final QpidByteBuffer src)
    {
        int sourceRemaining = src.remaining();
        if (sourceRemaining > remaining())
        {
            throw new BufferOverflowException();
        }

        _buffer.put(src.getUnderlyingBuffer());
        return this;
    }

    public final QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        _buffer.get(dst, offset, length);
        return this;
    }

    public final QpidByteBuffer get(final ByteBuffer dst)
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

    public final void copyTo(final ByteBuffer dst)
    {
        dst.put(_buffer.duplicate());
    }

    public final void putCopyOf(final QpidByteBuffer source)
    {
        int remaining = remaining();
        int sourceRemaining = source.remaining();
        if (sourceRemaining > remaining)
        {
            throw new BufferOverflowException();
        }

        put(source.getUnderlyingBuffer().duplicate());
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

    public final QpidByteBuffer putDouble(final double value)
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

    public final short getShort()
    {
        return _buffer.getShort();

    }

    public final float getFloat()
    {
        return _buffer.getFloat();
    }

    public QpidByteBuffer limit(final int newLimit)
    {
        _buffer.limit(newLimit);
        return this;
    }

    /**
     * Method does not respect mark.
     *
     * @return QpidByteBuffer
     */
    public QpidByteBuffer duplicate()
    {
        ByteBuffer buffer = _ref.getBuffer();
        if (!(_ref instanceof PooledByteBufferRef))
        {
            buffer = buffer.duplicate();
        }

        buffer.position(_offset );
        buffer.limit(_offset + _buffer.capacity());

        buffer = buffer.slice();

        buffer.limit(_buffer.limit());
        buffer.position(_buffer.position());
        return new QpidByteBuffer(_ref, buffer, _offset);
    }

    public final QpidByteBuffer put(final byte[] src, final int offset, final int length)
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

    public char getChar(final int index)
    {
        return _buffer.getChar(index);
    }

    public final byte get()
    {
        return _buffer.get();
    }

    public byte get(final int index)
    {
        return _buffer.get(index);
    }

    public final QpidByteBuffer get(final byte[] dst)
    {
        _buffer.get(dst);
        return this;
    }

    public final void copyTo(final byte[] dst)
    {
        if (remaining() < dst.length)
        {
            throw new BufferUnderflowException();
        }
        _buffer.duplicate().get(dst);
    }

    public final QpidByteBuffer putChar(final char value)
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

    public final char getChar()
    {
        return _buffer.getChar();
    }

    public final int getInt()
    {
        return _buffer.getInt();
    }

    public final QpidByteBuffer putLong(final long value)
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
        return view(0, _buffer.remaining());
    }

    public QpidByteBuffer view(int offset, int length)
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

        return new QpidByteBuffer(_ref, buffer, newPosition);
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

    ByteBuffer getUnderlyingBuffer()
    {
        return _buffer;
    }

    public static QpidByteBuffer allocate(boolean direct, int size)
    {
        return direct ? allocateDirect(size) : allocate(size);
    }

    public static QpidByteBuffer allocate(int size)
    {
        return new QpidByteBuffer(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
    }

    public static QpidByteBuffer allocateDirect(int size)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Cannot allocate QpidByteBuffer with size "
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
        if (_pooledBufferSize == 0)
        {
            return Collections.singleton(allocateDirect(size));
        }
        else
        {
            List<QpidByteBuffer> buffers = new ArrayList<>((size / _pooledBufferSize) + 2);
            int remaining = size;

            QpidByteBuffer buf = _cachedBuffer.get();
            if (buf == null)
            {
                buf = allocateDirect(_pooledBufferSize);
            }
            while (remaining > buf.remaining())
            {
                int bufRemaining = buf.remaining();
                if (buf == _cachedBuffer.get())
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
            src = new ByteBuffer[buffers.size()];
            Iterator<QpidByteBuffer> iterator = buffers.iterator();
            for (int i = 0; i < src.length; i++)
            {
                src[i] = iterator.next().getUnderlyingBuffer();
            }
        }
        return engine.wrap(src, dest.getUnderlyingBuffer());
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
        catch (IOException e)
        {
            for (QpidByteBuffer uncompressedBuffer : uncompressedBuffers)
            {
                uncompressedBuffer.dispose();
            }
            throw e;
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

        try(QpidByteBufferOutputStream compressedOutput = new QpidByteBufferOutputStream(isDirect, bufferSize);
            InputStream compressedInput = new CompositeInputStream(streams);
            GZIPOutputStream gzipStream = new GZIPOutputStream(new BufferedOutputStream(compressedOutput, bufferSize)))
        {
            byte[] buf = new byte[16384];
            int read;
            while ((read = compressedInput.read(buf)) > -1)
            {
                gzipStream.write(buf, 0, read);
            }
            gzipStream.finish();
            gzipStream.flush();
            return compressedOutput.fetchAccumulatedBuffers();
        }
    }

    public static long write(GatheringByteChannel channel, Collection<QpidByteBuffer> qpidByteBuffers)
            throws IOException
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[qpidByteBuffers.size()];
        Iterator<QpidByteBuffer> iterator = qpidByteBuffers.iterator();
        for (int i = 0; i < byteBuffers.length; i++)
        {
            byteBuffers[i] = iterator.next().getUnderlyingBuffer();
        }
        return channel.write(byteBuffers);
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
            final String errorMessage = String.format(
                    "QpidByteBuffer pool has already been initialised with bufferSize=%d and maxPoolSize=%d." +
                    "Re-initialisation with different bufferSize=%d and maxPoolSize=%d is not allowed.",
                    _pooledBufferSize,
                    _bufferPool.getMaxSize(),
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
        _isPoolInitialized = true;
    }

    private static final class BufferInputStream extends InputStream
    {
        private final QpidByteBuffer _qpidByteBuffer;

        private BufferInputStream(final QpidByteBuffer buffer)
        {
            _qpidByteBuffer = buffer;
        }

        @Override
        public int read() throws IOException
        {
            if (_qpidByteBuffer.hasRemaining())
            {
                return _qpidByteBuffer.get() & 0xFF;
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
        }
    }
}
