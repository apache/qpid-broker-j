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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.apache.qpid.streams.CompositeInputStream;

public abstract class QpidByteBuffer
{
    private static final AtomicIntegerFieldUpdater<QpidByteBuffer>
            DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
            QpidByteBuffer.class,
            "_disposed");
    private static final ThreadLocal<QpidByteBuffer> _cachedBuffer = new ThreadLocal<>();
    private volatile static boolean _isPoolInitialized;
    private volatile static BufferPool _bufferPool;
    private volatile static int _pooledBufferSize;
    private volatile static ByteBuffer _zeroed;
    final ByteBufferRef _ref;
    volatile ByteBuffer _buffer;
    @SuppressWarnings("unused")
    private volatile int _disposed;

    QpidByteBuffer(ByteBufferRef ref, ByteBuffer buffer)
    {
        _ref = ref;
        _buffer = buffer;
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
        ByteBuffer underlyingBuffer = getUnderlyingBuffer();
        try
        {
            return charset.decode(underlyingBuffer);
        }
        finally
        {
            updateFromLastUnderlying();
        }
    }

    public final int read(ReadableByteChannel channel) throws IOException
    {
        ByteBuffer underlyingBuffer = getUnderlyingBuffer();
        try
        {
            return channel.read(underlyingBuffer);
        }
        finally
        {
            updateFromLastUnderlying();
        }
    }

    public final SSLEngineResult decryptSSL(SSLEngine engine, QpidByteBuffer dest) throws SSLException
    {
        ByteBuffer underlyingBuffer = getUnderlyingBuffer();
        ByteBuffer destUnderlyingBuffer = dest.getUnderlyingBuffer();
        try
        {
            return engine.unwrap(underlyingBuffer, destUnderlyingBuffer);
        }
        finally
        {
            updateFromLastUnderlying();
            dest.updateFromLastUnderlying();
        }
    }

    @Override
    public String toString()
    {
        return "QpidByteBuffer{" +
               "_buffer=" + _buffer +
               ", _disposed=" + _disposed +
               '}';
    }

    public abstract boolean hasRemaining();

    public abstract QpidByteBuffer putInt(int index, int value);

    public abstract QpidByteBuffer putShort(int index, short value);

    public abstract QpidByteBuffer putChar(int index, char value);

    public abstract QpidByteBuffer put(byte b);

    public abstract QpidByteBuffer put(int index, byte b);

    public abstract short getShort(int index);

    public abstract QpidByteBuffer mark();

    public abstract long getLong();

    public abstract QpidByteBuffer putFloat(int index, float value);

    public abstract double getDouble(int index);

    public abstract boolean hasArray();

    public abstract double getDouble();

    public abstract QpidByteBuffer putFloat(float value);

    public abstract QpidByteBuffer putInt(int value);

    public abstract byte[] array();

    public abstract QpidByteBuffer putShort(short value);

    public abstract int getInt(int index);

    public abstract int remaining();

    public abstract QpidByteBuffer put(byte[] src);

    public abstract QpidByteBuffer put(ByteBuffer src);

    public abstract QpidByteBuffer put(QpidByteBuffer src);

    public abstract QpidByteBuffer get(byte[] dst, int offset, int length);

    public abstract QpidByteBuffer get(ByteBuffer dst);

    public abstract void copyTo(ByteBuffer dst);

    public abstract void putCopyOf(QpidByteBuffer buf);

    public abstract QpidByteBuffer rewind();

    public abstract QpidByteBuffer clear();

    public abstract QpidByteBuffer putLong(int index, long value);

    public abstract QpidByteBuffer compact();

    public abstract QpidByteBuffer putDouble(double value);

    public abstract int limit();

    public abstract QpidByteBuffer reset();

    public abstract QpidByteBuffer flip();

    public abstract short getShort();

    public abstract float getFloat();

    public abstract QpidByteBuffer limit(int newLimit);

    public abstract QpidByteBuffer duplicate();

    public abstract QpidByteBuffer put(byte[] src, int offset, int length);

    public abstract long getLong(int index);

    public abstract int capacity();

    public abstract char getChar(int index);

    public abstract byte get();

    public abstract byte get(int index);

    public abstract QpidByteBuffer get(byte[] dst);

    public abstract void copyTo(byte[] dst);

    public abstract QpidByteBuffer putChar(char value);

    public abstract QpidByteBuffer position(int newPosition);

    public abstract int arrayOffset();

    public abstract char getChar();

    public abstract int getInt();

    public abstract QpidByteBuffer putLong(long value);

    public abstract float getFloat(int index);

    public abstract QpidByteBuffer slice();

    public abstract QpidByteBuffer view(int offset, int length);

    public abstract int position();

    public abstract QpidByteBuffer putDouble(int index, double value);

    /**
     * Returns an underlying byte buffer for update operations.
     * <p></p>
     * Method {@link #updateFromLastUnderlying()} needs to be invoked to update the state of {@link QpidByteBuffer}
     *
     * @return ByteBuffer
     */
    abstract ByteBuffer getUnderlyingBuffer();

    /**
     * Used to update the state of {@link QpidByteBuffer} after underlying byte buffer is modified.
     *
     * @throws IllegalStateException when method is invoked without previous call to {@link #getUnderlyingBuffer()}
     */
    abstract void updateFromLastUnderlying();

    public static QpidByteBuffer allocate(boolean direct, int size)
    {
        return direct ? allocateDirect(size) : allocate(size);
    }

    public static QpidByteBuffer allocate(int size)
    {
        return new QpidByteBufferImpl(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
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
        return new QpidByteBufferImpl(ref);
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
        List<QpidByteBuffer> qpidBuffers = new ArrayList<>(buffers);
        final ByteBuffer[] src = new ByteBuffer[buffers.size()];
        for (int i = 0; i < src.length; i++)
        {
            src[i] = qpidBuffers.get(i).getUnderlyingBuffer();
        }
        ByteBuffer destinationUnderlyingBuffer = dest.getUnderlyingBuffer();
        try
        {
            return engine.wrap(src, destinationUnderlyingBuffer);
        }
        finally
        {
            for (QpidByteBuffer qpidByteBuffer : qpidBuffers)
            {
                qpidByteBuffer.updateFromLastUnderlying();
            }
            dest.updateFromLastUnderlying();
        }
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
        List<QpidByteBuffer> qpidBuffers = new ArrayList<>(qpidByteBuffers);
        ByteBuffer[] byteBuffers = new ByteBuffer[qpidBuffers.size()];
        for (int i = 0; i < byteBuffers.length; i++)
        {
            byteBuffers[i] = qpidBuffers.get(i).getUnderlyingBuffer();
        }
        try
        {
            return channel.write(byteBuffers);
        }
        finally
        {
            for (QpidByteBuffer qbb : qpidBuffers)
            {
                qbb.updateFromLastUnderlying();
            }
        }
    }

    public static QpidByteBuffer wrap(final ByteBuffer wrap)
    {
        return new QpidByteBufferImpl(new NonPooledByteBufferRef(wrap));
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
