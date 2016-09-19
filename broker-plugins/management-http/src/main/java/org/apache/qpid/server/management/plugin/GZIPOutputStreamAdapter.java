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

package org.apache.qpid.server.management.plugin;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class GZIPOutputStreamAdapter extends OutputStream
{
    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private static final int MIN_GZIP_HEADER_SIZE = 10; // |ID1|ID2|CM |FLG|MTIME (4 bytes)|XFL|OS |  ...excluding file name and comment

    private final OutputStream _targetOutputStream;
    private final BufferInputStream _bufferInputStream;
    private final int _size;
    private final long _limit;

    private GZIPInputStream _gzipInputStream;
    private long _writtenBytesNumber;

    public GZIPOutputStreamAdapter(final OutputStream targetOutputStream, final long limit)
    {
        this(targetOutputStream, DEFAULT_BUFFER_SIZE, limit);
    }

    public GZIPOutputStreamAdapter(final OutputStream targetOutputStream, final int size, final long limit)
    {
        if (size < MIN_GZIP_HEADER_SIZE)
        {
            throw new IllegalArgumentException("Buffer size should be greater than or equal " + MIN_GZIP_HEADER_SIZE);
        }
        _size = size;
        _targetOutputStream = targetOutputStream;
        _bufferInputStream = new BufferInputStream(size * 2);
        _limit = limit;
    }

    @Override
    public synchronized void close() throws IOException
    {
        try
        {
            if (_gzipInputStream != null)
            {
                _gzipInputStream.close();
                _gzipInputStream = null;
            }
            _bufferInputStream.close();
        }
        finally
        {
            _targetOutputStream.close();
        }
    }

    @Override
    public synchronized void write(final int byteValue) throws IOException
    {
        this.write(new byte[]{(byte) byteValue}, 0, 1);
    }

    @Override
    public synchronized void write(final byte data[], final int offset, final int length) throws IOException
    {
        if (_limit < 0 || (_limit > 0 && _writtenBytesNumber < _limit))
        {
            int numberOfWrittenBytes = 0;
            do
            {
                numberOfWrittenBytes +=
                        _bufferInputStream.write(data, offset + numberOfWrittenBytes, length - numberOfWrittenBytes);
                if (_gzipInputStream == null)
                {
                    _bufferInputStream.mark(_size);
                    try
                    {
                        _gzipInputStream = new GZIPInputStream(_bufferInputStream, _size);
                    }
                    catch (IOException e)
                    {
                        // no sufficient bytes to read gzip header
                        _bufferInputStream.reset();
                    }
                }

                if (_gzipInputStream != null && _bufferInputStream.available() > 0)
                {
                    tryToDecompressAndWrite();
                }
            }
            while (numberOfWrittenBytes < length);
        }
    }

    private void tryToDecompressAndWrite() throws IOException
    {
        int b = -1;
        do
        {
            try
            {
                b = _gzipInputStream.read();
            }
            catch (EOFException e)
            {
                // no sufficient data to decompress
                break;
            }

            if (b != -1)
            {
                _targetOutputStream.write(b);
                _writtenBytesNumber++;

                if (_limit > 0 && _writtenBytesNumber == _limit)
                {
                    break;
                }
            }
        }
        while (b != -1);
    }

    private static final class BufferInputStream extends InputStream
    {
        private final ByteBuffer _byteBuffer;

        private BufferInputStream(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Buffer size should be greater than zero");
            }
            _byteBuffer = ByteBuffer.allocate(size);
            _byteBuffer.limit(0);
        }

        @Override
        public int read() throws IOException
        {
            if (_byteBuffer.hasRemaining())
            {
                return _byteBuffer.get() & 0xFF;
            }
            return -1;
        }


        @Override
        public int read(byte[] data, int offset, int length) throws IOException
        {
            if (!_byteBuffer.hasRemaining())
            {
                return -1;
            }
            if (_byteBuffer.remaining() < length)
            {
                length = _byteBuffer.remaining();
            }
            _byteBuffer.get(data, offset, length);

            return length;
        }

        @Override
        public void mark(int readlimit)
        {
            _byteBuffer.mark();
        }

        @Override
        public void reset() throws IOException
        {
            _byteBuffer.reset();
        }

        @Override
        public boolean markSupported()
        {
            return true;
        }

        @Override
        public long skip(long n) throws IOException
        {
            _byteBuffer.position(_byteBuffer.position() + (int) n);
            return n;
        }

        @Override
        public int available() throws IOException
        {
            return _byteBuffer.remaining();
        }

        @Override
        public void close()
        {

        }

        public int write(byte[] data, int offset, int length)
        {
            int numberOfBytes = 0;
            int capacity = _byteBuffer.capacity();
            int remaining = _byteBuffer.remaining();
            if (remaining == 0)
            {
                numberOfBytes = Math.min(length, capacity);
                _byteBuffer.position(0);
                _byteBuffer.limit(numberOfBytes);
                _byteBuffer.put(data, offset, numberOfBytes);
                _byteBuffer.flip();
            }
            else if (remaining < capacity)
            {
                byte[] array = _byteBuffer.array();
                int position = _byteBuffer.position();
                if (position > 0)
                {
                    for (int i = 0; i < remaining; i++)
                    {
                        array[0] = array[i + position];
                    }
                }

                numberOfBytes = Math.min(length, capacity - remaining);
                System.arraycopy(data, offset, array, remaining, numberOfBytes);

                _byteBuffer.position(0);
                _byteBuffer.limit(remaining + numberOfBytes);
            }
            return numberOfBytes;
        }
    }
}
