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

package org.apache.qpid.server.bytebuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

final class QpidByteBufferOutputStream extends OutputStream
{
    private final LinkedList<QpidByteBuffer> _buffers = new LinkedList<>();
    private int _bufferPosition = 0;
    private final byte[] _buffer;
    private final boolean _isDirect;
    private final int _maximumBufferSize;
    private boolean _closed;

    public QpidByteBufferOutputStream(final boolean isDirect, final int maximumBufferSize)
    {
        if (maximumBufferSize <= 0)
        {
            throw new IllegalArgumentException("Negative or zero maximumBufferSize illegal : " + maximumBufferSize);
        }
        _isDirect = isDirect;
        _maximumBufferSize = maximumBufferSize;
        _buffer = new byte[_maximumBufferSize];
    }

    @Override
    public void write(int b) throws IOException
    {
        write(new byte[] {(byte)b});
    }

    @Override
    public void write(byte[] data) throws IOException
    {
        write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int offset, int len) throws IOException
    {
        allocateDataBuffers(data, offset, len);
    }

    @Override
    public void close() throws IOException
    {
        _closed = true;
        _buffers.forEach(QpidByteBuffer::dispose);
        _buffers.clear();
    }

    QpidByteBuffer fetchAccumulatedBuffer()
    {
        if (_bufferPosition != 0)
        {
            addSingleQpidByteBuffer(_buffer, 0, _bufferPosition);
        }
        final QpidByteBuffer combined = QpidByteBuffer.concatenate(_buffers);
        return combined;
    }

    private void allocateDataBuffers(byte[] data, int offset, int len) throws IOException
    {
        if (_closed)
        {
            throw new IOException("Stream is closed");
        }

        do
        {
            int bytesWeCanWrite = Math.min(_buffer.length - _bufferPosition, len);
            System.arraycopy(data, offset, _buffer, _bufferPosition, bytesWeCanWrite);
            offset += bytesWeCanWrite;
            len -= bytesWeCanWrite;
            _bufferPosition += bytesWeCanWrite;
            if (_buffer.length == _bufferPosition)
            {
                addSingleQpidByteBuffer(_buffer, 0, _buffer.length);
            }
        } while (len != 0);
    }

    private void addSingleQpidByteBuffer(final byte[] buffer, final int offset, final int length)
    {
        QpidByteBuffer current = _isDirect
                ? QpidByteBuffer.allocateDirect(length)
                : QpidByteBuffer.allocate(length);
        current.put(buffer, offset, length);
        current.flip();
        _buffers.add(current);
        _bufferPosition = 0;
    }
}
