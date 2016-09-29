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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

/**
 * OutputStream implementation that yields a list QpidByteBuffers that contain a copy
 * of the incoming bytes.  Use fetchAccumulatedBuffers to get the buffers.  Caller
 * has responsibility to dispose the buffers after use.
 *
 * It will be normally be desirable to front this stream with java.io.BufferedOutputStream
 * to minimise the number of write and thus the number of buffers created.
 *
 * Not thread safe.
 */
public class QpidByteBufferOutputStream extends OutputStream
{
    private final LinkedList<QpidByteBuffer> _buffers = new LinkedList<>();
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
    }

    @Override
    public void write(int b) throws IOException
    {
        int size = 1;
        byte[] data = new byte[] {(byte)b};
        allocateDataBuffers(data, 0, size);
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
        for (QpidByteBuffer buffer : _buffers)
        {
            buffer.dispose();
        }
        _buffers.clear();
    }

    public Collection<QpidByteBuffer> fetchAccumulatedBuffers()
    {
        Collection<QpidByteBuffer> bufs = new ArrayList<>(_buffers);
        _buffers.clear();
        return bufs;
    }

    private void allocateDataBuffers(byte[] data, int offset, int len) throws IOException
    {
        if (_closed)
        {
            throw new IOException("Stream is closed");
        }

        int size = Math.min(_maximumBufferSize, len);

        QpidByteBuffer current = _isDirect ? QpidByteBuffer.allocateDirect(len) : QpidByteBuffer.allocate(len);
        current.put(data, offset, size);
        current.flip();
        _buffers.add(current);
        if (len > size)
        {
            allocateDataBuffers(data, offset + size, len - size);
        }
    }
}
