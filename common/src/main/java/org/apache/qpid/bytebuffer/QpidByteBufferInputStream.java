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
 */

package org.apache.qpid.bytebuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.qpid.streams.CompositeInputStream;

/**
 * InputStream implementation that takes a list QpidByteBuffers.
 * The QpidByteBufferInputStream takes ownership of the buffers and disposes them on close().
 *
 * Not thread safe.
 */
public class QpidByteBufferInputStream extends InputStream
{
    private final CompositeInputStream _compositeInputStream;
    private final Collection<QpidByteBuffer> _buffers;

    public QpidByteBufferInputStream(Collection<QpidByteBuffer> buffers)
    {
        _buffers = buffers;

        final Collection<InputStream> streams = new ArrayList<>(buffers.size());
        for (QpidByteBuffer buffer : buffers)
        {
            streams.add(buffer.asInputStream());
        }
        _compositeInputStream = new CompositeInputStream(streams);
    }

    @Override
    public int read() throws IOException
    {
        return _compositeInputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        return _compositeInputStream.read(b, off, len);
    }

    @Override
    public void mark(int readlimit)
    {
        _compositeInputStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException
    {
        _compositeInputStream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return _compositeInputStream.markSupported();
    }

    @Override
    public long skip(long n) throws IOException
    {
        return _compositeInputStream.skip(n);
    }

    @Override
    public int available() throws IOException
    {
        return _compositeInputStream.available();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            _compositeInputStream.close();
        }
        finally
        {
            for (QpidByteBuffer buffer : _buffers)
            {
                buffer.dispose();
            }
        }
    }
}
