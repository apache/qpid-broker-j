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

package org.apache.qpid.server.management.plugin.portunification;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadPendingException;
import java.nio.channels.WritePendingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;

public class MarkableEndPoint implements EndPoint
{
    private final EndPoint _underlying;
    private final List<ByteBuffer> _preserved = new ArrayList<>();
    private volatile boolean _marked;

    MarkableEndPoint(final EndPoint underlying)
    {
        _underlying = underlying;
    }

    @Override
    public InetSocketAddress getLocalAddress()
    {
        return _underlying.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress()
    {
        return _underlying.getRemoteAddress();
    }

    @Override
    public boolean isOpen()
    {
        return _underlying.isOpen();
    }

    @Override
    public long getCreatedTimeStamp()
    {
        return _underlying.getCreatedTimeStamp();
    }

    @Override
    public void shutdownOutput()
    {
        _underlying.shutdownOutput();
    }

    @Override
    public boolean isOutputShutdown()
    {
        return _underlying.isOutputShutdown();
    }

    @Override
    public boolean isInputShutdown()
    {
        return _underlying.isInputShutdown();
    }

    @Override
    public void close()
    {
        _underlying.close();
    }

    @Override
    public boolean flush(final ByteBuffer... buffer) throws IOException
    {
        return _underlying.flush(buffer);
    }

    @Override
    public Object getTransport()
    {
        return _underlying.getTransport();
    }

    @Override
    public long getIdleTimeout()
    {
        return _underlying.getIdleTimeout();
    }

    @Override
    public void setIdleTimeout(final long idleTimeout)
    {
        _underlying.setIdleTimeout(idleTimeout);
    }

    @Override
    public void fillInterested(final Callback callback) throws ReadPendingException
    {
        _underlying.fillInterested(callback);
    }

    @Override
    public boolean tryFillInterested(final Callback callback)
    {
        return _underlying.tryFillInterested(callback);
    }

    @Override
    public boolean isFillInterested()
    {
        return _underlying.isFillInterested();
    }

    @Override
    public void write(final Callback callback, final ByteBuffer... buffers) throws WritePendingException
    {
        _underlying.write(callback, buffers);
    }

    @Override
    public Connection getConnection()
    {
        return _underlying.getConnection();
    }

    @Override
    public void setConnection(final Connection connection)
    {
        _underlying.setConnection(connection);
    }

    @Override
    public void onOpen()
    {
        _underlying.onOpen();
    }

    @Override
    public void onClose()
    {
        _underlying.onClose();
    }

    @Override
    public boolean isOptimizedForDirectBuffers()
    {
        return _underlying.isOptimizedForDirectBuffers();
    }

    @Override
    public void upgrade(final Connection newConnection)
    {
        _underlying.upgrade(newConnection);
    }

    @Override
    public synchronized int fill(final ByteBuffer dst) throws IOException
    {
        if (_marked)
        {
            final int oldLimit = dst.limit();
            final int i = _underlying.fill(dst);
            int newLimit = dst.limit();

            ByteBuffer buf = preserve(dst.duplicate(), newLimit, oldLimit);
            _preserved.add(buf);
            return i;
        }
        else
        {
            int i = 0;
            if (!_preserved.isEmpty())
            {
                i += fillFromPreserved(dst);
                if (!_preserved.isEmpty())
                {
                    return  i;
                }
            }
            i += _underlying.fill(dst);

            return i;
        }
    }

    synchronized void mark()
    {
        if (!_preserved.isEmpty())
        {
            throw new IllegalStateException("Already marked");
        }
        _marked = true;
    }

    synchronized void rewind()
    {
        if (!_marked)
        {
            throw new IllegalStateException("Not marked");
        }
        _marked = false;
    }

    private int fillFromPreserved(final ByteBuffer dst)
    {
        int filled = 0;
        final int pos = BufferUtil.flipToFill(dst);
        try
        {
            Iterator<ByteBuffer> bufferIterator = _preserved.iterator();
            while (bufferIterator.hasNext())
            {
                ByteBuffer buffer = bufferIterator.next();
                final int bufRemaining = buffer.remaining();
                int dstRemaining = dst.remaining();
                if (dstRemaining >= bufRemaining)
                {
                    dst.put(buffer);
                }
                else
                {
                    ByteBuffer slice = buffer.slice();
                    slice.limit(dstRemaining);
                    dst.put(slice);
                    buffer.position(buffer.position() + dstRemaining);
                }
                filled += bufRemaining - buffer.remaining();
                if (buffer.hasRemaining())
                {
                    return filled;
                }
                bufferIterator.remove();
            }
        }
        finally
        {
            BufferUtil.flipToFlush(dst, pos);
        }
        return filled;
    }

    private ByteBuffer preserve(final ByteBuffer dst, final int newLimit, final int oldLimit)
    {
        ByteBuffer buf = BufferUtil.allocate(newLimit - oldLimit);
        ByteBuffer slice = dst.slice();
        slice.position(oldLimit);
        slice.limit(newLimit);
        BufferUtil.append(buf, slice);
        return buf;
    }
}
