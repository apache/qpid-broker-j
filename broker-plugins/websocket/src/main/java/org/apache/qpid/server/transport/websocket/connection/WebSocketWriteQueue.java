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
package org.apache.qpid.server.transport.websocket.connection;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

final class WebSocketWriteQueue
{
    private final int _webSocketWriteBatchSize;

    private Deque<QpidByteBuffer> _buffers = new ArrayDeque<>(4);
    private long _bufferedSize;
    private QpidByteBuffer _writeBatchBoundary;
    private Deque<QpidByteBuffer> _additionalWriteBatchBoundaries;
    private ByteBuffer _writeBuffer;

    WebSocketWriteQueue(final WebSocketSettings settings)
    {
        Objects.requireNonNull(settings, "WebSocket settings must not be null");
        _webSocketWriteBatchSize = Math.max(1, settings.getBufferSize());
    }

    void append(final QpidByteBuffer message, final int size)
    {
        _buffers.add(message.duplicate());
        _bufferedSize += size;
    }

    boolean hasBytes()
    {
        return _bufferedSize > 0L;
    }

    void markFlushBoundary()
    {
        final QpidByteBuffer boundary = _buffers.peekLast();
        if (boundary == null || boundary == getLastWriteBatchBoundary())
        {
            return;
        }
        if (_writeBatchBoundary == null)
        {
            _writeBatchBoundary = boundary;
        }
        else
        {
            if (_additionalWriteBatchBoundaries == null)
            {
                _additionalWriteBatchBoundaries = new ArrayDeque<>(2);
            }
            _additionalWriteBatchBoundaries.add(boundary);
        }
    }

    ByteBuffer createBatch()
    {
        final int size = getBatchSize();
        ensureWriteBufferCapacity(size);
        final ByteBuffer data = _writeBuffer;
        data.clear();
        data.limit(size);
        drainTo(data, size);
        return data;
    }

    Deque<QpidByteBuffer> detachForCleanup()
    {
        final Deque<QpidByteBuffer> buffers = _buffers;
        _buffers = null;
        _bufferedSize = 0L;
        _writeBuffer = null;
        _writeBatchBoundary = null;
        _additionalWriteBatchBoundaries = null;
        return buffers;
    }

    private int getBatchSize()
    {
        long maximumSize = Math.min(_bufferedSize, _webSocketWriteBatchSize);
        if (_writeBatchBoundary != null)
        {
            long sizeToBoundary = 0L;
            for (final QpidByteBuffer buffer : _buffers)
            {
                sizeToBoundary += buffer.remaining();
                if (buffer == _writeBatchBoundary || sizeToBoundary >= maximumSize)
                {
                    break;
                }
            }
            maximumSize = Math.min(maximumSize, sizeToBoundary);
        }
        return (int) maximumSize;
    }

    private void ensureWriteBufferCapacity(final int size)
    {
        if (_writeBuffer == null || _writeBuffer.capacity() < size)
        {
            final int capacity = _writeBuffer == null
                    ? size
                    : (int) Math.max(size, Math.min(_webSocketWriteBatchSize, 2L * _writeBuffer.capacity()));
            _writeBuffer = ByteBuffer.allocate(capacity);
        }
    }

    private void drainTo(final ByteBuffer data, final int size)
    {
        int offset = 0;
        while (offset < size)
        {
            final QpidByteBuffer buffer = _buffers.element();
            final int length = Math.min(buffer.remaining(), size - offset);
            buffer.get(data.array(), offset, length);
            offset += length;
            if (!buffer.hasRemaining())
            {
                _buffers.remove();
                final boolean boundaryReached = buffer == _writeBatchBoundary;
                buffer.dispose();
                if (boundaryReached)
                {
                    advanceWriteBatchBoundary();
                }
            }
        }
        _bufferedSize -= size;
    }

    private QpidByteBuffer getLastWriteBatchBoundary()
    {
        if (_additionalWriteBatchBoundaries != null && !_additionalWriteBatchBoundaries.isEmpty())
        {
            return _additionalWriteBatchBoundaries.peekLast();
        }
        return _writeBatchBoundary;
    }

    private void advanceWriteBatchBoundary()
    {
        if (_additionalWriteBatchBoundaries == null || _additionalWriteBatchBoundaries.isEmpty())
        {
            _writeBatchBoundary = null;
            _additionalWriteBatchBoundaries = null;
        }
        else
        {
            _writeBatchBoundary = _additionalWriteBatchBoundaries.removeFirst();
            if (_additionalWriteBatchBoundaries.isEmpty())
            {
                _additionalWriteBatchBoundaries = null;
            }
        }
    }
}
