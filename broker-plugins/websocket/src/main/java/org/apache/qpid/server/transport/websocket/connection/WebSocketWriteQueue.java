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
    private WriteBatch _batch;

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

    WriteBatch getWriteBatch()
    {
        if (_batch == null)
        {
            _batch = new WriteBatch();
        }
        return _batch;
    }

    void reserveBatch(final WriteBatch batch)
    {
        while (!_buffers.isEmpty() && batch._size < _webSocketWriteBatchSize)
        {
            final QpidByteBuffer buffer = _buffers.element();
            final int remaining = buffer.remaining();
            final int length = Math.min(remaining, _webSocketWriteBatchSize - batch._size);
            batch._size += length;
            if (length < remaining)
            {
                batch._buffers.add(buffer.view(0, length));
                buffer.position(buffer.position() + length);
                break;
            }
            batch._buffers.add(buffer);
            _buffers.remove();
            if (buffer == _writeBatchBoundary)
            {
                advanceWriteBatchBoundary();
                break;
            }
        }
        _bufferedSize -= batch._size;
    }

    Deque<QpidByteBuffer> detachForCleanup()
    {
        final Deque<QpidByteBuffer> buffers = _buffers;
        _buffers = null;
        _bufferedSize = 0L;
        _batch = null;
        _writeBatchBoundary = null;
        _additionalWriteBatchBoundaries = null;
        return buffers;
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

    final class WriteBatch
    {
        private final Deque<QpidByteBuffer> _buffers = new ArrayDeque<>(4);
        private int _size;
        private ByteBuffer _data;

        ByteBuffer copy()
        {
            if (_data == null || _data.capacity() < _size)
            {
                final int capacity = _data == null
                        ? _size :
                        (int) Math.max(_size, Math.min(_webSocketWriteBatchSize, 2L * _data.capacity()));
                _data = ByteBuffer.allocate(capacity);
            }
            _data.clear();
            _data.limit(_size);
            int offset = 0;
            for (final QpidByteBuffer buffer : _buffers)
            {
                final int length = buffer.remaining();
                buffer.get(_data.array(), offset, length);
                offset += length;
            }
            return _data;
        }

        void release()
        {
            while (!_buffers.isEmpty())
            {
                _buffers.remove().dispose();
            }
            _size = 0;
        }
    }
}
