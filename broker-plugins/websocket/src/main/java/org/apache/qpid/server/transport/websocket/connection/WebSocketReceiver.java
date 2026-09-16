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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;

final class WebSocketReceiver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketReceiver.class);

    private final int _bufferSize;

    private volatile QpidByteBuffer _netInputBuffer;
    private volatile boolean _unexpectedByteBufferSizeReported;

    WebSocketReceiver(final WebSocketSettings settings)
    {
        Objects.requireNonNull(settings, "WebSocket settings must not be null");
        _bufferSize = settings.getBufferSize();
        _netInputBuffer = QpidByteBuffer.allocateDirect(_bufferSize);
    }

    void receiveBinary(final ByteBuffer payload,
                       final WebSocketConnection connection,
                       final MultiVersionProtocolEngine protocolEngine)
    {
        connection.lockProtocol();
        try
        {
            protocolEngine.clearWork();
            try
            {
                protocolEngine.setIOThread(Thread.currentThread());
                connection.processPendingWork();

                while (payload.hasRemaining() && connection.isOpen())
                {
                    final int chunkLength = Math.min(payload.remaining(), _netInputBuffer.remaining());
                    final int payloadLimit = payload.limit();
                    payload.limit(payload.position() + chunkLength);
                    try
                    {
                        _netInputBuffer.put(payload);
                    }
                    finally
                    {
                        payload.limit(payloadLimit);
                    }

                    _netInputBuffer.flip();
                    protocolEngine.received(_netInputBuffer);
                    restoreApplicationBufferForWrite();
                }
            }
            finally
            {
                protocolEngine.setIOThread(null);
            }
            if (payload.hasRemaining())
            {
                payload.position(payload.limit());
            }
        }
        finally
        {
            connection.unlockProtocol();
        }

        connection.doWrite();
    }

    private void restoreApplicationBufferForWrite()
    {
        try (final QpidByteBuffer oldNetInputBuffer = _netInputBuffer)
        {
            final int unprocessedDataLength = _netInputBuffer.remaining();

            _netInputBuffer.limit(_netInputBuffer.capacity());
            _netInputBuffer = oldNetInputBuffer.slice();
            _netInputBuffer.limit(unprocessedDataLength);
        }
        if (_netInputBuffer.limit() != _netInputBuffer.capacity())
        {
            _netInputBuffer.position(_netInputBuffer.limit());
            _netInputBuffer.limit(_netInputBuffer.capacity());
        }
        else
        {
            try (final QpidByteBuffer currentBuffer = _netInputBuffer)
            {
                final int newBufferSize;
                if (currentBuffer.capacity() >= _bufferSize)
                {
                    newBufferSize = currentBuffer.capacity() + _bufferSize;
                    reportUnexpectedByteBufferSizeUsage();
                }
                else
                {
                    newBufferSize = _bufferSize;
                }

                _netInputBuffer = QpidByteBuffer.allocateDirect(newBufferSize);
                _netInputBuffer.put(currentBuffer);
            }
        }
    }

    private void reportUnexpectedByteBufferSizeUsage()
    {
        if (!_unexpectedByteBufferSizeReported)
        {
            LOGGER.info("At least one frame unexpectedly does not fit into default byte buffer size ({}B) " +
                    "on a connection {}.", _bufferSize, this);
            _unexpectedByteBufferSizeReported = true;
        }
    }

    void dispose()
    {
        final QpidByteBuffer netInputBuffer = _netInputBuffer;
        _netInputBuffer = null;

        if (netInputBuffer != null)
        {
            netInputBuffer.dispose();
        }
    }
}
