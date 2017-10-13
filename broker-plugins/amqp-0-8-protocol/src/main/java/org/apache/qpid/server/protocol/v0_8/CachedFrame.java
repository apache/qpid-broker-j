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
package org.apache.qpid.server.protocol.v0_8;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.transport.ByteBufferSender;

class CachedFrame extends AMQDataBlock
{
    private final String _toString;
    private final QpidByteBuffer _buffer;
    private final long _size;
    private boolean _disposed;

    CachedFrame(AMQDataBlock original)
    {
        _toString = original.toString();
        _size = original.getSize();
        _buffer = QpidByteBuffer.allocate(true, (int)_size);
        original.writePayload(new BufferWriterSender(_buffer));
        _buffer.flip();
    }

    @Override
    public long getSize()
    {
        return _size;
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {

        try (QpidByteBuffer dup = _buffer.duplicate())
        {
            sender.send(dup);
        }
        return _size;
    }

    @Override
    public String toString()
    {
        return _toString;
    }

    private static class BufferWriterSender implements ByteBufferSender
    {
        private final QpidByteBuffer _buffer;

        BufferWriterSender(final QpidByteBuffer buffer)
        {
            _buffer = buffer;
        }

        @Override
        public boolean isDirectBufferPreferred()
        {
            return true;
        }

        @Override
        public void send(final QpidByteBuffer msg)
        {
            try (QpidByteBuffer dup = msg.duplicate())
            {
                _buffer.put(dup);
            }
        }

        @Override
        public void flush()
        {

        }

        @Override
        public void close()
        {

        }
    }

    public void dispose()
    {
        _buffer.dispose();
        _disposed = true;
    }
}
