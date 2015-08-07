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

package org.apache.qpid.server.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.cert.Certificate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class NonBlockingConnectionPlainDelegate implements NonBlockingConnectionDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnectionPlainDelegate.class);

    private final NonBlockingConnection _parent;
    private QpidByteBuffer _netInputBuffer;

    public NonBlockingConnectionPlainDelegate(NonBlockingConnection parent)
    {
        _parent = parent;
        _netInputBuffer = QpidByteBuffer.allocateDirect(parent.getReceiveBufferSize());
    }

    @Override
    public boolean readyForRead()
    {
        return true;
    }

    @Override
    public boolean processData()
    {
        _netInputBuffer.flip();
        _parent.processAmqpData(_netInputBuffer);

        restoreApplicationBufferForWrite();

        return false;
    }

    protected void restoreApplicationBufferForWrite()
    {
        _netInputBuffer = _netInputBuffer.slice();
        if (_netInputBuffer.limit() != _netInputBuffer.capacity())
        {
            _netInputBuffer.position(_netInputBuffer.limit());
            _netInputBuffer.limit(_netInputBuffer.capacity());
        }
        else
        {
            QpidByteBuffer currentBuffer = _netInputBuffer;
            int newBufSize = (currentBuffer.capacity() < _parent.getReceiveBufferSize())
                    ? _parent.getReceiveBufferSize()
                    : currentBuffer.capacity() + _parent.getReceiveBufferSize();

            _netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
            _netInputBuffer.put(currentBuffer);
        }

    }


    @Override
    public boolean doWrite(ByteBuffer[] bufferArray) throws IOException
    {
        int byteBuffersWritten = 0;

        _parent.writeToTransport(bufferArray);

        for (ByteBuffer buf : bufferArray)
        {
            if (buf.remaining() == 0)
            {
                byteBuffersWritten++;
                _parent.writeBufferProcessed();
            }
            else
            {
                break;
            }
        }

        return bufferArray.length == byteBuffersWritten;
    }

    @Override
    public Principal getPeerPrincipal()
    {
        return null;
    }

    @Override
    public Certificate getPeerCertificate()
    {
        return null;
    }

    @Override
    public boolean needsWork()
    {
        return false;
    }

    @Override
    public QpidByteBuffer getNetInputBuffer()
    {
        return _netInputBuffer;
    }

    @Override
    public void setNetInputBuffer(final QpidByteBuffer netInputBuffer)
    {
        _netInputBuffer = netInputBuffer;
    }
}
