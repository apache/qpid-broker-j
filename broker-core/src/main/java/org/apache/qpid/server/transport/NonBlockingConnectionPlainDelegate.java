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
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;

public class NonBlockingConnectionPlainDelegate implements NonBlockingConnectionDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnectionPlainDelegate.class);

    private final NonBlockingConnection _parent;
    private final int _networkBufferSize;
    private volatile QpidByteBuffer _netInputBuffer;

    public NonBlockingConnectionPlainDelegate(NonBlockingConnection parent, AmqpPort<?> port)
    {
        _parent = parent;
        _networkBufferSize = port.getNetworkBufferSize();
        _netInputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
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
        try (QpidByteBuffer oldNetInputBuffer = _netInputBuffer)
        {
            int unprocessedDataLength = _netInputBuffer.remaining();
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
            try (QpidByteBuffer currentBuffer = _netInputBuffer)
            {
                int newBufSize;

                if (currentBuffer.capacity() < _networkBufferSize)
                {
                    newBufSize = _networkBufferSize;
                }
                else
                {
                    newBufSize = currentBuffer.capacity() + _networkBufferSize;
                    _parent.reportUnexpectedByteBufferSizeUsage();
                }

                _netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
                _netInputBuffer.put(currentBuffer);
            }
        }

    }


    @Override
    public WriteResult doWrite(Collection<QpidByteBuffer> buffers) throws IOException
    {
        long bytesToWrite = 0L;
        if(!buffers.isEmpty())
        {
            for (QpidByteBuffer buf : buffers)
            {
                bytesToWrite += buf.remaining();
            }
        }
        if(bytesToWrite == 0L)
        {
            return new WriteResult(true, 0);
        }
        else
        {

            long bytesWritten = _parent.writeToTransport(buffers);
            return new WriteResult(bytesWritten >= bytesToWrite, bytesWritten);
        }

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
    public void shutdownInput()
    {
        if (_netInputBuffer != null)
        {
            _netInputBuffer.dispose();
            _netInputBuffer = null;
        }
    }

    @Override
    public void shutdownOutput()
    {

    }

    @Override
    public String getTransportInfo()
    {
        return "";
    }
}
