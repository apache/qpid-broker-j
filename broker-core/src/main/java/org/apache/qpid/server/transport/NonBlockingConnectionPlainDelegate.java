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

import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class NonBlockingConnectionPlainDelegate implements NonBlockingConnectionDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnectionPlainDelegate.class);

    private final SchedulableConnection _parent;
    private final int _networkBufferSize;
    private volatile QpidByteBuffer _netInputBuffer;

    public NonBlockingConnectionPlainDelegate(SchedulableConnection parent, int networkBufferSize)
    {
        _parent = parent;
        _networkBufferSize = networkBufferSize;
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
        QpidByteBuffer oldNetInputBuffer = _netInputBuffer;
        int unprocessedDataLength = _netInputBuffer.remaining();
        _netInputBuffer.limit(_netInputBuffer.capacity());
        _netInputBuffer = oldNetInputBuffer.slice();
        _netInputBuffer.limit(unprocessedDataLength);
        oldNetInputBuffer.dispose();
        if (_netInputBuffer.limit() != _netInputBuffer.capacity())
        {
            _netInputBuffer.position(_netInputBuffer.limit());
            _netInputBuffer.limit(_netInputBuffer.capacity());
        }
        else
        {
            QpidByteBuffer currentBuffer = _netInputBuffer;
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
            currentBuffer.dispose();
        }

    }


    @Override
    public boolean doWrite(Collection<QpidByteBuffer> bufferArray) throws IOException
    {
        long bytesToWrite = 0l;
        if(!bufferArray.isEmpty())
        {
            for (QpidByteBuffer buf : bufferArray)
            {
                bytesToWrite += buf.remaining();
            }
        }
        return bytesToWrite == 0l || _parent.writeToTransport(bufferArray) >= bytesToWrite;

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
