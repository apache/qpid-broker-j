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

public class NonBlockingConnectionPlainDelegate implements NonBlockingConnectionDelegate
{
    private final NonBlockingConnection _parent;

    public NonBlockingConnectionPlainDelegate(NonBlockingConnection parent)
    {
        _parent = parent;
    }

    @Override
    public boolean doRead() throws IOException
    {
        return _parent.readAndProcessData();
    }

    @Override
    public boolean processData(ByteBuffer buffer)
    {
        _parent.processAmqpData(buffer);
        buffer.position(buffer.limit());
        return false;
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
}
