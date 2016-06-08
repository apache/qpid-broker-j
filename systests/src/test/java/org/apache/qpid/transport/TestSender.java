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
package org.apache.qpid.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.qpid.bytebuffer.QpidByteBuffer;

class TestSender implements ByteBufferSender
{
    private final Collection<QpidByteBuffer> _sentBuffers = new ArrayList<>();
    private final OutputStream _output;


    TestSender(final OutputStream output)
    {
        _output = output;
    }

    @Override
    public boolean isDirectBufferPreferred()
    {
        return false;
    }

    @Override
    public void send(final QpidByteBuffer msg)
    {
        _sentBuffers.add(msg.duplicate());
        msg.position(msg.limit());
    }

    @Override
    public void flush()
    {
        int size = 0;
        for (QpidByteBuffer buf : _sentBuffers)
        {
            size += buf.remaining();
        }
        byte[] data = new byte[size];
        int offset = 0;
        for (QpidByteBuffer buf : _sentBuffers)
        {
            int bufSize = buf.remaining();
            buf.get(data, offset, bufSize);
            offset += bufSize;
            buf.dispose();
        }
        try
        {
            _output.write(data);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            _sentBuffers.clear();
        }
    }

    @Override
    public void close()
    {

    }
}
