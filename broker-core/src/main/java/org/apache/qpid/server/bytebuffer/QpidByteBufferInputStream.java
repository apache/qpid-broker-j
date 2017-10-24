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
package org.apache.qpid.server.bytebuffer;

import java.io.IOException;
import java.io.InputStream;

final class QpidByteBufferInputStream extends InputStream
{
    private final QpidByteBuffer _qpidByteBuffer;

    QpidByteBufferInputStream(final QpidByteBuffer buffer)
    {
        _qpidByteBuffer = buffer.duplicate();
    }

    @Override
    public int read() throws IOException
    {
        if (_qpidByteBuffer.hasRemaining())
        {
            return _qpidByteBuffer.getUnsignedByte();
        }
        return -1;
    }


    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (!_qpidByteBuffer.hasRemaining())
        {
            return -1;
        }
        int remaining = _qpidByteBuffer.remaining();
        if (remaining < len)
        {
            len = remaining;
        }
        _qpidByteBuffer.get(b, off, len);

        return len;
    }

    @Override
    public void mark(int readlimit)
    {
        _qpidByteBuffer.mark();
    }

    @Override
    public void reset() throws IOException
    {
        _qpidByteBuffer.reset();
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    @Override
    public long skip(long n) throws IOException
    {
        _qpidByteBuffer.position(_qpidByteBuffer.position() + (int) n);
        return n;
    }

    @Override
    public int available() throws IOException
    {
        return _qpidByteBuffer.remaining();
    }

    @Override
    public void close()
    {
        _qpidByteBuffer.dispose();
    }
}
