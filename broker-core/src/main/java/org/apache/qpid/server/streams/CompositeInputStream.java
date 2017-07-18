/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.qpid.server.streams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Allows a series of input streams to be treated as if they were one.
 * NotThreadSafe
 */
public class CompositeInputStream extends InputStream
{
    private final LinkedList<InputStream> _inputStreams;
    private InputStream _current = null;

    public CompositeInputStream(Collection<InputStream> streams)
    {
        if (streams == null)
        {
            throw new IllegalArgumentException("streams cannot be null");
        }
        _inputStreams = new LinkedList<>(streams);
    }

    @Override
    public int read() throws IOException
    {
        int count = -1;
        if (_current != null)
        {
            count = _current.read();
        }
        if (count == -1 && _inputStreams.size() > 0)
        {
            if (_current != null)
            {
                _current.close();
            }
            _current = _inputStreams.removeFirst();
            count = read();
        }
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int count = -1;
        if (_current != null)
        {
            count = _current.read(b, off, len);
        }

        if (count < len && _inputStreams.size() > 0)
        {
            if (_current != null)
            {
                _current.close();
            }

            _current = _inputStreams.removeFirst();
            int numRead = count <= 0 ? 0 : count;

            int recursiveCount = read(b, off + numRead, len - numRead);

            if (recursiveCount == -1 && count == -1)
            {
                count = -1;
            }
            else if (recursiveCount == -1)
            {
                count = numRead;
            }
            else
            {
                count = recursiveCount + numRead;
            }
        }
        return count;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int available() throws IOException
    {

        int available = 0;
        if (_current != null)
        {
            available = _current.available();
        }
        if (_inputStreams != null)
        {
            for (InputStream is : _inputStreams)
            {
                if (is != null)
                {
                    available += is.available();
                }
            }
        }
        return available;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public void mark(final int readlimit)
    {
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public void close() throws IOException
    {
        IOException ioException = null;
        try
        {
            if (_current != null)
            {
                try
                {
                    _current.close();
                    _current = null;
                }
                catch (IOException e)
                {
                    ioException = e;
                }
            }
            for (InputStream is : _inputStreams)
            {
                try
                {
                    is.close();
                }
                catch (IOException e)
                {
                    if (ioException != null)
                    {
                        ioException = e;
                    }
                }
            }
        }
        finally
        {
            if (ioException != null)
            {
                throw ioException;
            }
        }
    }

}
