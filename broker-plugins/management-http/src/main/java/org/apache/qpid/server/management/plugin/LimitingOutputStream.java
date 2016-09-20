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
 *
 */

package org.apache.qpid.server.management.plugin;

import java.io.IOException;
import java.io.OutputStream;

public class LimitingOutputStream extends OutputStream
{
    private final OutputStream _outputStream;
    private final long _limit;
    private long _counter;

    public LimitingOutputStream(OutputStream outputStream, long limit)
    {
        _outputStream = outputStream;
        _limit = limit;
    }

    @Override
    public void write(final int b) throws IOException
    {
        if (_counter < _limit)
        {
            _outputStream.write(b);
            _counter++;
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException
    {
        if (_counter < _limit)
        {
            int written = Math.min(len, (int)(_limit - _counter));
            _outputStream.write(b, off, written);
            _counter += written;
        }
    }

    @Override
    public void write(final byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void flush() throws IOException
    {
        _outputStream.flush();
    }

    @Override
    public void close() throws IOException
    {
        _outputStream.close();
    }
}
