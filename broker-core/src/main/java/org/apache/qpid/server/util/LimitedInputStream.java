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

package org.apache.qpid.server.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class LimitedInputStream extends FilterInputStream
{
    private long left;
    private long mark = -1;

    public LimitedInputStream(InputStream in, long limit)
    {
        super(in);
        Objects.requireNonNull(in);
        if (limit < 0)
        {
            throw new IllegalArgumentException("limit must be non-negative");
        };
        left = limit;
    }

    @Override
    public int available() throws IOException
    {
        return (int) Math.min(in.available(), left);
    }

    @Override
    public synchronized void mark(int readLimit)
    {
        in.mark(readLimit);
        mark = left;
    }

    @Override
    public int read() throws IOException
    {
        if (left == 0)
        {
            return -1;
        }

        int result = in.read();
        if (result != -1)
        {
            --left;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (left == 0)
        {
            return -1;
        }

        len = (int) Math.min(len, left);
        int result = in.read(b, off, len);
        if (result != -1)
        {
            left -= result;
        }
        return result;
    }

    @Override
    public synchronized void reset() throws IOException
    {
        if (!in.markSupported())
        {
            throw new IOException("Mark not supported");
        }
        if (mark == -1)
        {
            throw new IOException("Mark not set");
        }

        in.reset();
        left = mark;
    }

    @Override
    public long skip(long n) throws IOException
    {
        n = Math.min(n, left);
        long skipped = in.skip(n);
        left -= skipped;
        return skipped;
    }
}
