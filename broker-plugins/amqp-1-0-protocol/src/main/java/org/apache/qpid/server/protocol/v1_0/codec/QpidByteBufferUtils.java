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
package org.apache.qpid.server.protocol.v1_0.codec;

import java.nio.BufferUnderflowException;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class QpidByteBufferUtils
{
    public static boolean hasRemaining(List<QpidByteBuffer> in)
    {
        if (in.isEmpty())
        {
            return false;
        }
        for (int i = 0; i < in.size(); i++)
        {
            if (in.get(i).hasRemaining())
            {
                return true;
            }
        }
        return false;
    }

    public static long remaining(List<QpidByteBuffer> in)
    {
        long remaining = 0L;
        for (int i = 0; i < in.size(); i++)
        {
            remaining += in.get(i).remaining();
        }
        return remaining;
    }

    public static byte get(List<QpidByteBuffer> in)
    {
        for (int i = 0; i < in.size(); i++)
        {
            final QpidByteBuffer buffer = in.get(i);
            if (buffer.hasRemaining())
            {
                return buffer.get();
            }
        }
        throw new BufferUnderflowException();
    }

    public static boolean hasRemaining(final List<QpidByteBuffer> in, int len)
    {
        for (int i = 0; i < in.size(); i++)
        {
            final QpidByteBuffer buffer = in.get(i);
            int remaining = buffer.remaining();
            if (remaining >= len)
            {
                return true;
            }
            len -= remaining;
        }

        return false;
    }

    public static long getLong(final List<QpidByteBuffer> in)
    {
        boolean bytewise = false;
        int consumed = 0;
        long result = 0L;
        for (int i = 0; i < in.size(); i++)
        {
            final QpidByteBuffer buffer = in.get(i);
            int remaining = buffer.remaining();
            if (bytewise)
            {
                while (buffer.hasRemaining() && consumed < 8)
                {
                    result <<= 1;
                    result |= (0xFF & buffer.get());
                    consumed++;
                }
                if (consumed == 8)
                {
                    return result;
                }
            }
            else
            {
                if (remaining >= 8)
                {
                    return buffer.getLong();
                }
                else if (remaining != 0)
                {
                    bytewise = true;
                    while (buffer.hasRemaining())
                    {
                        result <<= 1;
                        result |= (0xFF & buffer.get());
                        consumed++;
                    }
                }
            }
        }
        throw new BufferUnderflowException();
    }

    public static int getInt(final List<QpidByteBuffer> in)
    {
        boolean bytewise = false;
        int consumed = 0;
        int result = 0;
        for (int i = 0; i < in.size(); i++)
        {
            final QpidByteBuffer buffer = in.get(i);
            int remaining = buffer.remaining();
            if (bytewise)
            {
                while (buffer.hasRemaining() && consumed < 4)
                {
                    result <<= 1;
                    result |= (0xFF & buffer.get());
                    consumed++;
                }
                if (consumed == 4)
                {
                    return result;
                }
            }
            else
            {
                if (remaining >= 4)
                {
                    return buffer.getInt();
                }
                else if (remaining != 0)
                {
                    bytewise = true;
                    while (buffer.hasRemaining())
                    {
                        result <<= 1;
                        result |= (0xFF & buffer.get());
                        consumed++;
                    }
                }
            }
        }
        throw new BufferUnderflowException();
    }

    public static float getFloat(final List<QpidByteBuffer> in)
    {
        return Float.intBitsToFloat(getInt(in));
    }

    public static double getDouble(final List<QpidByteBuffer> in)
    {
        return Double.longBitsToDouble(getLong(in));
    }

    public static Short getShort(final List<QpidByteBuffer> in)
    {
        boolean bytewise = false;
        int consumed = 0;
        short result = 0;
        for (int i = 0; i < in.size(); i++)
        {
            final QpidByteBuffer buffer = in.get(i);
            int remaining = buffer.remaining();
            if (bytewise)
            {
                while (buffer.hasRemaining() && consumed < 2)
                {
                    result <<= 1;
                    result |= (0xFF & buffer.get());
                    consumed++;
                }
                if (consumed == 2)
                {
                    return result;
                }
            }
            else
            {
                if (remaining >= 2)
                {
                    return buffer.getShort();
                }
                else if (remaining != 0)
                {
                    bytewise = true;
                    while (buffer.hasRemaining())
                    {
                        result <<= 1;
                        result |= (0xFF & buffer.get());
                        consumed++;
                    }
                }
            }
        }
        throw new BufferUnderflowException();
    }

    public static int get(final List<QpidByteBuffer> in, final byte[] data)
    {
        int copied = 0;
        int i = 0;
        while (copied < data.length && i < in.size())
        {
            QpidByteBuffer buf = in.get(i);
            if (buf.hasRemaining())
            {
                int remaining = buf.remaining();
                if (remaining >= data.length - copied)
                {
                    buf.get(data, copied, data.length - copied);
                    return data.length;
                }
                else
                {
                    buf.get(data, copied, remaining);
                    copied += remaining;
                }
            }
            i++;
        }
        return copied;
    }

    public static void skip(final List<QpidByteBuffer> in, int length)
    {
        int skipped = 0;
        int i = 0;
        while (skipped < length && i < in.size())
        {
            QpidByteBuffer buf = in.get(i);
            if (buf.hasRemaining())
            {
                int remaining = buf.remaining();
                if (remaining >= length - skipped)
                {
                    buf.position(buf.position() + length - skipped);
                    return;
                }
                else
                {
                    buf.position(buf.position() + remaining);
                    skipped += remaining;
                }
            }
            i++;
        }
    }
}
