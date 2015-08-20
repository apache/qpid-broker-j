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
package org.apache.qpid.util;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class ByteBufferUtils
{
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    public static ByteBuffer combine(Collection<QpidByteBuffer> bufs)
    {
        if(bufs == null || bufs.isEmpty())
        {
            return EMPTY_BYTE_BUFFER;
        }
        else
        {
            int size = 0;
            boolean isDirect = false;
            for(QpidByteBuffer buf : bufs)
            {
                size += buf.remaining();
                isDirect = isDirect || buf.isDirect();
            }
            ByteBuffer combined = isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);

            for(QpidByteBuffer buf : bufs)
            {
                buf.copyTo(combined);
            }
            combined.flip();
            return combined;
        }
    }

    public static int remaining(Collection<QpidByteBuffer> bufs)
    {
        int size = 0;
        if (bufs != null && !bufs.isEmpty())
        {
            for (QpidByteBuffer buf : bufs)
            {
                size += buf.remaining();
            }

        }
        return size;
    }
}
