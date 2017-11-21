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
package org.apache.qpid.tests.protocol.v0_10;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.BBEncoder;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolEvent;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.tests.protocol.OutputEncoder;

public class FrameEncoder implements OutputEncoder
{
    @Override
    public ByteBuffer encode(final Object msg)
    {
        if (msg instanceof ProtocolEvent)
        {
            final List<ByteBuffer> buffers = new ArrayList<>();
            final AtomicInteger totalSize = new AtomicInteger();
            Disassembler disassembler = new Disassembler(new ByteBufferSender()
            {
                @Override
                public boolean isDirectBufferPreferred()
                {
                    return false;
                }

                @Override
                public void send(final QpidByteBuffer msg)
                {
                    int remaining = msg.remaining();
                    byte[] data = new byte[remaining];
                    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
                    msg.get(data);
                    buffers.add(byteBuffer);
                    totalSize.addAndGet(remaining);
                }

                @Override
                public void flush()
                {

                }

                @Override
                public void close()
                {

                }
            }, 512);

            disassembler.send((ProtocolEvent) msg);
            ByteBuffer data = ByteBuffer.allocate(totalSize.get());
            for (ByteBuffer buffer : buffers)
            {
                data.put(buffer);
            }
            data.flip();
            return data;
        }
        return null;
    }
}
