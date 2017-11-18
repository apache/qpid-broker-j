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
package org.apache.qpid.tests.protocol;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class OutputHandler extends ChannelOutboundHandlerAdapter
{
    private final OutputEncoder _outputEncoder;

    OutputHandler(final OutputEncoder outputEncoder)
    {
        _outputEncoder = outputEncoder;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception
    {
        ByteBuffer byteBuffer = _outputEncoder.encode(msg);
        if (byteBuffer != null)
        {
            send(ctx, byteBuffer, promise);
        }
        else
        {
            super.write(ctx, msg, promise);
        }
    }

    private void send(ChannelHandlerContext ctx, final ByteBuffer dataByteBuffer, final ChannelPromise promise)
    {
        byte[] data = new byte[dataByteBuffer.remaining()];
        dataByteBuffer.get(data);
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(data);
        try
        {
            OutputHandler.super.write(ctx, buffer, promise);
        }
        catch (Exception e)
        {
            promise.setFailure(e);
        }
    }
}
