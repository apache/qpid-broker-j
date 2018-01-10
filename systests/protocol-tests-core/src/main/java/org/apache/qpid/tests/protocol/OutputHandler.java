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
import java.util.LinkedList;
import java.util.Queue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class OutputHandler extends ChannelOutboundHandlerAdapter
{
    private final OutputEncoder _outputEncoder;
    private Queue<ByteBufferPromisePair> _cachedEncodedFramePromisePairs;
    private int _encodedSize;

    OutputHandler(final OutputEncoder outputEncoder)
    {
        _outputEncoder = outputEncoder;
        _cachedEncodedFramePromisePairs = new LinkedList<>();
        _encodedSize = 0;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception
    {
        ByteBuffer byteBuffer = _outputEncoder.encode(msg);
        if (byteBuffer != null)
        {
            send(ctx, byteBuffer, promise);
        }
        else if (msg instanceof ByteBuf)
        {
            ByteBuf buf = (ByteBuf) msg;
            final ByteBuffer bytes = ByteBuffer.allocate(buf.readableBytes());
            buf.readBytes(bytes.array());
            buf.release();

            send(ctx, bytes, promise);
        }
        else
        {
            super.write(ctx, msg, promise);
        }
    }

    private synchronized void send(ChannelHandlerContext ctx, final ByteBuffer dataByteBuffer, final ChannelPromise promise)
    {
        _cachedEncodedFramePromisePairs.add(new ByteBufferPromisePair(dataByteBuffer, promise));
        _encodedSize += dataByteBuffer.remaining();
    }


    @Override
    public synchronized void flush(final ChannelHandlerContext ctx) throws Exception
    {
        final ChannelPromise promise = ctx.channel().newPromise();
        byte[] data  = new byte[_encodedSize];

        int offset = 0;
        while(offset < _encodedSize)
        {
            ByteBufferPromisePair currentPair = _cachedEncodedFramePromisePairs.poll();
            int remaining = currentPair.byteBuffer.remaining();
            currentPair.byteBuffer.get(data, offset, remaining) ;
            offset += remaining;

            promise.addListener(future -> {
                if (future.isSuccess())
                {
                    currentPair.channelPromise.setSuccess();
                }
                else
                {
                    currentPair.channelPromise.setFailure(future.cause());
                }
            });
        }

        _encodedSize = 0;

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

        super.flush(ctx);
    }

    class ByteBufferPromisePair
    {
        private ByteBuffer byteBuffer;
        private ChannelPromise channelPromise;

        ByteBufferPromisePair(final ByteBuffer byteBuffer, final ChannelPromise channelPromise)
        {
            this.byteBuffer = byteBuffer;
            this.channelPromise = channelPromise;
        }
    }

}
