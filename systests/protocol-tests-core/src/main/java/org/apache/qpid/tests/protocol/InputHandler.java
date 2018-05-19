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
 */

package org.apache.qpid.tests.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InputHandler.class);

    private final BlockingQueue<Response<?>> _responseQueue;
    private final InputDecoder _inputDecoder;

    private ByteBuffer _inputBuffer = ByteBuffer.allocate(0);

    InputHandler(final BlockingQueue<Response<?>> queue, InputDecoder inputDecoder)
    {
        _responseQueue = queue;
        _inputDecoder = inputDecoder;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;
        ByteBuffer byteBuffer = ByteBuffer.allocate(buf.readableBytes());
        byteBuffer.put(buf.nioBuffer());
        byteBuffer.flip();
        LOGGER.debug("Incoming {} byte(s)", byteBuffer.remaining());

        if (_inputBuffer.hasRemaining())
        {
            ByteBuffer old = _inputBuffer;
            _inputBuffer = ByteBuffer.allocate(_inputBuffer.remaining() + byteBuffer.remaining());
            _inputBuffer.put(old);
            _inputBuffer.put(byteBuffer);
            _inputBuffer.flip();
        }
        else
        {
            _inputBuffer = byteBuffer;
        }

        _responseQueue.addAll(_inputDecoder.decode(_inputBuffer));

        LOGGER.debug("After parsing, {} byte(s) remained", _inputBuffer.remaining());

        ReferenceCountUtil.release(msg);
    }
}
