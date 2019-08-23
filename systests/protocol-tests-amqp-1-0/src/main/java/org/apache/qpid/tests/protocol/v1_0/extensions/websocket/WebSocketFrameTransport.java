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

package org.apache.qpid.tests.protocol.v1_0.extensions.websocket;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class WebSocketFrameTransport extends FrameTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketFrameTransport.class);

    private final WebSocketFramingOutputHandler _webSocketFramingOutputHandler = new WebSocketFramingOutputHandler();
    private final WebSocketDeframingInputHandler _webSocketDeframingInputHandler = new WebSocketDeframingInputHandler();
    private final WebSocketClientHandler _webSocketClientHandler;

    public WebSocketFrameTransport(final BrokerAdmin brokerAdmin)
    {
        super(brokerAdmin, BrokerAdmin.PortType.ANONYMOUS_AMQPWS);
        URI uri = URI.create(String.format("tcp://%s:%d/",
                                           getBrokerAddress().getHostString(),
                                           getBrokerAddress().getPort()));
        _webSocketClientHandler = new WebSocketClientHandler(
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, "amqp", false, new DefaultHttpHeaders()));
    }

    @Override
    protected void buildInputOutputPipeline(final ChannelPipeline pipeline)
    {
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        pipeline.addLast(_webSocketClientHandler);
        pipeline.addLast(_webSocketFramingOutputHandler);
        pipeline.addLast(_webSocketDeframingInputHandler);
        super.buildInputOutputPipeline(pipeline);
    }

    @Override
    public WebSocketFrameTransport connect()
    {
        super.connect();
        _webSocketClientHandler.handshakeFuture().syncUninterruptibly();
        return this;
    }

    WebSocketFrameTransport splitAmqpFrames()
    {
        _webSocketFramingOutputHandler.splitAmqpFrames();
        return this;
    }

    private class WebSocketFramingOutputHandler extends ChannelOutboundHandlerAdapter
    {
        private boolean _splitFrames;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        {
            if (msg instanceof ByteBuf)
            {
                final ByteBuf buf = ((ByteBuf) msg).retain();

                if (_splitFrames)
                {
                    while(buf.isReadable())
                    {

                        byte b = buf.readByte();
                        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {b}));
                        if (buf.isReadable())
                        {
                            ctx.writeAndFlush(frame);
                        }
                        else
                        {
                            ctx.writeAndFlush(frame, promise);
                        }
                    }

                    buf.release();
                }
                else
                {
                    BinaryWebSocketFrame frame = new BinaryWebSocketFrame((ByteBuf) msg);
                    ctx.writeAndFlush(frame, promise);
                }
            }
            else
            {
                ctx.writeAndFlush(msg, promise);
            }
        }

        void splitAmqpFrames()
        {
            _splitFrames = true;
        }
    }

    private class WebSocketDeframingInputHandler extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
        {
            if (msg instanceof WebSocketFrame)
            {
                WebSocketFrame frame = (WebSocketFrame) msg;
                ctx.fireChannelRead(frame.content());
            }
            else
            {
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
        {
            ctx.flush();
        }
    }

    public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object>
    {

        private final WebSocketClientHandshaker _handshaker;
        private ChannelPromise _handshakeFuture;

        WebSocketClientHandler(final WebSocketClientHandshaker handshaker)
        {
            _handshaker = handshaker;
        }

        ChannelFuture handshakeFuture()
        {
            return _handshakeFuture;
        }

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx)
        {
            _handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx)
        {
            _handshaker.handshake(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg)
        {
            final Channel ch = ctx.channel();
            if (!_handshaker.isHandshakeComplete())
            {
                // web socket client connected
                _handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                _handshakeFuture.setSuccess();
                return;
            }

            if (msg instanceof FullHttpResponse)
            {
                final FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(String.format("Unexpected FullHttpResponse (getStatus=%s, content=%s)",
                                                  response.content().toString(StandardCharsets.UTF_8), response.status()));
            }

            WebSocketFrame frame = (WebSocketFrame) msg;
            ctx.fireChannelRead(frame.retain());
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        {
            LOGGER.error("exceptionCaught", cause);

            if (!_handshakeFuture.isDone())
            {
                _handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }
    }

}
