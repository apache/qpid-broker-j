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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class WebSocketFrameTransport extends FrameTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketFrameTransport.class);
    private static final int MAX_DECOMPRESSED_WEBSOCKET_MESSAGE_SIZE = 256 * 1024;
    private static final int MAX_WEBSOCKET_FRAME_SIZE = 2 * MAX_DECOMPRESSED_WEBSOCKET_MESSAGE_SIZE;

    private final AtomicInteger _firstBinaryWebSocketMessageSize = new AtomicInteger(-1);
    private final AtomicInteger _largestBinaryWebSocketMessageSize = new AtomicInteger();
    private final WebSocketFramingOutputHandler _webSocketFramingOutputHandler = new WebSocketFramingOutputHandler();
    private final WebSocketDeframingInputHandler _webSocketDeframingInputHandler =
            new WebSocketDeframingInputHandler(_firstBinaryWebSocketMessageSize, _largestBinaryWebSocketMessageSize);
    private final WebSocketClientHandler _webSocketClientHandler;
    private final boolean _compressionEnabled;

    public WebSocketFrameTransport(final BrokerAdmin brokerAdmin)
    {
        this(brokerAdmin, false);
    }

    WebSocketFrameTransport(final BrokerAdmin brokerAdmin, final boolean compressionEnabled)
    {
        super(brokerAdmin, BrokerAdmin.PortType.ANONYMOUS_AMQPWS);
        _compressionEnabled = compressionEnabled;
        final URI uri = URI.create(String.format("tcp://%s:%d/",
                                                 getBrokerAddress().getHostString(),
                                                 getBrokerAddress().getPort()));
        _webSocketClientHandler = new WebSocketClientHandler(
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, "amqp", compressionEnabled, new DefaultHttpHeaders(),
                        MAX_WEBSOCKET_FRAME_SIZE));
    }

    @Override
    protected void buildInputOutputPipeline(final ChannelPipeline pipeline)
    {
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        if (_compressionEnabled)
        {
            pipeline.addLast(new WebSocketClientCompressionHandler(MAX_DECOMPRESSED_WEBSOCKET_MESSAGE_SIZE));
        }
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

    WebSocketFrameTransport withholdWebSocketCloseResponse()
    {
        _webSocketClientHandler.withholdWebSocketCloseResponse();
        return this;
    }

    boolean awaitWebSocketCloseFrame(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return _webSocketClientHandler.awaitWebSocketCloseFrame(timeout, unit);
    }

    boolean isChannelOutputOpen()
    {
        return _webSocketClientHandler.isChannelOutputOpen();
    }

    String getNegotiatedExtensions()
    {
        return _webSocketClientHandler.getNegotiatedExtensions();
    }

    int getFirstBinaryWebSocketMessageSize()
    {
        return _firstBinaryWebSocketMessageSize.get();
    }

    int getLargestBinaryWebSocketMessageSize()
    {
        return _largestBinaryWebSocketMessageSize.get();
    }

    private static class WebSocketFramingOutputHandler extends ChannelOutboundHandlerAdapter
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

    private static class WebSocketDeframingInputHandler extends ChannelInboundHandlerAdapter
    {
        private final AtomicInteger _firstBinaryWebSocketMessageSize;
        private final AtomicInteger _largestBinaryWebSocketMessageSize;
        private int _currentBinaryWebSocketMessageSize = -1;

        private WebSocketDeframingInputHandler(final AtomicInteger firstBinaryWebSocketMessageSize,
                                               final AtomicInteger largestBinaryWebSocketMessageSize)
        {
            _firstBinaryWebSocketMessageSize = firstBinaryWebSocketMessageSize;
            _largestBinaryWebSocketMessageSize = largestBinaryWebSocketMessageSize;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg)
        {
            if (msg instanceof WebSocketFrame)
            {
                final WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof BinaryWebSocketFrame)
                {
                    _currentBinaryWebSocketMessageSize = frame.content().readableBytes();
                }
                else if (frame instanceof ContinuationWebSocketFrame && _currentBinaryWebSocketMessageSize >= 0)
                {
                    _currentBinaryWebSocketMessageSize += frame.content().readableBytes();
                }
                if (_currentBinaryWebSocketMessageSize >= 0 &&
                        (frame instanceof BinaryWebSocketFrame || frame instanceof ContinuationWebSocketFrame) &&
                        frame.isFinalFragment())
                {
                    _firstBinaryWebSocketMessageSize.compareAndSet(-1, _currentBinaryWebSocketMessageSize);
                    _largestBinaryWebSocketMessageSize.accumulateAndGet(_currentBinaryWebSocketMessageSize, Math::max);
                    _currentBinaryWebSocketMessageSize = -1;
                }
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

    public static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object>
    {

        private final WebSocketClientHandshaker _handshaker;
        private final CountDownLatch _webSocketCloseFrameReceived = new CountDownLatch(1);
        private ChannelPromise _handshakeFuture;
        private volatile String _negotiatedExtensions;
        private volatile Channel _channel;
        private volatile boolean _withholdWebSocketCloseResponse;

        WebSocketClientHandler(final WebSocketClientHandshaker handshaker)
        {
            _handshaker = handshaker;
        }

        ChannelFuture handshakeFuture()
        {
            return _handshakeFuture;
        }

        String getNegotiatedExtensions()
        {
            return _negotiatedExtensions;
        }

        void withholdWebSocketCloseResponse()
        {
            _withholdWebSocketCloseResponse = true;
        }

        boolean awaitWebSocketCloseFrame(final long timeout, final TimeUnit unit) throws InterruptedException
        {
            return _webSocketCloseFrameReceived.await(timeout, unit);
        }

        boolean isChannelOutputOpen()
        {
            final Channel channel = _channel;
            return channel instanceof DuplexChannel && !((DuplexChannel) channel).isOutputShutdown();
        }

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx)
        {
            _handshakeFuture = ctx.newPromise();
            _channel = ctx.channel();
            if (_withholdWebSocketCloseResponse)
            {
                // Keep the client output open when the broker closes its output. Otherwise Netty completes the TCP
                // shutdown on behalf of the test peer and accidentally satisfies the server-side closing handshake
                _channel.config().setOption(ChannelOption.ALLOW_HALF_CLOSURE, true);
            }
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx)
        {
            _handshaker.handshake(ctx.channel());
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg)
        {
            final Channel ch = ctx.channel();
            if (!_handshaker.isHandshakeComplete())
            {
                // web socket client connected
                final FullHttpResponse response = (FullHttpResponse) msg;
                _negotiatedExtensions = response.headers().get(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS);
                _handshaker.finishHandshake(ch, response);
                _handshakeFuture.setSuccess();
                return;
            }

            if (msg instanceof FullHttpResponse)
            {
                final FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(String.format("Unexpected FullHttpResponse (getStatus=%s, content=%s)",
                                                  response.content().toString(StandardCharsets.UTF_8), response.status()));
            }

            final WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof CloseWebSocketFrame)
            {
                _webSocketCloseFrameReceived.countDown();
                if (_withholdWebSocketCloseResponse)
                {
                    // consume the frame without writing the peer's WebSocket CLOSE response
                    return;
                }
            }
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
