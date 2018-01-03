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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public abstract class AbstractFrameTransport<I extends AbstractInteraction<I>> implements AutoCloseable
{
    static final long RESPONSE_TIMEOUT =
            Long.getLong("qpid.tests.protocol.frameTransport.responseTimeout", 6000);
    private static final Response CHANNEL_CLOSED_RESPONSE = new ChannelClosedResponse();

    private final BlockingQueue<Response<?>> _queue = new ArrayBlockingQueue<>(1000);
    private final EventLoopGroup _workerGroup;
    private final InetSocketAddress _brokerAddress;
    private final InputHandler _inputHandler;
    private final OutputHandler _outputHandler;

    private volatile Channel _channel;
    private volatile boolean _channelClosedSeen = false;

    public AbstractFrameTransport(final InetSocketAddress brokerAddress, InputDecoder inputDecoder, OutputEncoder outputEncoder)
    {
        _brokerAddress = brokerAddress;
        _inputHandler = new InputHandler(_queue, inputDecoder);
        _outputHandler = new OutputHandler(outputEncoder);
        _workerGroup = new NioEventLoopGroup();
    }

    public InetSocketAddress getBrokerAddress()
    {
        return _brokerAddress;
    }

    public AbstractFrameTransport<I> connect()
    {
        try
        {
            Bootstrap b = new Bootstrap();
            b.group(_workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>()
            {
                @Override
                public void initChannel(SocketChannel ch) throws Exception
                {
                    ChannelPipeline pipeline = ch.pipeline();
                    buildInputOutputPipeline(pipeline);
                }
            });

            _channel = b.connect(_brokerAddress).sync().channel();
            _channel.closeFuture().addListener(future ->
                                               {
                                                   _channelClosedSeen = true;
                                                   _queue.add(CHANNEL_CLOSED_RESPONSE);
                                               });
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return this;
    }

    protected void buildInputOutputPipeline(final ChannelPipeline pipeline)
    {
        pipeline.addLast(_inputHandler).addLast(_outputHandler);
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            if (_channel != null)
            {
                _channel.disconnect().sync();
                _channel.close().sync();
                _channel = null;
            }
        }
        finally
        {
            _workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }

    ListenableFuture<Void> sendProtocolHeader(final byte[] bytes) throws Exception
    {
        return sendBytes(bytes);
    }

    public ListenableFuture<Void> sendBytes(final byte[] bytes)
    {
        Preconditions.checkState(_channel != null, "Not connected");
        ChannelPromise promise = _channel.newPromise();
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(bytes);
        _channel.write(buffer, promise);
        return JdkFutureAdapters.listenInPoolThread(promise);
    }

    public ListenableFuture<Void> sendPerformative(final Object data) throws Exception
    {
        Preconditions.checkState(_channel != null, "Not connected");
        ChannelPromise promise = _channel.newPromise();
        _channel.write(data, promise);
        return JdkFutureAdapters.listenInPoolThread(promise);
    }

    <T extends Response<?>> T getNextResponse() throws Exception
    {
        return (T) _queue.poll(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public void assertNoMoreResponses() throws Exception
    {
        Response response = getNextResponse();
        assertThat(response, anyOf(nullValue(), instanceOf(ChannelClosedResponse.class)));
    }

    public void assertNoMoreResponsesAndChannelClosed() throws Exception
    {
        assertNoMoreResponses();
        assertThat(_channelClosedSeen, is(true));
    }

    public void flush()
    {
        _channel.flush();
    }

    public abstract byte[] getProtocolHeader();

    public abstract I newInteraction();
}
