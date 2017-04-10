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

package org.apache.qpid.tests.protocol.v1_0;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNull;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

public class FrameTransport implements AutoCloseable
{
    private static final long RESPONSE_TIMEOUT = 6000;
    private static final Set<Integer> _amqpConnectionIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Channel _channel;
    private final BlockingQueue<Response> _queue = new ArrayBlockingQueue<>(100);
    private final EventLoopGroup _workerGroup;

    private int _amqpConnectionId;
    private short _amqpChannelId;

    public FrameTransport(final InetSocketAddress brokerAddress)
    {
        _workerGroup = new NioEventLoopGroup();

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
                    ch.pipeline().addLast(new InputHandler(_queue))
                                 .addLast(new OutputHandler());
                }
            });

            _channel = b.connect(brokerAddress).sync().channel();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            _channel.close().sync();
        }
        finally
        {
            _amqpConnectionIds.remove(_amqpConnectionId);
            _workerGroup.shutdownGracefully();
        }
    }

    public ListenableFuture<Void> sendProtocolHeader(final byte[] bytes) throws Exception
    {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(bytes);
        ChannelFuture channelFuture = _channel.writeAndFlush(buffer);
        channelFuture.sync();
        return JdkFutureAdapters.listenInPoolThread(channelFuture);
    }

    public ListenableFuture<Void> sendPerformative(final TransportFrame transportFrame) throws Exception
    {
        ChannelFuture channelFuture = _channel.writeAndFlush(transportFrame);
        channelFuture.sync();
        return JdkFutureAdapters.listenInPoolThread(channelFuture);
    }

    public ListenableFuture<Void> sendPerformative(final FrameBody frameBody) throws Exception
    {
        TransportFrame transportFrame = new TransportFrame(_amqpChannelId, frameBody);
        return sendPerformative(transportFrame);
    }

    public Response getNextResponse() throws Exception
    {
        return _queue.poll(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public void doProtocolNegotiation() throws Exception
    {
        byte[] bytes = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
        sendProtocolHeader(bytes);
        HeaderResponse response = (HeaderResponse) getNextResponse();

        if (!Arrays.equals(bytes, response.getHeader()))
        {
            throw new IllegalStateException("Unexpected protocol header");
        }
    }

    public void doOpenConnection() throws Exception
    {
        doProtocolNegotiation();
        Open open = new Open();

        open.setContainerId(String.format("testContainer-%d", getConnectionId()));
        TransportFrame transportFrame = new TransportFrame((short) 0, open);
        sendPerformative(transportFrame);
        PerformativeResponse response = (PerformativeResponse) getNextResponse();
        if (!(response.getFrameBody() instanceof Open))
        {
            throw new IllegalStateException("Unexpected response to connection Open");
        }
    }

    public void doBeginSession() throws Exception
    {
        doOpenConnection();
        Begin begin = new Begin();
        begin.setNextOutgoingId(UnsignedInteger.ZERO);
        begin.setIncomingWindow(UnsignedInteger.ZERO);
        begin.setOutgoingWindow(UnsignedInteger.ZERO);
        _amqpChannelId = (short) 1;
        sendPerformative(new TransportFrame(_amqpChannelId, begin));
        PerformativeResponse response = (PerformativeResponse) getNextResponse();
        if (!(response.getFrameBody() instanceof Begin))
        {
            throw new IllegalStateException("Unexpected response to connection Begin");
        }
    }

    public void doAttachReceivingLink(String queueName) throws Exception
    {
        doBeginSession();
        Role localRole = Role.RECEIVER;
        Attach attach = new Attach();
        attach.setName("testReceivingLink");
        attach.setHandle(new UnsignedInteger(0));
        attach.setRole(localRole);
        Source source = new Source();
        source.setAddress(queueName);
        attach.setSource(source);
        Target target = new Target();
        attach.setTarget(target);

        sendPerformative(attach);
        PerformativeResponse response = (PerformativeResponse) getNextResponse();

        assertThat(response, is(notNullValue()));
        assertThat(response.getFrameBody(), is(instanceOf(Attach.class)));
        Attach responseAttach = (Attach) response.getFrameBody();
        assertThat(responseAttach.getSource(), is(notNullValue()));
    }

    public void assertNoMoreResponses() throws Exception
    {
        Response response = getNextResponse();
        assertNull("Unexpected response.", response);
    }

    private int getConnectionId()
    {
        if (_amqpConnectionId == 0)
        {
            _amqpConnectionId = 1;
            while (!_amqpConnectionIds.add(_amqpConnectionId))
            {
                ++_amqpConnectionId;
            }
        }
        return _amqpConnectionId;
    }
}
