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
 *
 */

package org.apache.qpid.tests.protocol.v1_0;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.framing.AMQFrame;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.transport.ByteBufferSender;

public class OutputHandler extends ChannelOutboundHandlerAdapter
{
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                            .registerTransportLayer()
                                                                                            .registerMessagingLayer()
                                                                                            .registerTransactionLayer()
                                                                                            .registerSecurityLayer()
                                                                                            .registerExtensionSoleconnLayer();


    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception
    {

        if (msg instanceof AMQFrame)
        {
            FrameWriter _frameWriter = new FrameWriter(TYPE_REGISTRY, new ByteBufferSender()
            {
                @Override
                public boolean isDirectBufferPreferred()
                {
                    return false;
                }

                @Override
                public void send(final QpidByteBuffer msg)
                {
                    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
                    buffer.writeBytes(msg.asByteBuffer());
                    try
                    {
                        OutputHandler.super.write(ctx, buffer, promise);
                    }
                    catch (Exception e)
                    {
                        promise.setFailure(e);
                    }
                }

                @Override
                public void flush()
                {
                }

                @Override
                public void close()
                {

                }
            });
            _frameWriter.send(((AMQFrame) msg));
        }
        else
        {
            super.write(ctx, msg, promise);
        }
    }


}
