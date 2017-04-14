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

import java.util.concurrent.BlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.ConnectionHandler;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.framing.FrameHandler;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.SaslFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslChallenge;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class InputHandler extends ChannelInboundHandlerAdapter
{
    private enum ParsingState
    {
        HEADER,
        PERFORMATIVES
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(InputHandler.class);
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                            .registerTransportLayer()
                                                                                            .registerMessagingLayer()
                                                                                            .registerTransactionLayer()
                                                                                            .registerSecurityLayer();
    private final ValueHandler _valueHandler;
    private final FrameHandler _frameHandler;

    private QpidByteBuffer _inputBuffer = QpidByteBuffer.allocate(0);
    private BlockingQueue<Response> _responseQueue;
    private ParsingState _state = ParsingState.HEADER;

    public InputHandler(final BlockingQueue<Response> queue)
    {

        _valueHandler = new ValueHandler(TYPE_REGISTRY);
        _frameHandler = new FrameHandler(_valueHandler, new MyConnectionHandler(), false);

        _responseQueue = queue;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
    {
        // TODO does Netty take care of saving the remaining bytes???
        ByteBuf buf = (ByteBuf) msg;
        QpidByteBuffer qpidBuf = QpidByteBuffer.wrap(buf.nioBuffer());

        if (_inputBuffer.hasRemaining())
        {
            QpidByteBuffer old = _inputBuffer;
            _inputBuffer = QpidByteBuffer.allocate(_inputBuffer.remaining() + qpidBuf.remaining());
            _inputBuffer.put(old);
            _inputBuffer.put(qpidBuf);
            old.dispose();
            qpidBuf.dispose();
            _inputBuffer.flip();
        }
        else
        {
            _inputBuffer.dispose();
            _inputBuffer = qpidBuf;
        }

        doParsing();

        if (_inputBuffer.hasRemaining())
        {
            _inputBuffer.compact();
        }

        ReferenceCountUtil.release(msg);
    }

    private void doParsing()
    {
        switch(_state)
        {
            case HEADER:
                if (_inputBuffer.remaining() >= 8)
                {
                    byte[] header = new byte[8];
                    _inputBuffer.get(header);
                    _responseQueue.add(new HeaderResponse(header));
                    _state = ParsingState.PERFORMATIVES;
                    doParsing();
                }
                break;
            case PERFORMATIVES:
                _frameHandler.parse(_inputBuffer);
                break;
            default:
                throw new IllegalStateException("Unexpected state : " + _state);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception
    {
        LOGGER.debug("KWDEBUG channelReadComplete");
        super.channelReadComplete(ctx);
    }

    private class MyConnectionHandler implements ConnectionHandler
    {
        @Override
        public void receiveOpen(final short channel, final Open close)
        {
            System.out.println();
        }

        @Override
        public void receiveClose(final short channel, final Close close)
        {

        }

        @Override
        public void receiveBegin(final short channel, final Begin begin)
        {

        }

        @Override
        public void receiveEnd(final short channel, final End end)
        {

        }

        @Override
        public void receiveAttach(final short channel, final Attach attach)
        {

        }

        @Override
        public void receiveDetach(final short channel, final Detach detach)
        {

        }

        @Override
        public void receiveTransfer(final short channel, final Transfer transfer)
        {

        }

        @Override
        public void receiveDisposition(final short channel, final Disposition disposition)
        {

        }

        @Override
        public void receiveFlow(final short channel, final Flow flow)
        {

        }

        @Override
        public int getMaxFrameSize()
        {
            return 512;
        }

        @Override
        public int getChannelMax()
        {
            return UnsignedShort.MAX_VALUE.intValue();
        }

        @Override
        public void handleError(final Error parsingError)
        {

        }

        @Override
        public boolean closedForInput()
        {
            return false;
        }

        @Override
        public void receive(final short channel, final Object val)
        {
            Response response;
            if (val instanceof FrameBody)
            {
                response = new PerformativeResponse(channel, (FrameBody) val);
            }
            else if (val instanceof SaslFrameBody)
            {
                throw new UnsupportedOperationException("TODO: ");
            }
            else
            {
                throw new UnsupportedOperationException("Unexoected frame type : " + val.getClass());
            }
            _responseQueue.add(response);

        }

        @Override
        public void receiveSaslInit(final SaslInit saslInit)
        {

        }

        @Override
        public void receiveSaslMechanisms(final SaslMechanisms saslMechanisms)
        {

        }

        @Override
        public void receiveSaslChallenge(final SaslChallenge saslChallenge)
        {

        }

        @Override
        public void receiveSaslResponse(final SaslResponse saslResponse)
        {

        }

        @Override
        public void receiveSaslOutcome(final SaslOutcome saslOutcome)
        {

        }
    }
}
