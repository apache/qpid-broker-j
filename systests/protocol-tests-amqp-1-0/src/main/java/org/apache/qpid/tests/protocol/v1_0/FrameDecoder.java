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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.ChannelFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.HeaderResponse;
import org.apache.qpid.tests.protocol.InputDecoder;
import org.apache.qpid.tests.protocol.Response;

public class FrameDecoder implements InputDecoder
{
    private static final Logger FRAME_LOGGER = LoggerFactory.getLogger("amqp.frame");
    private static final Logger LOGGER = LoggerFactory.getLogger(FrameDecoder.class);
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                            .registerTransportLayer()
                                                                                            .registerMessagingLayer()
                                                                                            .registerTransactionLayer()
                                                                                            .registerSecurityLayer()
                                                                                            .registerExtensionSoleconnLayer();
    private final MyConnectionHandler _connectionHandler;
    private volatile FrameHandler _frameHandler;

    private enum ParsingState
    {
        HEADER,
        PERFORMATIVES;
    }

    private final ValueHandler _valueHandler;

    private volatile ParsingState _state = ParsingState.HEADER;

    public FrameDecoder(final boolean isSasl)
    {
        _valueHandler = new ValueHandler(TYPE_REGISTRY);
        _connectionHandler = new MyConnectionHandler();
        _frameHandler = new FrameHandler(_valueHandler, _connectionHandler, isSasl);
    }

    @Override
    public Collection<Response<?>> decode(final ByteBuffer inputBuffer)
    {

        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(inputBuffer);
        int remaining;

        do
        {
            remaining = qpidByteBuffer.remaining();
            switch(_state)
            {
                case HEADER:
                    if (inputBuffer.remaining() >= 8)
                    {
                        byte[] header = new byte[8];
                        inputBuffer.get(header);

                        HeaderResponse headerResponse = new HeaderResponse(header);
                        FRAME_LOGGER.debug("RECV:" + headerResponse);
                        _connectionHandler._responseQueue.add(headerResponse);
                        _state = ParsingState.PERFORMATIVES;
                    }
                    break;
                case PERFORMATIVES:
                    _frameHandler.parse(qpidByteBuffer);
                    break;
                default:
                    throw new IllegalStateException("Unexpected state : " + _state);
            }
        }
        while (qpidByteBuffer.remaining() != remaining);

        List<Response<?>> responses = new ArrayList<>();
        Response<?> r;
        while((r = _connectionHandler._responseQueue.poll())!=null)
        {
            responses.add(r);
        }
        return responses;
    }

    private void resetInputHandlerAfterSaslOutcome()
    {
        _state = ParsingState.HEADER;
        _frameHandler = new FrameHandler(_valueHandler, _connectionHandler, false);
    }

    private class MyConnectionHandler implements ConnectionHandler
    {
        private volatile int _frameSize = 512;
        private Queue<Response<?>> _responseQueue = new ConcurrentLinkedQueue<>();

        @Override
        public void receiveOpen(final int channel, final Open close)
        {
        }

        @Override
        public void receiveClose(final int channel, final Close close)
        {

        }

        @Override
        public void receiveBegin(final int channel, final Begin begin)
        {

        }

        @Override
        public void receiveEnd(final int channel, final End end)
        {

        }

        @Override
        public void receiveAttach(final int channel, final Attach attach)
        {

        }

        @Override
        public void receiveDetach(final int channel, final Detach detach)
        {

        }

        @Override
        public void receiveTransfer(final int channel, final Transfer transfer)
        {

        }

        @Override
        public void receiveDisposition(final int channel, final Disposition disposition)
        {

        }

        @Override
        public void receiveFlow(final int channel, final Flow flow)
        {

        }

        @Override
        public int getMaxFrameSize()
        {
            return _frameSize;
        }

        @Override
        public int getChannelMax()
        {
            return UnsignedShort.MAX_VALUE.intValue();
        }

        @Override
        public void handleError(final Error parsingError)
        {
            LOGGER.error("Unexpected error {}", parsingError);
            _responseQueue.add(new FrameDecodingErrorResponse(parsingError));
        }

        @Override
        public boolean closedForInput()
        {
            return false;
        }

        @Override
        public void receive(final List<ChannelFrameBody> channelFrameBodies)
        {
            for (final ChannelFrameBody channelFrameBody : channelFrameBodies)
            {
                Response response;
                Object val = channelFrameBody.getFrameBody();
                int channel = channelFrameBody.getChannel();
                if (val instanceof FrameBody)
                {
                    FrameBody frameBody = (FrameBody) val;
                    if (frameBody instanceof Open && ((Open) frameBody).getMaxFrameSize() != null && ((Open) frameBody).getMaxFrameSize().intValue() > 512)
                    {
                        _frameSize = ((Open) frameBody).getMaxFrameSize().intValue();
                    }
                    response = new PerformativeResponse((short) channel, frameBody);
                }
                else if (val instanceof SaslFrameBody)
                {
                    SaslFrameBody frameBody = (SaslFrameBody) val;
                    response = new SaslPerformativeResponse((short) channel, frameBody);

                    if (frameBody instanceof SaslOutcome && ((SaslOutcome) frameBody).getCode().equals(SaslCode.OK))
                    {
                        resetInputHandlerAfterSaslOutcome();
                    }
                }
                else if (val == null)
                {
                    response = new EmptyResponse();
                }
                else
                {
                    throw new UnsupportedOperationException("Unexpected frame type : " + val.getClass());
                }

                FRAME_LOGGER.debug("RECV:" + response.getBody());
                _responseQueue.add(response);
            }
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

    private static class FrameDecodingErrorResponse implements Response<Error>, FrameDecodingError
    {
        private final Error _error;

        FrameDecodingErrorResponse(final Error error)
        {
            _error = error;
        }

        @Override
        public Error getBody()
        {
            return null;
        }

        @Override
        public Error getError()
        {
            return _error;
        }
    }
}
