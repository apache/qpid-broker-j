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
package org.apache.qpid.server.protocol.v1_0.framing;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.ConnectionHandler;
import org.apache.qpid.server.protocol.v1_0.codec.ProtocolHandler;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ChannelFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class FrameHandler implements ProtocolHandler
{
    public static final Logger LOGGER = LoggerFactory.getLogger(FrameHandler.class);
    private final boolean _isSasl;

    private final ConnectionHandler _connectionHandler;
    private final ValueHandler _valueHandler;
    private boolean _errored = false;

    public FrameHandler(final ValueHandler valueHandler, final ConnectionHandler connectionHandler, final boolean isSasl)
    {
        _valueHandler = valueHandler;
        _connectionHandler = connectionHandler;
        _isSasl = isSasl;
    }

    @Override
    public ProtocolHandler parse(QpidByteBuffer in)
    {
        try
        {
            LOGGER.debug("RECV {} bytes", in.remaining());
            Error frameParsingError = null;
            int size;

            int remaining;
            List<ChannelFrameBody> channelFrameBodies = new ArrayList<>();
            while ((remaining = in.remaining()) >=8 && frameParsingError == null)
            {

                size = in.getInt();

                if (size < 8)
                {
                    frameParsingError = createFramingError(
                            "specified frame size %d smaller than minimum frame header size %d",
                            size,
                            8);
                    break;
                }

                if(size > _connectionHandler.getMaxFrameSize())
                {
                    frameParsingError = createFramingError(
                            "specified frame size %d larger than maximum frame header size %d",
                            size,
                            _connectionHandler.getMaxFrameSize());
                    break;
                }

                if(remaining < size)
                {
                    in.position(in.position()-4);
                    break;
                }
                int dataOffset = (in.get() << 2) & 0x3FF;

                if (dataOffset < 8)
                {
                    frameParsingError = createFramingError(
                            "specified frame data offset %d smaller than minimum frame header size %d",
                            dataOffset,
                            8);
                    break;
                }

                if (dataOffset > size)
                {
                    frameParsingError = createFramingError(
                            "specified frame data offset %d larger than the frame size %d",
                            dataOffset,
                            size);
                    break;
                }


                byte type = in.get();

                switch(type)
                {
                    case 0:
                        if(_isSasl)
                        {
                            frameParsingError = createFramingError("received an AMQP frame type when expecting an SASL frame");
                        }
                        break;
                    case 1:
                        if(!_isSasl)
                        {
                            frameParsingError = createFramingError("received a SASL frame type when expecting an AMQP frame");
                        }
                        break;
                    default:
                        frameParsingError = createFramingError("unknown frame type: %d", type);
                }

                if(frameParsingError != null)
                {
                    break;
                }

                int channel = in.getUnsignedShort();

                if (dataOffset != 8)
                {
                    in.position(in.position() + dataOffset - 8);
                }

                try (QpidByteBuffer dup = in.slice())
                {
                    dup.limit(size - dataOffset);
                    in.position(in.position() + size - dataOffset);

                    final boolean hasFrameBody = dup.hasRemaining();
                    Object frameBody;
                    if (hasFrameBody)
                    {
                        frameBody = _valueHandler.parse(dup);
                        if (dup.hasRemaining())
                        {
                            if (frameBody instanceof Transfer)
                            {
                                try (QpidByteBuffer payload = dup.slice())
                                {
                                    ((Transfer) frameBody).setPayload(payload);
                                }
                            }
                            else
                            {
                                frameParsingError = createFramingError(
                                        "Frame length %d larger than contained frame body %s.",
                                        size,
                                        frameBody);
                                break;
                            }
                        }
                    }
                    else
                    {
                        frameBody = null;
                        if(_isSasl)
                        {
                            frameParsingError = createFramingError(
                                    "Empty (heartbeat) frames are not permitted during SASL negotiation");
                            break;
                        }
                    }

                    channelFrameBodies.add(new ChannelFrameBody()
                    {
                        @Override
                        public int getChannel()
                        {
                            return channel;
                        }

                        @Override
                        public Object getFrameBody()
                        {
                            return frameBody;
                        }
                    });

                    if (_isSasl)
                    {
                        break;
                    }
                }
                catch (AmqpErrorException ex)
                {
                    frameParsingError = ex.getError();
                }
            }

            if (frameParsingError != null)
            {
                _connectionHandler.handleError(frameParsingError);
                _errored = true;
            }
            else
            {
                _connectionHandler.receive(channelFrameBodies);
            }
        }
        catch (RuntimeException e)
        {
            if (e instanceof ServerScopedRuntimeException)
            {
                throw e;
            }
            LOGGER.warn("Unexpected exception handling frame", e);
            // This exception is unexpected. The up layer should handle error condition gracefully
            _connectionHandler.handleError(this.createError(AmqpError.INTERNAL_ERROR, e.toString()));
        }
        return this;
    }

    private Error createFramingError(String description, Object... args)
    {
        return createError(ConnectionError.FRAMING_ERROR, description, args);
    }

    private Error createError(final ErrorCondition framingError,
                              final String description,
                              final Object... args)
    {
        Error error = new Error();
        error.setCondition(framingError);
        Formatter formatter = new Formatter();
        error.setDescription(formatter.format(description, args).toString());
        return error;
    }

    @Override
    public boolean isDone()
    {
        return _errored || _connectionHandler.closedForInput();
    }
}
