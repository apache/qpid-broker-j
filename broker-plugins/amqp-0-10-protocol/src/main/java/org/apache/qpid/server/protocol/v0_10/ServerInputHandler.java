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
package org.apache.qpid.server.protocol.v0_10;

import static org.apache.qpid.server.transport.util.Functions.str;
import static org.apache.qpid.server.protocol.v0_10.ServerInputHandler.State.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.Constant;
import org.apache.qpid.server.transport.FrameSizeObserver;
import org.apache.qpid.server.transport.ProtocolError;
import org.apache.qpid.server.transport.ProtocolHeader;
import org.apache.qpid.server.transport.SegmentType;


public class ServerInputHandler implements FrameSizeObserver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerInputHandler.class);
    private static final QpidByteBuffer EMPTY_BYTE_BUFFER = QpidByteBuffer.allocateDirect(0);

    private int _maxFrameSize = Constant.MIN_MAX_FRAME_SIZE;


    public enum State
    {
        PROTO_HDR,
        FRAME_HDR,
        ERROR;
    }
    private final ServerAssembler _serverAssembler;

    private State _state = PROTO_HDR;

    private byte flags;
    private SegmentType type;
    private byte track;
    private int channel;


    public ServerInputHandler(ServerAssembler serverAssembler)
    {
        _serverAssembler = serverAssembler;
        _state = PROTO_HDR;
    }

    public void setMaxFrameSize(final int maxFrameSize)
    {
        _maxFrameSize = maxFrameSize;
    }

    private void error(String fmt, Object ... args)
    {
        _serverAssembler.error(new ProtocolError(ServerFrame.L1, fmt, args));
    }

    public void received(QpidByteBuffer buf)
    {
        int position = buf.position();
        while(buf.hasRemaining() && _state != ERROR)
        {
            parse(buf);

            int newPosition = buf.position();
            if(position == newPosition)
            {
                break;
            }
            else
            {
                position = newPosition;
            }
        }
    }

    private void parse(QpidByteBuffer buffer)
    {
        buffer.mark();
        switch (_state) {
            case PROTO_HDR:
                if(buffer.remaining() < 8)
                {
                    break;
                }
                if (buffer.get() != 'A' ||
                    buffer.get() != 'M' ||
                    buffer.get() != 'Q' ||
                    buffer.get() != 'P')
                {
                    buffer.reset();
                    error("bad protocol header: %s", str(buffer));
                    _state = ERROR;
                }
                else
                {
                    byte protoClass = buffer.get();
                    byte instance = buffer.get();
                    byte major = buffer.get();
                    byte minor = buffer.get();

                    _serverAssembler.init(new ProtocolHeader(protoClass, instance, major, minor));
                    _state = FRAME_HDR;
                }
                break;
            case FRAME_HDR:
                if(buffer.remaining() < ServerFrame.HEADER_SIZE)
                {
                    buffer.reset();
                }
                else
                {
                    flags = buffer.get();
                    type = SegmentType.get(buffer.get());
                    int size = (0xFFFF & buffer.getShort());

                    size -= ServerFrame.HEADER_SIZE;
                    if (size < 0 || size > (_maxFrameSize - ServerFrame.HEADER_SIZE))
                    {
                        error("bad frame size: %d", size);
                        _state = ERROR;
                    }
                    else
                    {
                        buffer.get(); // skip unused byte
                        byte b = buffer.get();
                        if ((b & 0xF0) != 0)
                        {
                            error("non-zero reserved bits in upper nibble of " +
                                  "frame header byte 5: '%x'", b);
                            _state = ERROR;
                        }
                        else
                        {
                            track = (byte) (b & 0xF);

                            channel = (0xFFFF & buffer.getShort());
                            buffer.position(buffer.position()+4);
                            if (size == 0)
                            {
                                ServerFrame frame = new ServerFrame(flags, type, track, channel, EMPTY_BYTE_BUFFER.duplicate());
                                _serverAssembler.received(frame);

                            }
                            else if (buffer.remaining() < size)
                            {
                                buffer.reset();
                            }
                            else
                            {
                                final QpidByteBuffer body = buffer.slice();
                                body.limit(size);
                                ServerFrame frame = new ServerFrame(flags, type, track, channel, body);
                                buffer.position(buffer.position() + size);

                                _serverAssembler.received(frame);
                            }
                        }
                    }
                }
                break;
            default:
                throw new IllegalStateException();
        }

    }

    public void exception(Throwable t)
    {
        _serverAssembler.exception(t);
    }

    public void closed()
    {
        _serverAssembler.closed();
    }

}
