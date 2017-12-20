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
package org.apache.qpid.tests.protocol.v0_10;

import static org.apache.qpid.server.transport.util.Functions.str;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;

import org.apache.qpid.server.protocol.v0_10.FrameSizeObserver;
import org.apache.qpid.server.protocol.v0_10.transport.Frame;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolError;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolHeader;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.tests.protocol.InputDecoder;
import org.apache.qpid.tests.protocol.Response;

public class FrameDecoder implements InputDecoder, FrameSizeObserver
{

    private final ProtocolEventReceiver _receiver;

    public enum State
    {
        PROTO_HDR,
        FRAME_HDR,
        FRAME_BODY,
        ERROR
    }

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private final Assembler _assembler;

    private int _maxFrameSize = 4096;
    private State _state;
    private ByteBuffer input = null;
    private int _needed;

    private byte _flags;
    private SegmentType _type;
    private byte _track;
    private int _channel;

    FrameDecoder(final byte[] headerBytes)
    {
        _receiver = new ProtocolEventReceiver(headerBytes, this);
        this._assembler = new Assembler(_receiver);
        this._state = State.PROTO_HDR;
        _needed = 8;

    }

    @Override
    public Collection<Response<?>> decode(final ByteBuffer buf) throws Exception
    {
        int limit = buf.limit();
        int remaining = buf.remaining();
        while (remaining > 0)
        {
            if (remaining >= _needed)
            {
                int consumed = _needed;
                int pos = buf.position();
                if (input == null)
                {
                    buf.limit(pos + _needed);
                    input = buf;
                    _state = next(pos);
                    buf.limit(limit);
                    buf.position(pos + consumed);
                }
                else
                {
                    buf.limit(pos + _needed);
                    input.put(buf);
                    buf.limit(limit);
                    input.flip();
                    _state = next(0);
                }

                remaining -= consumed;
                input = null;
            }
            else
            {
                if (input == null)
                {
                    input = ByteBuffer.allocate(_needed);
                }
                input.put(buf);
                _needed -= remaining;
                remaining = 0;
            }
        }
        return _receiver.getReceivedEvents();
    }

    private State next(int pos)
    {
        input.order(ByteOrder.BIG_ENDIAN);

        switch (_state) {
            case PROTO_HDR:
                if (input.get(pos) != 'A' &&
                    input.get(pos + 1) != 'M' &&
                    input.get(pos + 2) != 'Q' &&
                    input.get(pos + 3) != 'P')
                {
                    error("bad protocol header: %s", str(input));
                    return State.ERROR;
                }

                byte protoClass = input.get(pos + 4);
                byte instance = input.get(pos + 5);
                byte major = input.get(pos + 6);
                byte minor = input.get(pos + 7);
                _assembler.received(new ProtocolHeader(protoClass, instance, major, minor));
                _needed = Frame.HEADER_SIZE;
                return State.FRAME_HDR;
            case FRAME_HDR:
                _flags = input.get(pos);
                _type = SegmentType.get(input.get(pos + 1));
                int size = (0xFFFF & input.getShort(pos + 2));
                size -= Frame.HEADER_SIZE;
                _maxFrameSize = 64 * 1024;
                if (size < 0 || size > (_maxFrameSize - 12))
                {
                    error("bad frame size: %d", size);
                    return State.ERROR;
                }
                byte b = input.get(pos + 5);
                if ((b & 0xF0) != 0) {
                    error("non-zero reserved bits in upper nibble of " +
                          "frame header byte 5: '%x'", b);
                    return State.ERROR;
                } else {
                    _track = (byte) (b & 0xF);
                }
                _channel = (0xFFFF & input.getShort(pos + 6));
                if (size == 0)
                {
                    Frame frame = new Frame(_flags, _type, _track, _channel, EMPTY_BYTE_BUFFER);
                    _assembler.received(frame);
                    _needed = Frame.HEADER_SIZE;
                    return State.FRAME_HDR;
                }
                else
                {
                    _needed = size;
                    return State.FRAME_BODY;
                }
            case FRAME_BODY:
                Frame frame = new Frame(_flags, _type, _track, _channel, input.slice());
                _assembler.received(frame);
                _needed = Frame.HEADER_SIZE;
                return State.FRAME_HDR;
            default:
                throw new IllegalStateException();
        }
    }

    private void error(String fmt, Object ... args)
    {
        _assembler.received(new ProtocolError(Frame.L1, fmt, args));
    }

    @Override
    public void setMaxFrameSize(final int maxFrameSize)
    {
        _maxFrameSize = maxFrameSize;
    }
}
