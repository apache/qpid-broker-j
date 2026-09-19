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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Frame;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolError;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolEvent;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolHeader;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.server.protocol.v0_10.transport.Struct;
import org.apache.qpid.server.util.PeekingIterator;
import org.apache.qpid.server.util.PeekingIteratorImpl;

public class ServerAssembler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerAssembler.class);
    // Use a small array to store incomplete Methods for low-value channels, instead of allocating a huge
    // array or always boxing the channelId and looking it up in the map. This value must be of the form 2^X - 1.
    private static final int ARRAY_SIZE = 0xFF;
    private static final int SEGMENT_FLAG_MASK = ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG;

    private final ServerConnection _connection;
    private final Method[] _incompleteMethodArray = new Method[ARRAY_SIZE + 1];
    private final Map<Integer, Method> _incompleteMethodMap = new HashMap<>();
    private final Map<Integer, SegmentAccumulator> _segments = new HashMap<>();

    private boolean _segmentLimitsInitialized;
    private long _maxUnassembledSegmentBytes;
    private int _maxUnassembledSegmentFrames;
    private long _unassembledSegmentBytes;
    private int _unassembledSegmentFrames;

    public ServerAssembler(final ServerConnection connection)
    {
        _connection = Objects.requireNonNull(connection, "connection");
    }

    ServerAssembler(final ServerConnection connection,
                    final long maxUnassembledSegmentBytes,
                    final int maxUnassembledSegmentFrames)
    {
        _connection = Objects.requireNonNull(connection, "connection");
        _maxUnassembledSegmentBytes = Math.max(1L, maxUnassembledSegmentBytes);
        _maxUnassembledSegmentFrames = Math.max(1, maxUnassembledSegmentFrames);
        _segmentLimitsInitialized = true;
    }

    public final void received(final List<ServerFrame> frames)
    {
        if (!frames.isEmpty())
        {
            final PeekingIterator<ServerFrame> itr = new PeekingIteratorImpl(frames.iterator());

            boolean cleanExit = false;
            try
            {
                while (itr.hasNext())
                {
                    final ServerFrame frame = itr.next();
                    final int frameChannel = frame.getChannel();

                    final ServerSession channel = _connection.getSession(frameChannel);
                    if (channel != null)
                    {
                        channel.getSubjectExecutionContext().run(() ->
                        {
                            ServerFrame channelFrame = frame;
                            boolean nextIsSameChannel;
                            do
                            {
                                received(channelFrame);
                                nextIsSameChannel = itr.hasNext() && frameChannel == itr.peek().getChannel();
                                if (nextIsSameChannel)
                                {
                                    channelFrame = itr.next();
                                }
                            }
                            while (nextIsSameChannel);
                        });
                    }
                    else
                    {
                        received(frame);
                    }
                }
                cleanExit = true;
            }
            finally
            {
                if (!cleanExit)
                {
                    disposeRetainedState();
                    for (final ServerFrame frame : frames)
                    {
                        dispose(frame.getBody());
                    }
                }
            }
        }
    }

    private void received(final ServerFrame event)
    {
        if (!_connection.isIgnoreFutureInput())
        {
            frame(event);
        }
        else
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Ignored network event [channel={}, size={}, track={}, type={}, flags={}] as " +
                        "connection is ignoring further input", event.getChannel(), event.getSize(),
                        event.getTrack(), event.getType(), event.getFlags());
            }
            dispose(event.getBody());
        }
    }

    protected ByteBuffer allocateByteBuffer(final int size)
    {
        return ByteBuffer.allocateDirect(size);
    }


    private void initializeSegmentLimits()
    {
        if (!_segmentLimitsInitialized)
        {
            final AMQPConnection_0_10<?> amqpConnection = _connection.getAmqpConnection();
            final Integer configuredMaxBytes = amqpConnection.getContextValue(
                    Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES);
            _maxUnassembledSegmentBytes = Math.max(1L, configuredMaxBytes == null
                    ? AMQPConnection_0_10.DEFAULT_MAX_UNASSEMBLED_SEGMENT_BYTES
                    : configuredMaxBytes);

            final Integer configuredMaxFrames = amqpConnection.getContextValue(
                    Integer.class, AMQPConnection_0_10.CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES);
            _maxUnassembledSegmentFrames = Math.max(1, configuredMaxFrames == null
                    ? AMQPConnection_0_10.DEFAULT_MAX_UNASSEMBLED_SEGMENT_FRAMES
                    : configuredMaxFrames);
            _segmentLimitsInitialized = true;
        }
    }

    private int segmentKey(final ServerFrame frame)
    {
        return (frame.getChannel() << 4) | (frame.getTrack() & 0x0F);
    }

    private void emit(final int channel, final ProtocolEvent event)
    {
        event.setChannel(channel);
        _connection.received(event);
    }

    public void exception(final Throwable t)
    {
        disposeRetainedState();
        _connection.exception(t);
    }

    public void closed()
    {
        disposeRetainedState();
        _connection.closed();
    }

    public void init(final ProtocolHeader header)
    {
        emit(0, header);
    }

    public void error(final ProtocolError error)
    {
        disposeRetainedState();
        emit(0, error);
    }

    public void frame(final ServerFrame frame)
    {
        Objects.requireNonNull(frame, "frame");
        if (frame.getBody() == null)
        {
            throw reject(frame, "frame has no body on channel %d, track %d", frame.getChannel(), frame.getTrack());
        }
        if (frame.getType() == null)
        {
            throw reject(frame, "frame has no segment type on channel %d, track %d", frame.getChannel(),
                    frame.getTrack());
        }

        if (frame.isFirstFrame() && frame.isLastFrame() && _segments.isEmpty())
        {
            assemble(frame, frame.getBody());
            return;
        }

        final int key = segmentKey(frame);
        final SegmentAccumulator segment = _segments.get(key);

        if (frame.isFirstFrame())
        {
            if (segment != null)
            {
                throw reject(frame, "segment already in progress on channel %d, track %d", frame.getChannel(),
                        frame.getTrack());
            }

            if (frame.isLastFrame())
            {
                assemble(frame, frame.getBody());
            }
            else
            {
                final SegmentAccumulator newSegment = new SegmentAccumulator(frame);
                retain(frame, newSegment);
                _segments.put(key, newSegment);
            }
        }
        else
        {
            if (segment == null)
            {
                throw reject(frame, "segment continuation without a first frame on channel %d, track %d",
                        frame.getChannel(), frame.getTrack());
            }
            if (!segment.matches(frame))
            {
                throw reject(frame, "segment continuation does not match the first frame on channel %d, track %d",
                        frame.getChannel(), frame.getTrack());
            }

            retain(frame, segment);
            if (frame.isLastFrame())
            {
                _segments.remove(key);
                release(segment);
                final QpidByteBuffer combined = segment.concatenate();
                assemble(frame, combined);
            }
        }
    }

    private void retain(final ServerFrame frame, final SegmentAccumulator segment)
    {
        initializeSegmentLimits();

        if (_unassembledSegmentFrames >= _maxUnassembledSegmentFrames)
        {
            throw reject(frame, "unassembled segment frame limit (%d) exceeded on channel %d, track %d",
                    _maxUnassembledSegmentFrames, frame.getChannel(), frame.getTrack());
        }

        final int frameSize = frame.getBody().remaining();
        final long maxBytes = Math.min(_maxUnassembledSegmentBytes, Math.max(1L, _connection.getMaxMessageSize()));
        if (frameSize > maxBytes - _unassembledSegmentBytes)
        {
            throw reject(frame, "unassembled segment byte limit (%d) exceeded on channel %d, track %d",
                    maxBytes, frame.getChannel(), frame.getTrack());
        }

        segment.add(frame.getBody(), frameSize);
        _unassembledSegmentFrames++;
        _unassembledSegmentBytes += frameSize;
    }

    private void release(final SegmentAccumulator segment)
    {
        _unassembledSegmentFrames -= segment.getFrameCount();
        _unassembledSegmentBytes -= segment.getByteCount();
    }

    private IllegalArgumentException reject(final ServerFrame frame,
                                            final String format,
                                            final Object... arguments)
    {
        dispose(frame.getBody());
        final ProtocolError protocolError = new ProtocolError(Frame.L2, format, arguments);
        error(protocolError);
        return new IllegalArgumentException(protocolError.getMessage());
    }

    private void disposeRetainedState()
    {
        for (final SegmentAccumulator segment : _segments.values())
        {
            segment.dispose();
        }
        _segments.clear();
        _unassembledSegmentBytes = 0L;
        _unassembledSegmentFrames = 0;

        Arrays.fill(_incompleteMethodArray, null);
        _incompleteMethodMap.clear();
    }

    private static void dispose(final QpidByteBuffer buffer)
    {
        if (buffer != null)
        {
            buffer.dispose();
        }
    }

    private void assemble(final ServerFrame frame, final QpidByteBuffer frameBuffer)
    {
        try
        {
            final AMQPConnection_0_10<?> amqpConnection = _connection.getAmqpConnection();
            final ServerDecoder dec =
                    new ServerDecoder(frameBuffer, amqpConnection.getMaxZeroWidthArrayElements(),
                                      amqpConnection.getMaxNestedObjects());

            final int channel = frame.getChannel();

            switch (frame.getType())
            {
                case CONTROL:
                    final int controlType = dec.readUint16();
                    final Method control = Method.create(controlType);
                    control.read(dec);
                    emit(channel, control);
                    break;
                case COMMAND:
                    if (getIncompleteCommand(channel) != null)
                    {
                        throw new IllegalStateException("command received before previous command was complete " +
                                "on channel " + channel);
                    }
                    final int commandType = dec.readUint16();
                    // read in the session header, right now we don't use it
                    final int hdr = dec.readUint16();
                    final Method command = Method.create(commandType);
                    command.setSync((0x0001 & hdr) != 0);
                    command.read(dec);
                    if (command.hasPayload() && !frame.isLastSegment())
                    {
                        setIncompleteCommand(channel, command);
                    }
                    else
                    {
                        emit(channel, command);
                    }
                    break;
                case HEADER:
                    final Method headerCommand = getIncompleteCommand(channel);
                    if (headerCommand == null)
                    {
                        throw new IllegalStateException("header received without an incomplete command on channel " +
                                channel);
                    }
                    List<Struct> structs = null;
                    DeliveryProperties deliveryProps = null;
                    MessageProperties messageProps = null;

                    while (dec.hasRemaining())
                    {
                        Struct struct = dec.readStruct32();
                        if (struct instanceof DeliveryProperties && deliveryProps == null)
                        {
                            deliveryProps = (DeliveryProperties) struct;
                        }
                        else if (struct instanceof MessageProperties && messageProps == null)
                        {
                            messageProps = (MessageProperties) struct;
                        }
                        else
                        {
                            if (structs == null)
                            {
                                structs = new ArrayList<>(2);
                            }
                            structs.add(struct);
                        }
                    }
                    headerCommand.setHeader(new Header(deliveryProps, messageProps, structs));

                    if (frame.isLastSegment())
                    {
                        setIncompleteCommand(channel, null);
                        emit(channel, headerCommand);
                    }
                    break;
                case BODY:
                    final Method bodyCommand = getIncompleteCommand(channel);
                    if (bodyCommand == null)
                    {
                        throw new IllegalStateException("body received without an incomplete command on channel " +
                                channel);
                    }
                    bodyCommand.setBody(frameBuffer);
                    setIncompleteCommand(channel, null);
                    emit(channel, bodyCommand);
                    break;
                default:
                    throw new IllegalStateException("unknown frame type: " + frame.getType());
            }
        }
        finally
        {
            frameBuffer.dispose();
        }
    }

    private void setIncompleteCommand(final int channelId, final Method incomplete)
    {
        if ((channelId & ARRAY_SIZE) == channelId)
        {
            _incompleteMethodArray[channelId] = incomplete;
        }
        else
        {
            if(incomplete != null)
            {
                _incompleteMethodMap.put(channelId, incomplete);
            }
            else
            {
                _incompleteMethodMap.remove(channelId);
            }
        }
    }

    private Method getIncompleteCommand(final int channelId)
    {
        if ((channelId & ARRAY_SIZE) == channelId)
        {
            return _incompleteMethodArray[channelId];
        }
        else
        {
            return _incompleteMethodMap.get(channelId);
        }
    }

    private static final class SegmentAccumulator
    {
        private static final int INITIAL_CAPACITY = 4;

        private final int _channel;
        private final byte _track;
        private final SegmentType _type;
        private final int _segmentFlags;
        private final List<QpidByteBuffer> _buffers = new ArrayList<>(INITIAL_CAPACITY);

        private long _byteCount;

        private SegmentAccumulator(final ServerFrame frame)
        {
            _channel = frame.getChannel();
            _track = frame.getTrack();
            _type = frame.getType();
            _segmentFlags = frame.getFlags() & SEGMENT_FLAG_MASK;
        }

        private boolean matches(final ServerFrame frame)
        {
            return _channel == frame.getChannel() &&
                    _track == frame.getTrack() &&
                    _type == frame.getType() &&
                    _segmentFlags == (frame.getFlags() & SEGMENT_FLAG_MASK);
        }

        private void add(final QpidByteBuffer buffer, final int size)
        {
            _buffers.add(buffer);
            _byteCount += size;
        }

        private int getFrameCount()
        {
            return _buffers.size();
        }

        private long getByteCount()
        {
            return _byteCount;
        }

        private QpidByteBuffer concatenate()
        {
            try
            {
                return QpidByteBuffer.concatenate(_buffers);
            }
            finally
            {
                dispose();
            }
        }

        private void dispose()
        {
            for (final QpidByteBuffer buffer : _buffers)
            {
                buffer.dispose();
            }
            _buffers.clear();
            _byteCount = 0L;
        }
    }
}
