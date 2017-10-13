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
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
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
import org.apache.qpid.server.protocol.v0_10.transport.Struct;

public class ServerAssembler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerAssembler.class);


    private final ServerConnection _connection;



    // Use a small array to store incomplete Methods for low-value channels, instead of allocating a huge
    // array or always boxing the channelId and looking it up in the map. This value must be of the form 2^X - 1.
    private static final int ARRAY_SIZE = 0xFF;
    private final Method[] _incompleteMethodArray = new Method[ARRAY_SIZE + 1];
    private final Map<Integer, Method> _incompleteMethodMap = new HashMap<>();

    private final Map<Integer,List<ServerFrame>> _segments;

    public ServerAssembler(ServerConnection connection)
    {
        _connection = connection;
        _segments = new HashMap<>();
    }

    public void received(final List<ServerFrame> frames)
    {
        if (!frames.isEmpty())
        {
            PeekingIterator<ServerFrame> itr = Iterators.peekingIterator(frames.iterator());

            boolean cleanExit = false;
            try
            {
                while(itr.hasNext())
                {
                    final ServerFrame frame = itr.next();
                    final int frameChannel = frame.getChannel();

                    ServerSession channel = _connection.getSession(frameChannel);
                    if (channel != null)
                    {
                        final AccessControlContext context = channel.getAccessControllerContext();
                        AccessController.doPrivileged((PrivilegedAction<Void>) () ->
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
                            return null;
                        }, context);
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
                    while (itr.hasNext())
                    {
                        final QpidByteBuffer body = itr.next().getBody();
                        if (body != null)
                        {
                            body.dispose();
                        }
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
            LOGGER.debug("Ignored network event " + event + " as connection is ignoring further input ");
        }
    }

    protected ByteBuffer allocateByteBuffer(int size)
    {
        return ByteBuffer.allocateDirect(size);
    }


    private int segmentKey(ServerFrame frame)
    {
        return (frame.getTrack() + 1) * frame.getChannel();
    }

    private List<ServerFrame> getSegment(ServerFrame frame)
    {
        return _segments.get(segmentKey(frame));
    }

    private void setSegment(ServerFrame frame, List<ServerFrame> segment)
    {
        int key = segmentKey(frame);
        if (_segments.containsKey(key))
        {
            error(new ProtocolError(Frame.L2, "segment in progress: %s",
                                    frame));
        }
        _segments.put(segmentKey(frame), segment);
    }

    private void clearSegment(ServerFrame frame)
    {
        _segments.remove(segmentKey(frame));
    }

    private void emit(int channel, ProtocolEvent event)
    {
        event.setChannel(channel);
        _connection.received(event);
    }

    public void exception(Throwable t)
    {
        _connection.exception(t);
    }

    public void closed()
    {
        _connection.closed();
    }

    public void init(ProtocolHeader header)
    {
        emit(0, header);
    }

    public void error(ProtocolError error)
    {
        emit(0, error);
    }

    public void frame(ServerFrame frame)
    {
        if (frame.isFirstFrame() && frame.isLastFrame())
        {
            assemble(frame, frame.getBody());
        }
        else
        {
            List<ServerFrame> frames;
            if (frame.isFirstFrame())
            {
                frames = new ArrayList<>();
                setSegment(frame, frames);
            }
            else
            {
                frames = getSegment(frame);
            }

            frames.add(frame);

            if (frame.isLastFrame())
            {
                clearSegment(frame);
                List<QpidByteBuffer> frameBuffers = new ArrayList<>(frames.size());
                for (ServerFrame f : frames)
                {
                    frameBuffers.add(f.getBody());
                }
                QpidByteBuffer combined = QpidByteBuffer.concatenate(frameBuffers);
                for (QpidByteBuffer buffer : frameBuffers)
                {
                    buffer.dispose();
                }
                assemble(frame, combined);
            }
        }

    }

    private void assemble(ServerFrame frame, QpidByteBuffer frameBuffer)
    {
        try
        {
            ServerDecoder dec = new ServerDecoder(frameBuffer);

            int channel = frame.getChannel();
            Method command;

            switch (frame.getType())
            {
                case CONTROL:
                    int controlType = dec.readUint16();
                    Method control = Method.create(controlType);
                    control.read(dec);
                    emit(channel, control);
                    break;
                case COMMAND:
                    int commandType = dec.readUint16();
                    // read in the session header, right now we don't use it
                    int hdr = dec.readUint16();
                    command = Method.create(commandType);
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
                    command = getIncompleteCommand(channel);
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
                    command.setHeader(new Header(deliveryProps, messageProps, structs));

                    if (frame.isLastSegment())
                    {
                        setIncompleteCommand(channel, null);
                        emit(channel, command);
                    }
                    break;
                case BODY:
                    command = getIncompleteCommand(channel);
                    command.setBody(frameBuffer);
                    setIncompleteCommand(channel, null);
                    emit(channel, command);

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

    private void setIncompleteCommand(int channelId, Method incomplete)
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

    private Method getIncompleteCommand(int channelId)
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
}
