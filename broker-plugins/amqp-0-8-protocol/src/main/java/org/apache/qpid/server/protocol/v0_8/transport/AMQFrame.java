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
package org.apache.qpid.server.protocol.v0_8.transport;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;

public class AMQFrame extends AMQDataBlock implements EncodableAMQDataBlock
{
    private static final int HEADER_SIZE = 7;
    private final int _channel;

    private final AMQBody _bodyFrame;
    public static final byte FRAME_END_BYTE = (byte) 0xCE;

    public AMQFrame(final int channel, final AMQBody bodyFrame)
    {
        _channel = channel;
        _bodyFrame = bodyFrame;
    }

    @Override
    public long getSize()
    {
        return 1 + 2 + 4 + _bodyFrame.getSize() + 1;
    }

    public static final int getFrameOverhead()
    {
        return 1 + 2 + 4 + 1;
    }


    private static final QpidByteBuffer FRAME_END_BYTE_BUFFER = QpidByteBuffer.allocateDirect(1);
    static
    {
        FRAME_END_BYTE_BUFFER.put(FRAME_END_BYTE);
        FRAME_END_BYTE_BUFFER.flip();
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        try (QpidByteBuffer frameHeader = QpidByteBuffer.allocate(sender.isDirectBufferPreferred(), HEADER_SIZE))
        {
            frameHeader.put(_bodyFrame.getFrameType());
            frameHeader.putUnsignedShort(_channel);
            frameHeader.putUnsignedInt((long) _bodyFrame.getSize());
            frameHeader.flip();
            sender.send(frameHeader);
        }
        long size = 8 + _bodyFrame.writePayload(sender);

        try (QpidByteBuffer endFrame = FRAME_END_BYTE_BUFFER.duplicate())
        {
            sender.send(endFrame);
        }
        return size;
    }

    public final int getChannel()
    {
        return _channel;
    }

    public final AMQBody getBodyFrame()
    {
        return _bodyFrame;
    }

    @Override
    public String toString()
    {
        return "Frame channelId: " + _channel + ", bodyFrame: " + String.valueOf(_bodyFrame);
    }

}
