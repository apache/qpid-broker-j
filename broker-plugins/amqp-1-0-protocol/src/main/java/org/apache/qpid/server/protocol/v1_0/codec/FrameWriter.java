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

package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.framing.AMQFrame;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.transport.ByteBufferSender;

public class FrameWriter
{
    private final ByteBufferSender _sender;
    private final ValueWriter.Registry _registry;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    public FrameWriter(final ValueWriter.Registry registry, final ByteBufferSender sender)
    {
        _registry = registry;
        _sender = sender;
    }

    public <T> int send(AMQFrame<T> frame)
    {
        final QpidByteBuffer payload = frame.getPayload();

        final int payloadLength = payload == null ? 0 : payload.remaining();
        final T frameBody = frame.getFrameBody();

        final ValueWriter<T> typeWriter = frameBody == null ? null : _registry.getValueWriter(frameBody);
        int bodySize;
        if (typeWriter == null)
        {
            bodySize = 8;
        }
        else
        {
            bodySize = 8 + typeWriter.getEncodedSize();
        }

        final int totalSize;
        try (QpidByteBuffer body = QpidByteBuffer.allocate(_sender.isDirectBufferPreferred(), bodySize))
        {
            totalSize = bodySize + payloadLength;
            body.putInt(totalSize);
            body.put((byte) 2); // DOFF
            body.put(frame.getFrameType()); // AMQP Frame Type
            body.putShort(UnsignedShort.valueOf(frame.getChannel()).shortValue());
            if (typeWriter != null)
            {
                typeWriter.writeToBuffer(body);
            }
            body.flip();

            _sender.send(body);
        }
        if(payload != null)
        {
            _sender.send(payload);
        }
        return totalSize;
    }

}
