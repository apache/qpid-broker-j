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

import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.framing.AMQFrame;
import org.apache.qpid.transport.ByteBufferSender;

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
        final List<QpidByteBuffer> payload = frame.getPayload();

        final int payloadLength = payload == null ? 0 : (int) QpidByteBufferUtils.remaining(payload);
        final T frameBody = frame.getFrameBody();

        final ValueWriter<T> typeWriter = frameBody == null ? null : _registry.getValueWriter(frameBody);
        int bodySize;
        if (typeWriter == null)
        {
            bodySize = 8;
        }
        else
        {
            typeWriter.setValue(frame.getFrameBody());
            QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(EMPTY_BYTE_ARRAY);
            bodySize = typeWriter.writeToBuffer(qpidByteBuffer) + 8;
        }

        QpidByteBuffer body = QpidByteBuffer.allocate(_sender.isDirectBufferPreferred(), bodySize);
        final int totalSize = bodySize + payloadLength;
        body.putInt(totalSize);
        body.put((byte)2); // DOFF
        body.put(frame.getFrameType()); // AMQP Frame Type
        body.putShort(frame.getChannel());
        if(typeWriter != null)
        {
            typeWriter.writeToBuffer(body);
        }
        body.flip();

        _sender.send(body);
        body.dispose();
        if(payload != null)
        {
            for(QpidByteBuffer buf : payload)
            {
                _sender.send(buf);
            }
        }
        return totalSize;
    }

}
