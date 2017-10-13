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

import java.nio.ByteBuffer;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;

public class ContentBody implements AMQBody
{
    public static final byte TYPE = 3;

    private QpidByteBuffer _payload;


    public ContentBody(ByteBuffer payload)
    {
        _payload = QpidByteBuffer.wrap(payload.duplicate());
    }

    public ContentBody(QpidByteBuffer payload)
    {
        _payload = payload.duplicate();
    }


    @Override
    public byte getFrameType()
    {
        return TYPE;
    }

    @Override
    public int getSize()
    {
        return _payload == null ? 0 : _payload.remaining();
    }

    @Override
    public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
            throws QpidException
    {
        session.contentBodyReceived(channelId, this);
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        if(_payload != null)
        {
            try (QpidByteBuffer duplicate = _payload.duplicate())
            {
                sender.send(duplicate);
            }
            return _payload.remaining();
        }
        else
        {
            return 0l;
        }
    }

    public QpidByteBuffer getPayload()
    {
        return _payload;
    }

    public void dispose()
    {
        if (_payload != null)
        {
            _payload.dispose();
            _payload = null;
        }
    }

    public static void process(final QpidByteBuffer in,
                               final ChannelMethodProcessor methodProcessor, final long bodySize)
    {
        try (QpidByteBuffer payload = in.view(0, (int) bodySize))
        {
            if (!methodProcessor.ignoreAllButCloseOk())
            {
                methodProcessor.receiveMessageContent(payload);
            }
        }
        in.position(in.position()+(int)bodySize);
    }

    public static AMQFrame createAMQFrame(int channelId, ContentBody body)
    {
        return new AMQFrame(channelId, body);
    }
}
