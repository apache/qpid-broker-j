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
package org.apache.qpid.framing;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.qpid.QpidException;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.codec.MarkableDataInput;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.transport.ByteBufferSender;

public class ContentBody implements AMQBody
{
    public static final byte TYPE = 3;

    private QpidByteBuffer _payload;

    public ContentBody()
    {
    }


    public ContentBody(ByteBuffer payload)
    {
        _payload = QpidByteBuffer.wrap(payload);
    }

    public ContentBody(QpidByteBuffer payload)
    {
        _payload = payload;
    }


    public byte getFrameType()
    {
        return TYPE;
    }

    public int getSize()
    {
        return _payload == null ? 0 : _payload.remaining();
    }

    public void writePayload(DataOutput buffer) throws IOException
    {
        byte[] data = new byte[_payload.remaining()];
        _payload.duplicate().get(data);
        buffer.write(data);
    }

    public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
            throws QpidException
    {
        session.contentBodyReceived(channelId, this);
    }

    @Override
    public long writePayload(final ByteBufferSender sender) throws IOException
    {
        if(_payload != null)
        {
            sender.send(_payload.duplicate());
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

    public static void process(final MarkableDataInput in,
                               final ChannelMethodProcessor methodProcessor, final long bodySize)
            throws IOException
    {

        QpidByteBuffer payload = in.readAsByteBuffer((int) bodySize);

        if(!methodProcessor.ignoreAllButCloseOk())
        {
            methodProcessor.receiveMessageContent(payload);
        }
    }

    public static AMQFrame createAMQFrame(int channelId, ContentBody body)
    {
        return new AMQFrame(channelId, body);
    }
}
