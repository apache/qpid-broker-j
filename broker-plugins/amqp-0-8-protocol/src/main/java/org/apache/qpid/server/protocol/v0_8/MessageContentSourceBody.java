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
package org.apache.qpid.server.protocol.v0_8;

import org.apache.qpid.QpidException;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQBody;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.server.message.MessageContentSource;
import org.apache.qpid.transport.ByteBufferSender;

public class MessageContentSourceBody implements AMQBody
{
    public static final byte TYPE = 3;
    private final int _length;
    private final MessageContentSource _content;
    private final int _offset;

    public MessageContentSourceBody(MessageContentSource content, int offset, int length)
    {
        _content = content;
        _offset = offset;
        _length = length;
    }

    public byte getFrameType()
    {
        return TYPE;
    }

    public int getSize()
    {
        return _length;
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        long size = 0L;
        for(QpidByteBuffer buf : _content.getContent(_offset, _length))
        {
            size += buf.remaining();

            sender.send(buf);
            buf.dispose();
        }
        return size;
    }

    public void handle(int channelId, AMQVersionAwareProtocolSession amqProtocolSession) throws QpidException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "[" + getClass().getSimpleName() + " offset: " + _offset + ", length: " + _length + "]";
    }

}
