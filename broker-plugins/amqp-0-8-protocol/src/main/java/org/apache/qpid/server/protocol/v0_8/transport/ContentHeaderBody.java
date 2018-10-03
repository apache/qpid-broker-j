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

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.transport.ByteBufferSender;

public class ContentHeaderBody implements AMQBody
{
    public static final byte TYPE = 2;
    public static final int CLASS_ID =  60;
    private static final int HEADER_SIZE = 14;

    private long _bodySize;

    /** must never be null */
    private final BasicContentHeaderProperties _properties;

    public ContentHeaderBody(QpidByteBuffer buffer, long size) throws AMQFrameDecodingException
    {
        buffer.getUnsignedShort();
        buffer.getUnsignedShort();
        _bodySize = buffer.getLong();
        int propertyFlags = buffer.getUnsignedShort();
        ContentHeaderPropertiesFactory factory = ContentHeaderPropertiesFactory.getInstance();
        _properties = factory.createContentHeaderProperties(CLASS_ID, propertyFlags, buffer, (int)size - 14);

    }

    public ContentHeaderBody(BasicContentHeaderProperties props)
    {
        _properties = props;
    }

    public ContentHeaderBody(BasicContentHeaderProperties props, long bodySize)
    {
        _properties = props;
        _bodySize = bodySize;
    }

    @Override
    public byte getFrameType()
    {
        return TYPE;
    }

    /**
     * Helper method that is used currently by the persistence layer.
     * @param buffer buffer to decode
     * @param size size of the body
     *
     * @return the decoded header body
     * @throws AMQFrameDecodingException if there is a decoding issue
     * @throws AMQProtocolVersionException if there is a version issue
     */
    public static ContentHeaderBody createFromBuffer(QpidByteBuffer buffer, long size)
        throws AMQFrameDecodingException, AMQProtocolVersionException
    {
        ContentHeaderBody body = new ContentHeaderBody(buffer, size);
        
        return body;
    }

    @Override
    public int getSize()
    {
        return 2 + 2 + 8 + 2 + _properties.getPropertyListSize();
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        try (QpidByteBuffer data = QpidByteBuffer.allocate(sender.isDirectBufferPreferred(), HEADER_SIZE))
        {
            data.putUnsignedShort(CLASS_ID);
            data.putUnsignedShort(0);
            data.putLong(_bodySize);
            data.putUnsignedShort(_properties.getPropertyFlags());
            data.flip();
            sender.send(data);
        }
        return HEADER_SIZE + _properties.writePropertyListPayload(sender);
    }

    public long writePayload(final QpidByteBuffer buf)
    {
        buf.putUnsignedShort(CLASS_ID);
        buf.putUnsignedShort(0);
        buf.putLong(_bodySize);
        buf.putUnsignedShort(_properties.getPropertyFlags());
        return HEADER_SIZE + _properties.writePropertyListPayload(buf);
    }

    @Override
    public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
            throws QpidException
    {
        session.contentHeaderReceived(channelId, this);
    }

    public static AMQFrame createAMQFrame(int channelId,
                                          BasicContentHeaderProperties properties,
                                          long bodySize)
    {
        return new AMQFrame(channelId, new ContentHeaderBody(properties, bodySize));
    }

    public BasicContentHeaderProperties getProperties()
    {
        return _properties;
    }

    @Override
    public String toString()
    {
        return "ContentHeaderBody{" +
                "classId=" + CLASS_ID +
                ", weight=" + 0 +
                ", bodySize=" + _bodySize +
                ", properties=" + _properties +
                '}';
    }

    public int getClassId()
    {
        return CLASS_ID;
    }

    public int getWeight()
    {
        return 0;
    }

    /** unsigned long but java can't handle that anyway when allocating byte array
     *
     * @return the body size */
    public long getBodySize()
    {
        return _bodySize;
    }

    public void setBodySize(long bodySize)
    {
        _bodySize = bodySize;
    }

    public static void process(final QpidByteBuffer buffer,
                               final ChannelMethodProcessor methodProcessor, final long size)
            throws AMQFrameDecodingException
    {
        int classId = buffer.getUnsignedShort();
        buffer.getUnsignedShort();
        long bodySize = buffer.getLong();
        int propertyFlags = buffer.getUnsignedShort();

        BasicContentHeaderProperties properties;

        if (classId != CLASS_ID)
        {
            throw new AMQFrameDecodingException("Unsupported content header class id: " + classId, null);
        }
        properties = new BasicContentHeaderProperties(buffer, propertyFlags, (int)(size-14));

        if(!methodProcessor.ignoreAllButCloseOk())
        {
            methodProcessor.receiveMessageHeader(properties, bodySize);
        }
        else
        {
            properties.dispose();
        }
    }

    public void dispose()
    {
        _properties.dispose();
    }

    public void clearEncodedForm()
    {
        _properties.clearEncodedForm();
    }

    public void reallocate()
    {
        _properties.reallocate();
    }
}
