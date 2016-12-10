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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.MessagePublishInfo;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.transport.codec.BBEncoder;

@PluggableService
public class MessageConverter_Internal_to_v0_8 implements MessageConverter<InternalMessage, AMQMessage>
{


    public Class<InternalMessage> getInputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public Class<AMQMessage> getOutputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public AMQMessage convert(InternalMessage serverMsg, NamedAddressSpace addressSpace)
    {
        return new AMQMessage(convertToStoredMessage(serverMsg), null);
    }

    @Override
    public void dispose(final AMQMessage message)
    {

    }

    private StoredMessage<MessageMetaData> convertToStoredMessage(final InternalMessage serverMsg)
    {
        final byte[] messageContent = convertToBody(serverMsg.getMessageBody());
        final MessageMetaData messageMetaData_0_8 = convertMetaData(serverMsg,
                                                                    getBodyMimeType(serverMsg.getMessageBody(), serverMsg.getMessageHeader()),
                                                                    messageContent.length);

        return new StoredMessage<MessageMetaData>()
        {
            @Override
            public MessageMetaData getMetaData()
            {
                return messageMetaData_0_8;
            }

            @Override
            public long getMessageNumber()
            {
                return serverMsg.getMessageNumber();
            }

            @Override
            public Collection<QpidByteBuffer> getContent(final int offset, final int length)
            {
                return Collections.singleton(QpidByteBuffer.wrap(messageContent, offset, length));
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isInMemory()
            {
                return true;
            }

            @Override
            public boolean flowToDisk()
            {
                return false;
            }
        };
    }

    private MessageMetaData convertMetaData(final InternalMessage serverMsg, final String bodyMimeType, final int size)
    {

        MessagePublishInfo publishInfo = new MessagePublishInfo(AMQShortString.EMPTY_STRING,
                                                                false,
                                                                false,
                                                                AMQShortString.valueOf(serverMsg.getInitialRoutingAddress()));


        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setAppId(serverMsg.getMessageHeader().getAppId());
        props.setContentType(bodyMimeType);
        props.setCorrelationId(serverMsg.getMessageHeader().getCorrelationId());
        props.setDeliveryMode(serverMsg.isPersistent() ? BasicContentHeaderProperties.PERSISTENT : BasicContentHeaderProperties.NON_PERSISTENT);
        props.setExpiration(serverMsg.getExpiration());
        props.setMessageId(serverMsg.getMessageHeader().getMessageId());
        props.setPriority(serverMsg.getMessageHeader().getPriority());
        props.setReplyTo(serverMsg.getMessageHeader().getReplyTo());
        props.setTimestamp(serverMsg.getMessageHeader().getTimestamp());
        props.setUserId(serverMsg.getMessageHeader().getUserId());


        Map<String,Object> headerProps = new LinkedHashMap<String, Object>();

        for(String headerName : serverMsg.getMessageHeader().getHeaderNames())
        {
            headerProps.put(headerName, serverMsg.getMessageHeader().getHeader(headerName));
        }

        props.setHeaders(FieldTable.convertToFieldTable(headerProps));

        final ContentHeaderBody chb = new ContentHeaderBody(props);
        chb.setBodySize(size);
        return new MessageMetaData(publishInfo, chb, serverMsg.getArrivalTime());
    }


    @Override
    public String getType()
    {
        return "Internal to v0-8";
    }


    public static byte[] convertToBody(Object object)
    {
        if(object instanceof String)
        {
            return ((String)object).getBytes(StandardCharsets.UTF_8);
        }
        else if(object instanceof byte[])
        {
            return (byte[]) object;
        }
        else if(object instanceof Map)
        {
            BBEncoder encoder = new BBEncoder(1024);
            encoder.writeMap((Map)object);
            ByteBuffer buf = encoder.segment();
            int remaining = buf.remaining();
            byte[] data = new byte[remaining];
            buf.get(data);
            return data;

        }
        else if(object instanceof List)
        {
            BBEncoder encoder = new BBEncoder(1024);
            encoder.writeList((List) object);
            ByteBuffer buf = encoder.segment();
            int remaining = buf.remaining();
            byte[] data = new byte[remaining];
            buf.get(data);
            return data;
        }
        else
        {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try
            {
                ObjectOutputStream os = new ObjectOutputStream(bytesOut);
                os.writeObject(object);
                return bytesOut.toByteArray();
            }
            catch (IOException e)
            {
                throw new ConnectionScopedRuntimeException(e);
            }
        }
    }

    public static String getBodyMimeType(Object object, final InternalMessageHeader header)
    {
        if(object instanceof String)
        {
            String mimeType;
            if(header == null || (mimeType = header.getMimeType()) == null || !mimeType.trim().startsWith("text/"))
            {
                return "text/plain";
            }
            else
            {
                return mimeType;
            }
        }
        else if(object instanceof byte[])
        {
            String mimeType;
            if(header == null || (mimeType = header.getMimeType()) == null || "".equals(mimeType.trim()))
            {
                return "application/octet-stream";
            }
            else
            {
                return mimeType;
            }
        }
        else if(object instanceof Map)
        {
            return "amqp/map";
        }
        else if(object instanceof List)
        {
            return "amqp/list";
        }
        else
        {
            return "application/java-object-stream";
        }
    }

}
