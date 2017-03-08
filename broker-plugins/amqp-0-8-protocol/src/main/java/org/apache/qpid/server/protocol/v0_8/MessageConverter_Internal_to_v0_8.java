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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;

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
        Object messageBody = serverMsg.getMessageBody();
        ObjectToMimeContentConverter converter = MimeContentConverterRegistry.getBestFitObjectToMimeContentConverter(messageBody);
        final byte[] messageContent = converter == null ? new byte[] {} : converter.toMimeContent(messageBody);
        String mimeType = converter == null ? null  : converter.getMimeType();

        mimeType = improveMimeType(serverMsg, mimeType);

        final MessageMetaData messageMetaData_0_8 = convertMetaData(serverMsg,
                                                                    mimeType,
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
            public int getContentSize()
            {
                return messageContent.length;
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

    private String improveMimeType(final InternalMessage serverMsg, String mimeType)
    {
        if (serverMsg.getMessageHeader() != null && serverMsg.getMessageHeader().getMimeType() != null)
        {
            if ("text/plain".equals(mimeType) && serverMsg.getMessageHeader().getMimeType().startsWith("text/"))
            {
                mimeType = serverMsg.getMessageHeader().getMimeType();
            }
            else if ("application/octet-stream".equals(mimeType))
            {
                mimeType = serverMsg.getMessageHeader().getMimeType();
            }
        }
        return mimeType;
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
}
