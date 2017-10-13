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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.LIST_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.MAP_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.OBJECT_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.TEXT_CONTENT_TYPES;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.mimecontentconverter.ConversionUtils;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentToObjectConverter;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.url.AMQBindingURL;
import org.apache.qpid.server.util.GZIPUtils;

@PluggableService
public class MessageConverter_v0_8_to_Internal implements MessageConverter<AMQMessage, InternalMessage>
{
    @Override
    public Class<AMQMessage> getInputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public Class<InternalMessage> getOutputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public InternalMessage convert(AMQMessage serverMessage, NamedAddressSpace addressSpace)
    {
        final String mimeType = serverMessage.getMessageHeader().getMimeType();
        byte[] data = new byte[(int) serverMessage.getSize()];
        try (QpidByteBuffer content = serverMessage.getContent())
        {
            content.get(data);
        }

        String encoding = serverMessage.getMessageHeader().getEncoding();
        byte[] uncompressed;
        if (GZIPUtils.GZIP_CONTENT_ENCODING.equals(encoding)
            && (uncompressed = GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(data))) != null)
        {
            data = uncompressed;
            encoding =  null;
        }

        Object body = convertMessageBody(mimeType, data);
        final AMQMessageHeader convertedHeader = convertHeader(serverMessage, addressSpace, body, encoding);
        return InternalMessage.convert(serverMessage, convertedHeader, body);
    }

    private AMQMessageHeader convertHeader(final AMQMessage serverMessage,
                                           final NamedAddressSpace addressSpace,
                                           final Object convertedBodyObject, final String encoding)
    {
        final String convertedMimeType = getInternalConvertedMimeType(serverMessage, convertedBodyObject);
        final AMQMessageHeader messageHeader = serverMessage.getMessageHeader();

        Map<String, Object> headers = new HashMap<>();
        messageHeader.getHeaderNames()
                     .forEach(headerName -> headers.put(headerName, messageHeader.getHeader(headerName)));

        final InternalMessageHeader header = new InternalMessageHeader(headers,
                                                                       messageHeader.getCorrelationId(),
                                                                       messageHeader.getExpiration(),
                                                                       messageHeader.getUserId(),
                                                                       messageHeader.getAppId(),
                                                                       messageHeader.getMessageId(),
                                                                       convertedMimeType,
                                                                       messageHeader.getEncoding(),
                                                                       messageHeader.getPriority(),
                                                                       messageHeader.getTimestamp(),
                                                                       messageHeader.getNotValidBefore(),
                                                                       messageHeader.getType(),
                                                                       messageHeader.getReplyTo(),
                                                                       serverMessage.getArrivalTime());
        return new DelegatingMessageHeader(header, encoding);
    }

    private String getInternalConvertedMimeType(final AMQMessage serverMessage, final Object convertedBodyObject)
    {
        String originalMimeType = serverMessage.getMessageHeader().getMimeType();
        if (originalMimeType != null)
        {
            if (ConversionUtils.LIST_MESSAGE_CONTENT_TYPES.matcher(originalMimeType).matches()
                || ConversionUtils.MAP_MESSAGE_CONTENT_TYPES.matcher(originalMimeType).matches())
            {
                return null;
            }
            else if (ConversionUtils.OBJECT_MESSAGE_CONTENT_TYPES.matcher(originalMimeType).matches())
            {
                return "application/x-java-serialized-object";
            }
        }

        return originalMimeType;
    }

    @Override
    public void dispose(final InternalMessage message)
    {

    }

    private static class ReplyToComponents
    {
        private String _exchange;
        private String _queue;
        private String _routingKey;

        public void setExchange(final String exchange)
        {
            _exchange = exchange;
        }

        public void setQueue(final String queue)
        {
            _queue = queue;
        }

        public void setRoutingKey(final String routingKey)
        {
            _routingKey = routingKey;
        }

        public String getExchange()
        {
            return _exchange;
        }

        public String getQueue()
        {
            return _queue;
        }

        public String getRoutingKey()
        {
            return _routingKey;
        }

        public boolean hasExchange()
        {
            return _exchange != null;
        }

        public boolean hasQueue()
        {
            return _queue != null;
        }

        public boolean hasRoutingKey()
        {
            return _routingKey != null;
        }
    }

    private static class DelegatingMessageHeader implements AMQMessageHeader
    {
        private final AMQMessageHeader _delegate;
        private final String _encoding;

        private DelegatingMessageHeader(final AMQMessageHeader delegate, String encoding)
        {
            _delegate = delegate;
            _encoding = encoding;
        }

        @Override
        public String getCorrelationId()
        {
            return _delegate.getCorrelationId();
        }

        @Override
        public long getExpiration()
        {
            return _delegate.getExpiration();
        }

        @Override
        public String getUserId()
        {
            return _delegate.getUserId();
        }

        @Override
        public String getAppId()
        {
            return _delegate.getAppId();
        }

        @Override
        public String getGroupId()
        {
            return _delegate.getGroupId();
        }

        @Override
        public String getMessageId()
        {
            return _delegate.getMessageId();
        }

        @Override
        public String getMimeType()
        {
            return _delegate.getMimeType();
        }

        @Override
        public String getEncoding()
        {
            return _encoding;
        }

        @Override
        public byte getPriority()
        {
            return _delegate.getPriority();
        }

        @Override
        public long getTimestamp()
        {
            return _delegate.getTimestamp();
        }

        @Override
        public long getNotValidBefore()
        {
            return _delegate.getNotValidBefore();
        }

        @Override
        public String getType()
        {
            return _delegate.getType();
        }

        @Override
        public String getReplyTo()
        {
            String originalReplyTo = _delegate.getReplyTo();
            ReplyToComponents replyTo = convertReplyTo(originalReplyTo);
            if(replyTo != null)
            {
                if(replyTo.hasExchange())
                {
                    return replyTo.getExchange() + (replyTo.hasRoutingKey() ? "/" + replyTo.getRoutingKey() : "");
                }
                else
                {
                    return replyTo.hasQueue() ? replyTo.getQueue() : replyTo.getRoutingKey();
                }
            }
            else
            {
                return originalReplyTo;
            }
        }

        private ReplyToComponents convertReplyTo(final String origReplyToString)
        {
            try
            {
                AMQBindingURL burl = new AMQBindingURL(origReplyToString);
                ReplyToComponents replyTo = new ReplyToComponents();
                String routingKey = burl.getRoutingKey();
                if(routingKey != null && !"".equals(routingKey))
                {
                    replyTo.setRoutingKey(routingKey);
                }

                String exchangeName = burl.getExchangeName();
                if(exchangeName != null && !"".equals(exchangeName))
                {
                    replyTo.setExchange(exchangeName);
                }

                String queueName = burl.getQueueName();
                if(queueName != null && !"".equals(queueName))
                {
                    replyTo.setQueue(queueName);
                }
                return replyTo;
            }
            catch (URISyntaxException e)
            {
                return null;
            }
        }

        @Override
        public Object getHeader(final String name)
        {
            return _delegate.getHeader(name);
        }

        @Override
        public boolean containsHeaders(final Set<String> names)
        {
            return _delegate.containsHeaders(names);
        }

        @Override
        public boolean containsHeader(final String name)
        {
            return _delegate.containsHeader(name);
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return _delegate.getHeaderNames();
        }
    }


    private static Object convertMessageBody(String mimeType, byte[] data)
    {
        MimeContentToObjectConverter converter = MimeContentConverterRegistry.getMimeContentToObjectConverter(mimeType);
        if (data != null && data.length != 0)
        {
            if (converter != null)
            {
                return converter.toObject(data);
            }
            else if (mimeType != null && TEXT_CONTENT_TYPES.matcher(mimeType).matches())
            {
                return new String(data, UTF_8);
            }
        }
        else if (mimeType == null)
        {
            return null;
        }
        else if (OBJECT_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return new byte[0];
        }
        else if (ConversionUtils.TEXT_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return "";
        }
        else if (MAP_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return Collections.emptyMap();
        }
        else if (LIST_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return Collections.emptyList();
        }
        return data;
    }

    @Override
    public String getType()
    {
        return "v0-8 to Internal";
    }
}
