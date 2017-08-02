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
package org.apache.qpid.server.protocol.v0_10;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentToObjectConverter;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.util.GZIPUtils;

@PluggableService
public class MessageConverter_v0_10_to_Internal implements MessageConverter<MessageTransferMessage, InternalMessage>
{
    @Override
    public Class<MessageTransferMessage> getInputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public Class<InternalMessage> getOutputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public InternalMessage convert(MessageTransferMessage serverMessage, NamedAddressSpace addressSpace)
    {
        final String mimeType = serverMessage.getMessageHeader().getMimeType();
        byte[] data = new byte[(int) serverMessage.getSize()];
        int total = 0;
        for(QpidByteBuffer b : serverMessage.getContent(0, (int) serverMessage.getSize()))
        {
            int len = b.remaining();
            b.get(data, total, len);
            b.dispose();
            total += len;
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
        MessageProperties messageProps = serverMessage.getHeader().getMessageProperties();
        AMQMessageHeader fixedHeader = new DelegatingMessageHeader(serverMessage.getMessageHeader(), messageProps == null ? null : messageProps.getReplyTo(), encoding);
        return InternalMessage.convert(serverMessage, fixedHeader, body);
    }

    @Override
    public void dispose(final InternalMessage message)
    {

    }

    private static class DelegatingMessageHeader implements AMQMessageHeader
    {
        private final AMQMessageHeader _delegate;
        private final ReplyTo _replyTo;
        private final String _encoding;


        private DelegatingMessageHeader(final AMQMessageHeader delegate, final ReplyTo replyTo, final String encoding)
        {
            _delegate = delegate;
            _replyTo = replyTo;
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
            return _replyTo == null
                    ? null
                    : _replyTo.getExchange() == null || _replyTo.getExchange().equals("")
                        ? _replyTo.getRoutingKey()
                        : _replyTo.getRoutingKey() == null || _replyTo.getRoutingKey().equals("")
                            ? _replyTo.getExchange()
                            : _replyTo.getExchange() + "/" + _replyTo.getRoutingKey();
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
        if (converter != null)
        {
            return converter.toObject(data);
        }
        else
        {
            return data;
        }
    }

    @Override
    public String getType()
    {
        return "v0-10 to Internal";
    }
}
