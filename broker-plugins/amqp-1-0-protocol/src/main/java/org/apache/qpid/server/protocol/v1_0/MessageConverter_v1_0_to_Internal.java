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
package org.apache.qpid.server.protocol.v1_0;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.mimecontentconverter.ConversionUtils;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

@PluggableService
public class MessageConverter_v1_0_to_Internal implements MessageConverter<Message_1_0, InternalMessage>
{

    static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance();
    static
    {
        TYPE_REGISTRY.registerTransportLayer();
        TYPE_REGISTRY.registerMessagingLayer();
        TYPE_REGISTRY.registerTransactionLayer();
        TYPE_REGISTRY.registerSecurityLayer();
    }

    @Override
    public Class<Message_1_0> getInputClass()
    {
        return Message_1_0.class;
    }

    @Override
    public Class<InternalMessage> getOutputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public InternalMessage convert(Message_1_0 serverMessage, NamedAddressSpace addressSpace)
    {
        Object bodyObject = MessageConverter_from_1_0.convertBodyToObject(serverMessage);
        final AMQMessageHeader convertHeader = convertHeader(serverMessage, addressSpace, bodyObject);
        return InternalMessage.convert(serverMessage, convertHeader, bodyObject);
    }

    private AMQMessageHeader convertHeader(final Message_1_0 serverMessage,
                                           final NamedAddressSpace addressSpace,
                                           final Object convertedBodyObject)
    {
        final String convertedMimeType = getInternalConvertedMimeType(serverMessage, convertedBodyObject);
        final MessageMetaData_1_0.MessageHeader_1_0 messageHeader = serverMessage.getMessageHeader();
        return new InternalMessageHeader(messageHeader.getHeadersAsMap(),
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
    }

    @Override
    public void dispose(final InternalMessage message)
    {

    }

    @Override
    public String getType()
    {
        return "v1-0 to Internal";
    }

    private static String getInternalConvertedMimeType(final Message_1_0 serverMsg,
                                                       final Object convertedBodyObject)
    {
        MessageConverter_from_1_0.ContentHint contentHint = getInternalTypeHint(serverMsg);

        final Class<?> contentClassHint = contentHint.getContentClass();
        final String originalContentType = contentHint.getContentType();
        String mimeType = originalContentType;
        if (convertedBodyObject == null)
        {
            if (contentClassHint == Void.class
                || contentClassHint == Map.class
                || contentClassHint == List.class)
            {
                mimeType = null;
            }
            else if (contentClassHint == Serializable.class)
            {
                mimeType = "application/x-java-serialized-object";
            }
            else if (contentClassHint == byte[].class)
            {
                mimeType = "application/octet-stream";
            }
            else if (contentClassHint == String.class
                     && (originalContentType == null
                         || !ConversionUtils.TEXT_CONTENT_TYPES.matcher(originalContentType).matches()))
            {
                mimeType = "text/plain";
            }
        }
        else if (convertedBodyObject instanceof byte[]
                 && originalContentType == null)
        {
            if (contentClassHint == Serializable.class)
            {
                mimeType = "application/x-java-serialized-object";
            }
            else
            {
                mimeType = "application/octet-stream";
            }
        }
        else if (convertedBodyObject instanceof List
                 || convertedBodyObject instanceof Map)
        {
            mimeType = null;
        }
        else if (convertedBodyObject instanceof String
                 && (originalContentType == null
                     || !ConversionUtils.TEXT_CONTENT_TYPES.matcher(originalContentType).matches()))
        {
            mimeType = "text/plain";
        }

        return mimeType;
    }

    private static MessageConverter_from_1_0.ContentHint getInternalTypeHint(final Message_1_0 serverMsg)
    {
        Symbol contentType = MessageConverter_from_1_0.getContentType(serverMsg);

        JmsMessageTypeAnnotation jmsMessageTypeAnnotation = MessageConverter_from_1_0.getJmsMessageTypeAnnotation(serverMsg);

        Class<?> classHint = MessageConverter_from_1_0.getContentTypeClassHint(jmsMessageTypeAnnotation);
        String mimeTypeHint = null;

        if (contentType != null)
        {
            Class<?> contentTypeClassHint = null;
            String type = contentType.toString();
            if (ConversionUtils.TEXT_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = String.class;
            }
            else if (ConversionUtils.MAP_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = Map.class;
            }
            else if (ConversionUtils.LIST_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = List.class;
            }
            else if (ConversionUtils.OBJECT_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = Serializable.class;
            }
            else if (ConversionUtils.BYTES_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = byte[].class;
            }

            if (classHint == null || classHint == contentTypeClassHint)
            {
                classHint = contentTypeClassHint;
            }
            mimeTypeHint = contentType.toString();
        }

        return new MessageConverter_from_1_0.ContentHint(classHint, mimeTypeHint);
    }
}
