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


import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.FooterSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class Message_1_0 extends AbstractServerMessageImpl<Message_1_0, MessageMetaData_1_0>
{

    private static final AMQPDescribedTypeRegistry DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                                      .registerTransportLayer()
                                                                                                      .registerMessagingLayer()
                                                                                                      .registerTransactionLayer()
                                                                                                      .registerSecurityLayer();
    private static final MessageMetaData_1_0 DELETED_MESSAGE_METADATA = new MessageMetaData_1_0(null, null, null, null, null, null, 0L, 0L);
    private static final String AMQP_1_0 = "AMQP 1.0";

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage)
    {
        super(storedMessage, null);
    }

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage,
                       final Object connectionReference)
    {
        super(storedMessage, connectionReference);
    }

    @Override
    public String getInitialRoutingAddress()
    {
        MessageMetaData_1_0.MessageHeader_1_0 messageHeader = getMessageHeader();
        final String routingAddress;
        if (messageHeader.getHeader("routing-key") instanceof String)
        {
            routingAddress = (String) messageHeader.getHeader("routing-key");
        }
        else if (messageHeader.getHeader("routing_key") instanceof String)
        {
            routingAddress = (String) messageHeader.getHeader("routing_key");
        }
        else if (messageHeader.getSubject() != null)
        {
            routingAddress = messageHeader.getSubject();
        }
        else
        {
            routingAddress = "";
        }

        return routingAddress;
    }

    @Override
    public String getTo()
    {
        MessageMetaData_1_0.MessageHeader_1_0 messageHeader = getMessageHeader();
        return messageHeader.getTo();
    }

    private MessageMetaData_1_0 getMessageMetaData()
    {
        MessageMetaData_1_0 metaData = getStoredMessage().getMetaData();
        return metaData == null ? DELETED_MESSAGE_METADATA : metaData;
    }

    @Override
    public MessageMetaData_1_0.MessageHeader_1_0 getMessageHeader()
    {
        return getMessageMetaData().getMessageHeader();
    }

    @Override
    public long getExpiration()
    {
        return getMessageMetaData().getMessageHeader().getExpiration();
    }

    @Override
    public String getMessageType()
    {
        return AMQP_1_0;
    }

    @Override
    public long getArrivalTime()
    {
        return getMessageMetaData().getArrivalTime();
    }


    @Override
    public boolean isResourceAcceptable(final TransactionLogResource resource)
    {
        return getMessageHeader().getNotValidBefore() == 0L || resourceSupportsDeliveryDelay(resource);
    }

    private boolean resourceSupportsDeliveryDelay(final TransactionLogResource resource)
    {
        return resource instanceof Queue && ((Queue<?>)resource).isHoldOnPublishEnabled();
    }

    public HeaderSection getHeaderSection()
    {
        return getMessageMetaData().getHeaderSection();
    }

    public PropertiesSection getPropertiesSection()
    {
        return getMessageMetaData().getPropertiesSection();
    }

    public DeliveryAnnotationsSection getDeliveryAnnotationsSection()
    {
        return getMessageMetaData().getDeliveryAnnotationsSection();
    }

    public MessageAnnotationsSection getMessageAnnotationsSection()
    {
        return getMessageMetaData().getMessageAnnotationsSection();
    }

    public ApplicationPropertiesSection getApplicationPropertiesSection()
    {
        return getMessageMetaData().getApplicationPropertiesSection();
    }

    public FooterSection getFooterSection()
    {
        return getMessageMetaData().getFooterSection();
    }

    @Override
    public QpidByteBuffer getContent(final int offset, final int length)
    {
        if(getMessageMetaData().getVersion() == 0)
        {
            SectionDecoder sectionDecoder = new SectionDecoderImpl(DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry());

            try
            {
                List<EncodingRetainingSection<?>> sections;
                // The v0 message format put all sections within the content, so we need to read all the stored content
                // not just #getSize()
                try (QpidByteBuffer allSectionsContent = super.getContent(0, Integer.MAX_VALUE))
                {
                    sections = sectionDecoder.parseAll(allSectionsContent);
                }
                List<QpidByteBuffer> bodySectionContent = new ArrayList<>();

                for (final EncodingRetainingSection<?> section : sections)
                {
                    if (section instanceof DataSection
                        || section instanceof AmqpValueSection
                        || section instanceof AmqpSequenceSection)
                    {
                        bodySectionContent.add(section.getEncodedForm());
                    }
                    section.dispose();
                }
                try (QpidByteBuffer bodyContent = QpidByteBuffer.concatenate(bodySectionContent))
                {
                    bodySectionContent.forEach(QpidByteBuffer::dispose);
                    return bodyContent.view(offset, length);
                }
            }
            catch (AmqpErrorException e)
            {
                throw new ConnectionScopedRuntimeException(e);
            }
        }
        else
        {
            return super.getContent(offset, length);
        }
    }
}
