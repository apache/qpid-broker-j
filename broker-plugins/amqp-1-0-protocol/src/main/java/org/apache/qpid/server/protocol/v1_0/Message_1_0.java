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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.NonEncodingRetainingSection;
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
    public static final MessageMetaData_1_0 DELETED_MESSAGE_METADATA = new MessageMetaData_1_0(null, null, null, null, null, null, 0L, 0L);

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage)
    {
        super(storedMessage, null, storedMessage.getMetaData().getContentSize());
    }

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage,
                       final Object connectionReference)
    {
        super(storedMessage, connectionReference, storedMessage.getMetaData().getContentSize());
    }

    public String getInitialRoutingAddress()
    {
        Object routingKey = getMessageHeader().getHeader("routing-key");
        if(routingKey != null)
        {
            return routingKey.toString();
        }
        else
        {
            return getMessageHeader().getTo();
        }
    }

    private MessageMetaData_1_0 getMessageMetaData()
    {
        MessageMetaData_1_0 metaData = getStoredMessage().getMetaData();
        return metaData == null ? DELETED_MESSAGE_METADATA : metaData;
    }

    public MessageMetaData_1_0.MessageHeader_1_0 getMessageHeader()
    {
        return getMessageMetaData().getMessageHeader();
    }

    @Override
    public long getExpiration()
    {
        return getMessageMetaData().getMessageHeader().getExpiration();
    }

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


    public Collection<QpidByteBuffer> getFragments()
    {
        return getContent(0, (int) getSize());
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
    public Collection<QpidByteBuffer> getContent(final int offset, final int length)
    {
        if(getMessageMetaData().getVersion() == 0)
        {
            SectionDecoder sectionDecoder = new SectionDecoderImpl(DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry());

            try
            {
                final Collection<QpidByteBuffer> allSectionsContent = super.getContent(0, Integer.MAX_VALUE);

                List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(new ArrayList<>(allSectionsContent));

                List<QpidByteBuffer> bodySectionContent = new ArrayList<>();
                for(QpidByteBuffer buf : allSectionsContent)
                {
                    buf.dispose();
                }
                Iterator<EncodingRetainingSection<?>> iter = sections.iterator();

                while (iter.hasNext())
                {
                    final EncodingRetainingSection<?> section = iter.next();
                    if (section instanceof DataSection
                        || section instanceof AmqpValueSection
                        || section instanceof AmqpSequenceSection)
                    {
                        bodySectionContent.addAll(section.getEncodedForm());
                    }
                    section.dispose();
                }
                if(offset == 0 && length >= QpidByteBufferUtils.remaining(bodySectionContent))
                {
                    return bodySectionContent;
                }
                else
                {
                    final Collection<QpidByteBuffer> contentView = new ArrayList<>();
                    int position = 0;
                    for(QpidByteBuffer buf :bodySectionContent)
                    {
                        if (position < offset)
                        {
                            if (offset - position < buf.remaining())
                            {
                                QpidByteBuffer view = buf.view(offset - position,
                                                               Math.min(length, buf.remaining() - (offset - position)));
                                contentView.add(view);
                                position += view.remaining();
                            }
                            else
                            {
                                position += buf.remaining();
                            }
                        }
                        else if (position <= offset + length)
                        {
                            QpidByteBuffer view = buf.view(0, Math.min(length - (position - offset), buf.remaining()));
                            contentView.add(view);
                            position += view.remaining();
                        }

                        buf.dispose();
                    }
                    return contentView;
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
