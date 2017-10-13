/*
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
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorRuntimeException;
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
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

@SuppressWarnings("unused")
@PluggableService
public class MessageFormat_1_0 implements MessageFormat<Message_1_0>
{
    public static final int AMQP_MESSAGE_FORMAT_1_0 = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageFormat_1_0.class);
    private final SectionDecoder _sectionDecoder = new SectionDecoderImpl(AMQPDescribedTypeRegistry.newInstance()
                                                                                               .registerTransportLayer()
                                                                                               .registerMessagingLayer()
                                                                                               .registerTransactionLayer()
                                                                                               .registerSecurityLayer()
                                                                                               .registerExtensionSoleconnLayer()
                                                                                               .getSectionDecoderRegistry());

    @Override
    public String getType()
    {
        return "AMQP_1_0";
    }

    @Override
    public int getSupportedFormat()
    {
        return AMQP_MESSAGE_FORMAT_1_0;
    }

    @Override
    public Class<Message_1_0> getMessageClass()
    {
        return Message_1_0.class;
    }

    @Override
    public QpidByteBuffer convertToMessageFormat(final Message_1_0 message)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Message_1_0 createMessage(final QpidByteBuffer payload,
                                     final MessageStore store,
                                     final Object connectionReference)
    {
        List<EncodingRetainingSection<?>> dataSections = new ArrayList<>();

        List<EncodingRetainingSection<?>> allSections;
        try
        {
            allSections = getSectionDecoder().parseAll(payload);
        }
        catch (AmqpErrorException e)
        {
            throw new AmqpErrorRuntimeException(e);
        }
        MessageMetaData_1_0 mmd = createMessageMetaData(allSections, dataSections);

        MessageHandle<MessageMetaData_1_0> handle = store.addMessage(mmd);

        for (EncodingRetainingSection<?> dataSection : dataSections)
        {
            try (QpidByteBuffer encodedForm = dataSection.getEncodedForm())
            {
                handle.addContent(encodedForm);
            }
            dataSection.dispose();
        }
        final StoredMessage<MessageMetaData_1_0> storedMessage = handle.allContentAdded();

        return new Message_1_0(storedMessage, connectionReference);
    }

    private MessageMetaData_1_0 createMessageMetaData(final List<EncodingRetainingSection<?>> allSections,
                                                      final List<EncodingRetainingSection<?>> dataSections)
    {
        long contentSize = 0L;

        HeaderSection headerSection = null;
        PropertiesSection propertiesSection = null;
        DeliveryAnnotationsSection deliveryAnnotationsSection = null;
        MessageAnnotationsSection messageAnnotationsSection = null;
        ApplicationPropertiesSection applicationPropertiesSection = null;
        FooterSection footerSection = null;

        Iterator<EncodingRetainingSection<?>> iter = allSections.iterator();
        EncodingRetainingSection<?> s = iter.hasNext() ? iter.next() : null;
        if (s instanceof HeaderSection)
        {
            headerSection = (HeaderSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof DeliveryAnnotationsSection)
        {
            deliveryAnnotationsSection = (DeliveryAnnotationsSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof MessageAnnotationsSection)
        {
            messageAnnotationsSection = (MessageAnnotationsSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof PropertiesSection)
        {
            propertiesSection = (PropertiesSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof ApplicationPropertiesSection)
        {
            applicationPropertiesSection = (ApplicationPropertiesSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof AmqpValueSection)
        {
            contentSize = s.getEncodedSize();
            dataSections.add(s);
            s = iter.hasNext() ? iter.next() : null;
        }
        else if (s instanceof DataSection)
        {
            do
            {
                contentSize += s.getEncodedSize();
                dataSections.add(s);
                s = iter.hasNext() ? iter.next() : null;
            }
            while (s instanceof DataSection);
        }
        else if (s instanceof AmqpSequenceSection)
        {
            do
            {
                contentSize += s.getEncodedSize();
                dataSections.add(s);
                s = iter.hasNext() ? iter.next() : null;
            }
            while (s instanceof AmqpSequenceSection);
        }

        if (s instanceof FooterSection)
        {
            footerSection = (FooterSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }
        if (s != null)
        {
            throw new ConnectionScopedRuntimeException(String.format("Encountered unexpected section '%s'", s.getClass().getSimpleName()));
        }
        return new MessageMetaData_1_0(headerSection,
                                       deliveryAnnotationsSection,
                                       messageAnnotationsSection,
                                       propertiesSection,
                                       applicationPropertiesSection,
                                       footerSection,
                                       System.currentTimeMillis(),
                                       contentSize);
    }

    private SectionDecoder getSectionDecoder()
    {
        return _sectionDecoder;
    }
}
