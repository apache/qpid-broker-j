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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessageMutator;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Footer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.FooterSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;

public class Message_1_0_Mutator implements ServerMessageMutator<Message_1_0>
{
    private final Message_1_0 _message;
    private final MessageStore _messageStore;

    private Header _header;
    private Map<Symbol, Object> _deliveryAnnotations;
    private Map<Symbol, Object> _messageAnnotations;
    private Properties _properties;
    private Map<String, Object> _applicationProperties;
    private Map<Symbol, Object> _footer;

    Message_1_0_Mutator(final Message_1_0 message, final MessageStore messageStore)
    {
        _message = message;
        _messageStore = messageStore;
        final HeaderSection headerSection = message.getHeaderSection();
        if (headerSection != null)
        {
            final Header header = headerSection.getValue();
            if (header != null)
            {
                _header = new Header();
                _header.setDeliveryCount(header.getDeliveryCount());
                _header.setDurable(header.getDurable());
                _header.setFirstAcquirer(header.getFirstAcquirer());
                _header.setPriority(header.getPriority());
                _header.setTtl(header.getTtl());
            }
            headerSection.dispose();
        }
        final DeliveryAnnotationsSection deliveryAnnotationsSection = message.getDeliveryAnnotationsSection();
        if (deliveryAnnotationsSection != null)
        {
            final Map<Symbol, Object> deliveryAnnotations = deliveryAnnotationsSection.getValue();
            if (deliveryAnnotations != null)
            {
                _deliveryAnnotations = new HashMap<>(deliveryAnnotations);
            }
            deliveryAnnotationsSection.dispose();
        }
        final MessageAnnotationsSection messageAnnotationsSection = message.getMessageAnnotationsSection();
        if (messageAnnotationsSection != null)
        {
            final Map<Symbol, Object> messageAnnotations = messageAnnotationsSection.getValue();
            if (messageAnnotations != null)
            {
                _messageAnnotations = new HashMap<>(messageAnnotations);
            }
            messageAnnotationsSection.dispose();
        }
        final PropertiesSection propertiesSection = message.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                _properties = new Properties();
                _properties.setCorrelationId(properties.getCorrelationId());
                _properties.setAbsoluteExpiryTime(properties.getAbsoluteExpiryTime());
                _properties.setContentEncoding(properties.getContentEncoding());
                _properties.setContentType(properties.getContentType());
                _properties.setCreationTime(properties.getCreationTime());
                _properties.setGroupId(properties.getGroupId());
                _properties.setGroupSequence(properties.getGroupSequence());
                _properties.setMessageId(properties.getMessageId());
                _properties.setReplyTo(properties.getReplyTo());
                _properties.setReplyToGroupId(properties.getReplyToGroupId());
                _properties.setSubject(properties.getSubject());
                _properties.setTo(properties.getTo());
                _properties.setUserId(properties.getUserId());
            }
            propertiesSection.dispose();
        }
        final ApplicationPropertiesSection applicationPropertiesSection = message.getApplicationPropertiesSection();
        if (applicationPropertiesSection != null)
        {
            final Map<String, Object> applicationProperties = applicationPropertiesSection.getValue();
            if (applicationProperties != null)
            {
                _applicationProperties = new HashMap<>(applicationProperties);
            }
            applicationPropertiesSection.dispose();
        }
        final FooterSection footerSection = message.getFooterSection();
        if (footerSection != null)
        {
            final Map<Symbol, Object> footer = footerSection.getValue();
            if (footer != null)
            {
                _footer = new HashMap<>(footer);
            }
            footerSection.dispose();
        }
    }

    @Override
    public void setPriority(final byte priority)
    {
        if (_header == null)
        {
            _header = new Header();
        }
        _header.setPriority(UnsignedByte.valueOf(priority));
    }


    @Override
    public byte getPriority()
    {
        if (_header == null || _header.getPriority() == null)
        {
            return 4; //javax.jms.Message.DEFAULT_PRIORITY;
        }
        else
        {
            return _header.getPriority().byteValue();
        }
    }

    @Override
    public Message_1_0 create()
    {
        final long contentSize = _message.getSize();

        HeaderSection headerSection = null;
        if (_header != null)
        {
            headerSection = _header.createEncodingRetainingSection();
        }

        DeliveryAnnotationsSection deliveryAnnotationsSection = null;
        if (_deliveryAnnotations != null)
        {
            deliveryAnnotationsSection = new DeliveryAnnotations(_deliveryAnnotations).createEncodingRetainingSection();
        }

        MessageAnnotationsSection messageAnnotationsSection = null;
        if (_messageAnnotations != null)
        {
            messageAnnotationsSection = new MessageAnnotations(_messageAnnotations).createEncodingRetainingSection();
        }

        PropertiesSection propertiesSection = null;
        if (_properties != null)
        {
            propertiesSection = _properties.createEncodingRetainingSection();
        }

        ApplicationPropertiesSection applicationPropertiesSection = null;
        if (_applicationProperties != null)
        {
            applicationPropertiesSection =
                    new ApplicationProperties(_applicationProperties).createEncodingRetainingSection();
        }

        FooterSection footerSection = null;
        if (_footer != null)
        {
            footerSection = new Footer(_footer).createEncodingRetainingSection();
        }

        final QpidByteBuffer content = _message.getContent();
        final MessageMetaData_1_0 mmd = new MessageMetaData_1_0(headerSection,
                                                                deliveryAnnotationsSection,
                                                                messageAnnotationsSection,
                                                                propertiesSection,
                                                                applicationPropertiesSection,
                                                                footerSection,
                                                                _message.getArrivalTime(),
                                                                contentSize);

        final MessageHandle<MessageMetaData_1_0> handle = _messageStore.addMessage(mmd);
        if (content != null)
        {
            handle.addContent(content);
        }
        return new Message_1_0(handle.allContentAdded(), _message.getConnectionReference());
    }
}
