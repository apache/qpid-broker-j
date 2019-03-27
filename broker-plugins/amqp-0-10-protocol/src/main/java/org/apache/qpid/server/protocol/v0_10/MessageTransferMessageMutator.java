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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessageMutator;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;

public class MessageTransferMessageMutator implements ServerMessageMutator<MessageTransferMessage>
{
    private final MessageTransferMessage _message;
    private final MessageStore _messageStore;
    private MessageProperties _messageProperties;
    private DeliveryProperties _deliveryProperties;

    MessageTransferMessageMutator(final MessageTransferMessage message, final MessageStore messageStore)
    {
        _message = message;
        _messageStore = messageStore;
        final MessageProperties messageProperties = message.getHeader().getMessageProperties();
        _messageProperties = messageProperties == null ? null : new MessageProperties(messageProperties);
        final DeliveryProperties deliveryProperties = _message.getHeader().getDeliveryProperties();
        DeliveryProperties properties = null;
        if (deliveryProperties != null)
        {
            properties = new DeliveryProperties();
            if (deliveryProperties.hasDeliveryMode())
            {
                properties.setDeliveryMode(deliveryProperties.getDeliveryMode());
            }
            if (deliveryProperties.hasDiscardUnroutable())
            {
                properties.setDiscardUnroutable(deliveryProperties.getDiscardUnroutable());
            }
            if (deliveryProperties.hasExchange())
            {
                properties.setExchange(deliveryProperties.getExchange());
            }
            if (deliveryProperties.hasExpiration())
            {
                properties.setExpiration(deliveryProperties.getExpiration());
            }
            if (deliveryProperties.hasTtl())
            {
                properties.setTtl(deliveryProperties.getTtl());
            }
            if (deliveryProperties.hasImmediate())
            {
                properties.setImmediate(deliveryProperties.getImmediate());
            }
            if (deliveryProperties.hasPriority())
            {
                properties.setPriority(deliveryProperties.getPriority());
            }
            if (deliveryProperties.hasRedelivered())
            {
                properties.setRedelivered(deliveryProperties.getRedelivered());
            }
            if (deliveryProperties.hasResumeId())
            {
                properties.setResumeId(deliveryProperties.getResumeId());
            }
            if (deliveryProperties.hasResumeTtl())
            {
                properties.setResumeTtl(deliveryProperties.getResumeTtl());
            }
            if (deliveryProperties.hasRoutingKey())
            {
                properties.setRoutingKey(deliveryProperties.getRoutingKey());
            }
            if (deliveryProperties.hasTimestamp())
            {
                properties.setTimestamp(deliveryProperties.getTimestamp());
            }
        }
        _deliveryProperties = properties;
    }

    @Override
    public void setPriority(final byte priority)
    {
        if (_deliveryProperties == null)
        {
            _deliveryProperties = new DeliveryProperties();
        }
        _deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
    }


    @Override
    public byte getPriority()
    {
        MessageDeliveryPriority priority = _deliveryProperties == null || !_deliveryProperties.hasPriority()
                ? MessageDeliveryPriority.MEDIUM
                : _deliveryProperties.getPriority();
        return (byte) priority.getValue();
    }

    @Override
    public MessageTransferMessage create()
    {
        final Header header = new Header(_deliveryProperties, _messageProperties);
        final MessageMetaData_0_10 messageMetaData =
                new MessageMetaData_0_10(header, (int) _message.getSize(), _message.getArrivalTime());
        final QpidByteBuffer content = _message.getContent();
        final MessageHandle<MessageMetaData_0_10> addedMessage = _messageStore.addMessage(messageMetaData);
        if (content != null)
        {
            addedMessage.addContent(content);
        }
        return new MessageTransferMessage(addedMessage.allContentAdded(), _message.getConnectionReference());
    }
}
