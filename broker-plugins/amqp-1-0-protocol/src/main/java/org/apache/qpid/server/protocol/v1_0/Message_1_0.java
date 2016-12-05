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


import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Section;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;

public class Message_1_0 extends AbstractServerMessageImpl<Message_1_0, MessageMetaData_1_0>
{

    private static final AMQPDescribedTypeRegistry DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();
    public static final MessageMetaData_1_0 DELETED_MESSAGE_METADATA = new MessageMetaData_1_0(Collections.<Section>emptyList(), new SectionEncoderImpl(DESCRIBED_TYPE_REGISTRY));

    private long _arrivalTime;

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage)
    {
        super(storedMessage, null, storedMessage.getMetaData().getContentSize());
    }

    public Message_1_0(final StoredMessage<MessageMetaData_1_0> storedMessage,
                       final Object connectionReference)
    {
        super(storedMessage, connectionReference, storedMessage.getMetaData().getContentSize());
        _arrivalTime = System.currentTimeMillis();
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
        return _arrivalTime;
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

}
