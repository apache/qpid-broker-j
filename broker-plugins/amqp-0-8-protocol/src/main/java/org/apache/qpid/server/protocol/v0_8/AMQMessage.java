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

import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;

/**
 * A deliverable message.
 */
public class AMQMessage extends AbstractServerMessageImpl<AMQMessage, MessageMetaData>
{
    private static final MessageMetaData DELETED_MESSAGE_METADATA = new MessageMetaData(new MessagePublishInfo(), new ContentHeaderBody(new BasicContentHeaderProperties()), 0);
    private static final String AMQP_0_9_1 = "AMQP 0-9-1";

    public AMQMessage(StoredMessage<MessageMetaData> handle)
    {
        this(handle, null);
    }

    public AMQMessage(StoredMessage<MessageMetaData> handle, Object connectionReference)
    {
        super(handle, connectionReference);
    }

    public MessageMetaData getMessageMetaData()
    {
        MessageMetaData metaData = getStoredMessage().getMetaData();

        return metaData == null ? DELETED_MESSAGE_METADATA : metaData;
    }

    public ContentHeaderBody getContentHeaderBody()
    {
        return getMessageMetaData().getContentHeaderBody();
    }

    @Override
    public String getInitialRoutingAddress()
    {
        MessageMetaData messageMetaData = getMessageMetaData();
        AMQShortString routingKey = messageMetaData.getMessagePublishInfo().getRoutingKey();
        if (routingKey != null)
        {
            return routingKey.toString();
        }
        return "";
    }

    @Override
    public String getTo()
    {
        return AMQShortString.toString(getMessagePublishInfo().getExchange());
    }

    @Override
    public AMQMessageHeader getMessageHeader()
    {
        return getMessageMetaData().getMessageHeader();
    }

    public MessagePublishInfo getMessagePublishInfo()
    {
        return getMessageMetaData().getMessagePublishInfo();
    }

    @Override
    public long getArrivalTime()
    {
        return getMessageMetaData().getArrivalTime();
    }

    @Override
    public boolean isResourceAcceptable(final TransactionLogResource resource)
    {
        return true;
    }

    public boolean isImmediate()
    {
        return getMessagePublishInfo().isImmediate();
    }

    public boolean isMandatory()
    {
        return getMessagePublishInfo().isMandatory();
    }

    @Override
    public long getExpiration()
    {
        return getMessageMetaData().getContentHeaderBody().getProperties().getExpiration();
    }

    @Override
    public String getMessageType()
    {
        return AMQP_0_9_1;
    }

    @Override
    protected void validate()
    {
        getMessageMetaData().validate();
    }
}
