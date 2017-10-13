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
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;


public class MessageTransferMessage extends AbstractServerMessageImpl<MessageTransferMessage, MessageMetaData_0_10>
{

    private final static MessageMetaData_0_10 DELETED_MESSAGE_METADATA = new MessageMetaData_0_10(null, 0, 0);
    private static final String AMQP_0_10 = "AMQP 0-10";

    public MessageTransferMessage(StoredMessage<MessageMetaData_0_10> storeMessage, Object connectionRef)
    {
        super(storeMessage, connectionRef);
    }

    private MessageMetaData_0_10 getMetaData()
    {
        MessageMetaData_0_10 metaData = getStoredMessage().getMetaData();

        return metaData == null ? DELETED_MESSAGE_METADATA : metaData;
    }

    @Override
    public String getInitialRoutingAddress()
    {
        final String routingKey = getMetaData().getRoutingKey();
        return routingKey == null ? "" : routingKey;
    }

    @Override
    public String getTo()
    {
        return getMetaData().getExchange();
    }

    @Override
    public AMQMessageHeader getMessageHeader()
    {
        return getMetaData().getMessageHeader();
    }


    public boolean isImmediate()
    {
        return getMetaData().isImmediate();
    }

    @Override
    public long getExpiration()
    {
        return getMetaData().getExpiration();
    }

    @Override
    public String getMessageType()
    {
        return AMQP_0_10;
    }

    @Override
    public long getArrivalTime()
    {
        return getMetaData().getArrivalTime();
    }

    @Override
    public boolean isResourceAcceptable(final TransactionLogResource resource)
    {
        return true;
    }

    public Header getHeader()
    {
        return getMetaData().getHeader();
    }

    public QpidByteBuffer getBody()
    {
        return  getContent();
    }
}
