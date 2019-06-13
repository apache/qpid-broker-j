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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_v0_10 implements MessageConverter<ServerMessage, MessageTransferMessage>
{

    @Override
    public Class<ServerMessage> getInputClass()
    {
        return ServerMessage.class;
    }

    @Override
    public Class<MessageTransferMessage> getOutputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public MessageTransferMessage convert(ServerMessage serverMsg, NamedAddressSpace addressSpace)
    {
        return new MessageTransferMessage(convertToStoredMessage(serverMsg), null);
    }

    @Override
    public void dispose(final MessageTransferMessage message)
    {

    }

    private StoredMessage<MessageMetaData_0_10> convertToStoredMessage(final ServerMessage<?> serverMsg)
    {
        final MessageMetaData_0_10 messageMetaData_0_10 = convertMetaData(serverMsg);
        final int metadataSize = messageMetaData_0_10.getStorableSize();

        return new StoredMessage<MessageMetaData_0_10>()
                {
                    @Override
                    public MessageMetaData_0_10 getMetaData()
                    {
                        return messageMetaData_0_10;
                    }

                    @Override
                    public long getMessageNumber()
                    {
                        return serverMsg.getMessageNumber();
                    }

                    @Override
                    public QpidByteBuffer getContent(final int offset, final int length)
                    {
                        return serverMsg.getContent(offset, length);
                    }

                    @Override
                    public int getContentSize()
                    {
                        return messageMetaData_0_10.getContentSize();
                    }

                    @Override
                    public int getMetadataSize()
                    {
                        return metadataSize;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isInContentInMemory()
                    {
                        return true;
                    }

                    @Override
                    public long getInMemorySize()
                    {
                        return getContentSize() + getMetadataSize();
                    }

                    @Override
                    public boolean flowToDisk()
                    {
                        return false;
                    }

                    @Override
                    public void reallocate()
                    {

                    }
                };
    }

    private MessageMetaData_0_10 convertMetaData(ServerMessage serverMsg)
    {
        DeliveryProperties deliveryProps = new DeliveryProperties();
        MessageProperties messageProps = new MessageProperties();

        int size = (int) serverMsg.getSize();

        deliveryProps.setExpiration(serverMsg.getExpiration());
        deliveryProps.setPriority(MessageDeliveryPriority.get(serverMsg.getMessageHeader().getPriority()));
        deliveryProps.setRoutingKey(serverMsg.getInitialRoutingAddress());
        deliveryProps.setTimestamp(serverMsg.getMessageHeader().getTimestamp());

        messageProps.setContentEncoding(serverMsg.getMessageHeader().getEncoding());
        messageProps.setContentLength(size);
        messageProps.setContentType(serverMsg.getMessageHeader().getMimeType());
        if(serverMsg.getMessageHeader().getCorrelationId() != null)
        {
            messageProps.setCorrelationId(serverMsg.getMessageHeader().getCorrelationId().getBytes(UTF_8));
        }

        Header header = new Header(deliveryProps, messageProps, null);
        return new MessageMetaData_0_10(header, size, serverMsg.getArrivalTime());
    }


    @Override
    public String getType()
    {
        return "Unknown to v0-10";
    }
}
