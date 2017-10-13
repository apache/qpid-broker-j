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

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Struct;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

@PluggableService
public class MessageFormat_0_10 implements MessageFormat<MessageTransferMessage>
{

    public static final int AMQP_MESSAGE_FORMAT_0_10 = 100;

    @Override
    public String getType()
    {
        return "AMQP_0_10";
    }

    @Override
    public int getSupportedFormat()
    {
        return AMQP_MESSAGE_FORMAT_0_10;
    }

    @Override
    public Class<MessageTransferMessage> getMessageClass()
    {
        return MessageTransferMessage.class;
    }

    // format: <int header count> <headers> <body>

    @Override
    public QpidByteBuffer convertToMessageFormat(final MessageTransferMessage message)
    {
        ServerEncoder encoder = new ServerEncoder(4096, true);
        Struct[] structs = message.getHeader().getStructs();
        encoder.writeInt32(structs.length);
        for(Struct struct : structs)
        {
            encoder.writeStruct32(struct);
        }
        try (QpidByteBuffer headerBuf = encoder.getBuffer();
             QpidByteBuffer content = message.getContent())
        {
            return QpidByteBuffer.concatenate(headerBuf, content);
        }
    }

    @Override
    public MessageTransferMessage createMessage(final QpidByteBuffer payload,
                                                final MessageStore store,
                                                final Object connectionReference)
    {
        try
        {
            ServerDecoder serverDecoder = new ServerDecoder(payload);
            int headerCount = serverDecoder.readInt32();
            DeliveryProperties deliveryProperties = null;
            MessageProperties messageProperties = null;
            List<Struct> nonStandard = null;
            for(int i = 0; i<headerCount; i++)
            {

                final Struct struct = serverDecoder.readStruct32();
                switch(struct.getStructType())
                {
                    case DeliveryProperties.TYPE:
                        deliveryProperties = (DeliveryProperties)struct;
                        break;
                    case MessageProperties.TYPE:
                        messageProperties = (MessageProperties)struct;
                        break;
                    default:
                        if(nonStandard == null)
                        {
                            nonStandard = new ArrayList<>();
                        }
                        nonStandard.add(struct);
                }
            }
            Header header = new Header(deliveryProperties, messageProperties, nonStandard);
            MessageMetaData_0_10 metaData = new MessageMetaData_0_10(header,
                                                                     payload.remaining(),
                                                                     System.currentTimeMillis());
            final MessageHandle<MessageMetaData_0_10> handle = store.addMessage(metaData);
            handle.addContent(payload);
            final StoredMessage<MessageMetaData_0_10> storedMessage = handle.allContentAdded();
            return new MessageTransferMessage(storedMessage, connectionReference);

        }
        catch (BufferUnderflowException e )
        {
            throw new ConnectionScopedRuntimeException("Error parsing AMQP 0-10 message format", e);
        }
    }
}
