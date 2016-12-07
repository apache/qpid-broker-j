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

package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v1_0.MessageConverter_to_1_0;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.transport.DeliveryProperties;
import org.apache.qpid.transport.MessageDeliveryMode;
import org.apache.qpid.transport.MessageProperties;
import org.apache.qpid.util.GZIPUtils;

@PluggableService
public class MessageConverter_0_10_to_1_0  extends MessageConverter_to_1_0<MessageTransferMessage>
{
    @Override
    public Class<MessageTransferMessage> getInputClass()
    {
        return MessageTransferMessage.class;
    }


    @Override
    protected MessageMetaData_1_0 convertMetaData(MessageTransferMessage serverMessage,
                                                  final EncodingRetainingSection<?> bodySection,
                                                  SectionEncoder sectionEncoder)
    {
        final MessageProperties msgProps = serverMessage.getHeader().getMessageProperties();
        final DeliveryProperties deliveryProps = serverMessage.getHeader().getDeliveryProperties();

        Header header = new Header();
        if(deliveryProps != null)
        {
            header.setDurable(deliveryProps.hasDeliveryMode() && deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT);
            if(deliveryProps.hasPriority())
            {
                header.setPriority(UnsignedByte.valueOf((byte) deliveryProps.getPriority().getValue()));
            }
            if(deliveryProps.hasTtl())
            {
                header.setTtl(UnsignedInteger.valueOf(deliveryProps.getTtl()));
            }
        }

        Properties props = new Properties();
        ApplicationProperties applicationProperties = null;


        /*
            TODO: the current properties are not currently set:

            absoluteExpiryTime
            creationTime
            groupId
            groupSequence
            replyToGroupId
            to
        */

        if(msgProps != null)
        {
            if(msgProps.hasContentEncoding()
               && !GZIPUtils.GZIP_CONTENT_ENCODING.equals(msgProps.getContentEncoding())
               && bodySection instanceof Data)
            {
                props.setContentEncoding(Symbol.valueOf(msgProps.getContentEncoding()));
            }

            if(msgProps.hasCorrelationId())
            {
                props.setCorrelationId(msgProps.getCorrelationId());
            }

            if(msgProps.hasMessageId())
            {
                props.setMessageId(msgProps.getMessageId());
            }
            if(msgProps.hasReplyTo())
            {
                props.setReplyTo(msgProps.getReplyTo().getExchange()+"/"+msgProps.getReplyTo().getRoutingKey());
            }
            if(msgProps.hasContentType())
            {
                props.setContentType(Symbol.valueOf(msgProps.getContentType()));

                // Modify the content type when we are dealing with java object messages produced by the Qpid 0.x client
                if(props.getContentType() == Symbol.valueOf("application/java-object-stream"))
                {
                    props.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
                }
            }

            props.setSubject(serverMessage.getInitialRoutingAddress());

            if(msgProps.hasUserId())
            {
                props.setUserId(new Binary(msgProps.getUserId()));
            }

            Map<String, Object> applicationPropertiesMap = msgProps.getApplicationHeaders();
            if(applicationPropertiesMap != null)
            {
                if(applicationPropertiesMap.containsKey("qpid.subject"))
                {
                    props.setSubject(String.valueOf(applicationPropertiesMap.get("qpid.subject")));
                    applicationPropertiesMap = new LinkedHashMap<>(applicationPropertiesMap);
                    applicationPropertiesMap.remove("qpid.subject");
                }
                applicationProperties = new ApplicationProperties(applicationPropertiesMap);

            }
        }
        return new MessageMetaData_1_0(header.createEncodingRetainingSection(sectionEncoder),
                                       null,
                                       null,
                                       props.createEncodingRetainingSection(sectionEncoder),
                                       applicationProperties == null ? null : applicationProperties.createEncodingRetainingSection(sectionEncoder),
                                       null,
                                       serverMessage.getArrivalTime(),
                                       bodySection.getEncodedSize());
    }

    @Override
    public String getType()
    {
        return "v0-10 to v1-0";
    }
}
