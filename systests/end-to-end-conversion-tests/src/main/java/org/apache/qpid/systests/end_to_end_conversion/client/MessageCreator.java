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

package org.apache.qpid.systests.end_to_end_conversion.client;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.StreamMessage;

public class MessageCreator
{
    public static Message fromMessageDescription(final Session session,
                                                 final MessageDescription messageDescription)
            throws Exception
    {
        Message message = createMessage(messageDescription, session);
        setJmsHeader(messageDescription, message);
        setProperties(messageDescription, message);
        return message;
    }

    private static void setProperties(final MessageDescription messageDescription,
                                      final Message message)
    {
        final HashMap<String, Serializable> properties = messageDescription.getProperties();
        if (properties == null)
        {
            return;
        }

        for (Map.Entry<String, Serializable> entry : properties.entrySet())
        {
            try
            {
                message.setObjectProperty(entry.getKey(), entry.getValue());
            }
            catch (JMSException e)
            {
                throw new RuntimeException(String.format("Could not set message property '%s' to this value: %s",
                                                         entry.getKey(),
                                                         String.valueOf(entry.getValue())), e);
            }
        }
    }

    private static Message createMessage(final MessageDescription messageDescription,
                                         final Session session)
            throws Exception
    {
        Message message;
        try
        {
            switch (messageDescription.getMessageType())
            {
                case MESSAGE:
                    message = session.createTextMessage();
                    break;
                case BYTES_MESSAGE:
                    message = session.createBytesMessage();
                    ((BytesMessage) message).writeBytes(((byte[]) messageDescription.getContent()));
                    break;
                case MAP_MESSAGE:
                    message = session.createMapMessage();
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) messageDescription.getContent()).entrySet())
                    {
                        ((MapMessage) message).setObject(entry.getKey(), entry.getValue());
                    }
                    break;
                case OBJECT_MESSAGE:
                    message = session.createObjectMessage((Serializable) messageDescription.getContent());
                    break;
                case STREAM_MESSAGE:
                    message = session.createStreamMessage();
                    for (Object item : (Collection<?>) messageDescription.getContent())
                    {
                        ((StreamMessage) message).writeObject(item);
                    }
                    break;
                case TEXT_MESSAGE:
                    message = session.createTextMessage(messageDescription.getContent().toString());
                    break;
                default:
                    throw new RuntimeException(String.format("unexpected message type '%s'",
                                                             messageDescription.getMessageType()));
            }
        }
        catch (ClassCastException e)
        {
            throw new RuntimeException(String.format("Could not create message of type '%s' with this body: %s",
                                                     messageDescription.getMessageType(),
                                                     messageDescription.getContent().toString()), e);
        }
        return message;
    }

    private static void setJmsHeader(final MessageDescription messageDescription,
                                     final Message message)
            throws JMSException
    {
        final HashMap<MessageDescription.MessageHeader, Serializable> header =
                messageDescription.getHeaders();

        if (header == null)
        {
            return;
        }

        for (Map.Entry<MessageDescription.MessageHeader, Serializable> entry : header.entrySet())
        {
            try
            {
                switch (entry.getKey())
                {
                    case DESTINATION:
                        message.setJMSDestination((Destination) entry.getValue());
                        break;
                    case DELIVERY_MODE:
                        message.setJMSDeliveryMode((Integer) entry.getValue());
                        break;
                    case MESSAGE_ID:
                        message.setJMSMessageID((String) entry.getValue());
                        break;
                    case TIMESTAMP:
                        message.setJMSTimestamp((Long) entry.getValue());
                        break;
                    case CORRELATION_ID:
                        if (entry.getValue() instanceof byte[])
                        {
                            message.setJMSCorrelationIDAsBytes((byte[]) entry.getValue());
                        }
                        else
                        {
                            message.setJMSCorrelationID((String) entry.getValue());
                        }
                        break;
                    case REPLY_TO:
                        throw new RuntimeException("The Test should not set the replyTo header."
                                                   + " It should rather use the dedicated method");
                    case REDELIVERED:
                        message.setJMSRedelivered((Boolean) entry.getValue());
                        break;
                    case TYPE:
                        message.setJMSType((String) entry.getValue());
                        break;
                    case EXPIRATION:
                        message.setJMSExpiration((Long) entry.getValue());
                        break;
                    case PRIORITY:
                        message.setJMSPriority((Integer) entry.getValue());
                        break;
                    default:
                        throw new RuntimeException(String.format("unexpected message header '%s'", entry.getKey()));
                }
            }
            catch (ClassCastException e)
            {
                throw new RuntimeException(String.format("Could not set message header '%s' to this value: %s",
                                                         entry.getKey(),
                                                         entry.getValue()), e);
            }
        }
    }
}
