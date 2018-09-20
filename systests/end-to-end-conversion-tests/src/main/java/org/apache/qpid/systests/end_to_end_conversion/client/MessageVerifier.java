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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

public class MessageVerifier
{
    public static void verifyMessage(final MessageDescription messageDescription, final Message message)
            throws VerificationException
    {
        verifyNotNull("No message received", message);
        verifyMessageTypeAndContent(messageDescription, message);
        verifyMessageHeaders(messageDescription, message);
        verifyMessageProperties(messageDescription, message);
    }

    private static void verifyMessageTypeAndContent(final MessageDescription messageDescription,
                                                    final Message message) throws VerificationException
    {
        final MessageDescription.MessageType messageType = messageDescription.getMessageType();
        Object expectedMessageContent = messageDescription.getContent();
        Serializable actualContent;
        Class<? extends Message> expectedMessageClass;

        try
        {
            switch (messageType)
            {
                case MESSAGE:
                    expectedMessageClass = Message.class;
                    actualContent = null;
                    break;
                case BYTES_MESSAGE:
                    expectedMessageClass = BytesMessage.class;
                    final int bodyLength = (int) ((BytesMessage) message).getBodyLength();
                    actualContent = new byte[bodyLength];
                    ((BytesMessage) message).readBytes((byte[]) actualContent);
                    break;
                case MAP_MESSAGE:
                    expectedMessageClass = MapMessage.class;
                    final HashMap<Object, Object> content = new HashMap<>();
                    final MapMessage mapMessage = (MapMessage) message;
                    for (Object name : Collections.list(mapMessage.getMapNames()))
                    {
                        content.put(name, mapMessage.getObject((String) name));
                    }
                    actualContent = content;
                    break;
                case OBJECT_MESSAGE:
                    expectedMessageClass = ObjectMessage.class;
                    actualContent = ((ObjectMessage) message).getObject();
                    break;
                case STREAM_MESSAGE:
                    expectedMessageClass = StreamMessage.class;
                    actualContent = new ArrayList<>();
                    try
                    {
                        while (true)
                        {
                            ((List) actualContent).add(((StreamMessage) message).readObject());
                        }
                    }
                    catch (MessageEOFException e)
                    {
                        // pass
                    }
                    break;
                case TEXT_MESSAGE:
                    expectedMessageClass = TextMessage.class;
                    actualContent = ((TextMessage) message).getText();
                    break;
                default:
                    throw new RuntimeException(String.format("unexpected message type '%s'",
                                                             messageType));
            }
            verifyEquals("Unexpected message type", expectedMessageClass, message.getClass());
            verifyEquals("Unexpected message content", expectedMessageContent, actualContent);
        }
        catch (JMSException e)
        {
            throw new RuntimeException("Unexpected exception during message type and/or content verification", e);
        }
    }

    private static void verifyMessageHeaders(final MessageDescription messageDescription,
                                             final Message message) throws VerificationException
    {
        try
        {
            for (Map.Entry<MessageDescription.MessageHeader, Serializable> entry : messageDescription.getHeaders()
                                                                                                     .entrySet())
            {
                Object actualValue;

                switch (entry.getKey())
                {
                    case DESTINATION:
                        actualValue = message.getJMSDestination();
                        break;
                    case DELIVERY_MODE:
                        actualValue = message.getJMSDeliveryMode();
                        break;
                    case MESSAGE_ID:
                        actualValue = message.getJMSMessageID();
                        break;
                    case TIMESTAMP:
                        actualValue = message.getJMSTimestamp();
                        break;
                    case CORRELATION_ID:
                        if (entry.getValue() instanceof byte[])
                        {
                            actualValue = message.getJMSCorrelationIDAsBytes();
                        }
                        else
                        {
                            actualValue = message.getJMSCorrelationID();
                        }
                        break;
                    case REPLY_TO:
                        actualValue = message.getJMSReplyTo();
                        break;
                    case REDELIVERED:
                        actualValue = message.getJMSRedelivered();
                        break;
                    case TYPE:
                        actualValue = message.getJMSType();
                        break;
                    case EXPIRATION:
                        actualValue = message.getJMSExpiration();
                        break;
                    case PRIORITY:
                        actualValue = message.getJMSPriority();
                        break;
                    default:
                        throw new RuntimeException(String.format("unexpected message header '%s'", entry.getKey()));
                }

                verifyEquals(String.format("Unexpected message header '%s'", entry.getKey()),
                             entry.getValue(),
                             actualValue);
            }
        }
        catch (JMSException e)
        {
            throw new RuntimeException("Unexpected exception during message header verification", e);
        }
    }

    private static void verifyMessageProperties(final MessageDescription messageDescription,
                                                final Message message) throws VerificationException
    {
        try
        {
            final ArrayList<String> actualPropertyNames =
                    Collections.list(((Enumeration<String>) message.getPropertyNames()));
            final HashMap<String, Serializable> properties = messageDescription.getProperties();
            for (Map.Entry<String, Serializable> entry : properties.entrySet())
            {
                final String key = entry.getKey();
                verifyEquals(String.format("expected property '%s' not set", key),
                             true,
                             actualPropertyNames.contains(key));
                final Object actualValue = message.getObjectProperty(key);
                verifyEquals(String.format("Unexpected message property '%s'", key),
                             entry.getValue(),
                             actualValue);
            }
        }
        catch (JMSException e)
        {
            throw new RuntimeException("Unexpected exception during message property verification", e);
        }
    }

    private static <T, S> void verifyNotNull(final String failureMessage, final S actualValue)
    {
        if (actualValue == null)
        {
            throw new VerificationException(String.format("%s: expected non-null value", failureMessage));
        }
    }

    private static <T, S> void verifyEquals(final String failureMessage,
                                            final T expectedValue,
                                            final S actualValue)
    {
        if (expectedValue == null && actualValue == null)
        {
            return;
        }
        else if (expectedValue == null && actualValue != null)
        {
            throw new VerificationException(String.format("%s: expected <%s>, actual <%s>", failureMessage, null, actualValue));
        }
        else if (expectedValue != null && actualValue == null)
        {
            throw new VerificationException(String.format("%s: expected <%s>, actual <%s>", failureMessage, expectedValue, null));
        }
        else if (expectedValue.getClass() != actualValue.getClass())
        {
            throw new VerificationException(String.format("%s: expected type <%s>, actual type <%s>",
                                                          failureMessage,
                                                          expectedValue.getClass(),
                                                          actualValue.getClass()));
        }
        else
        {
            if (expectedValue instanceof Class && ((Class<?>) expectedValue).isInterface())
            {
                if (!((Class<?>) expectedValue).isAssignableFrom(((Class<?>) actualValue)))
                {
                    throw new VerificationException(String.format("%s: expected subclass of <%s>, actual <%s>",
                                                                  failureMessage,
                                                                  ((Class<?>) expectedValue).getName(),
                                                                  ((Class<?>) actualValue).getName()));
                }
            }
            else if (expectedValue instanceof byte[])
            {
                final byte[] expectedValueAsBytes = (byte[]) expectedValue;
                final byte[] actualValueAsBytes = (byte[]) actualValue;
                if (expectedValueAsBytes.length != actualValueAsBytes.length)
                {
                    throw new VerificationException(String.format(
                            "%s: expected array of length <%d>, actual length <%d> ('%s' vs '%s')",
                            failureMessage,
                            expectedValueAsBytes.length,
                            actualValueAsBytes.length,
                            encode(expectedValueAsBytes),
                            encode(actualValueAsBytes)));
                }
                if (!Arrays.equals(expectedValueAsBytes, actualValueAsBytes))
                {
                    throw new VerificationException(String.format("%s: arrays do not match ('%s' vs '%s')",
                                                                  failureMessage,
                                                                  encode(expectedValueAsBytes),
                                                                  encode(actualValueAsBytes)));
                }
            }
            else if (expectedValue instanceof Map)
            {
                if (!(actualValue instanceof Map))
                {
                    throw new VerificationException(String.format("%s: expected type <Map>, actual <%s>",
                                                                  failureMessage,
                                                                  actualValue.getClass()));
                }
                Map<String, Object> actualValueAsMap = (Map<String, Object>) actualValue;
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) expectedValue).entrySet())
                {
                    if (!actualValueAsMap.containsKey(entry.getKey()))
                    {
                        throw new VerificationException(String.format("%s: Map does not contain expected key <%s>",
                                                                      failureMessage,
                                                                      entry.getKey()));
                    }
                    else
                    {
                        verifyEquals(String.format("%s: MapVerification for key '%s' failed",
                                                   failureMessage,
                                                   entry.getKey()),
                                     entry.getValue(),
                                     actualValueAsMap.get(entry.getKey()));
                    }
                }
            }
            else if (expectedValue instanceof Collection)
            {
                if (!(actualValue instanceof Collection))
                {
                    throw new VerificationException(String.format("%s: expected type <Collection>, actual type <%s>",
                                                                  failureMessage,
                                                                  actualValue.getClass()));
                }
                Collection<Object> actualValueAsCollection = (Collection<Object>) actualValue;
                final Collection expectedValueAsCollection = (Collection) expectedValue;
                if (expectedValueAsCollection.size() != actualValueAsCollection.size())
                {
                    throw new VerificationException(String.format("%s: expected Collection of size <%s>, actual size <%s>",
                                                                  failureMessage,
                                                                  expectedValueAsCollection.size(),
                                                                  actualValueAsCollection.size()));
                }
                final Iterator<Object> actualValueIterator = actualValueAsCollection.iterator();
                int index = 0;
                for (Object entry : expectedValueAsCollection)
                {
                    verifyEquals(String.format("%s: CollectionVerification for index %d failed", failureMessage, index),
                                 entry,
                                 actualValueIterator.next());
                    index++;
                }
            }
            else
            {
                if (!expectedValue.equals(actualValue))
                {
                    throw new VerificationException(String.format("%s: expected <%s>, actual <%s>",
                                                                  failureMessage,
                                                                  expectedValue,
                                                                  actualValue));
                }
            }
        }
    }

    private static String encode(final byte[] expectedValueAsBytes)
    {
        String expectedValueAsString = toHex(expectedValueAsBytes);
        if (expectedValueAsString.length() > 20)
        {
            expectedValueAsString = expectedValueAsString.substring(0, 20) + "...";
        }
        return expectedValueAsString;
    }

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private static String toHex(byte[] bin)
    {
        StringBuilder result = new StringBuilder(2 * bin.length);
        for (byte b : bin) {
            result.append(HEX[(b >> 4) & 0xF]);
            result.append(HEX[(b & 0xF)]);
        }
        return result.toString();
    }
}
