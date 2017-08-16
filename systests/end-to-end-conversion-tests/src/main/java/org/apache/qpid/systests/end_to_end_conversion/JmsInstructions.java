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

package org.apache.qpid.systests.end_to_end_conversion;

import java.io.Serializable;
import java.util.HashMap;

public abstract class JmsInstructions implements Serializable
{
    private final MessageDescription _messageDescription;

    public JmsInstructions(final MessageDescription messageDescription)
    {
        _messageDescription = messageDescription;
    }

    public MessageDescription getMessageDescription()
    {
        return _messageDescription;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "_messageDescription=" + _messageDescription +
               '}';
    }

    public static class PublishMessage extends JmsInstructions
    {
        PublishMessage(final MessageDescription messageDescription)
        {
            super(messageDescription);
        }
    }

    public static class ReceiveMessage extends JmsInstructions
    {
        public ReceiveMessage(final MessageDescription messageDescription)
        {
            super(messageDescription);
        }
    }

    public static class ReplyToMessage extends JmsInstructions
    {
        public ReplyToMessage(final MessageDescription messageDescription)
        {
            super(messageDescription);
        }
    }

    public static class MessageDescription implements Serializable
    {
        private final HashMap<MessageDescription.MessageHeader, Serializable> _header;
        private final HashMap<String, Serializable> _properties;
        private MessageDescription.MessageType _messageType;
        private Object _content;
        public MessageDescription()
        {
            _header = new HashMap<>();
            _properties = new HashMap<>();
            _messageType = MessageType.MESSAGE;
        }

        public MessageDescription.MessageType getMessageType()
        {
            return _messageType;
        }

        public void setMessageType(final MessageDescription.MessageType messageType)
        {
            _messageType = messageType;
        }

        public Object getContent()
        {
            return _content;
        }

        public void setContent(final Object content)
        {
            _content = content;
        }

        public HashMap<MessageDescription.MessageHeader, Serializable> getHeaders()
        {
            return _header;
        }

        public <T extends Serializable> T getHeader(final MessageDescription.MessageHeader header, final T defaultValue)
        {
            return (T) (_header != null ? _header.getOrDefault(header, defaultValue) : defaultValue);
        }

        public void setHeader(final MessageDescription.MessageHeader header, final Serializable value)
        {
            _header.put(header, value);
        }

        public HashMap<String, Serializable> getProperties()
        {
            return _properties;
        }

        public void setProperty(final String property, final Serializable value)
        {
            _properties.put(property, value);
        }

        @Override
        public String toString()
        {
            return "MessageDescription{" +
                   "_messageType=" + _messageType +
                   ", _content=" + _content +
                   ", _header=" + _header +
                   ", _properties=" + _properties +
                   '}';
        }

        public enum MessageType
        {
            MESSAGE,
            BYTES_MESSAGE,
            MAP_MESSAGE,
            OBJECT_MESSAGE,
            STREAM_MESSAGE,
            TEXT_MESSAGE;
        }

        public enum MessageHeader
        {
            DESTINATION,
            DELIVERY_MODE,
            MESSAGE_ID,
            TIMESTAMP,
            CORRELATION_ID,
            REPLY_TO,
            REDELIVERED,
            TYPE,
            EXPIRATION,
            PRIORITY
        }
    }
}
