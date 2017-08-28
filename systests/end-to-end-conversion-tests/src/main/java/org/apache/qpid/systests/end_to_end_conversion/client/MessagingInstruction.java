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

public abstract class MessagingInstruction implements ClientInstruction
{
    private final MessageDescription _messageDescription;
    private final String _destinationJndiName;

    public MessagingInstruction(final String destinationJndiName, final MessageDescription messageDescription)
    {
        _messageDescription = messageDescription;
        _destinationJndiName = destinationJndiName;
    }

    public MessageDescription getMessageDescription()
    {
        return _messageDescription;
    }

    public String getDestinationJndiName()
    {
        return _destinationJndiName;
    }

    @Override
    public String toString()
    {
        return "MessagingInstruction{" +
               "_messageDescription=" + _messageDescription +
               ", _destinationJndiName='" + _destinationJndiName + '\'' +
               '}';
    }

    public static class PublishMessage extends MessagingInstruction
    {
        private String _consumeReplyToJndiName;

        public PublishMessage(final String destinationJndiName, final MessageDescription messageDescription)
        {
            super(destinationJndiName, messageDescription);
        }

        public String getConsumeReplyToJndiName()
        {
            return _consumeReplyToJndiName;
        }

        public void setConsumeReplyToJndiName(final String consumeReplyToJndiName)
        {
            _consumeReplyToJndiName = consumeReplyToJndiName;
        }
    }

    public static class ReceiveMessage extends MessagingInstruction
    {
        public ReceiveMessage(final String destinationJndiName, final MessageDescription messageDescription)
        {
            super(destinationJndiName, messageDescription);
        }
    }
}
