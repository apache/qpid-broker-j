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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.qpid.systests.end_to_end_conversion.client.AugumentConnectionUrl;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstruction;
import org.apache.qpid.systests.end_to_end_conversion.client.ConfigureDestination;
import org.apache.qpid.systests.end_to_end_conversion.client.MessageDescription;
import org.apache.qpid.systests.end_to_end_conversion.client.MessagingInstruction;

public class ClientInstructionBuilder
{
    private List<ClientInstruction> _clientInstructions = new ArrayList<>();
    private MessageDescription _latestMessageDescription;

    public ClientInstructionBuilder configureConnectionUrl(final Map<String, String> connectionUrlConfig)
    {
        _clientInstructions.add(new AugumentConnectionUrl(connectionUrlConfig));
        return this;
    }

    public ClientInstructionBuilder publishMessage(final String destinationJndiName)
    {
        return publishMessage(destinationJndiName, new MessageDescription());
    }

    public ClientInstructionBuilder publishMessage(final String destinationJndiName,
                                                   final MessageDescription messageDescription)
    {
        _latestMessageDescription = new MessageDescription(messageDescription);
        _clientInstructions.add(new MessagingInstruction.PublishMessage(destinationJndiName,
                                                                        _latestMessageDescription));
        return this;
    }

    public ClientInstructionBuilder receiveMessage(final String destinationJndiName)
    {
        return receiveMessage(destinationJndiName, new MessageDescription());
    }


    public ClientInstructionBuilder receiveMessage(final String destinationJndiName,
                                                   final MessageDescription messageDescription)
    {
        _latestMessageDescription = new MessageDescription(messageDescription);
        _clientInstructions.add(new MessagingInstruction.ReceiveMessage(destinationJndiName,
                                                                        _latestMessageDescription));
        return this;
    }

    public ClientInstructionBuilder withMessageType(MessageDescription.MessageType messageType)
    {
        _latestMessageDescription.setMessageType(messageType);
        return this;
    }

    public ClientInstructionBuilder withMessageContent(Serializable content)
    {
        _latestMessageDescription.setContent(content);
        return this;
    }

    public ClientInstructionBuilder withHeader(final MessageDescription.MessageHeader header,
                                               final Serializable value)
    {
        _latestMessageDescription.setHeader(header, value);
        return this;
    }

    public ClientInstructionBuilder withProperty(final String property, final Serializable value)
    {
        _latestMessageDescription.setProperty(property, value);
        return this;
    }

    public ClientInstructionBuilder withReplyToJndiName(final String replyToJndiName)
    {
        _latestMessageDescription.setReplyToJndiName(replyToJndiName);
        return this;
    }

    public ClientInstructionBuilder withConsumeReplyToJndiName(final String consumeReplyToJndiName)
    {
        MessagingInstruction.PublishMessage publishMessageInstruction =
                (MessagingInstruction.PublishMessage) _clientInstructions.get(_clientInstructions.size() - 1);
        publishMessageInstruction.setConsumeReplyToJndiName(consumeReplyToJndiName);
        return this;
    }

    public List<ClientInstruction> build()
    {
        return _clientInstructions;
    }

    public ClientInstructionBuilder configureDestinations(final Map<String, String> destinations)
    {
        _clientInstructions.add(new ConfigureDestination(destinations));
        return this;
    }
}
