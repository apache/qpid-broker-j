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

public class JmsInstructionBuilder
{
    private List<JmsInstructions> _jmsInstructions = new ArrayList<>();
    private JmsInstructions.MessageDescription _latestMessageDescription;

    public static List<JmsInstructions> publishSingleMessage(final JmsInstructions.MessageDescription messageDescription)
    {
        return new JmsInstructionBuilder().publishMessage(messageDescription).build();
    }

    public static List<JmsInstructions> receiveSingleMessage(final JmsInstructions.MessageDescription messageDescription)
    {
        return new JmsInstructionBuilder().receiveMessage(messageDescription).build();
    }

    public JmsInstructionBuilder publishMessage()
    {
        return publishMessage(new JmsInstructions.MessageDescription());
    }

    public JmsInstructionBuilder publishMessage(final JmsInstructions.MessageDescription messageDescription)
    {
        _latestMessageDescription = messageDescription;
        _jmsInstructions.add(new JmsInstructions.PublishMessage(_latestMessageDescription));
        return this;
    }

    public JmsInstructionBuilder withMessageType(JmsInstructions.MessageDescription.MessageType messageType)
    {
        _latestMessageDescription.setMessageType(messageType);
        return this;
    }

    public JmsInstructionBuilder withMessageContent(Serializable content)
    {
        _latestMessageDescription.setContent(content);
        return this;
    }

    public JmsInstructionBuilder withHeader(final JmsInstructions.MessageDescription.MessageHeader header,
                                            final Serializable value)
    {
        _latestMessageDescription.setHeader(header, value);
        return this;
    }

    public JmsInstructionBuilder withProperty(final String property, final Serializable value)
    {
        _latestMessageDescription.setProperty(property, value);
        return this;
    }

    public JmsInstructionBuilder receiveMessage()
    {
        return receiveMessage(new JmsInstructions.MessageDescription());
    }

    public JmsInstructionBuilder receiveMessage(final JmsInstructions.MessageDescription messageDescription)
    {
        _latestMessageDescription = messageDescription;
        _jmsInstructions.add(new JmsInstructions.ReceiveMessage(_latestMessageDescription));
        return this;
    }

    public JmsInstructionBuilder replyToMessage()
    {
        return replyToMessage(new JmsInstructions.MessageDescription());
    }

    public JmsInstructionBuilder replyToMessage(final JmsInstructions.MessageDescription latestMessageDescription)
    {
        _latestMessageDescription = latestMessageDescription;
        _jmsInstructions.add(new JmsInstructions.ReplyToMessage(_latestMessageDescription));
        return this;
    }

    public List<JmsInstructions> build()
    {
        return _jmsInstructions;
    }
}
