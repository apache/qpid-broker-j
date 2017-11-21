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
package org.apache.qpid.tests.protocol.v0_10;

import org.apache.qpid.server.protocol.v0_10.transport.MessageAccept;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlow;
import org.apache.qpid.server.protocol.v0_10.transport.MessageSubscribe;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.RangeSet;

public class MessageInteraction
{
    private final Interaction _interaction;
    private MessageTransfer _transfer;
    private MessageSubscribe _subscribe;
    private MessageFlow _flow;
    private MessageAccept _accept;

    public MessageInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _transfer = new MessageTransfer();
        _subscribe = new MessageSubscribe();
        _flow = new MessageFlow();
        _accept = new MessageAccept();
    }

    public MessageInteraction transferId(final int id)
    {
        _transfer.setId(id);
        return this;
    }

    public MessageInteraction transferDesitnation(final String destination)
    {
        _transfer.setDestination(destination);
        return this;
    }

    public Interaction transfer() throws Exception
    {
        _interaction.sendPerformative(_transfer);
        return _interaction;
    }

    public MessageInteraction subscribeQueue(final String queueName)
    {
        _subscribe.setQueue(queueName);
        return this;
    }

    public MessageInteraction subscribeId(final int id)
    {
        _subscribe.setId(id);
        return this;
    }

    public Interaction subscribe() throws Exception
    {
        return _interaction.sendPerformative(_subscribe);
    }

    public MessageInteraction subscribeDestination(final String destination)
    {
        _subscribe.setDestination(destination);
        return this;
    }

    public Interaction flow() throws Exception
    {
        return _interaction.sendPerformative(_flow);
    }

    public MessageInteraction flowId(final int id)
    {
        _flow.setId(id);
        return this;
    }

    public MessageInteraction flowDestination(final String destination)
    {
        _flow.setDestination(destination);
        return this;
    }

    public MessageInteraction flowUnit(final MessageCreditUnit unit)
    {
        _flow.setUnit(unit);
        return this;
    }

    public MessageInteraction flowValue(final long value)
    {
        _flow.setValue(value);
        return this;
    }

    public MessageInteraction subscribeAcceptMode(final MessageAcceptMode acceptMode)
    {
        _subscribe.setAcceptMode(acceptMode);
        return this;
    }

    public MessageInteraction subscribeAcquireMode(final MessageAcquireMode acquireMode)
    {
        _subscribe.setAcquireMode(acquireMode);
        return this;
    }

    public Interaction accept() throws Exception
    {
        return _interaction.sendPerformative(_accept);
    }

    public MessageInteraction acceptId(final int id)
    {
        _accept.setId(id);
        return this;
    }

    public MessageInteraction acceptTransfers(final RangeSet transfers)
    {
        _accept.setTransfers(transfers);
        return this;
    }
}
