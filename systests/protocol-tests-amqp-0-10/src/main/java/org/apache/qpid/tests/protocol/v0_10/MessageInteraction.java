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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAccept;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquire;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCancel;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlow;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageSubscribe;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.RangeSet;

public class MessageInteraction
{
    private final Interaction _interaction;
    private MessageTransfer _transfer;
    private MessageSubscribe _subscribe;
    private MessageCancel _cancel;
    private MessageFlow _flow;
    private MessageAccept _accept;
    private MessageAcquire _acquire;

    public MessageInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _transfer = new MessageTransfer();
        _subscribe = new MessageSubscribe();
        _cancel = new MessageCancel();
        _flow = new MessageFlow();
        _accept = new MessageAccept();
        _acquire = new MessageAcquire();
    }

    public MessageInteraction transferId(final int id)
    {
        _transfer.setId(id);
        return this;
    }

    public MessageInteraction transferDestination(final String destination)
    {
        _transfer.setDestination(destination);
        return this;
    }

    public MessageInteraction transferHeader(final DeliveryProperties deliveryProperties,
                                             final MessageProperties messageProperties)
    {
        if (deliveryProperties == null && messageProperties == null)
        {
            _transfer.setHeader(null);
        }
        else
        {
            _transfer.setHeader(new Header(deliveryProperties, messageProperties));
        }
        return this;
    }

    public MessageInteraction transferBody(final byte[] messageContent)
    {
        if (messageContent != null)
        {
            try (QpidByteBuffer buf = QpidByteBuffer.wrap(messageContent))
            {
                _transfer.setBody(buf);
            }
        }
        else
        {
            _transfer.setBody(null);
        }
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

    public MessageInteraction cancelId(final int id)
    {
        _cancel.setId(id);
        return this;
    }

    public MessageInteraction cancelDestination(final String destination)
    {
        _cancel.setDestination(destination);
        return this;
    }

    public Interaction cancel() throws Exception
    {
        _interaction.sendPerformative(_cancel);
        return _interaction;
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

    public Interaction acquire() throws Exception
    {
        return _interaction.sendPerformative(_acquire);
    }

    public MessageInteraction acquireId(final int id)
    {
        _acquire.setId(id);
        return this;
    }

    public MessageInteraction acquireTransfers(final RangeSet transfers)
    {
        _acquire.setTransfers(transfers);
        return this;
    }
}
