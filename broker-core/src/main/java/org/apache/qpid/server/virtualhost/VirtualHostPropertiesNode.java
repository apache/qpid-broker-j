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
package org.apache.qpid.server.virtualhost;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.model.NamedAddressSpace;

public class VirtualHostPropertiesNode extends AbstractSystemMessageSource
{

    public VirtualHostPropertiesNode(final NamedAddressSpace virtualHost)
    {
        this(virtualHost, "$virtualhostProperties");
    }
    public VirtualHostPropertiesNode(final NamedAddressSpace virtualHost, String name)
    {
        super(name, virtualHost);
    }

    @Override
    public <T extends ConsumerTarget<T>> Consumer<T> addConsumer(final T target,
                                final FilterManager filters,
                                final Class<? extends ServerMessage> messageClass,
                                final String consumerName,
                                final EnumSet<ConsumerOption> options, final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {
        final Consumer<T> consumer = super.addConsumer(target, filters, messageClass, consumerName, options, priority);
        consumer.send(createMessage());
        target.noMessagesAvailable();
        return consumer;
    }

    @Override
    public void close()
    {
    }

    protected InternalMessage createMessage()
    {

        Map<String, Object> headers = new HashMap<>();

        final List<String> globalAddressDomains = _addressSpace.getGlobalAddressDomains();
        if (globalAddressDomains != null && !globalAddressDomains.isEmpty())
        {
            String primaryDomain = globalAddressDomains.get(0);
            if(primaryDomain != null)
            {
                primaryDomain = primaryDomain.trim();
                if(!primaryDomain.endsWith("/"))
                {
                    primaryDomain += "/";
                }
                headers.put("virtualHost.temporaryQueuePrefix", primaryDomain);
            }
        }

        InternalMessageHeader header = new InternalMessageHeader(headers,
                                                                 null, 0l, null, null, UUID.randomUUID().toString(),
                                                                 null, null, (byte) 4, System.currentTimeMillis(),
                                                                 0L, null, null, System.currentTimeMillis());
        final InternalMessage message =
                InternalMessage.createBytesMessage(_addressSpace.getMessageStore(), header, new byte[0]);
        message.setInitialRoutingAddress(getName());
        return message;
    }


}
