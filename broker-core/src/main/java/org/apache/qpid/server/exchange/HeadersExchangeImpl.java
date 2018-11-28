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
package org.apache.qpid.server.exchange;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

/**
 * An exchange that binds queues based on a set of required headers and header values
 * and routes messages to these queues by matching the headers of the message against
 * those with which the queues were bound.
 * <p>
 * <pre>
 * The Headers Exchange
 *
 *  Routes messages according to the value/presence of fields in the message header table.
 *  (Basic and JMS content has a content header field called "headers" that is a table of
 *   message header fields).
 *
 *  class = "headers"
 *  routing key is not used
 *
 *  Has the following binding arguments:
 *
 *  the X-match field - if "all", does an AND match (used for GRM), if "any", does an OR match.
 *  other fields prefixed with "X-" are ignored (and generate a console warning message).
 *  a field with no value or empty value indicates a match on presence only.
 *  a field with a value indicates match on field presence and specific value.
 *
 *  Standard instances:
 *
 *  amq.match - pub/sub on field content/value
 *  </pre>
 */
public class HeadersExchangeImpl extends AbstractExchange<HeadersExchangeImpl> implements HeadersExchange<HeadersExchangeImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(HeadersExchangeImpl.class);

    private final Set<HeadersBinding> _bindingHeaderMatchers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @ManagedObjectFactoryConstructor
    public HeadersExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> void doRoute(M payload,
                                                                                     String routingKey,
                                                                                     final InstanceProperties instanceProperties,
                                                                                     RoutingResult<M> routingResult)
    {
        LOGGER.debug("Exchange {}: routing message with headers {}", getName(), payload.getMessageHeader());

        for (HeadersBinding hb : _bindingHeaderMatchers)
        {
            if (hb.matches(Filterable.Factory.newInstance(payload,instanceProperties)))
            {
                MessageDestination destination = hb.getBinding().getDestination();

                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Exchange '{}' delivering message with headers '{}' to '{}'",
                                  getName(), payload.getMessageHeader(), destination.getName());
                }
                String actualRoutingKey = hb.getReplacementRoutingKey() == null
                        ? routingKey
                        : hb.getReplacementRoutingKey();
                routingResult.add(destination.route(payload, actualRoutingKey, instanceProperties));
            }
        }
    }


    @Override
    protected void onBind(final BindingIdentifier binding, Map<String,Object> arguments) throws AMQInvalidArgumentException
    {
        _bindingHeaderMatchers.add(new HeadersBinding(binding, arguments));
    }

    @Override
    protected void onBindingUpdated(final BindingIdentifier binding, final Map<String, Object> arguments)  throws AMQInvalidArgumentException
    {
        _bindingHeaderMatchers.add(new HeadersBinding(binding, arguments));
    }

    @Override
    protected void onUnbind(final BindingIdentifier binding)
    {
        try
        {
            _bindingHeaderMatchers.remove(new HeadersBinding(binding, Collections.emptyMap()));
        }
        catch (AMQInvalidArgumentException e)
        {
            // ignore
        }
    }

}
