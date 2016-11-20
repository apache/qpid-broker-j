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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.BaseQueue;
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

    private static final Logger _logger = LoggerFactory.getLogger(HeadersExchangeImpl.class);

    private final ConcurrentMap<String, CopyOnWriteArraySet<Binding<?>>> _bindingsByKey =
                            new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<HeadersBinding> _bindingHeaderMatchers =
                            new CopyOnWriteArrayList<HeadersBinding>();

    @ManagedObjectFactoryConstructor
    public HeadersExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    public ArrayList<BaseQueue> doRoute(ServerMessage payload,
                                        final String routingKey,
                                        final InstanceProperties instanceProperties)
    {
        _logger.debug("Exchange {}: routing message with headers {}", getName(), payload.getMessageHeader());

        LinkedHashSet<BaseQueue> queues = new LinkedHashSet<BaseQueue>();

        for (HeadersBinding hb : _bindingHeaderMatchers)
        {
            if (hb.matches(Filterable.Factory.newInstance(payload,instanceProperties)))
            {
                Binding<?> b = hb.getBinding();

                b.incrementMatches();

                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Exchange " + getName() + ": delivering message with headers " +
                                  payload.getMessageHeader() + " to " + b.getQueue().getName());
                }
                queues.add(b.getQueue());
            }
        }

        return new ArrayList<>(queues);
    }

    @Override
    protected void onBind(final Binding<?> binding)
    {
        String bindingKey = binding.getBindingKey();
        Queue<?> queue = binding.getQueue();

        assert queue != null;
        assert bindingKey != null;

        CopyOnWriteArraySet<Binding<?>> bindings = _bindingsByKey.get(bindingKey);

        if(bindings == null)
        {
            bindings = new CopyOnWriteArraySet<>();
            CopyOnWriteArraySet<Binding<?>> newBindings;
            if((newBindings = _bindingsByKey.putIfAbsent(bindingKey, bindings)) != null)
            {
                bindings = newBindings;
            }
        }

        if(_logger.isDebugEnabled())
        {
            _logger.debug("Exchange " + getName() + ": Binding " + queue.getName() +
                          " with binding key '" +bindingKey + "' and args: " + binding.getArguments());
        }

        _bindingHeaderMatchers.add(new HeadersBinding(binding));
        bindings.add(binding);

    }

    @Override
    protected void onBindingUpdated(final Binding<?> binding, final Map<String, Object> oldArguments)
    {
        HeadersBinding headersBinding = new HeadersBinding(binding);
        ListIterator<HeadersBinding> iter = _bindingHeaderMatchers.listIterator();
        while(iter.hasNext())
        {
            if(iter.next().equals(headersBinding))
            {
                iter.set(headersBinding);
            }
        }

    }

    protected void onUnbind(final Binding<?> binding)
    {
        assert binding != null;

        CopyOnWriteArraySet<Binding<?>> bindings = _bindingsByKey.get(binding.getBindingKey());
        if(bindings != null)
        {
            bindings.remove(binding);
        }

        boolean removedBinding = _bindingHeaderMatchers.remove(new HeadersBinding(binding));
        _logger.debug("Removing Binding: {}", removedBinding);

    }

}
