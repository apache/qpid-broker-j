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

import static org.apache.qpid.server.model.Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.FilterSupport;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

class FanoutExchangeImpl extends AbstractExchange<FanoutExchangeImpl> implements FanoutExchange<FanoutExchangeImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FanoutExchangeImpl.class);

    private final class BindingSet
    {
        private final Map<MessageDestination, Map<BindingIdentifier, String>> _unfilteredDestinations;
        private final Map<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>>
                _filteredDestinations;

        BindingSet(final Map<MessageDestination, Map<BindingIdentifier, String>> unfilteredDestinations,
                   final Map<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>> filteredDestinations)
        {
            _unfilteredDestinations = unfilteredDestinations;
            _filteredDestinations = filteredDestinations;
        }

        BindingSet()
        {
            _unfilteredDestinations = Collections.emptyMap();
            _filteredDestinations = Collections.emptyMap();
        }

        BindingSet addBinding(final BindingIdentifier binding, final Map<String, Object> arguments)
                throws AMQInvalidArgumentException
        {
            MessageDestination destination = binding.getDestination();
            if (FilterSupport.argumentsContainFilter(arguments))
            {
                Map<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>>
                        filteredDestinations = new HashMap<>(_filteredDestinations);

                filteredDestinations.computeIfAbsent(destination, messageDestination -> new HashMap<>());

                Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple> bindingsForDestination =
                        new HashMap<>(filteredDestinations.get(destination));

                FilterManager filterManager = FilterSupport.createMessageFilter(arguments, destination);
                String replacementRoutingKey = arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY) != null
                        ? String.valueOf(arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY))
                        : null;

                bindingsForDestination.put(binding,
                                           new FilterManagerReplacementRoutingKeyTuple(filterManager,
                                                                                       replacementRoutingKey));
                filteredDestinations.put(destination, Collections.unmodifiableMap(bindingsForDestination));
                return new BindingSet(_unfilteredDestinations, Collections.unmodifiableMap(filteredDestinations));
            }
            else
            {
                Map<MessageDestination, Map<BindingIdentifier, String>> unfilteredDestinations =
                        new HashMap<>(_unfilteredDestinations);
                unfilteredDestinations.computeIfAbsent(destination, messageDestination -> new HashMap<>());

                String replacementRoutingKey = null;
                if (arguments != null && arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY) != null)
                {
                    replacementRoutingKey = String.valueOf(arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY));
                }

                Map<BindingIdentifier, String> replacementRoutingKeysForDestination =
                        new HashMap<>(unfilteredDestinations.get(destination));
                replacementRoutingKeysForDestination.put(binding, replacementRoutingKey);

                unfilteredDestinations.put(destination,
                                           Collections.unmodifiableMap(replacementRoutingKeysForDestination));
                return new BindingSet(Collections.unmodifiableMap(unfilteredDestinations), _filteredDestinations);
            }
        }

        BindingSet updateBinding(final BindingIdentifier binding, final Map<String, Object> newArguments)
                throws AMQInvalidArgumentException
        {
            return removeBinding(binding).addBinding(binding, newArguments);
        }

        BindingSet removeBinding(final BindingIdentifier binding)
        {
            MessageDestination destination = binding.getDestination();
            if(_filteredDestinations.containsKey(destination) && _filteredDestinations.get(destination).containsKey(binding))
            {
                final Map<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>> filteredDestinations = new HashMap<>(_filteredDestinations);
                final Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple> bindingsForDestination = new HashMap<>(filteredDestinations.get(destination));
                bindingsForDestination.remove(binding);
                if (bindingsForDestination.isEmpty())
                {
                    filteredDestinations.remove(destination);
                }
                else
                {
                    filteredDestinations.put(destination, Collections.unmodifiableMap(bindingsForDestination));
                }
                return new BindingSet(_unfilteredDestinations, Collections.unmodifiableMap(filteredDestinations));
            }
            else if(_unfilteredDestinations.containsKey(destination) && _unfilteredDestinations.get(destination).containsKey(binding))
            {
                Map<MessageDestination, Map<BindingIdentifier, String>> unfilteredDestinations = new HashMap<>(_unfilteredDestinations);
                final Map<BindingIdentifier, String> bindingsForDestination = new HashMap<>(unfilteredDestinations.get(destination));
                bindingsForDestination.remove(binding);
                if (bindingsForDestination.isEmpty())
                {
                    unfilteredDestinations.remove(destination);
                }
                else
                {
                    unfilteredDestinations.put(destination, Collections.unmodifiableMap(bindingsForDestination));
                }

                return new BindingSet(Collections.unmodifiableMap(unfilteredDestinations), _filteredDestinations);
            }
            else
            {
                return this;
            }
        }
    }

    private volatile BindingSet _bindingSet = new BindingSet();


    @ManagedObjectFactoryConstructor
    public FanoutExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    protected <M extends ServerMessage<? extends StorableMessageMetaData>> void doRoute(final M message,
                                                                                        final String routingAddress,
                                                                                        final InstanceProperties instanceProperties,
                                                                                        final RoutingResult<M> result)
    {
        BindingSet bindingSet = _bindingSet;

        if (!bindingSet._unfilteredDestinations.isEmpty())
        {
            for (MessageDestination destination : bindingSet._unfilteredDestinations.keySet())
            {
                Set<String> replacementRoutingKeys =
                        new HashSet<>(bindingSet._unfilteredDestinations.get(destination).values());

                replacementRoutingKeys.forEach(
                        replacementRoutingKey -> result.add(destination.route(message,
                                                                              replacementRoutingKey == null
                                                                                      ? routingAddress
                                                                                      : replacementRoutingKey,
                                                                              instanceProperties)));
            }
        }

        final Map<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>>
                filteredDestinations = bindingSet._filteredDestinations;
        if (!filteredDestinations.isEmpty())
        {
            for (Map.Entry<MessageDestination, Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple>> entry :
                    filteredDestinations.entrySet())
            {
                MessageDestination destination = entry.getKey();
                final Map<BindingIdentifier, FilterManagerReplacementRoutingKeyTuple> bindingMessageFilterMap =
                        entry.getValue();
                for (FilterManagerReplacementRoutingKeyTuple tuple : bindingMessageFilterMap.values())
                {

                    FilterManager filter = tuple.getFilterManager();
                    if (filter.allAllow(Filterable.Factory.newInstance(message, instanceProperties)))
                    {
                        String routingKey = tuple.getReplacementRoutingKey() == null
                                ? routingAddress
                                : tuple.getReplacementRoutingKey();
                        result.add(destination.route(message, routingKey, instanceProperties));
                    }
                }
            }
        }
    }

    @Override
    protected void onBindingUpdated(final BindingIdentifier binding,
                                    final Map<String, Object> newArguments) throws AMQInvalidArgumentException
    {
        _bindingSet = _bindingSet.updateBinding(binding, newArguments);
    }

    @Override
    protected void onBind(final BindingIdentifier binding, final Map<String, Object> arguments)
            throws AMQInvalidArgumentException
    {
        _bindingSet = _bindingSet.addBinding(binding, arguments);
    }

    @Override
    protected void onUnbind(final BindingIdentifier binding)
    {
        _bindingSet = _bindingSet.removeBinding(binding);
    }
}
