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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

public class DirectExchangeImpl extends AbstractExchange<DirectExchangeImpl> implements DirectExchange<DirectExchangeImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectExchangeImpl.class);

    private final class BindingSet
    {
        private final Map<MessageDestination, String> _unfilteredDestinations;
        private final Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> _filteredDestinations;

        BindingSet()
        {
            _unfilteredDestinations = Collections.emptyMap();
            _filteredDestinations = Collections.emptyMap();
        }

        private BindingSet(final Map<MessageDestination, String> unfilteredDestinations,
                           final Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> filteredDestinations)
        {
            _unfilteredDestinations = unfilteredDestinations;
            _filteredDestinations = filteredDestinations;
        }

        Map<MessageDestination, String> getUnfilteredDestinations()
        {
            return _unfilteredDestinations;
        }

        boolean hasFilteredQueues()
        {
            return !_filteredDestinations.isEmpty();
        }

        boolean isEmpty()
        {
            return _unfilteredDestinations.isEmpty() && _filteredDestinations.isEmpty();
        }

        Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> getFilteredDestinations()
        {
            return _filteredDestinations;
        }

        BindingSet putBinding(MessageDestination destination, Map<String, Object> arguments, boolean force)
                throws AMQInvalidArgumentException
        {
            if (!force && (_unfilteredDestinations.containsKey(destination) || _filteredDestinations.containsKey(
                    destination)))
            {
                return this;
            }
            else if(FilterSupport.argumentsContainFilter(arguments))
            {
                FilterManager messageFilter = FilterSupport.createMessageFilter(arguments, destination);
                Map<MessageDestination, String> unfilteredDestinations;
                Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> filteredDestinations;
                if (_unfilteredDestinations.containsKey(destination))
                {
                    unfilteredDestinations = new HashMap<>(_unfilteredDestinations);
                    unfilteredDestinations.remove(destination);
                }
                else
                {
                    unfilteredDestinations = _unfilteredDestinations;
                }

                filteredDestinations = new HashMap<>(_filteredDestinations);

                String replacementRoutingKey = arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY) != null
                        ? String.valueOf(arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY))
                        : null;
                filteredDestinations.put(destination,
                                   new FilterManagerReplacementRoutingKeyTuple(messageFilter,
                                                                               replacementRoutingKey));

                return new BindingSet(Collections.unmodifiableMap(unfilteredDestinations),
                                      Collections.unmodifiableMap(filteredDestinations));
            }
            else
            {
                Map<MessageDestination, String> unfilteredDestinations;
                Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> filteredDestinations;
                if (_filteredDestinations.containsKey(destination))
                {
                    filteredDestinations = new HashMap<>(_filteredDestinations);
                    filteredDestinations.remove(destination);
                }
                else
                {
                    filteredDestinations = _filteredDestinations;
                }

                unfilteredDestinations = new HashMap<>(_unfilteredDestinations);
                Object replacementRoutingKey = arguments == null ? null : arguments.get(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY);
                unfilteredDestinations.put(destination, replacementRoutingKey == null ? null : String.valueOf(replacementRoutingKey));
                return new BindingSet(Collections.unmodifiableMap(unfilteredDestinations), Collections.unmodifiableMap(filteredDestinations));
            }
        }

        BindingSet removeBinding(final MessageDestination destination)
        {
            Map<MessageDestination, String> unfilteredDestinations;
            Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> filteredDestinations;
            if (_unfilteredDestinations.containsKey(destination))
            {
                unfilteredDestinations = new HashMap<>(_unfilteredDestinations);
                unfilteredDestinations.remove(destination);

                return new BindingSet(Collections.unmodifiableMap(unfilteredDestinations), _filteredDestinations);
            }
            else if(_filteredDestinations.containsKey(destination))
            {
                filteredDestinations = new HashMap<>(_filteredDestinations);
                filteredDestinations.remove(destination);
                return new BindingSet(_unfilteredDestinations, Collections.unmodifiableMap(filteredDestinations));
            }
            else
            {
                return this;
            }

        }
    }

    private final ConcurrentMap<String, BindingSet> _bindingsByKey = new ConcurrentHashMap<>();

    @ManagedObjectFactoryConstructor
    DirectExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }


    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> void doRoute(final M payload,
                                                                                     final String routingKey,
                                                                                     final InstanceProperties instanceProperties,
                                                                                     final RoutingResult<M> result)
    {
        BindingSet bindings = _bindingsByKey.get(routingKey == null ? "" : routingKey);
        if (bindings != null)
        {
            final Map<MessageDestination, String> unfilteredDestinations = bindings.getUnfilteredDestinations();
            for (MessageDestination destination : unfilteredDestinations.keySet())
            {
                String actualRoutingKey = unfilteredDestinations.get(destination) == null
                        ? routingKey
                        : unfilteredDestinations.get(destination);
                result.add(destination.route(payload, actualRoutingKey, instanceProperties));
            }

            if (bindings.hasFilteredQueues())
            {
                Filterable filterable = Filterable.Factory.newInstance(payload, instanceProperties);

                Map<MessageDestination, FilterManagerReplacementRoutingKeyTuple> filteredDestinations =
                        bindings.getFilteredDestinations();
                for (Map.Entry<MessageDestination, FilterManagerReplacementRoutingKeyTuple> entry : filteredDestinations
                        .entrySet())
                {
                    FilterManagerReplacementRoutingKeyTuple tuple = entry.getValue();
                    String actualRoutingKey = tuple.getReplacementRoutingKey() == null
                            ? routingKey
                            : tuple.getReplacementRoutingKey();

                    if (tuple.getFilterManager().allAllow(filterable))
                    {
                        result.add(entry.getKey().route(payload, actualRoutingKey, instanceProperties));
                    }
                }
            }
        }
    }

    @Override
    protected void onBindingUpdated(final BindingIdentifier binding, final Map<String, Object> newArguments)
            throws AMQInvalidArgumentException
    {
        String bindingKey = binding.getBindingKey();

        BindingSet bindings = _bindingsByKey.get(bindingKey);
        _bindingsByKey.put(bindingKey, bindings.putBinding(binding.getDestination(), newArguments, true));
    }

    @Override
    protected void onBind(final BindingIdentifier binding, final Map<String, Object> arguments)
            throws AMQInvalidArgumentException
    {
        String bindingKey = binding.getBindingKey();

        BindingSet bindings = _bindingsByKey.get(bindingKey);
        if(bindings == null)
        {
            bindings = new BindingSet();
        }
        _bindingsByKey.put(bindingKey, bindings.putBinding(binding.getDestination(), arguments, true));

    }

    @Override
    protected void onUnbind(final BindingIdentifier binding)
    {
        String bindingKey = binding.getBindingKey();

        BindingSet bindings = _bindingsByKey.get(bindingKey);
        final BindingSet replacementSet = bindings.removeBinding(binding.getDestination());
        if(replacementSet.isEmpty())
        {
            _bindingsByKey.remove(bindingKey);
        }
        else
        {
            _bindingsByKey.put(bindingKey, replacementSet);
        }
    }

}
