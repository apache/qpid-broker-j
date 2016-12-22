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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class DirectExchangeImpl extends AbstractExchange<DirectExchangeImpl> implements DirectExchange<DirectExchangeImpl>
{

    private static final Logger _logger = LoggerFactory.getLogger(DirectExchangeImpl.class);

    private final class BindingSet
    {
        private final Set<BaseQueue> _unfilteredQueues;
        private final Map<BaseQueue, FilterManager> _filteredQueues;

        public BindingSet()
        {
            _unfilteredQueues = Collections.emptySet();
            _filteredQueues = Collections.emptyMap();
        }

        private BindingSet(final Set<BaseQueue> unfilteredQueues,
                           final Map<BaseQueue, FilterManager> filteredQueues)
        {
            _unfilteredQueues = unfilteredQueues;
            _filteredQueues = filteredQueues;
        }

        public Set<BaseQueue> getUnfilteredQueues()
        {
            return _unfilteredQueues;
        }

        public boolean hasFilteredQueues()
        {
            return !_filteredQueues.isEmpty();
        }

        boolean isEmpty()
        {
            return _unfilteredQueues.isEmpty() && _filteredQueues.isEmpty();
        }

        public Map<BaseQueue,FilterManager> getFilteredQueues()
        {
            return _filteredQueues;
        }

        BindingSet putBinding(MessageDestination destination, Map<String, Object> arguments, boolean force)
        {
            if(!force && (_unfilteredQueues.contains(destination) || _filteredQueues.containsKey(destination)))
            {
                return this;
            }
            else if(FilterSupport.argumentsContainFilter(arguments))
            {
                try
                {
                    Set<BaseQueue> unfilteredQueues;
                    Map<BaseQueue, FilterManager> filteredQueues;
                    if (_unfilteredQueues.contains(destination))
                    {
                        unfilteredQueues = new HashSet<>(_unfilteredQueues);
                        unfilteredQueues.remove(destination);
                    }
                    else
                    {
                        unfilteredQueues = _unfilteredQueues;
                    }

                    filteredQueues = new HashMap<>(_filteredQueues);
                    filteredQueues.put((BaseQueue) destination,
                                       FilterSupport.createMessageFilter(arguments, (Queue<?>) destination));

                    return new BindingSet(Collections.unmodifiableSet(unfilteredQueues), Collections.unmodifiableMap(filteredQueues));

                }
                catch (AMQInvalidArgumentException e)
                {
                    _logger.warn("Binding ignored: cannot parse filter on binding of queue '" + destination.getName()
                                 + "' to exchange '" + DirectExchangeImpl.this.getName()
                                 + "' with arguments: " + arguments, e);
                    return this;
                }

            }
            else
            {
                Set<BaseQueue> unfilteredQueues;
                Map<BaseQueue, FilterManager> filteredQueues;
                if (_filteredQueues.containsKey(destination))
                {
                    filteredQueues = new HashMap<>(_filteredQueues);
                    filteredQueues.remove(destination);
                }
                else
                {
                    filteredQueues = _filteredQueues;
                }

                unfilteredQueues = new HashSet<>(_unfilteredQueues);
                unfilteredQueues.add((BaseQueue)destination);

                return new BindingSet(Collections.unmodifiableSet(unfilteredQueues), Collections.unmodifiableMap(filteredQueues));

            }
        }

        public BindingSet removeBinding(final MessageDestination destination)
        {
            Set<BaseQueue> unfilteredQueues;
            Map<BaseQueue, FilterManager> filteredQueues;
            if (_unfilteredQueues.contains(destination))
            {
                unfilteredQueues = new HashSet<>(_unfilteredQueues);
                unfilteredQueues.remove(destination);

                return new BindingSet(Collections.unmodifiableSet(unfilteredQueues),_filteredQueues);
            }
            else if(_filteredQueues.containsKey(destination))
            {
                filteredQueues = new HashMap<>(_filteredQueues);
                filteredQueues.remove(destination);
                return new BindingSet(_unfilteredQueues, Collections.unmodifiableMap(filteredQueues));
            }
            else
            {
                return this;
            }

        }
    }

    private final ConcurrentMap<String, BindingSet> _bindingsByKey =
            new ConcurrentHashMap<String, BindingSet>();

    @ManagedObjectFactoryConstructor
    public DirectExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    public List<? extends BaseQueue> doRoute(ServerMessage payload,
                                             final String routingKey,
                                             final InstanceProperties instanceProperties)
    {

        BindingSet bindings = _bindingsByKey.get(routingKey == null ? "" : routingKey);

        if(bindings != null)
        {
            // TODO - remove this garbage generation
            List<BaseQueue> queues = new ArrayList<>(bindings.getUnfilteredQueues());

            if(bindings.hasFilteredQueues())
            {
                Set<BaseQueue> queuesSet = new HashSet<BaseQueue>(queues);
                Filterable filterable = Filterable.Factory.newInstance(payload, instanceProperties);

                Map<BaseQueue, FilterManager> filteredQueues = bindings.getFilteredQueues();
                for(Map.Entry<BaseQueue, FilterManager> entry : filteredQueues.entrySet())
                {
                    if(!queuesSet.contains(entry.getKey()))
                    {
                        FilterManager filter = entry.getValue();
                        if(filter.allAllow(filterable))
                        {
                            queuesSet.add(entry.getKey());
                        }
                    }
                }
                if(queues.size() != queuesSet.size())
                {
                    queues = new ArrayList<>(queuesSet);
                }
            }
            return queues;
        }
        else
        {
            return Collections.emptyList();
        }


    }

    @Override
    protected void onBindingUpdated(final BindingIdentifier binding, final Map<String, Object> newArguments)
    {
        String bindingKey = binding.getBindingKey();

        BindingSet bindings = _bindingsByKey.get(bindingKey);
        _bindingsByKey.put(bindingKey, bindings.putBinding(binding.getDestination(), newArguments, true));
    }

    @Override
    protected void onBind(final BindingIdentifier binding, final Map<String, Object> arguments)
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
