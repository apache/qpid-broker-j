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
import java.util.List;
import java.util.Map;

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

class FanoutExchangeImpl extends AbstractExchange<FanoutExchangeImpl> implements FanoutExchange<FanoutExchangeImpl>
{
    private static final Logger _logger = LoggerFactory.getLogger(FanoutExchangeImpl.class);

    private static final Integer ONE = Integer.valueOf(1);

    private final class BindingSet
    {
        private final Map<MessageDestination,Integer> _queues;

        private final List<Queue<?>> _unfilteredQueues;
        private final List<Queue<?>> _filteredQueues;

        private final Map<Queue<?>,Map<BindingIdentifier, FilterManager>> _filteredBindings;

        public BindingSet(final Map<MessageDestination, Integer> queues,
                          final List<Queue<?>> unfilteredQueues,
                          final List<Queue<?>> filteredQueues,
                          final Map<Queue<?>, Map<BindingIdentifier, FilterManager>> filteredBindings)
        {
            _queues = queues;
            _unfilteredQueues = unfilteredQueues;
            _filteredQueues = filteredQueues;
            _filteredBindings = filteredBindings;
        }

        public BindingSet()
        {
            _queues = Collections.emptyMap();
            _unfilteredQueues = Collections.emptyList();
            _filteredQueues = Collections.emptyList();
            _filteredBindings = Collections.emptyMap();
        }

        public BindingSet addBinding(final BindingIdentifier binding, final Map<String, Object> arguments)
        {
                if(FilterSupport.argumentsContainFilter(arguments))
                {
                    try
                    {
                        List<Queue<?>> filteredQueues;
                        if (!(_filteredQueues.contains(binding.getDestination())
                              || _unfilteredQueues.contains(binding.getDestination())))
                        {
                            filteredQueues = new ArrayList<>(_filteredQueues);
                            filteredQueues.add((Queue<?>) binding.getDestination());
                            filteredQueues = Collections.unmodifiableList(filteredQueues);
                        }
                        else
                        {
                            filteredQueues = _filteredQueues;
                        }
                        Map<Queue<?>, Map<BindingIdentifier, FilterManager>> filteredBindings =
                                new HashMap<>(_filteredBindings);
                        Map<BindingIdentifier, FilterManager> bindingsForQueue =
                                filteredBindings.get(binding.getDestination());
                        if (bindingsForQueue == null)
                        {
                            bindingsForQueue = new HashMap<>();
                        }
                        else
                        {
                            bindingsForQueue = new HashMap<>(bindingsForQueue);
                        }
                        bindingsForQueue.put(binding,
                                             FilterSupport.createMessageFilter(arguments,
                                                                               (Queue<?>) binding.getDestination()));
                        filteredBindings.put((Queue<?>) binding.getDestination(), bindingsForQueue);
                        return new BindingSet(_queues, _unfilteredQueues, filteredQueues, Collections.unmodifiableMap(filteredBindings));
                    }
                    catch (AMQInvalidArgumentException e)
                    {
                        _logger.warn("Binding ignored: cannot parse filter on binding of queue '" + binding.getDestination().getName()
                                     + "' to exchange '" + FanoutExchangeImpl.this.getName()
                                     + "' with arguments: " + arguments, e);
                        return this;
                    }
                }
                else
                {
                    Map<MessageDestination, Integer> queues = new HashMap<>(_queues);
                    List<Queue<?>> unfilteredQueues;
                    List<Queue<?>> filteredQueues;
                    if (queues.containsKey(binding.getDestination()))
                    {
                        queues.put(binding.getDestination(), queues.get(binding.getDestination()) + 1);
                        unfilteredQueues = _unfilteredQueues;
                        filteredQueues = _filteredQueues;
                    }
                    else
                    {
                        queues.put(binding.getDestination(), ONE);
                        unfilteredQueues = new ArrayList<>(_unfilteredQueues);
                        unfilteredQueues.add((Queue<?>)binding.getDestination());
                        unfilteredQueues = Collections.unmodifiableList(unfilteredQueues);
                        if(_filteredQueues.contains(binding.getDestination()))
                        {
                            filteredQueues = new ArrayList<>(_filteredQueues);
                            filteredQueues.remove(binding.getDestination());
                            filteredQueues = Collections.unmodifiableList(filteredQueues);
                        }
                        else
                        {
                            filteredQueues = _filteredQueues;
                        }
                    }
                    return new BindingSet(queues, unfilteredQueues, filteredQueues, _filteredBindings);
                }
        }

        public BindingSet updateBinding(final BindingIdentifier binding, final Map<String, Object> newArguments)
        {

            return removeBinding(binding).addBinding(binding, newArguments);
        }

        public BindingSet removeBinding(final BindingIdentifier binding)
        {
            Queue<?> queue = (Queue<?>) binding.getDestination();
            if(_filteredBindings.get(queue).containsKey(binding))
            {
                final Map<Queue<?>, Map<BindingIdentifier, FilterManager>> filteredBindings = new HashMap<>(_filteredBindings);
                final Map<BindingIdentifier, FilterManager> bindingsForQueue = new HashMap<>(filteredBindings.remove(queue));
                bindingsForQueue.remove(binding);
                List<Queue<?>> filteredQueues;
                if(bindingsForQueue.isEmpty())
                {
                    filteredQueues = new ArrayList<>(_filteredQueues);
                    filteredQueues.remove(queue);
                    filteredQueues = Collections.unmodifiableList(filteredQueues);
                }
                else
                {
                    filteredBindings.put(queue, bindingsForQueue);
                    filteredQueues = _filteredQueues;
                }
                return new BindingSet(_queues, _unfilteredQueues, filteredQueues, Collections.unmodifiableMap(filteredBindings));
            }
            else if(_unfilteredQueues.contains(queue))
            {
                Map<MessageDestination, Integer> queues = new HashMap<>(_queues);
                int count = queues.remove(queue);
                List<Queue<?>> unfilteredQueues;
                List<Queue<?>> filteredQueues;
                if(count > 1)
                {
                    queues.put(queue, --count);
                    unfilteredQueues = _unfilteredQueues;
                    filteredQueues = _filteredQueues;
                }
                else
                {
                    unfilteredQueues = new ArrayList<>(_unfilteredQueues);
                    unfilteredQueues.remove(queue);
                    unfilteredQueues = Collections.unmodifiableList(unfilteredQueues);
                    if(_filteredBindings.containsKey(queue))
                    {
                        filteredQueues = new ArrayList<>(_filteredQueues);
                        filteredQueues.add(queue);
                        filteredQueues = Collections.unmodifiableList(filteredQueues);
                    }
                    else
                    {
                        filteredQueues = _filteredQueues;
                    }
                }
                return new BindingSet(Collections.unmodifiableMap(queues), unfilteredQueues, filteredQueues, _filteredBindings);
            }
            else
            {
                return this;
            }
        }
    }


    private volatile BindingSet _bindingSet = new BindingSet();

    /**
     * Maps from queue name to queue instances
     */

    @ManagedObjectFactoryConstructor
    public FanoutExchangeImpl(final Map<String, Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    public ArrayList<BaseQueue> doRoute(ServerMessage payload,
                                        final String routingKey,
                                        final InstanceProperties instanceProperties)
    {

        BindingSet bindingSet = _bindingSet;
        final ArrayList<BaseQueue> result = new ArrayList<BaseQueue>(bindingSet._unfilteredQueues);


        final Map<Queue<?>, Map<BindingIdentifier, FilterManager>> filteredBindings = bindingSet._filteredBindings;
        if(!bindingSet._filteredQueues.isEmpty())
        {
            for(Queue<?> q : bindingSet._filteredQueues)
            {
                final Map<BindingIdentifier, FilterManager> bindingMessageFilterMap = filteredBindings.get(q);
                if(!(bindingMessageFilterMap == null || result.contains(q)))
                {
                    for(FilterManager filter : bindingMessageFilterMap.values())
                    {
                        if(filter.allAllow(Filterable.Factory.newInstance(payload, instanceProperties)))
                        {
                            result.add(q);
                            break;
                        }
                    }
                }
            }

        }


        _logger.debug("Publishing message to queue {}", result);

        return result;

    }

    @Override
    protected void onBindingUpdated(final BindingIdentifier binding,
                                    final Map<String, Object> newArguments)
    {
        _bindingSet = _bindingSet.updateBinding(binding, newArguments);
    }

    @Override
    protected void onBind(final BindingIdentifier binding, final Map<String, Object> arguments)
    {
        _bindingSet = _bindingSet.addBinding(binding, arguments);
    }

    @Override
    protected void onUnbind(final BindingIdentifier binding)
    {
        _bindingSet = _bindingSet.removeBinding(binding);
    }
}
