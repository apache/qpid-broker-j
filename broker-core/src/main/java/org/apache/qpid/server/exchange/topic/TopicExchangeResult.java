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
package org.apache.qpid.server.exchange.topic;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.exchange.AbstractExchange;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Binding;

public final class TopicExchangeResult implements TopicMatcherResult
{
    private final Map<MessageDestination, Integer> _unfilteredDestinations = new ConcurrentHashMap<>();
    private final ConcurrentMap<MessageDestination, Map<FilterManager,Integer>> _filteredDestinations = new ConcurrentHashMap<>();
    private final Map<MessageDestination, String> _replacementKeys = new ConcurrentHashMap<>();

    public void addUnfilteredDestination(MessageDestination destination)
    {
        _unfilteredDestinations.merge(destination, 1, (oldCount, increment) -> oldCount + increment);
    }

    public void removeUnfilteredDestination(MessageDestination destination)
    {
        Integer instances = _unfilteredDestinations.get(destination);
        if(instances == 1)
        {
            _unfilteredDestinations.remove(destination);
        }
        else
        {
            _unfilteredDestinations.put(destination, instances - 1);
        }
    }

    public void addBinding(AbstractExchange.BindingIdentifier binding, Map<String, Object> bindingArguments)
    {
        Object keyObject = bindingArguments != null ? bindingArguments.get(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY) : null;
        if (keyObject == null)
        {
            _replacementKeys.remove(binding.getDestination());
        }
        else
        {
            _replacementKeys.put(binding.getDestination(), String.valueOf(keyObject));
        }
    }

    public void removeBinding(AbstractExchange.BindingIdentifier binding)
    {
        _replacementKeys.remove(binding.getDestination());
    }

    public void addFilteredDestination(MessageDestination destination, FilterManager filter)
    {
        Map<FilterManager, Integer> filters =
                _filteredDestinations.computeIfAbsent(destination, filterManagerMap -> new ConcurrentHashMap<>());
        filters.merge(filter, 1, (oldCount, increment) -> oldCount + increment);
    }

    public void removeFilteredDestination(MessageDestination destination, FilterManager filter)
    {
        Map<FilterManager,Integer> filters = _filteredDestinations.get(destination);
        if(filters != null)
        {
            Integer instances = filters.get(filter);
            if(instances != null)
            {
                if(instances == 1)
                {
                    filters.remove(filter);
                    if(filters.isEmpty())
                    {
                        _filteredDestinations.remove(destination);
                    }
                }
                else
                {
                    filters.put(filter, instances - 1);
                }
            }

        }

    }

    public void replaceDestinationFilter(MessageDestination queue,
                                         FilterManager oldFilter,
                                         FilterManager newFilter)
    {
        Map<FilterManager,Integer> filters = _filteredDestinations.get(queue);
        Map<FilterManager,Integer> newFilters = new ConcurrentHashMap<>(filters);
        Integer oldFilterInstances = filters.get(oldFilter);
        if(oldFilterInstances == 1)
        {
            newFilters.remove(oldFilter);
        }
        else
        {
            newFilters.put(oldFilter, oldFilterInstances-1);
        }
        Integer newFilterInstances = filters.get(newFilter);
        if(newFilterInstances == null)
        {
            newFilters.put(newFilter, 1);
        }
        else
        {
            newFilters.put(newFilter, newFilterInstances+1);
        }
        _filteredDestinations.put(queue, newFilters);
    }

    @Deprecated
    public Map<MessageDestination, String> processMessage(Filterable msg)
    {
        Map<MessageDestination, String> result = new HashMap<>();
        for (MessageDestination unfilteredDestination : _unfilteredDestinations.keySet())
        {
            result.put(unfilteredDestination, _replacementKeys.get(unfilteredDestination));
        }

        if(!_filteredDestinations.isEmpty())
        {
            for(Map.Entry<MessageDestination, Map<FilterManager, Integer>> entry : _filteredDestinations.entrySet())
            {
                MessageDestination destination = entry.getKey();
                if(!_unfilteredDestinations.containsKey(destination))
                {
                    for(FilterManager filter : entry.getValue().keySet())
                    {
                        if(filter.allAllow(msg))
                        {
                            result.put(destination, _replacementKeys.get(destination));
                        }
                    }
                }
            }
        }
        return result;
    }

    public void processMessage(final Filterable msg,
                               final Map<MessageDestination, Set<String>> result,
                               final String routingKey)
    {
        if (!_unfilteredDestinations.isEmpty())
        {
            for (MessageDestination unfilteredDestination : _unfilteredDestinations.keySet())
            {
                addMatch(unfilteredDestination, result, routingKey);
            }
        }

        if (!_filteredDestinations.isEmpty())
        {
            for (Map.Entry<MessageDestination, Map<FilterManager, Integer>> entry : _filteredDestinations.entrySet())
            {
                MessageDestination destination = entry.getKey();
                if (!_unfilteredDestinations.containsKey(destination))
                {
                    for (FilterManager filter : entry.getValue().keySet())
                    {
                        if (filter.allAllow(msg))
                        {
                            addMatch(destination, result, routingKey);
                            break;
                        }
                    }
                }
            }
        }
    }

    private void addMatch(MessageDestination destination,
                          Map<MessageDestination, Set<String>> result,
                          String routingKey)
    {
        String replacementKey = _replacementKeys.getOrDefault(destination, routingKey);
        Set<String> currentKeys = result.get(destination);
        if (currentKeys == null)
        {
            result.put(destination, Collections.singleton(replacementKey));
        }
        else if (!currentKeys.contains(replacementKey))
        {
            if (currentKeys.size() == 1)
            {
                currentKeys = new HashSet<>(currentKeys);
                result.put(destination, currentKeys);
            }
            currentKeys.add(replacementKey);
        }
    }

}
