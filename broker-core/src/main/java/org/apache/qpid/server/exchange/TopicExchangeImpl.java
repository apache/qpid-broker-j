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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.exchange.topic.TopicExchangeResult;
import org.apache.qpid.server.exchange.topic.TopicMatcherResult;
import org.apache.qpid.server.exchange.topic.TopicNormalizer;
import org.apache.qpid.server.exchange.topic.TopicParser;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.FilterSupport;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

class TopicExchangeImpl extends AbstractExchange<TopicExchangeImpl> implements TopicExchange<TopicExchangeImpl>
{
    private static final Logger _logger = LoggerFactory.getLogger(TopicExchangeImpl.class);

    private final TopicParser _parser = new TopicParser();

    private final Map<String, TopicExchangeResult> _topicExchangeResults = new ConcurrentHashMap<>();

    private final Map<BindingIdentifier, Map<String,Object>> _bindings = new HashMap<>();

    @ManagedObjectFactoryConstructor
    public TopicExchangeImpl(final Map<String,Object> attributes, final QueueManagingVirtualHost<?> vhost)
    {
        super(attributes, vhost);
    }

    @Override
    protected synchronized void onBindingUpdated(final BindingIdentifier binding, final Map<String, Object> newArguments)
    {
        final String bindingKey = binding.getBindingKey();
        final MessageDestination destination = binding.getDestination();

        _logger.debug("Updating binding of queue {} with routing key {}", destination.getName(), bindingKey);

        String routingKey = TopicNormalizer.normalize(bindingKey);

        try
        {
            if (_bindings.containsKey(binding))
            {
                TopicExchangeResult result = _topicExchangeResults.get(routingKey);
                updateTopicExchangeResult(result, binding, newArguments);
            }
        }
        catch (AMQInvalidArgumentException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    private synchronized void bind(final BindingIdentifier binding, Map<String,Object> arguments) throws AMQInvalidArgumentException
    {
        final String bindingKey = binding.getBindingKey();
        MessageDestination messageDestination = binding.getDestination();

        _logger.debug("Registering messageDestination {} with routing key {}", messageDestination.getName(), bindingKey);

        String routingKey = TopicNormalizer.normalize(bindingKey);
        TopicExchangeResult result = _topicExchangeResults.get(routingKey);

        if(_bindings.containsKey(binding))
        {
            updateTopicExchangeResult(result, binding, arguments);
        }
        else
        {
            if(result == null)
            {
                result = new TopicExchangeResult();
                if(FilterSupport.argumentsContainFilter(arguments))
                {
                    result.addFilteredDestination(messageDestination, FilterSupport.createMessageFilter(arguments, messageDestination));
                }
                else
                {
                    result.addUnfilteredDestination(messageDestination);
                }
                _parser.addBinding(routingKey, result);
                _topicExchangeResults.put(routingKey,result);
            }
            else
            {
                if(FilterSupport.argumentsContainFilter(arguments))
                {
                    result.addFilteredDestination(messageDestination, FilterSupport.createMessageFilter(arguments, messageDestination));
                }
                else
                {
                    result.addUnfilteredDestination(messageDestination);
                }
            }

            _bindings.put(binding, arguments);
            result.addBinding(binding, arguments);
        }
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> void doRoute(M payload,
                                                                                     String routingAddress,
                                                                                     InstanceProperties instanceProperties,
                                                                                     RoutingResult<M> result)
    {
        final String routingKey = routingAddress == null
                ? ""
                : routingAddress;

        final Map<MessageDestination, Set<String>> matchedDestinations =
                getMatchedDestinations(Filterable.Factory.newInstance(payload, instanceProperties), routingKey);

        for(Map.Entry<MessageDestination, Set<String>> entry : matchedDestinations.entrySet())
        {
            MessageDestination destination = entry.getKey();
            Set<String> replacementKeys = entry.getValue();
            replacementKeys.forEach(replacementKey -> result.add(destination.route(payload, replacementKey == null ? routingAddress : replacementKey, instanceProperties)));

        }
    }


    private synchronized boolean unbind(final BindingIdentifier binding)
    {
        if(_bindings.containsKey(binding))
        {
            Map<String,Object> bindingArgs = _bindings.remove(binding);

            _logger.debug("deregisterQueue args: {}", bindingArgs);

            String bindingKey = TopicNormalizer.normalize(binding.getBindingKey());
            TopicExchangeResult result = _topicExchangeResults.get(bindingKey);

            result.removeBinding(binding);

            if(FilterSupport.argumentsContainFilter(bindingArgs))
            {
                try
                {
                    result.removeFilteredDestination(binding.getDestination(),
                                                     FilterSupport.createMessageFilter(bindingArgs,
                                                                                       binding.getDestination()));
                }
                catch (AMQInvalidArgumentException e)
                {
                    return false;
                }
            }
            else
            {
                result.removeUnfilteredDestination(binding.getDestination());
            }

            // shall we delete the result from _topicExchangeResults if result is empty?
            return true;
        }
        else
        {
            return false;
        }
    }

    private Map<MessageDestination, Set<String>> getMatchedDestinations(Filterable message, String routingKey)
    {
        Collection<TopicMatcherResult> results = _parser.parse(routingKey);
        if (!results.isEmpty())
        {
            Map<MessageDestination, Set<String>> matchedDestinations = new HashMap<>();
            for (TopicMatcherResult result : results)
            {
                TopicExchangeResult topicExchangeResult = (TopicExchangeResult) result;
                Map<MessageDestination, String> destinations = topicExchangeResult.processMessage(message);
                if (!destinations.isEmpty())
                {
                    destinations.forEach((destination, replacementKey) ->
                                 {
                                     Set<String> currentKeys = matchedDestinations.get(destination);
                                     if (currentKeys == null)
                                     {
                                         matchedDestinations.put(destination, Collections.singleton(replacementKey));
                                     }
                                     else if (!currentKeys.contains(replacementKey))
                                     {
                                         Set<String> newKeys = new HashSet<>(currentKeys);
                                         newKeys.add(replacementKey);
                                         matchedDestinations.put(destination, newKeys);
                                     }
                                 });
                }
            }
            return matchedDestinations;
        }
        return Collections.emptyMap();
    }

    @Override
    protected void onBind(final BindingIdentifier binding, Map<String, Object> arguments)
    {
        try
        {
            bind(binding, arguments);
        }
        catch (AMQInvalidArgumentException e)
        {
            // TODO - this seems incorrect, handling of invalid bindings should be propagated more cleanly
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    @Override
    protected void onUnbind(final BindingIdentifier binding)
    {
        unbind(binding);
    }

    private void updateTopicExchangeResult(final TopicExchangeResult result, final BindingIdentifier binding,
                                           final Map<String, Object> newArguments)
            throws AMQInvalidArgumentException
    {
        Map<String, Object> oldArgs = _bindings.put(binding, newArguments);
        MessageDestination destination = binding.getDestination();

        if (FilterSupport.argumentsContainFilter(newArguments))
        {
            if (FilterSupport.argumentsContainFilter(oldArgs))
            {
                result.replaceDestinationFilter(destination,
                                                FilterSupport.createMessageFilter(oldArgs, destination),
                                                FilterSupport.createMessageFilter(newArguments, destination));
            }
            else
            {
                result.addFilteredDestination(destination, FilterSupport.createMessageFilter(newArguments, destination));
                result.removeUnfilteredDestination(destination);
            }
        }
        else if (FilterSupport.argumentsContainFilter(oldArgs))
        {
            result.addUnfilteredDestination(destination);
            result.removeFilteredDestination(destination, FilterSupport.createMessageFilter(oldArgs, destination));
        }
        result.addBinding(binding, newArguments);
    }

}
