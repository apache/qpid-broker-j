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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

class TopicExchangeImpl extends AbstractExchange<TopicExchangeImpl> implements TopicExchange<TopicExchangeImpl>
{
    private static final Logger _logger = LoggerFactory.getLogger(TopicExchangeImpl.class);

    private final TopicParser _parser = new TopicParser();

    private final Map<String, TopicExchangeResult> _topicExchangeResults =
            new ConcurrentHashMap<String, TopicExchangeResult>();

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
        Queue<?> queue = (Queue<?>) binding.getDestination();

        _logger.debug("Updating binding of queue {} with routing key {}", queue.getName(), bindingKey);


        String routingKey = TopicNormalizer.normalize(bindingKey);

        try
        {

            if (_bindings.containsKey(binding))
            {
                Map<String, Object> oldArgs = _bindings.put(binding, newArguments);
                TopicExchangeResult result = _topicExchangeResults.get(routingKey);

                if (FilterSupport.argumentsContainFilter(newArguments))
                {
                    if (FilterSupport.argumentsContainFilter(oldArgs))
                    {
                        result.replaceQueueFilter(queue,
                                                  FilterSupport.createMessageFilter(oldArgs, queue),
                                                  FilterSupport.createMessageFilter(newArguments, queue));
                    }
                    else
                    {
                        result.addFilteredQueue(queue, FilterSupport.createMessageFilter(newArguments, queue));
                        result.removeUnfilteredQueue(queue);
                    }
                }
                else
                {
                    if (FilterSupport.argumentsContainFilter(oldArgs))
                    {
                        result.addUnfilteredQueue(queue);
                        result.removeFilteredQueue(queue, FilterSupport.createMessageFilter(oldArgs, queue));
                    }
                    else
                    {
                        // TODO - fix control flow
                        return;
                    }
                }

            }
        }
        catch (AMQInvalidArgumentException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }


    }

    protected synchronized void registerQueue(final BindingIdentifier binding, Map<String,Object> arguments) throws AMQInvalidArgumentException
    {
        final String bindingKey = binding.getBindingKey();
        Queue<?> queue = (Queue<?>) binding.getDestination();

        _logger.debug("Registering queue {} with routing key {}", queue.getName(), bindingKey);


        String routingKey = TopicNormalizer.normalize(bindingKey);

        if(_bindings.containsKey(binding))
        {
            Map<String,Object> oldArgs = _bindings.put(binding, arguments);
            TopicExchangeResult result = _topicExchangeResults.get(routingKey);

            if(FilterSupport.argumentsContainFilter(arguments))
            {
                if(FilterSupport.argumentsContainFilter(oldArgs))
                {
                    result.replaceQueueFilter(queue,
                                              FilterSupport.createMessageFilter(oldArgs, queue),
                                              FilterSupport.createMessageFilter(arguments, queue));
                }
                else
                {
                    result.addFilteredQueue(queue, FilterSupport.createMessageFilter(arguments, queue));
                    result.removeUnfilteredQueue(queue);
                }
            }
            else
            {
                if(FilterSupport.argumentsContainFilter(oldArgs))
                {
                    result.addUnfilteredQueue(queue);
                    result.removeFilteredQueue(queue, FilterSupport.createMessageFilter(oldArgs, queue));
                }
                else
                {
                    // TODO - fix control flow
                    return;
                }
            }

            result.addBinding(binding);

        }
        else
        {

            TopicExchangeResult result = _topicExchangeResults.get(routingKey);
            if(result == null)
            {
                result = new TopicExchangeResult();
                if(FilterSupport.argumentsContainFilter(arguments))
                {
                    result.addFilteredQueue(queue, FilterSupport.createMessageFilter(arguments, queue));
                }
                else
                {
                    result.addUnfilteredQueue(queue);
                }
                _parser.addBinding(routingKey, result);
                _topicExchangeResults.put(routingKey,result);
            }
            else
            {
                if(FilterSupport.argumentsContainFilter(arguments))
                {
                    result.addFilteredQueue(queue, FilterSupport.createMessageFilter(arguments, queue));
                }
                else
                {
                    result.addUnfilteredQueue(queue);
                }
            }

            result.addBinding(binding);
            _bindings.put(binding, arguments);
        }

    }

    @Override
    public ArrayList<BaseQueue> doRoute(ServerMessage payload,
                                        final String routingAddress,
                                        final InstanceProperties instanceProperties)
    {

        final String routingKey = routingAddress == null
                                          ? ""
                                          : routingAddress;

        final Collection<Queue<?>> matchedQueues =
                getMatchedQueues(Filterable.Factory.newInstance(payload,instanceProperties), routingKey);

        ArrayList<BaseQueue> queues;

        if(matchedQueues.getClass() == ArrayList.class)
        {
            queues = (ArrayList) matchedQueues;
        }
        else
        {
            queues = new ArrayList<BaseQueue>();
            queues.addAll(matchedQueues);
        }

        if(queues == null || queues.isEmpty())
        {
            _logger.info("Message routing key: " + routingAddress + " No routes.");
        }

        return queues;

    }

    private synchronized boolean deregisterQueue(final BindingIdentifier binding)
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
                    result.removeFilteredQueue((Queue<?>) binding.getDestination(), FilterSupport.createMessageFilter(bindingArgs,
                                                                                                                      (Queue<?>) binding.getDestination()));
                }
                catch (AMQInvalidArgumentException e)
                {
                    return false;
                }
            }
            else
            {
                result.removeUnfilteredQueue((Queue<?>) binding.getDestination());
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    private Collection<Queue<?>> getMatchedQueues(Filterable message, String routingKey)
    {

        Collection<TopicMatcherResult> results = _parser.parse(routingKey);
        switch(results.size())
        {
            case 0:
                return Collections.EMPTY_SET;
            case 1:
                TopicMatcherResult[] resultQueues = new TopicMatcherResult[1];
                results.toArray(resultQueues);
                return ((TopicExchangeResult)resultQueues[0]).processMessage(message, null);
            default:
                Collection<Queue<?>> queues = new HashSet<>();
                for(TopicMatcherResult result : results)
                {
                    TopicExchangeResult res = (TopicExchangeResult)result;

                    queues = res.processMessage(message, queues);
                }
                return queues;
        }


    }

    @Override
    protected void onBind(final BindingIdentifier binding, Map<String, Object> arguments)
    {
        try
        {
            registerQueue(binding, arguments);
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
        deregisterQueue(binding);
    }

}
