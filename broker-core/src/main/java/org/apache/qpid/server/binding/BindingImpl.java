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
package org.apache.qpid.server.binding;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.exchange.AbstractExchange;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.FilterSupport;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.BindingMessages;
import org.apache.qpid.server.logging.subjects.BindingLogSubject;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.security.access.Operation;

public class BindingImpl
        extends AbstractConfiguredObject<BindingImpl>
        implements org.apache.qpid.server.model.Binding<BindingImpl>
{
    private final String _bindingKey;
    private final Queue<?> _queue;
    private final Exchange<?> _exchange;
    @ManagedAttributeField
    private Map<String, Object> _arguments;
    private final AtomicLong _matches = new AtomicLong();
    private BindingLogSubject _logSubject;

    public BindingImpl(Map<String, Object> attributes, Queue<?> queue, Exchange<?> exchange)
    {
        super(parentsMap(queue,exchange),stripEmptyArguments(enhanceWithDurable(attributes, queue, exchange)));
        _bindingKey = getName();
        _queue = queue;
        _exchange = exchange;
    }

    private static Map<String, Object> stripEmptyArguments(final Map<String, Object> attributes)
    {
        Map<String,Object> returnVal;
        if(attributes != null
           && attributes.containsKey(Binding.ARGUMENTS)
           && (attributes.get(Binding.ARGUMENTS) instanceof Map)
           && ((Map)(attributes.get(Binding.ARGUMENTS))).isEmpty())
        {
            returnVal = new HashMap<>(attributes);
            returnVal.remove(Binding.ARGUMENTS);
        }
        else
        {
            returnVal = attributes;
        }

        return returnVal;
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _logSubject = new BindingLogSubject(_bindingKey,_exchange,_queue);

        getEventLogger().message(_logSubject, BindingMessages.CREATED(String.valueOf(getArguments()),
                                                                      getArguments() != null
                                                                      && !getArguments().isEmpty()));
        if(_exchange instanceof AbstractExchange)
        {
            ((AbstractExchange)_exchange).doAddBinding(this);
        }
    }

    private static Map<String, Object> enhanceWithDurable(Map<String, Object> attributes,
                                                          final Queue<?> queue,
                                                          final Exchange<?> exchange)
    {
        if(!attributes.containsKey(DURABLE))
        {
            attributes = new HashMap<>(attributes);
            attributes.put(DURABLE, queue.isDurable() && exchange.isDurable());
        }
        return attributes;
    }

    @Override
    public String getBindingKey()
    {
        return _bindingKey;
    }

    @Override
    public Queue<?> getQueue()
    {
        return _queue;
    }

    @Override
    public Exchange<?> getExchange()
    {
        return _exchange;
    }

    public Map<String, Object> getArguments()
    {
        return _arguments;
    }

    public void incrementMatches()
    {
        _matches.incrementAndGet();
    }

    public long getMatches()
    {
        return _matches.get();
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
    {
        return Collections.emptySet();
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(BindingMessages.OPERATION(operation));
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        
        if (!(o instanceof BindingImpl))
        {
            return false;
        }

        final BindingImpl binding = (BindingImpl) o;

        return (_bindingKey == null ? binding.getBindingKey() == null : _bindingKey.equals(binding.getBindingKey()))
            && (_exchange == null ? binding.getExchange() == null : _exchange.equals(binding.getExchange()))
            && (_queue == null ? binding.getQueue() == null : _queue.equals(binding.getQueue()));
    }

    @Override
    public int hashCode()
    {
        int result = _bindingKey == null ? 1 : _bindingKey.hashCode();
        result = 31 * result + (_queue == null ? 3 : _queue.hashCode());
        result = 31 * result + (_exchange == null ? 5 : _exchange.hashCode());
        return result;
    }

    @Override
    public String toString()
    {
        return "Binding{bindingKey="+_bindingKey+", exchange="+_exchange+", queue="+_queue+", id= " + getId() + " }";
    }

    @StateTransition(currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        ListenableFuture<Void> removeBinding = _exchange.removeBindingAsync(this);
        return doAfter(removeBinding, new Runnable()
        {
            @Override
            public void run()
            {
                getEventLogger().message(_logSubject, BindingMessages.DELETED());
                deleted();
                setState(State.DELETED);
            }
        });
    }

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.ACTIVE)
    private ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    private EventLogger getEventLogger()
    {
        return _exchange.getEventLogger();
    }

    public void setArguments(final Map<String, Object> arguments)
    {
        runTask(new Task<Void, RuntimeException>()
                {
                    @Override
                    public Void execute()
                    {
                        _arguments = arguments;
                        BindingImpl.super.setAttributes(Collections.<String, Object>singletonMap(ARGUMENTS, arguments));
                        return null;
                    }

                    @Override
                    public String getObject()
                    {
                        return BindingImpl.this.toString();
                    }

                    @Override
                    public String getAction()
                    {
                        return "set arguments";
                    }

                    @Override
                    public String getArguments()
                    {
                        return arguments.toString();
                    }
                }
               );

    }

    @Override
    public void validateOnCreate()
    {
        authorise(Operation.CREATE);

        Queue<?> queue = getQueue();
        Map<String, Object> arguments = getArguments();
        if (arguments!=null && !arguments.isEmpty() && FilterSupport.argumentsContainFilter(arguments))
        {
            try
            {
                FilterSupport.createMessageFilter(arguments, queue);
            }
            catch (AMQInvalidArgumentException e)
            {
                throw new IllegalConfigurationException(e.getMessage(), e);
            }
        }
    }


}
