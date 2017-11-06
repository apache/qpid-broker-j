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
package org.apache.qpid.server.protocol.v1_0;

import static org.apache.qpid.server.protocol.v1_0.Session_1_0.GLOBAL_CAPABILITY;
import static org.apache.qpid.server.protocol.v1_0.Session_1_0.SHARED_CAPABILITY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ExactSubjectFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MatchingSubjectFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class ExchangeSendingDestination extends StandardSendingDestination
{
    private static final Accepted ACCEPTED = new Accepted();
    private static final Rejected REJECTED = new Rejected();
    private static final Outcome[] OUTCOMES = { ACCEPTED, REJECTED};
    public static final Symbol TOPIC_CAPABILITY = Symbol.getSymbol("topic");

    private final Exchange<?> _exchange;
    private final Symbol[] _capabilities;
    private final Map<Symbol, Filter> _filters;

    ExchangeSendingDestination(Exchange<?> exchange,
                               String linkName,
                               String bindingKey,
                               String remoteContainerId,
                               Source source) throws AmqpErrorException
    {
        this(exchange, bindingKey, source, getMangledSubscriptionName(linkName, remoteContainerId, source));
    }

    private ExchangeSendingDestination(final Exchange<?> exchange,
                                       final String bindingKey,
                                       final Source source,
                                       final String subscriptionName) throws AmqpErrorException
    {
        this(exchange, source, subscriptionName, createBindingInfo(exchange, subscriptionName, bindingKey, source));
    }

    private ExchangeSendingDestination(final Exchange<?> exchange,
                                       final Source source,
                                       final String subscriptionName,
                                       final BindingInfo bindingInfo)
            throws AmqpErrorException
    {
        this(exchange, getQueue(exchange, source, subscriptionName, bindingInfo), bindingInfo, source.getCapabilities());
    }

    private ExchangeSendingDestination(final Exchange<?> exchange,
                                       final Queue<?> queue,
                                       final BindingInfo bindingInfo,
                                       final Symbol[] capabilities)
    {
        super(queue);
        _exchange = exchange;
        _filters = bindingInfo.getActualFilters().isEmpty() ? null : bindingInfo.getActualFilters();
        List<Symbol> sourceCapabilities = new ArrayList<>();

        if (hasCapability(capabilities, GLOBAL_CAPABILITY))
        {
            sourceCapabilities.add(GLOBAL_CAPABILITY);
        }
        if (hasCapability(capabilities, SHARED_CAPABILITY))
        {
            sourceCapabilities.add(SHARED_CAPABILITY);
        }

        sourceCapabilities.add(TOPIC_CAPABILITY);

        _capabilities = sourceCapabilities.toArray(new Symbol[sourceCapabilities.size()]);
    }

    private static BindingInfo createBindingInfo(final Exchange<?> exchange,
                                                 final String subscriptionName,
                                                 final String bindingKey, final Source source)
            throws AmqpErrorException
    {
        return new BindingInfo(exchange, subscriptionName,
                               bindingKey, source.getFilter());
    }

    private static String getMangledSubscriptionName(final String linkName,
                                                     final String remoteContainerId,
                                                     final Source source)
    {
        boolean isDurable = source.getExpiryPolicy() == TerminusExpiryPolicy.NEVER;
        boolean isShared = hasCapability(source.getCapabilities(), SHARED_CAPABILITY);
        boolean isGlobal = hasCapability(source.getCapabilities(), GLOBAL_CAPABILITY);

        return getMangledSubscriptionName(linkName, isDurable, isShared, isGlobal, remoteContainerId);

    }


    private static Queue<?> getQueue(Exchange<?> exchange, Source source, String subscriptionName, BindingInfo bindingInfo)
            throws AmqpErrorException
    {
        boolean isDurable = source.getExpiryPolicy() == TerminusExpiryPolicy.NEVER;
        boolean isShared = hasCapability(source.getCapabilities(), SHARED_CAPABILITY);

        QueueManagingVirtualHost virtualHost;
        if (exchange.getAddressSpace() instanceof QueueManagingVirtualHost)
        {
            virtualHost = (QueueManagingVirtualHost) exchange.getAddressSpace();
        }
        else
        {
            throw new AmqpErrorException(new Error(AmqpError.INTERNAL_ERROR,
                                                   "Address space of unexpected type"));
        }

        Queue<?> queue;
        final Map<String, Object> attributes = new HashMap<>();

        ExclusivityPolicy exclusivityPolicy;
        if (isShared)
        {
            exclusivityPolicy = ExclusivityPolicy.SHARED_SUBSCRIPTION;
        }
        else
        {
            exclusivityPolicy = ExclusivityPolicy.LINK;
        }

        org.apache.qpid.server.model.LifetimePolicy lifetimePolicy = getLifetimePolicy(source.getExpiryPolicy());

        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, subscriptionName);
        attributes.put(Queue.LIFETIME_POLICY, lifetimePolicy);
        attributes.put(Queue.EXCLUSIVE, exclusivityPolicy);
        attributes.put(Queue.DURABLE, isDurable);

        Map<String, Map<String, Object>> bindings = bindingInfo.getBindings();
        try
        {
            queue = virtualHost.getSubscriptionQueue(exchange.getName(), attributes, bindings);
        }
        catch (NotFoundException e)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND, e.getMessage()));
        }
        catch(IllegalStateException e)
        {
            throw new AmqpErrorException(new Error(AmqpError.RESOURCE_LOCKED,
                                                   "Subscription is already in use"));
        }
        return queue;
    }



    private static boolean hasCapability(final Symbol[] capabilities,
                                  final Symbol expectedCapability)
    {
        return (capabilities != null && Arrays.asList(capabilities).contains(expectedCapability));
    }

    private static LifetimePolicy getLifetimePolicy(final TerminusExpiryPolicy expiryPolicy) throws AmqpErrorException
    {
        LifetimePolicy lifetimePolicy;
        if (expiryPolicy == null || expiryPolicy == TerminusExpiryPolicy.SESSION_END)
        {
            lifetimePolicy = LifetimePolicy.DELETE_ON_SESSION_END;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.LINK_DETACH)
        {
            lifetimePolicy = LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.CONNECTION_CLOSE)
        {
            lifetimePolicy = LifetimePolicy.DELETE_ON_CONNECTION_CLOSE;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.NEVER)
        {
            lifetimePolicy = LifetimePolicy.PERMANENT;
        }
        else
        {
            Error error = new Error(AmqpError.NOT_IMPLEMENTED,
                                    String.format("unknown ExpiryPolicy '%s'", expiryPolicy.getValue()));
            throw new AmqpErrorException(error);
        }
        return lifetimePolicy;
    }

    private static String getMangledSubscriptionName(final String linkName,
                                                     final boolean isDurable,
                                                     final boolean isShared,
                                                     final boolean isGlobal,
                                                     String remoteContainerId)
    {
        if (isGlobal)
        {
            remoteContainerId = "_global_";
        }
        else
        {
            remoteContainerId = sanitizeName(remoteContainerId);
        }

        String subscriptionName;
        if (!isDurable && !isShared)
        {
            subscriptionName = UUID.randomUUID().toString();
        }
        else
        {
            subscriptionName = linkName;
            if (isShared)
            {
                int separator = subscriptionName.indexOf("|");
                if (separator > 0)
                {
                    subscriptionName = subscriptionName.substring(0, separator);
                }
            }
            subscriptionName = sanitizeName(subscriptionName);
        }
        return "qpidsub_/" + remoteContainerId + "_/" + subscriptionName + "_/" + (isDurable
                ? "durable"
                : "nondurable");
    }

    private static String sanitizeName(String name)
    {
        return name.replace("_", "__")
                   .replace(".", "_:")
                   .replace("(", "_O")
                   .replace(")", "_C")
                   .replace("<", "_L")
                   .replace(">", "_R");
    }

    @Override
    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    public Exchange<?> getExchange()
    {
        return _exchange;
    }

    Map<Symbol, Filter> getFilters()
    {
        return _filters == null ? null : Collections.unmodifiableMap(_filters);
    }

    @Override
    public Symbol[] getCapabilities()
    {
        return _capabilities;
    }

    public Queue<?> getQueue()
    {
        return (Queue<?>) getMessageSource();
    }

    private static final class BindingInfo
    {
        private final Map<Symbol, Filter> _actualFilters = new HashMap<>();
        private final Map<String, Map<String, Object>> _bindings = new HashMap<>();

        BindingInfo(Exchange<?> exchange,
                            final String queueName,
                            String bindingKey,
                            Map<Symbol, Filter> filters) throws AmqpErrorException
        {
            String binding = null;
            final Map<String, Object> arguments = new HashMap<>();
            if (filters != null && !filters.isEmpty())
            {
                boolean hasBindingFilter = false;
                boolean hasMessageFilter = false;
                for(Map.Entry<Symbol,Filter> entry : filters.entrySet())
                {
                    if(!hasBindingFilter
                       && entry.getValue() instanceof ExactSubjectFilter
                       && exchange.getType().equals(ExchangeDefaults.DIRECT_EXCHANGE_CLASS))
                    {
                        ExactSubjectFilter filter = (ExactSubjectFilter) entry.getValue();
                        binding = filter.getValue();
                        _actualFilters.put(entry.getKey(), filter);
                        hasBindingFilter = true;
                    }
                    else if(!hasBindingFilter
                            && entry.getValue() instanceof MatchingSubjectFilter
                            && exchange.getType().equals(ExchangeDefaults.TOPIC_EXCHANGE_CLASS))
                    {
                        MatchingSubjectFilter filter = (MatchingSubjectFilter) entry.getValue();
                        binding = filter.getValue();
                        _actualFilters.put(entry.getKey(), filter);
                        hasBindingFilter = true;
                    }
                    else if(entry.getValue() instanceof NoLocalFilter)
                    {
                        _actualFilters.put(entry.getKey(), entry.getValue());
                        arguments.put(AMQPFilterTypes.NO_LOCAL.toString(), true);
                    }
                    else if (!hasMessageFilter
                             && entry.getValue() instanceof org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter)
                    {
                        org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter selectorFilter =
                                (org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter) entry.getValue();

                        // TODO: QPID-7642 - due to inconsistent handling of invalid filters
                        // by different exchange implementations
                        // we need to validate filter before creation of binding
                        try
                        {
                            new org.apache.qpid.server.filter.JMSSelectorFilter(selectorFilter.getValue());
                        }
                        catch (ParseException | SelectorParsingException | TokenMgrError e)
                        {
                            Error error = new Error();
                            error.setCondition(AmqpError.INVALID_FIELD);
                            error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                            error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                            throw new AmqpErrorException(error);
                        }

                        arguments.put(AMQPFilterTypes.JMS_SELECTOR.toString(), selectorFilter.getValue());
                        _actualFilters.put(entry.getKey(), selectorFilter);
                        hasMessageFilter = true;
                    }
                }
            }

            if(binding != null)
            {
                _bindings.put(binding, arguments);
            }
            if(bindingKey != null)
            {
                _bindings.put(bindingKey, arguments);
            }
            if(binding == null
               && bindingKey == null
               && exchange.getType().equals(ExchangeDefaults.FANOUT_EXCHANGE_CLASS))
            {
                _bindings.put(queueName, arguments);
            }
            else if(binding == null
                    && bindingKey == null
                    && exchange.getType().equals(ExchangeDefaults.TOPIC_EXCHANGE_CLASS))
            {
                _bindings.put("#", arguments);
            }
        }

        Map<Symbol, Filter> getActualFilters()
        {
            return _actualFilters;
        }

        Map<String, Map<String, Object>> getBindings()
        {
            return _bindings;
        }


        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final BindingInfo that = (BindingInfo) o;

            return _actualFilters.equals(that._actualFilters) && _bindings.equals(that._bindings);
        }

        @Override
        public int hashCode()
        {
            int result = _actualFilters.hashCode();
            result = 31 * result + _bindings.hashCode();
            return result;
        }
    }
}
