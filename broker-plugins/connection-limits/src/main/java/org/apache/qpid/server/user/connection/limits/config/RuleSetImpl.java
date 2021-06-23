/*
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
 */
package org.apache.qpid.server.user.connection.limits.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.user.connection.limits.logging.ConnectionLimitEventLogger;
import org.apache.qpid.server.user.connection.limits.logging.FullConnectionLimitEventLogger;
import org.apache.qpid.server.user.connection.limits.outcome.RejectRegistration;

final class RuleSetImpl implements RuleSet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleSet.class);

    private final String _name;

    private final Function<EventLoggerProvider, ConnectionLimitEventLogger> _loggerFactory;

    private final Map<String, PortConnectionCounter> _portConnectionCounters;

    private final Function<String, PortConnectionCounter> _defaultPortConnectionCounterProvider;

    private final AtomicReference<ConnectionLimiter> _appendedLimiter;

    RuleSetImpl(RuleSetBuilderImpl builder)
    {
        _name = builder.getName();
        _loggerFactory = builder.getLoggerFactory();
        _portConnectionCounters = new ConcurrentHashMap<>(builder.getPortConnectionCounters());
        _defaultPortConnectionCounterProvider = builder.getDefaultPortConnectionCounterProvider();
        _appendedLimiter = new AtomicReference<>(ConnectionLimiter.noLimits());
    }

    private RuleSetImpl(RuleSetImpl ruleSet, ConnectionLimiter limiter)
    {
        _name = ruleSet._name;
        _loggerFactory = ruleSet._loggerFactory;
        _portConnectionCounters = ruleSet._portConnectionCounters;
        _defaultPortConnectionCounterProvider = ruleSet._defaultPortConnectionCounterProvider;
        _appendedLimiter = new AtomicReference<>(limiter);
    }

    @Override
    public ConnectionSlot register(AMQPConnection<?> connection)
    {
        try
        {
            return _portConnectionCounters.computeIfAbsent(connection.getPort().getName(), _defaultPortConnectionCounterProvider)
                    .register(connection, _appendedLimiter.get())
                    .logMessage(_loggerFactory.apply(connection));
        }
        catch (RejectRegistration result)
        {
            LOGGER.debug(String.format("Connection limiter %s, user limit exceeded", _name), result);
            throw new ConnectionLimitException(result.logMessage(_loggerFactory.apply(connection)));
        }
    }

    @Override
    public ConnectionLimiter append(final ConnectionLimiter limiter)
    {
        return new RuleSetImpl(this, _appendedLimiter.get().append(limiter));
    }

    @Override
    public String toString()
    {
        return _name;
    }

    static final class RuleSetBuilderImpl implements Builder
    {
        private final String _name;

        private final List<Rule> _forAllPorts = new ArrayList<>();

        private final Duration _defaultFrequencyPeriod;

        private final Map<String, PortConnectionCounter.Builder> _builders;

        private final PortConnectionCounter.Builder _defaultBuilder;

        private Function<EventLoggerProvider, ConnectionLimitEventLogger> _loggerFactory;

        RuleSetBuilderImpl(String name, Duration defaultFrequencyPeriod)
        {
            super();
            _name = name;
            _defaultFrequencyPeriod = defaultFrequencyPeriod;
            _loggerFactory = loggerProvider -> new ConnectionLimitEventLogger(name, loggerProvider);

            _builders = new HashMap<>();
            _defaultBuilder = PortConnectionCounter.newBuilder(defaultFrequencyPeriod);
        }

        @Override
        public Builder logAllMessages(boolean all)
        {
            if (all)
            {
                _loggerFactory = loggerProvider -> new FullConnectionLimitEventLogger(_name, loggerProvider);
            }
            else
            {
                _loggerFactory = loggerProvider -> new ConnectionLimitEventLogger(_name, loggerProvider);
            }
            return this;
        }

        @Override
        public Builder addRule(Rule rule)
        {
            addRuleImpl(rule);
            return this;
        }

        @Override
        public Builder addRules(Collection<? extends Rule> rules)
        {
            if (rules != null)
            {
                rules.forEach(this::addRuleImpl);
            }
            return this;
        }

        private void addRuleImpl(Rule rule)
        {
            if (rule == null)
            {
                return;
            }

            final String port = rule.getPort();
            if (RulePredicates.isAllPort(port))
            {
                _forAllPorts.add(rule);
                _defaultBuilder.add(rule);
                _builders.values().forEach(builder -> builder.add(rule));
            }
            else
            {
                _builders.computeIfAbsent(
                        port,
                        portName -> PortConnectionCounter.newBuilder(_defaultFrequencyPeriod).addAll(_forAllPorts)
                ).add(rule);
            }
        }

        @Override
        public RuleSet build()
        {
            return new RuleSetImpl(this);
        }

        Function<EventLoggerProvider, ConnectionLimitEventLogger> getLoggerFactory()
        {
            return _loggerFactory;
        }

        Map<String, PortConnectionCounter> getPortConnectionCounters()
        {
            final Map<String, PortConnectionCounter> limiters = new HashMap<>();
            _builders.forEach((port, builder) -> limiters.put(port, builder.build()));
            return limiters;
        }

        Function<String, PortConnectionCounter> getDefaultPortConnectionCounterProvider()
        {
            return portName -> _defaultBuilder.build();
        }

        String getName()
        {
            return _name;
        }
    }
}
