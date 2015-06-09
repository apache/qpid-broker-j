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
package org.apache.qpid.server.logging;

import java.util.Map;
import java.util.Set;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public class BrokerNameAndLevelFilterImpl extends AbstractConfiguredObject<BrokerNameAndLevelFilterImpl>
        implements BrokerNameAndLevelFilter<BrokerNameAndLevelFilterImpl>
{
    @ManagedAttributeField
    private String _loggerName;
    @ManagedAttributeField(afterSet = "logLevelAfterSet")
    private LogLevel _level;

    private volatile Level _logbackLevel;

    @ManagedObjectFactoryConstructor
    protected BrokerNameAndLevelFilterImpl(final Map<String, Object> attributes, BrokerLogger<?> logger)
    {
        super(parentsMap(logger), attributes);
    }

    @Override
    protected void validateChange(ConfiguredObject<?> proxyForValidation, Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        BrokerNameAndLevelFilter proxyFilter = (BrokerNameAndLevelFilter)proxyForValidation;
        if (changedAttributes.contains(LOGGER_NAME) &&
                ((getLoggerName() != null && !getLoggerName().equals(proxyFilter.getLoggerName())) ||
                        (getLoggerName() == null && proxyFilter.getLoggerName() != null)))
        {
            throw new IllegalConfigurationException("Attribute '" + LOGGER_NAME + " cannot be changed");
        }
    }

    @Override
    public String getLoggerName()
    {
        return _loggerName;
    }

    @Override
    public LogLevel getLevel()
    {
        return _level;
    }

    private void logLevelAfterSet()
    {
        _logbackLevel = Level.toLevel(getLevel().name());
    }

    @Override
    public Filter<ILoggingEvent> asFilter()
    {
        final String loggerName = getLoggerName();
        if("".equals(loggerName) || Logger.ROOT_LOGGER_NAME.equals(loggerName))
        {
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    return event.getLevel().isGreaterOrEqual(_logbackLevel) ? FilterReply.ACCEPT : FilterReply.NEUTRAL;
                }
            };
        }
        else if(loggerName.endsWith(".*"))
        {
            final String prefixName = loggerName.substring(0,loggerName.length()-2);
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    return event.getLevel().isGreaterOrEqual(_logbackLevel) && event.getLoggerName().startsWith(prefixName) ? FilterReply.ACCEPT : FilterReply.NEUTRAL;
                }
            };
        }
        else
        {
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    return event.getLevel().isGreaterOrEqual(_logbackLevel) && event.getLoggerName().equals(loggerName) ? FilterReply.ACCEPT : FilterReply.NEUTRAL;
                }
            };
        }
    }

    @StateTransition( currentState = { State.ACTIVE, State.ERRORED, State.UNINITIALIZED }, desiredState = State.DELETED )
    private ListenableFuture<Void> doDelete()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        closeAsync().addListener(new Runnable()
        {
            @Override
            public void run()
            {
                setState(State.DELETED);
                returnVal.set(null);

            }
        }, getTaskExecutor().getExecutor());
        return returnVal;
    }

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }
}
