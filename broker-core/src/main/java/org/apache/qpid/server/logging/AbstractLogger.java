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

import java.util.Collection;
import java.util.Map;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public abstract class AbstractLogger<X extends AbstractLogger<X>> extends AbstractConfiguredObject<X>
{
    private final static ch.qos.logback.classic.Logger ROOT_LOGGER = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));

    private final CompositeFilter _compositeFilter = new CompositeFilter();

    protected AbstractLogger(Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        super(parentsMap(parent), attributes);
        addChangeListener(new FilterListener());
    }

    protected final void addFilter(LoggerFilter filter)
    {
        _compositeFilter.addFilter(filter);
    }

    protected final void removeFilter(LoggerFilter filter)
    {
        _compositeFilter.removeFilter(filter);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        LoggerContext loggerContext = ROOT_LOGGER.getLoggerContext();
        QpidLoggerTurboFilter.installIfNecessary(loggerContext);
        Appender<ILoggingEvent> appender = createAppenderInstance(loggerContext);
        appender.setName(getName());
        appender.setContext(loggerContext);

        for(LoggerFilter filter : getLoggerFilters())
        {
            _compositeFilter.addFilter(filter);
        }
        appender.addFilter(_compositeFilter);

        ROOT_LOGGER.addAppender(appender);
        appender.start();

        // TODO generalise for virtualhost too
        StartupAppender startupAppender = (StartupAppender) ROOT_LOGGER.getAppender(StartupAppender.class.getName());
        if (startupAppender != null)
        {
            startupAppender.replayAccumulatedEvents(appender);
        }
    }

    protected abstract Appender<ILoggingEvent> createAppenderInstance(Context context);

    protected abstract Collection<? extends LoggerFilter> getLoggerFilters();

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED, State.STOPPED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @StateTransition(currentState = {State.ACTIVE, State.UNINITIALIZED, State.ERRORED, State.STOPPED}, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        closeAsync().addListener(new Runnable()
        {
            @Override
            public void run()
            {
                deleted();
                setState(State.DELETED);
                stopLogging();
                returnVal.set(null);
            }
        }, getTaskExecutor().getExecutor());
        return returnVal;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass, Map<String, Object> attributes, ConfiguredObject... otherParents)
    {
        return getObjectFactory().createAsync(childClass, attributes, this);
    }

    public final long getErrorCount()
    {
        return _compositeFilter.getErrorCount();
    }

    public final long getWarnCount()
    {
        return _compositeFilter.getWarnCount();
    }

    public void stopLogging()
    {
        Appender appender = ROOT_LOGGER.getAppender(getName());
        if (appender != null)
        {
            appender.stop();
            ROOT_LOGGER.detachAppender(appender);
        }
    }

    private class FilterListener implements ConfigurationChangeListener
    {

        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof LoggerFilter)
            {
                addFilter((LoggerFilter) child);
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof LoggerFilter)
            {
                removeFilter((LoggerFilter) child);
            }
        }

        @Override
        public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
        {
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
        }
    }
}
