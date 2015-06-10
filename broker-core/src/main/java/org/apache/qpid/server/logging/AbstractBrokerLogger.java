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
 * under the License.i
 *
 */
package org.apache.qpid.server.logging;

import java.util.Map;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerLoggerFilter;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBrokerLogger<X extends AbstractBrokerLogger<X>> extends AbstractConfiguredObject<X> implements BrokerLogger<X>, ConfigurationChangeListener
{
    protected CompositeFilter _compositeFilter = new CompositeFilter();

    protected AbstractBrokerLogger(Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);
    }

    protected CompositeFilter getCompositeFilter()
    {
        return _compositeFilter;
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        ch.qos.logback.classic.Logger rootLogger = getRootLogger();
        final Appender<ILoggingEvent> appender = asAppender();
        rootLogger.addAppender(appender);
        appender.start();

        StartupAppender startupAppender = (StartupAppender) rootLogger.getAppender(StartupAppender.class.getName());
        if (startupAppender != null)
        {
            startupAppender.replayAccumulatedEvents(appender);
        }
    }

    @Override
    public void stopLogging()
    {
        Appender appender = getLoggerAppender();
        appender.stop();
        getRootLogger().detachAppender(getName());
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
                setState(State.DELETED);
                stopLogging();
                returnVal.set(null);
            }
        }, getTaskExecutor().getExecutor());
        return returnVal;
    }

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED, State.STOPPED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    private Appender<ILoggingEvent> getLoggerAppender()
    {
        return getRootLogger().getAppender(getName());
    }

    private ch.qos.logback.classic.Logger getRootLogger()
    {
        return (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass, Map<String, Object> attributes, ConfiguredObject... otherParents)
    {
        final ListenableFuture<C> filterFuture = getObjectFactory().createAsync(childClass, attributes, this);
        doAfter(filterFuture, new CallableWithArgument<ListenableFuture<C>, C>()
        {
            @Override
            public ListenableFuture<C> call(final C child) throws Exception
            {
                BrokerLoggerFilter<?> filter = (BrokerLoggerFilter) child;
                filter.addChangeListener(AbstractBrokerLogger.this);
                getCompositeFilter().addFilter(filter);
                return Futures.immediateFuture(child);
            }
        });
        return filterFuture;
    }

    @Override
    public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
    {
        if (newState == State.DELETED && object instanceof BrokerLoggerFilter)
        {
            getCompositeFilter().removeFilter((BrokerLoggerFilter)object);
            object.removeChangeListener(this);
        }
    }

    @Override
    public void childAdded(ConfiguredObject<?> object, ConfiguredObject<?> child)
    {
        // no-op
    }

    @Override
    public void childRemoved(ConfiguredObject<?> object, ConfiguredObject<?> child)
    {
        // no-op
    }

    @Override
    public void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue)
    {
        // no-op
    }

    @Override
    public void bulkChangeStart(ConfiguredObject<?> object)
    {
        // no-op
    }

    @Override
    public void bulkChangeEnd(ConfiguredObject<?> object)
    {
        // no-op
    }

    protected void initializeAppender(Appender<ILoggingEvent> appender)
    {
        ch.qos.logback.classic.Logger rootLogger = getRootLogger();
        LoggerContext loggerContext = rootLogger.getLoggerContext();

        appender.setContext(loggerContext);

        CompositeFilter compositeFilter = getCompositeFilter();
        compositeFilter.addFilters(getChildren(BrokerLoggerFilter.class));
        appender.addFilter(compositeFilter);
        appender.setName(getName());
    }
}
