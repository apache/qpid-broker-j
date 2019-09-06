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
package org.apache.qpid.server.logging.logback;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogInclusionRule;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.VirtualHostLogInclusionRule;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public abstract class AbstractLogger<X extends AbstractLogger<X>> extends AbstractConfiguredObject<X>
{
    private final static ch.qos.logback.classic.Logger ROOT_LOGGER = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));

    private final CompositeFilter _compositeFilter = new CompositeFilter();

    protected AbstractLogger(Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        super(parent, attributes);
        addChangeListener(new LogInclusionRuleListener());
    }

    protected final void addLogInclusionRule(LogBackLogInclusionRule logInclusionRule)
    {
        _compositeFilter.addLogInclusionRule(logInclusionRule);
    }

    protected final void removeLogInclusionRule(LogBackLogInclusionRule logInclusionRule)
    {
        _compositeFilter.removeLogInclusionRule(logInclusionRule);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        LoggerContext loggerContext = ROOT_LOGGER.getLoggerContext();
        Appender<ILoggingEvent> appender = createAppenderInstance(loggerContext);
        appender.setName(getName());
        appender.setContext(loggerContext);

        for(LogInclusionRule logInclusionRule : getLogInclusionRules())
        {
            _compositeFilter.addLogInclusionRule((LogBackLogInclusionRule)logInclusionRule);
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

    protected abstract Collection<? extends LogInclusionRule> getLogInclusionRules();

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED, State.STOPPED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        stopLogging();
        return super.onDelete();
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
        final Appender appender = getAppender();
        if (appender != null)
        {
            appender.stop();
            ROOT_LOGGER.detachAppender(appender);
        }
    }

    protected Appender<ILoggingEvent> getAppender()
    {
        return ROOT_LOGGER.getAppender(getName());
    }

    private class LogInclusionRuleListener extends AbstractConfigurationChangeListener
    {

        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof LogBackLogInclusionRule)
            {
                addLogInclusionRule((LogBackLogInclusionRule) child);
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof LogBackLogInclusionRule)
            {
                removeLogInclusionRule((LogBackLogInclusionRule) child);
            }
        }
    }

    public static Map<String, Collection<String>> getSupportedVirtualHostLoggerChildTypes()
    {
        return getSupportedLoggerChildTypes(VirtualHostLogInclusionRule.class);
    }

    public static Map<String, Collection<String>> getSupportedBrokerLoggerChildTypes()
    {
        return getSupportedLoggerChildTypes(BrokerLogInclusionRule.class);
    }

    private static Map<String, Collection<String>> getSupportedLoggerChildTypes(Class<? extends ConfiguredObject> clazz)
    {
        return Collections.singletonMap(clazz.getSimpleName(), getSupportedLogInclusionRules(clazz));
    }


    private static Collection<String> getSupportedLogInclusionRules(Class<? extends ConfiguredObject> clazz)
    {

        final Iterable<ConfiguredObjectRegistration> registrations =
                (new QpidServiceLoader()).instancesOf(ConfiguredObjectRegistration.class);

        Set<String> supportedTypes = new HashSet<>();

        for(ConfiguredObjectRegistration registration : registrations)
        {
            for(Class<? extends ConfiguredObject> typeClass : registration.getConfiguredObjectClasses())
            {
                if(clazz.isAssignableFrom(typeClass))
                {
                    ManagedObject annotation = typeClass.getAnnotation(ManagedObject.class);

                    if (annotation.creatable() && annotation.defaultType().equals("") && LogBackLogInclusionRule.class.isAssignableFrom(typeClass))
                    {
                        supportedTypes.add(ConfiguredObjectTypeRegistry.getType(typeClass));
                    }
                }
            }
        }
        return Collections.unmodifiableCollection(supportedTypes);
    }

}
