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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostLogInclusionRule;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

class ScopedLogInclusionRuleTest extends UnitTestBase
{
    private final List<AbstractLogger<?>> _loggers = new ArrayList<>();
    private final List<Appender<ILoggingEvent>> _appenders = new ArrayList<>();
    private final List<LogBackLogInclusionRule> _rules = new ArrayList<>();
    private TaskExecutor _taskExecutor;
    private Broker<?> _broker;
    private Logger _rootLogger;
    private Logger _messageLogger;
    private boolean _rootContextHadTurboFilter;

    @BeforeEach
    void setUp()
    {
        _loggers.clear();
        _appenders.clear();
        _rules.clear();
        _rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        _rootContextHadTurboFilter = _rootLogger.getLoggerContext().getTurboFilterList()
                .stream().anyMatch(QpidLoggerTurboFilter.class::isInstance);
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();
        _broker = mock(Broker.class);
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        doReturn(Broker.class).when(_broker).getCategoryClass();
        doReturn(Broker.class).when(_broker).getTypeClass();
        _messageLogger = (Logger) LoggerFactory.getLogger(getClass().getName() + "." + getTestName());
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            for (final AbstractLogger<?> logger : _loggers)
            {
                logger.delete();
            }
        }
        finally
        {
            for (final Appender<ILoggingEvent> appender : _appenders)
            {
                _rootLogger.detachAppender(appender);
                appender.stop();
            }
            for (final LogBackLogInclusionRule rule : _rules)
            {
                QpidLoggerTurboFilter.filterRemovedFromRootContext((EffectiveLevelFilter) rule.asFilter());
            }
            if (!_rootContextHadTurboFilter)
            {
                QpidLoggerTurboFilter.uninstallFromRootContext();
            }
            _taskExecutor.stopImmediately();
        }
    }

    @ParameterizedTest
    @CsvSource({"ERROR,false", "ERROR,true", "OFF,false", "OFF,true"})
    void testScopedVetoPreservesDestinationAndVirtualHostIsolation(final String threshold, final boolean fluent)
    {
        final ListAppender<ILoggingEvent> brokerAppender = new ListAppender<>();
        final AbstractBrokerLogger<?> brokerLogger = createBrokerLogger(brokerAppender);
        final Map<String, Object> brokerRuleAttributes = Map.of("name", "rule",
                "type", BrokerNameAndLevelLogInclusionRule.TYPE,
                "loggerName", _messageLogger.getName(),
                "level", LogLevel.DEBUG);
        final BrokerLogInclusionRule<?> brokerRule = brokerLogger
                .createChild(BrokerLogInclusionRule.class, brokerRuleAttributes);
        _rules.add((LogBackLogInclusionRule) brokerRule);

        final VirtualHost<?> firstHost = createVirtualHost();
        final VirtualHost<?> secondHost = createVirtualHost();
        final ListAppender<ILoggingEvent> firstAppender = new ListAppender<>();
        final ListAppender<ILoggingEvent> secondAppender = new ListAppender<>();
        final AbstractVirtualHostLogger<?> firstLogger = createVirtualHostLogger(firstHost, firstAppender, "first");
        final AbstractVirtualHostLogger<?> secondLogger = createVirtualHostLogger(secondHost, secondAppender, "second");
        addVirtualHostRule(firstLogger);
        addVirtualHostRule(secondLogger);
        final Map<String, Object> virtualHostRuleAttributes = Map.of("name", "scoped",
                "type", VirtualHostUserOrConnectionLogInclusionRule.TYPE,
                "loggerName", _messageLogger.getName(),
                "level", threshold,
                "username", "alice@.*");
        final VirtualHostLogInclusionRule<?> scopedRule = firstLogger
                .createChild(VirtualHostLogInclusionRule.class, virtualHostRuleAttributes);
        _rules.add((LogBackLogInclusionRule) scopedRule);

        logAs(firstHost, "alice", "first alice", fluent);
        logAs(firstHost, "bob", "first bob", fluent);
        logAs(secondHost, "alice", "second alice", fluent);
        logAs(secondHost, "bob", "second bob", fluent);
        SubjectExecutionContext.withSubject(null, () -> log("no subject", fluent));

        assertEquals(List.of("first bob"), messages(firstAppender));
        assertEquals(List.of("second alice", "second bob"), messages(secondAppender));
        assertEquals(List.of("first alice", "first bob", "second alice", "second bob", "no subject"),
                messages(brokerAppender));
    }

    private AbstractBrokerLogger<?> createBrokerLogger(final Appender<ILoggingEvent> appender)
    {
        final Map<String, Object> attributes = Map.of("name", getTestName() + "-broker");
        final AbstractBrokerLogger<?> logger = new AbstractBrokerLogger(attributes, _broker)
        {
            @Override
            protected Appender<ILoggingEvent> createAppenderInstance(final Context context)
            {
                return appender;
            }
        };
        _loggers.add(logger);
        _appenders.add(appender);
        logger.open();
        return logger;
    }

    private VirtualHost<?> createVirtualHost()
    {
        final VirtualHostNode<?> node = mock();
        doReturn(_broker).when(node).getParent();
        doReturn(VirtualHostNode.class).when(node).getCategoryClass();
        final VirtualHost<?> virtualHost = mock();
        doReturn(node).when(virtualHost).getParent();
        when(virtualHost.getModel()).thenReturn(BrokerModel.getInstance());
        when(virtualHost.getChildExecutor()).thenReturn(_taskExecutor);
        doReturn(VirtualHost.class).when(virtualHost).getCategoryClass();
        doReturn(VirtualHost.class).when(virtualHost).getTypeClass();
        when(virtualHost.getPrincipal()).thenReturn(mock(Principal.class));
        return virtualHost;
    }

    private AbstractVirtualHostLogger<?> createVirtualHostLogger(final VirtualHost<?> virtualHost,
                                                                 final Appender<ILoggingEvent> appender,
                                                                 final String name)
    {
        final Map<String, Object> attributes = Map.of("name", getTestName() + "-" + name);
        final AbstractVirtualHostLogger<?> logger = new AbstractVirtualHostLogger(attributes, virtualHost)
        {
            @Override
            protected Appender<ILoggingEvent> createAppenderInstance(final Context context)
            {
                return appender;
            }
        };
        _loggers.add(logger);
        _appenders.add(appender);
        logger.open();
        return logger;
    }

    private void addVirtualHostRule(final AbstractVirtualHostLogger<?> logger)
    {
        final Map<String, Object> attributes = Map.of("name", "rule",
                "type", VirtualHostNameAndLevelLogInclusionRule.TYPE,
                "loggerName", _messageLogger.getName(),
                "level", LogLevel.DEBUG);
        final VirtualHostLogInclusionRule<?> rule = logger.createChild(VirtualHostLogInclusionRule.class, attributes);
        _rules.add((LogBackLogInclusionRule) rule);
    }

    private void logAs(final VirtualHost<?> virtualHost, final String username, final String message,
                       final boolean fluent)
    {
        final GenericPrincipal principal = new GenericPrincipal(username + "@test('auth')");
        final AuthenticatedPrincipal user = new AuthenticatedPrincipal(principal);
        final Subject subject = new Subject(true, Set.of(virtualHost.getPrincipal(), user), Set.of(), Set.of());
        SubjectExecutionContext.withSubject(subject, () -> log(message, fluent));
    }

    private void log(final String message, final boolean fluent)
    {
        if (fluent)
        {
            _messageLogger.atDebug().log(message);
        }
        else
        {
            _messageLogger.debug(message);
        }
    }

    private List<String> messages(final ListAppender<ILoggingEvent> appender)
    {
        return appender.list.stream()
                .filter(event -> _messageLogger.getName().equals(event.getLoggerName()))
                .map(ILoggingEvent::getFormattedMessage).toList();
    }
}
