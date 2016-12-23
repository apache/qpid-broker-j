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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.logging.logback.BrokerNameAndLevelLogInclusionRule;
import org.apache.qpid.server.logging.logback.LoggerNameAndLevelFilter;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRule;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRuleImpl;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostLogger;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.QpidTestCase;

public class VirtualHostNameAndLevelLogInclusionRuleImplTest extends QpidTestCase
{
    private VirtualHostLogger _virtualHostLogger;
    private TaskExecutor _taskExecutor;
    private final VirtualHost _virtualhost = mock(VirtualHost.class);

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _taskExecutor =  new TaskExecutorImpl();
        _taskExecutor.start();

        Model model = BrokerModel.getInstance();

        Broker broker = mock(Broker.class);
        when(broker.getModel()).thenReturn(model);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);
        doReturn(Broker.class).when(broker).getCategoryClass();

        VirtualHostNode<?> node =  mock(VirtualHostNode.class);
        when(node.getModel()).thenReturn(model);
        when(node.getChildExecutor()).thenReturn(_taskExecutor);
        when(node.getParent()).thenReturn(broker);
        doReturn(VirtualHostNode.class).when(node).getCategoryClass();

        when(_virtualhost.getModel()).thenReturn(model);
        when(_virtualhost.getParent()).thenReturn(node);
        doReturn(VirtualHost.class).when(_virtualhost).getCategoryClass();

        _virtualHostLogger = mock(VirtualHostLogger.class);
        when(_virtualHostLogger.getModel()).thenReturn(model);
        when(_virtualHostLogger.getChildExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostLogger.getParent()).thenReturn(_virtualhost);
        doReturn(VirtualHostLogger.class).when(_virtualHostLogger).getCategoryClass();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _taskExecutor.stopImmediately();
        }
        finally
        {
            super.tearDown();
        }
    }


    public void testAsFilter()
    {
        VirtualHostNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);

        Filter<ILoggingEvent> filter = rule.asFilter();

        assertTrue("Unexpected filter instance", filter instanceof LoggerNameAndLevelFilter);

        LoggerNameAndLevelFilter f = (LoggerNameAndLevelFilter)filter;
        assertEquals("Unexpected log level", Level.INFO, f.getLevel());
        assertEquals("Unexpected logger name", "org.apache.qpid", f.getLoggerName());
    }

    public void testLevelChangeAffectsFilter()
    {
        VirtualHostNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);

        LoggerNameAndLevelFilter filter = (LoggerNameAndLevelFilter)rule.asFilter();

        assertEquals("Unexpected log level", Level.INFO, filter.getLevel());

        rule.setAttributes(Collections.<String, Object>singletonMap("level", LogLevel.DEBUG));
        assertEquals("Unexpected log level attribute", Level.DEBUG, filter.getLevel());
    }

    public void testLoggerNameChangeNotAllowed()
    {
        VirtualHostNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);

        LoggerNameAndLevelFilter filter = (LoggerNameAndLevelFilter)rule.asFilter();

        assertEquals("Unexpected logger name", "org.apache.qpid", filter.getLoggerName());

        try
        {
            rule.setAttributes(Collections.<String, Object>singletonMap(BrokerNameAndLevelLogInclusionRule.LOGGER_NAME, "org.apache.qpid.foo"));
            fail("IllegalConfigurationException is expected to throw on attempt to change logger name");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }

        assertEquals("Unexpected logger name", "org.apache.qpid", filter.getLoggerName());
    }


    private VirtualHostNameAndLevelLogInclusionRule createRule(String loggerName, LogLevel logLevel)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("loggerName", loggerName);
        attributes.put("level", logLevel);
        attributes.put("name", "test");

        VirtualHostNameAndLevelLogInclusionRuleImpl rule = new VirtualHostNameAndLevelLogInclusionRuleImpl(attributes,
                                                                                                           _virtualHostLogger);
        rule.open();
        return rule;
    }



}
