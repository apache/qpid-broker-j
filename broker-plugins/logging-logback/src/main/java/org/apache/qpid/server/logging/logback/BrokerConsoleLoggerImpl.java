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

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.Context;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

public class BrokerConsoleLoggerImpl extends AbstractBrokerLogger<BrokerConsoleLoggerImpl>
        implements BrokerConsoleLogger<BrokerConsoleLoggerImpl>
{
    @ManagedAttributeField
    private String _layout;

    @ManagedAttributeField
    private ConsoleStreamTarget _consoleStreamTarget;

    @ManagedObjectFactoryConstructor
    protected BrokerConsoleLoggerImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    public ConsoleStreamTarget getConsoleStreamTarget()
    {
        return _consoleStreamTarget;
    }

    @Override
    public String getLayout()
    {
        return _layout;
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(Context context)
    {
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();

        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(getLayout());
        encoder.setContext(context);
        encoder.start();

        if (_consoleStreamTarget == ConsoleStreamTarget.STDERR)
        {
            consoleAppender.setTarget("System.err");
        }
        consoleAppender.setEncoder(encoder);

        return consoleAppender;
    }

    @SuppressWarnings("unused")
    public static Collection<String> getAllConsoleStreamTarget()
    {
        List<String> validValues = new ArrayList<>();
        for (ConsoleStreamTarget level : EnumSet.allOf(ConsoleStreamTarget.class))
        {
            validValues.add(level.name());
        }
        return validValues;
    }

}
