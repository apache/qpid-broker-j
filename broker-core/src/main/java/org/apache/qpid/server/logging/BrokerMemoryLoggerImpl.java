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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLoggerFilter;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

public class BrokerMemoryLoggerImpl extends AbstractConfiguredObject<BrokerMemoryLoggerImpl> implements BrokerMemoryLogger<BrokerMemoryLoggerImpl>
{

    @ManagedAttributeField
    private int _maxRecords;

    @ManagedObjectFactoryConstructor
    protected BrokerMemoryLoggerImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker),attributes);
    }

    @Override
    public int getMaxRecords()
    {
        return _maxRecords;
    }

    @Override
    public Appender<ILoggingEvent> asAppender()
    {
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        LoggerContext loggerContext = rootLogger.getLoggerContext();

        final RecordEventAppender appender = new RecordEventAppender(getMaxRecords());
        appender.setName(getName());
        appender.setContext(loggerContext);

        for(BrokerLoggerFilter<?> filter : getChildren(BrokerLoggerFilter.class))
        {
            appender.addFilter(filter.asFilter());
        }
        appender.addFilter(DenyAllFilter.getInstance());
        appender.start();
        return appender;
    }
}
