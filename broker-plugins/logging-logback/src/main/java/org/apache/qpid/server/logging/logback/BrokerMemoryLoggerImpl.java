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
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

public class BrokerMemoryLoggerImpl extends AbstractBrokerLogger<BrokerMemoryLoggerImpl> implements BrokerMemoryLogger<BrokerMemoryLoggerImpl>
{
    @ManagedAttributeField
    private int _maxRecords;
    private LogRecorder _logRecorder;

    @ManagedObjectFactoryConstructor
    protected BrokerMemoryLoggerImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    public int getMaxRecords()
    {
        return _maxRecords;
    }

    @Override
    protected void postResolveChildren()
    {
        // Validate early (i.e. rather than onValidate) as super.postResolveChildren() creates the buffer
        int maxRecords = getMaxRecords();
        validateLimits(maxRecords);

        super.postResolveChildren();
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        BrokerMemoryLogger brokerMemoryLogger = (BrokerMemoryLogger) proxyForValidation;
        if (changedAttributes.contains(MAX_RECORDS))
        {
            final int maxRecords = brokerMemoryLogger.getMaxRecords();
            validateLimits(maxRecords);
        }
    }

    private void validateLimits(int maxRecords)
    {
        if (maxRecords > MAX_RECORD_LIMIT)
        {
            throw new IllegalConfigurationException(String.format("Maximum number of records (%d) exceeds limit (%d)", maxRecords, MAX_RECORD_LIMIT));
        }
        else if (maxRecords < 1)
        {
            throw new IllegalConfigurationException(String.format("Maximum number of records (%d) must be larger than zero", maxRecords));
        }
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(Context context)
    {
        if (_logRecorder != null)
        {
            throw new IllegalStateException("RecordEventAppender is already created");
        }
        RecordEventAppender appender =  new RecordEventAppender(getMaxRecords());
        _logRecorder = new LogRecorder(appender);
        return appender;
    }

    @Override
    public Collection<LogRecord> getLogEntries(long lastLogId)
    {
        List<LogRecord> logRecords = new ArrayList<>();
        for(LogRecord record : _logRecorder)
        {
            if (record.getId() > lastLogId)
            {
                logRecords.add(record);
            }
        }
        return logRecords;
    }

}
