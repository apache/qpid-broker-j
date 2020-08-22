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

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;

import org.apache.qpid.server.logging.logback.validator.GelfConfigurationValidator;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ManagedObject(category = false, type = GraylogLogger.TYPE,
        description = "Logger implementation that writes log events to a remote graylog server",
        validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedBrokerLoggerChildTypes()")
public class BrokerGraylogLoggerImpl extends AbstractBrokerLogger<BrokerGraylogLoggerImpl>
        implements BrokerGraylogLogger<BrokerGraylogLoggerImpl>
{
    @ManagedObjectFactoryConstructor
    public BrokerGraylogLoggerImpl(Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @ManagedAttributeField
    private String _remoteHost;

    @ManagedAttributeField
    private int _port = GelfAppenderDefaults.PORT.value();

    @ManagedAttributeField
    private int _reconnectionInterval = GelfAppenderDefaults.RECONNECTION_INTERVAL.value();

    @ManagedAttributeField
    private int _connectionTimeout = GelfAppenderDefaults.CONNECTION_TIMEOUT.value();

    @ManagedAttributeField
    private int _maximumReconnectionAttempts = GelfAppenderDefaults.MAXIMUM_RECONNECTION_ATTEMPTS.value();

    @ManagedAttributeField
    private int _retryDelay = GelfAppenderDefaults.RETRY_DELAY.value();

    @ManagedAttributeField
    private int _messagesFlushTimeOut = GelfAppenderDefaults.MESSAGES_FLUSH_TIMEOUT.value();

    @ManagedAttributeField
    private int _messageBufferCapacity = GelfAppenderDefaults.MESSAGE_BUFFER_CAPACITY.value();

    @ManagedAttributeField
    private boolean _rawMessageIncluded = GelfEncoderDefaults.RAW_MESSAGE_INCLUDED.value();

    @ManagedAttributeField
    private boolean _eventMarkerIncluded = GelfEncoderDefaults.EVENT_MARKER_INCLUDED.value();

    @ManagedAttributeField
    private boolean _mdcPropertiesIncluded = GelfEncoderDefaults.MDC_PROPERTIES_INCLUDED.value();

    @ManagedAttributeField
    private boolean _callerDataIncluded = GelfEncoderDefaults.CALLER_DATA_INCLUDED.value();

    @ManagedAttributeField
    private boolean _rootExceptionDataIncluded = GelfEncoderDefaults.ROOT_EXCEPTION_DATA_INCLUDED.value();

    @ManagedAttributeField
    private boolean _logLevelNameIncluded = GelfEncoderDefaults.LOG_LEVEL_NAME_INCLUDED.value();

    @ManagedAttributeField
    private Map<String, Object> _staticFields = Collections.emptyMap();

    @ManagedAttributeField
    private String _messageOriginHost;

    private AsyncAppender _appender;

    @Override
    public String getRemoteHost()
    {
        return _remoteHost;
    }

    @Override
    public int getPort()
    {
        return _port;
    }

    @Override
    public int getReconnectionInterval()
    {
        return _reconnectionInterval;
    }

    @Override
    public int getConnectionTimeout()
    {
        return _connectionTimeout;
    }

    @Override
    public int getMaximumReconnectionAttempts()
    {
        return _maximumReconnectionAttempts;
    }

    @Override
    public int getRetryDelay()
    {
        return _retryDelay;
    }

    @Override
    public int getMessagesFlushTimeOut()
    {
        return _messagesFlushTimeOut;
    }

    @Override
    public int getMessageBufferCapacity()
    {
        return _messageBufferCapacity;
    }

    @Override
    public boolean isRawMessageIncluded()
    {
        return _rawMessageIncluded;
    }

    @Override
    public boolean isEventMarkerIncluded()
    {
        return _eventMarkerIncluded;
    }

    @Override
    public boolean hasMdcPropertiesIncluded()
    {
        return _mdcPropertiesIncluded;
    }

    @Override
    public boolean isCallerDataIncluded()
    {
        return _callerDataIncluded;
    }

    @Override
    public boolean hasRootExceptionDataIncluded()
    {
        return _rootExceptionDataIncluded;
    }

    @Override
    public boolean isLogLevelNameIncluded()
    {
        return _logLevelNameIncluded;
    }

    @Override
    public Map<String, Object> getStaticFields()
    {
        return _staticFields;
    }

    @Override
    public String getMessageOriginHost()
    {
        return _messageOriginHost;
    }

    @Override
    public AsyncAppender appender()
    {
        return _appender;
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(Context context)
    {
        _appender = GraylogAppender.newInstance(context, this);
        return _appender;
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        GelfConfigurationValidator.validateConfiguration(this, this);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        GelfConfigurationValidator.validateConfiguration(this, this);
    }

    @Override
    protected void postSetAttributes(Set<String> actualUpdatedAttributes)
    {
        super.postSetAttributes(actualUpdatedAttributes);
        GelfConfigurationValidator.validateConfiguration(this, this, actualUpdatedAttributes);
    }
}
