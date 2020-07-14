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
import ch.qos.logback.core.Context;
import de.siegmar.logbackgelf.GelfEncoder;
import de.siegmar.logbackgelf.GelfTcpAppender;
import org.apache.qpid.server.logging.logback.event.LoggingEvent;

import java.util.Objects;

final class GraylogAppender extends AsyncAppender
{
    private final GelfAppenderConfiguration _configuration;

    static GraylogAppender newInstance(Context context, GelfAppenderConfiguration config)
    {
        final GraylogAppender appender = new GraylogAppender(config);
        appender.setContext(context);
        appender.setQueueSize(config.getMessageBufferCapacity());
        appender.setNeverBlock(true);
        appender.setMaxFlushTime(config.getMessagesFlushTimeOut());
        appender.setIncludeCallerData(config.isCallerDataIncluded());
        return appender;
    }

    private GraylogAppender(GelfAppenderConfiguration configuration)
    {
        super();
        this._configuration = Objects.requireNonNull(configuration);
    }

    @Override
    public void start()
    {
        if (!isStarted())
        {
            final GelfEncoder encoder = buildEncoder(getContext(), _configuration);
            encoder.start();

            final GelfTcpAppender appender = newGelfTcpAppender(getContext(), _configuration);
            appender.setEncoder(encoder);
            appender.setName(getName());
            appender.start();
            addAppender(appender);
        }
        super.start();
    }

    @Override
    public void setName(final String name)
    {
        super.setName(name);
        iteratorForAppenders().forEachRemaining(item -> item.setName(name));
    }

    @Override
    public void setContext(final Context context)
    {
        super.setContext(context);
        iteratorForAppenders().forEachRemaining(item -> item.setContext(context));
    }

    @Override
    public void doAppend(ILoggingEvent eventObject)
    {
        super.doAppend(LoggingEvent.wrap(eventObject));
    }

    private GelfTcpAppender newGelfTcpAppender(Context context, GelfAppenderConfiguration logger)
    {
        final GelfTcpAppender appender = new GelfTcpAppender();
        appender.setContext(context);
        appender.setGraylogHost(logger.getRemoteHost());
        appender.setGraylogPort(logger.getPort());
        appender.setReconnectInterval(calculateReconnectionInterval(logger));
        appender.setConnectTimeout(logger.getConnectionTimeout());
        appender.setMaxRetries(logger.getMaximumReconnectionAttempts());
        appender.setRetryDelay(logger.getRetryDelay());
        return appender;
    }

    private int calculateReconnectionInterval(GelfAppenderConfiguration logger)
    {
        final int modulo = logger.getReconnectionInterval() % 1000;
        final int auxiliary = logger.getReconnectionInterval() / 1000;
        return modulo > 0 ? auxiliary + 1 : auxiliary;
    }

    private GelfEncoder buildEncoder(Context context, GelfEncoderConfiguration settings)
    {
        final GelfEncoder encoder = new GelfEncoder();
        encoder.setContext(context);
        encoder.setOriginHost(settings.getMessageOriginHost());
        encoder.setIncludeRawMessage(settings.isRawMessageIncluded());
        encoder.setIncludeMarker(settings.isEventMarkerIncluded());
        encoder.setIncludeMdcData(settings.hasMdcPropertiesIncluded());
        encoder.setIncludeCallerData(settings.isCallerDataIncluded());
        encoder.setIncludeRootCauseData(settings.hasRootExceptionDataIncluded());
        encoder.setIncludeLevelName(settings.isLogLevelNameIncluded());
        encoder.setStaticFields(settings.getStaticFields());
        return encoder;
    }
}
