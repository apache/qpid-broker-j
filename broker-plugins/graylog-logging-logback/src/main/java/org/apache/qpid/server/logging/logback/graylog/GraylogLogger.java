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
package org.apache.qpid.server.logging.logback.graylog;

import ch.qos.logback.classic.AsyncAppender;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedStatistic;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;

import java.util.Map;

@ManagedObject
public interface GraylogLogger<X extends GraylogLogger<X>> extends GelfAppenderConfiguration, ConfiguredObject<X>
{
    String TYPE = "Graylog";

    @Override
    @ManagedAttribute(mandatory = true, description = "The graylog server remote host.")
    String getRemoteHost();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.PORT_AS_STRING,
            description = "The graylog server port number.")
    int getPort();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.RECONNECTION_INTERVAL_AS_STRING,
            description = "The reconnection interval.")
    int getReconnectionInterval();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.CONNECTION_TIMEOUT_AS_STRING,
            description = "The connection timeout.")
    int getConnectionTimeout();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.MAXIMUM_RECONNECTION_ATTEMPTS_AS_STRING,
            description = "The maximum reconnection attempts.")
    int getMaximumReconnectionAttempts();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.RETRY_DELAY_AS_STRING,
            description = "The retry delay.")
    int getRetryDelay();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.MESSAGES_FLUSH_TIMEOUT_AS_STRING,
            description = "The messages flush time out at logger stop.")
    int getMessagesFlushTimeOut();

    @Override
    @ManagedAttribute(defaultValue = GelfAppenderDefaults.MESSAGE_BUFFER_CAPACITY_AS_STRING,
            description = "The capacity of the message buffer.")
    int getMessageBufferCapacity();

    @Override
    @ManagedAttribute(mandatory = true, description = "The origin host that is included in the GELF log message.")
    String getMessageOriginHost();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.RAW_MESSAGE_INCLUDED_AS_STRING,
            description = "Include the raw error in the GELF log message.")
    boolean isRawMessageIncluded();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.EVENT_MARKER_INCLUDED_AS_STRING,
            description = "Include the event marker in the GELF log message.")
    boolean isEventMarkerIncluded();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.MDC_PROPERTIES_INCLUDED_AS_STRING,
            description = "Include the MDC properties in the GELF log message.")
    boolean hasMdcPropertiesIncluded();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.CALLER_DATA_INCLUDED_AS_STRING,
            description = "Include the caller data in the GELF log message.")
    boolean isCallerDataIncluded();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.ROOT_EXCEPTION_DATA_INCLUDED_AS_STRING,
            description = "Include the root cause of error in the GELF log message.")
    boolean hasRootExceptionDataIncluded();

    @Override
    @ManagedAttribute(defaultValue = GelfEncoderDefaults.LOG_LEVEL_NAME_INCLUDED_AS_STRING,
            description = "Include the log level in the GELF log message.")
    boolean isLogLevelNameIncluded();

    @Override
    @ManagedAttribute(description = "Additional static fields for the GELF log message.")
    Map<String, Object> getStaticFields();

    AsyncAppender appender();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Graylog Appender Buffer Size",
            description = "The buffer size of the Broker Graylog appender.")
    default int getAppenderBufferUsage()
    {
        final AsyncAppender appender = appender();
        if (appender != null)
        {
            return appender.getQueueSize() - appender.getRemainingCapacity();
        }
        return 0;
    }
}
