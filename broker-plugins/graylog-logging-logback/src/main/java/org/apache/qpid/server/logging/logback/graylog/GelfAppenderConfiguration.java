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

public interface GelfAppenderConfiguration extends GelfEncoderConfiguration
{
    String getRemoteHost();

    default int getPort()
    {
        return GelfAppenderDefaults.PORT.value();
    }

    default int getReconnectionInterval()
    {
        return GelfAppenderDefaults.RECONNECTION_INTERVAL.value();
    }

    default int getConnectionTimeout()
    {
        return GelfAppenderDefaults.CONNECTION_TIMEOUT.value();
    }

    default int getMaximumReconnectionAttempts()
    {
        return GelfAppenderDefaults.MAXIMUM_RECONNECTION_ATTEMPTS.value();
    }

    default int getRetryDelay()
    {
        return GelfAppenderDefaults.RETRY_DELAY.value();
    }

    default int getMessagesFlushTimeOut()
    {
        return GelfAppenderDefaults.MESSAGES_FLUSH_TIMEOUT.value();
    }

    default int getMessageBufferCapacity()
    {
        return GelfAppenderDefaults.MESSAGE_BUFFER_CAPACITY.value();
    }
}
