/*
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
 */

package org.apache.qpid.server.logging.logback;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.SystemConfig;

public class BrokerLoggerStatusListener implements StatusListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerLoggerStatusListener.class);

    private final SystemConfig<?> _systemConfig;
    private final BrokerLogger<?> _brokerLogger;
    private final String _contextFlag;
    private final Set<Class<?>> _errorClasses;

    public BrokerLoggerStatusListener(final BrokerLogger<?> brokerLogger,
                               final SystemConfig<?> systemConfig,
                               final String contextFlag,
                               final Class<?>... errorClass)
    {
        _brokerLogger = brokerLogger;
        _systemConfig = systemConfig;
        _contextFlag = contextFlag;
        _errorClasses =
                errorClass == null ? Collections.emptySet() : Arrays.stream(errorClass).collect(Collectors.toSet());
    }

    @Override
    public void addStatusEvent(Status status)
    {
        Throwable throwable = status.getThrowable();
        if (status.getEffectiveLevel() == Status.ERROR
            && _errorClasses.stream().anyMatch(c -> c.isInstance(throwable)))
        {
            LOGGER.error("Unexpected error whilst trying to store log entry. Log messages could be lost.", throwable);
            if (_brokerLogger.getContextValue(Boolean.class, _contextFlag))
            {
                try
                {
                    _brokerLogger.stopLogging();
                    _systemConfig.getEventLogger().message(BrokerMessages.FATAL_ERROR(
                            String.format(
                                    "Shutting down the broker because context variable '%s' is set and unexpected logging issue occurred: %s",
                                    _contextFlag,
                                    throwable.getMessage())));
                }
                finally
                {
                    _systemConfig.closeAsync();
                }
            }
        }
    }
}
