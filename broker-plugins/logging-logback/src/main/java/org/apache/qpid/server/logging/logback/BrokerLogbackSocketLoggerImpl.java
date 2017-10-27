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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import ch.qos.logback.classic.net.SocketAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.util.Action;

public class BrokerLogbackSocketLoggerImpl
        extends AbstractBrokerLogger<BrokerLogbackSocketLoggerImpl> implements BrokerLogbackSocketLogger<BrokerLogbackSocketLoggerImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerLogbackSocketLoggerImpl.class);

    private final List<Action<Void>> _stopLoggingActions = new CopyOnWriteArrayList<>();

    @ManagedAttributeField
    private String _remoteHost;

    @ManagedAttributeField
    private long _reconnectionDelay;

    @ManagedAttributeField
    private int _port;

    @ManagedAttributeField
    private boolean _includeCallerData;

    @ManagedAttributeField
    private Map<String,String> _mappedDiagnosticContext;

    @ManagedAttributeField
    private Map<String, String> _contextProperties;

    @ManagedObjectFactoryConstructor
    protected BrokerLogbackSocketLoggerImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

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
    public long getReconnectionDelay()
    {
        return _reconnectionDelay;
    }

    @Override
    public boolean getIncludeCallerData()
    {
        return _includeCallerData;
    }

    @Override
    public Map<String, String> getMappedDiagnosticContext()
    {
        return _mappedDiagnosticContext;
    }

    @Override
    public Map<String, String> getContextProperties()
    {
        return _contextProperties;
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(Context loggerContext)
    {
        if (_contextProperties != null && !_contextProperties.isEmpty())
        {
            for (Map.Entry<String, String> property : _contextProperties.entrySet())
            {
                final String key = property.getKey();
                final String value = property.getValue();
                final String existingValue = loggerContext.getProperty(key);
                if (existingValue != null && !Objects.equals(existingValue, value))
                {
                    LOGGER.warn("Logback context property key '{}' value '{}' overwritten with value '{}", key, existingValue, value);
                }
                loggerContext.putProperty(key, value);

                _stopLoggingActions.add(object ->
                                  {
                                      loggerContext.putProperty(key, existingValue);
                                  });
            }
        }

        SocketAppender socketAppender = new SocketAppender()
                                        {
                                            @Override
                                            protected void append(final ILoggingEvent event)
                                            {
                                                Set<String> keys = new HashSet<>();
                                                try
                                                {
                                                    for (Map.Entry<String, String> entry : _mappedDiagnosticContext.entrySet())
                                                    {
                                                        MDC.put(entry.getKey(), entry.getValue());
                                                        keys.add(entry.getKey());
                                                    }

                                                    // Workaround for suspected Logback defect LOGBACK-1088
                                                    event.prepareForDeferredProcessing();
                                                    super.append(event);
                                                }
                                                finally
                                                {
                                                    for (String key : keys)
                                                    {
                                                        MDC.remove(key);
                                                    }
                                                }
                                            }
                                        };
        socketAppender.setPort(_port);
        socketAppender.setRemoteHost(_remoteHost);
        socketAppender.setIncludeCallerData(_includeCallerData);
        socketAppender.setReconnectionDelay(Duration.buildByMilliseconds(_reconnectionDelay));
        return socketAppender;
    }

    @Override
    public void stopLogging()
    {
        try
        {
            for (Action action : _stopLoggingActions)
            {
                action.performAction(null);
            }
            _stopLoggingActions.clear();
        }
        finally
        {
            super.stopLogging();
        }
    }
}
