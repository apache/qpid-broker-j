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

import ch.qos.logback.classic.net.SocketAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.util.Duration;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

public class BrokerLogbackSocketLoggerImpl
        extends AbstractBrokerLogger<BrokerLogbackSocketLoggerImpl> implements BrokerLogbackSocketLogger<BrokerLogbackSocketLoggerImpl>
{
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
    protected Appender<ILoggingEvent> createAppenderInstance(Context loggerContext)
    {
        SocketAppender socketAppender = new SocketAppender()
        {
            @Override
            protected void append(final ILoggingEvent event)
            {
                augmentWithMDC(event);
                super.append(event);
            }
        };
        socketAppender.setPort(_port);
        socketAppender.setRemoteHost(_remoteHost);
        socketAppender.setIncludeCallerData(_includeCallerData);
        socketAppender.setReconnectionDelay(Duration.buildByMilliseconds(_reconnectionDelay));
        return socketAppender;

    }

    private void augmentWithMDC(final ILoggingEvent event)
    {
        event.getMDCPropertyMap().putAll(_mappedDiagnosticContext);
    }

}
