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

import ch.qos.logback.classic.net.SyslogAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;

import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHost;

import java.util.Map;

public class VirtualHostSyslogLoggerImpl extends AbstractVirtualHostLogger<VirtualHostSyslogLoggerImpl>
        implements VirtualHostSyslogLogger<VirtualHostSyslogLoggerImpl>
{
    @ManagedAttributeField
    private String _syslogHost;
    @ManagedAttributeField
    private int _port;
    @ManagedAttributeField
    private String _suffixPattern;
    @ManagedAttributeField
    private String _stackTracePattern;
    @ManagedAttributeField
    private boolean _throwableExcluded;

    @ManagedObjectFactoryConstructor
    protected VirtualHostSyslogLoggerImpl(final Map<String, Object> attributes, VirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
    }

    @Override
    public String getSyslogHost()
    {
        return _syslogHost;
    }

    @Override
    public int getPort()
    {
        return _port;
    }

    @Override
    public String getSuffixPattern()
    {
        return _suffixPattern;
    }

    @Override
    public String getStackTracePattern()
    {
        return _stackTracePattern;
    }

    @Override
    public boolean isThrowableExcluded()
    {
        return _throwableExcluded;
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(Context context)
    {
        SyslogAppender syslogAppender = new SyslogAppender();
        syslogAppender.setSyslogHost(_syslogHost);
        syslogAppender.setPort(_port);
        syslogAppender.setSuffixPattern(_suffixPattern);
        syslogAppender.setStackTracePattern(_stackTracePattern);
        syslogAppender.setThrowableExcluded(_throwableExcluded);
        syslogAppender.setFacility("USER");
        return syslogAppender;
    }
}
