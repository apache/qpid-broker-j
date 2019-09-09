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

package org.apache.qpid.server.logging.logback.jdbc;

import java.util.Map;
import java.util.Set;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.status.StatusManager;

import org.apache.qpid.server.logging.logback.AbstractVirtualHostLogger;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

@SuppressWarnings("unused")
@ManagedObject(category = false, type = JDBCVirtualHostLoggerImpl.CONFIGURED_OBJECT_TYPE,
        validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedVirtualHostLoggerChildTypes()")
public class JDBCVirtualHostLoggerImpl extends AbstractVirtualHostLogger<JDBCVirtualHostLoggerImpl>
        implements JDBCVirtualHostLogger<JDBCVirtualHostLoggerImpl>
{
    static final String CONFIGURED_OBJECT_TYPE = "JDBC";

    private final JDBCLoggerHelper _jdbcLoggerHelper;

    private StatusManager _statusManager;

    @ManagedAttributeField(afterSet = "restartConnectionSourceIfExists")
    private String _connectionUrl;

    @ManagedAttributeField(afterSet = "restartConnectionSourceIfExists")
    private String _connectionPoolType;

    @ManagedAttributeField(afterSet = "restartConnectionSourceIfExists")
    private String _username;

    @ManagedAttributeField(afterSet = "restartConnectionSourceIfExists")
    private String _password;

    @ManagedAttributeField(afterSet = "restartAppenderIfExists")
    private String _tableNamePrefix;

    @ManagedObjectFactoryConstructor
    protected JDBCVirtualHostLoggerImpl(final Map<String, Object> attributes, VirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
        _jdbcLoggerHelper = new JDBCLoggerHelper();
    }

    @Override
    public String getConnectionUrl()
    {
        return _connectionUrl;
    }

    @Override
    public String getConnectionPoolType()
    {
        return _connectionPoolType;
    }

    @Override
    public String getUsername()
    {
        return _username;
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    @Override
    public String getTableNamePrefix()
    {
        return _tableNamePrefix;
    }

    @Override
    protected void validateChange(ConfiguredObject<?> proxyForValidation, Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if (changedAttributes.contains(JDBCSettings.CONNECTION_URL)
            || changedAttributes.contains(JDBCSettings.USERNAME)
            || changedAttributes.contains(JDBCSettings.PASSWORD)
            || changedAttributes.contains(JDBCSettings.CONNECTION_POOL_TYPE))
        {
            _jdbcLoggerHelper.validateConnectionSourceSettings(this, (JDBCVirtualHostLogger) proxyForValidation);
        }
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        _jdbcLoggerHelper.validateConnectionSourceSettings(this, this);
    }

    @Override
    protected Appender<ILoggingEvent> createAppenderInstance(final Context context)
    {
        return _jdbcLoggerHelper.createAppenderInstance(context, this, this);
    }

    private void restartAppenderIfExists()
    {
        _jdbcLoggerHelper.restartAppenderIfExists(getAppender());
    }

    private void restartConnectionSourceIfExists()
    {
        _jdbcLoggerHelper.restartConnectionSourceIfExists(getAppender());
    }
}
