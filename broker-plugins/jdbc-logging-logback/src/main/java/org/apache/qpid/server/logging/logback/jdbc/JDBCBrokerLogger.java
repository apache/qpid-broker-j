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

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.store.jdbc.DefaultConnectionProviderFactory;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

public interface JDBCBrokerLogger<X extends JDBCBrokerLogger<X>> extends BrokerLogger<X>, JDBCSettings
{
    String BROKER_FAIL_ON_JDBC_LOGGER_ERROR = "qpid.broker.failOnJdbcLoggerError";
    @ManagedContextDefault(name = BROKER_FAIL_ON_JDBC_LOGGER_ERROR)
    @SuppressWarnings("unused")
    String DEFAULT_BROKER_FAIL_ON_JDBC_LOGGER_ERROR = "false";

    @Override
    @ManagedAttribute(mandatory = true)
    String getConnectionUrl();

    @Override
    @ManagedAttribute(defaultValue = DefaultConnectionProviderFactory.TYPE,
            validValues = {"org.apache.qpid.server.store.jdbc.DefaultConnectionProviderFactory#getAllAvailableConnectionProviderTypes()"})
    String getConnectionPoolType();

    @Override
    @ManagedAttribute
    String getUsername();

    @Override
    @ManagedAttribute(secure = true)
    String getPassword();

    @Override
    @ManagedAttribute
    String getTableNamePrefix();
}
