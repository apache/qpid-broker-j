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

package org.apache.qpid.server.virtualhost.jdbc;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.store.jdbc.DefaultConnectionProviderFactory;
import org.apache.qpid.server.store.jdbc.JDBCSettings;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public interface JDBCVirtualHost<X extends JDBCVirtualHost<X>> extends QueueManagingVirtualHost<X>,
                                                                       JDBCSettings
{
    @Override
    @ManagedAttribute(mandatory=true)
    String getConnectionUrl();

    @Override
    @ManagedAttribute(defaultValue=DefaultConnectionProviderFactory.TYPE,
            validValues = {"org.apache.qpid.server.store.jdbc.DefaultConnectionProviderFactory#getAllAvailableConnectionProviderTypes()"} )
    String getConnectionPoolType();

    @Override
    @ManagedAttribute
    String getUsername();

    @Override
    @ManagedAttribute(secure=true)
    String getPassword();

    @ManagedContextDefault(name = "jdbcvirtualhost.tableNamePrefix",
            description = "Default value for optional database table prefix")
    String DEFAULT_JDBC_VIRTUALHOST_TABLE_NAME_PREFIX = "";

    @Override
    @ManagedAttribute(
            description = "Optional database table prefix so multiple VirtualHosts can share the same database",
            defaultValue = "${jdbcvirtualhost.tableNamePrefix}",
            validValuePattern = "[a-zA-Z_0-9]*",
            immutable = true)
    String getTableNamePrefix();

}
