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

package org.apache.qpid.server.store.berkeleydb;

import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;

public interface BDBEnvironmentContainer<X extends ConfiguredObject<X>> extends ConfiguredObject<X>
{
    void setBDBCacheSize(long cacheSize);

    @ManagedOperation(description = "Update BDB mutable configuration from settings in context variables",
            changesConfiguredObjectState = false)
    void updateMutableConfig();

    @ManagedOperation(description = "Instruct BDB to attempt to clean up its log files",
            changesConfiguredObjectState = false)
    int cleanLog();

    @ManagedOperation(description = "Instruct BDB to perform a checkpoint operation",
            changesConfiguredObjectState = false)
    void checkpoint(@Param(name = "force", defaultValue = "false") boolean force);

    @ManagedOperation(description = "Get the BDB environment statistics", nonModifying = true,
            changesConfiguredObjectState = false)
    Map<String,Map<String,Object>> environmentStatistics(@Param(name="reset", defaultValue = "false", description = "If true, reset the statistics") boolean reset);

    @ManagedOperation(description = "Get the BDB transaction statistics", nonModifying = true,
            changesConfiguredObjectState = false)
    Map<String, Object> transactionStatistics(@Param(name="reset", defaultValue = "false", description = "If true, reset the statistics")boolean reset);

    @ManagedOperation(description = "Get the BDB database statistics", nonModifying = true,
            changesConfiguredObjectState = false)
    Map<String, Object> databaseStatistics(@Param(name="database", description = "database table for which to retrieve statistics", mandatory = true)String database, @Param(name="reset", defaultValue = "false", description = "If true, reset the statistics") boolean reset);

    EnvironmentFacade getEnvironmentFacade();
}
