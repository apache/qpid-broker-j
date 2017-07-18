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

package org.apache.qpid.server.virtualhost.berkeleydb;


import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.berkeleydb.BDBEnvironmentContainer;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public interface BDBVirtualHost<X extends BDBVirtualHost<X>> extends QueueManagingVirtualHost<X>,
                                                                     FileBasedSettings,
                                                                     SizeMonitoringSettings,
                                                                     BDBEnvironmentContainer<X>
{

    String STORE_PATH = "storePath";

    long BDB_MIN_CACHE_SIZE = 10*1024*1024;
    String QPID_BROKER_BDB_TOTAL_CACHE_SIZE = "qpid.broker.bdbTotalCacheSize";

    // Default the JE cache to 5% of total memory, but no less than 10Mb
    @ManagedContextDefault(name= QPID_BROKER_BDB_TOTAL_CACHE_SIZE)
    long DEFAULT_JE_CACHE_SIZE = Math.max(BDB_MIN_CACHE_SIZE, Runtime.getRuntime().maxMemory()/20l);

    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "${qpid.work_dir}${file.separator}${this:name}${file.separator}messages")
    String getStorePath();

    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "0")
    Long getStoreUnderfullSize();

    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "0")
    Long getStoreOverfullSize();

}
