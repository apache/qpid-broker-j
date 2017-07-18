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

package org.apache.qpid.server.virtualhostnode.derby;


import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferenceStoreProvider;

public interface DerbyVirtualHostNode<X extends DerbyVirtualHostNode<X>>
        extends VirtualHostNode<X>, FileBasedSettings, PreferenceStoreProvider
{
    String STORE_PATH = "storePath";

    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "${qpid.work_dir}${file.separator}${this:name}${file.separator}config")
    String getStorePath();

    @Override
    @ManagedAttribute(description = "Configuration for the preference store, e.g. type, path, etc.",
            defaultValue = "{\"type\": \"Provided\"}")
    PreferenceStoreAttributes getPreferenceStoreAttributes();
}
