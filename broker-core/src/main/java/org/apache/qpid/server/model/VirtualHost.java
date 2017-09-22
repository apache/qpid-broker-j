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
package org.apache.qpid.server.model;

import java.util.Collection;

@ManagedObject( defaultType = "ProvidedStore", description = VirtualHost.CLASS_DESCRIPTION, amqpName = "org.apache.qpid.VirtualHost")
public interface VirtualHost<X extends VirtualHost<X>> extends ConfiguredObject<X>,
                                                               NamedAddressSpace
{
    String CLASS_DESCRIPTION = "<p>A virtualhost is a namespace in which messaging is performed. Virtualhosts are "
                               + "independent; the messaging goes on a within a virtualhost is independent of any "
                               + "messaging that goes on in another virtualhost. For instance, a queue named <i>foo</i> "
                               + "defined in one virtualhost is completely independent of a queue named <i>foo</i> in "
                               + "another virtualhost.</p>"
                               + "<p>A virtualhost is backed by storage which is used to store the messages.</p>";

    String MODEL_VERSION                        = "modelVersion";
    String VIRTUALHOST_WORK_DIR_VAR             = "virtualhost.work_dir";
    String VIRTUALHOST_WORK_DIR_VAR_EXPRESSION  = "${qpid.work_dir}${file.separator}${ancestor:virtualhost:name}";
    String PREFERENCE_STORE_ATTRIBUTES          = "preferenceStoreAttributes";

    @ManagedContextDefault( name = VIRTUALHOST_WORK_DIR_VAR)
    String VIRTUALHOST_WORK_DIR = VIRTUALHOST_WORK_DIR_VAR_EXPRESSION;

    @DerivedAttribute( persist = true )
    String getModelVersion();

    @DerivedAttribute
    String getProductVersion();

    @Override
    @ManagedOperation(nonModifying = true,
            changesConfiguredObjectState = false,
            associateAsIfChildren = true,
            skipAclCheck = true)
    Collection<? extends Connection<?>> getConnections();

}
