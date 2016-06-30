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

import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.util.List;

import javax.net.ssl.TrustManager;

@ManagedObject( defaultType = "FileTrustStore" )
public interface TrustStore<X extends TrustStore<X>> extends ConfiguredObject<X>
{
    @ManagedAttribute( defaultValue = "false", description = "If true the Trust Store will expose its certificates as a special artificial message source.")
    boolean isExposedAsMessageSource();

    @ManagedAttribute( defaultValue = "[]", description = "If 'exposedAsMessageSource' is true, the trust store will expose its certificates only to VirtualHostNodes in this list or if this list is empty to all VirtualHostNodes who are not in the 'excludedVirtualHostNodeMessageSources' list." )
    List<VirtualHostNode<?>> getIncludedVirtualHostNodeMessageSources();

    @ManagedAttribute( defaultValue = "[]", description = "If 'exposedAsMessageSource' is true and 'includedVirtualHostNodeMessageSources' is empty, the trust store will expose its certificates only to VirtualHostNodes who are not in this list." )
    List<VirtualHostNode<?>> getExcludedVirtualHostNodeMessageSources();

    TrustManager[] getTrustManagers() throws GeneralSecurityException;

    Certificate[] getCertificates() throws GeneralSecurityException;

}
