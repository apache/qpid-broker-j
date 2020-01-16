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
package org.apache.qpid.server.security;

import static org.apache.qpid.server.model.Initialization.materialize;

import javax.net.ssl.TrustManagerFactory;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject( category = false, type = "FileTrustStore" )
public interface FileTrustStore<X extends FileTrustStore<X>> extends TrustStore<X>
{
    String TRUST_MANAGER_FACTORY_ALGORITHM = "trustManagerFactoryAlgorithm";
    String TRUST_STORE_TYPE = "trustStoreType";
    String PASSWORD = "password";
    String STORE_URL = "storeUrl";
    @ManagedContextDefault(name = "trustStoreFile.trustStoreType")
    String DEFAULT_TRUSTSTORE_TYPE = java.security.KeyStore.getDefaultType();

    @ManagedContextDefault(name = "trustStoreFile.trustManagerFactoryAlgorithm")
    String DEFAULT_TRUST_MANAGER_FACTORY_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();
    String PEERS_ONLY = "peersOnly";

    @Override
    @ManagedAttribute(defaultValue = "${this:storeUrl}")
    String getDescription();

    @ManagedAttribute( mandatory = true, oversize = true, oversizedAltText = OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT)
    String getStoreUrl();

    @DerivedAttribute
    String getPath();

    @ManagedAttribute( defaultValue = "${trustStoreFile.trustManagerFactoryAlgorithm}", initialization = materialize )
    String getTrustManagerFactoryAlgorithm();

    @ManagedAttribute( defaultValue = "${trustStoreFile.trustStoreType}", initialization = materialize)
    String getTrustStoreType();

    @ManagedAttribute( defaultValue = "false",
            description = "If true, the Truststore it will authenticate only those clients that present a certificate"
                          + " exactly matching a certificate contained within the Truststore.")
    boolean isPeersOnly();

    @ManagedAttribute( secure = true, mandatory = true )
    String getPassword();

    void setPassword(String password);

    @ManagedOperation( description = "Reloads trust store.", changesConfiguredObjectState = true)
    void reload();
}
