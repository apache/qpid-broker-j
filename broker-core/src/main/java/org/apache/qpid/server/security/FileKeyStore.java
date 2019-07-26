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

import javax.net.ssl.KeyManagerFactory;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;

@ManagedObject( category = false, type = "FileKeyStore" )
public interface FileKeyStore<X extends FileKeyStore<X>> extends KeyStore<X>
{
    String KEY_MANAGER_FACTORY_ALGORITHM = "keyManagerFactoryAlgorithm";
    String CERTIFICATE_ALIAS = "certificateAlias";
    String KEY_STORE_TYPE = "keyStoreType";
    String PASSWORD = "password";
    String STORE_URL = "storeUrl";
    String USE_HOST_NAME_MATCHING = "useHostNameMatching";

    @ManagedContextDefault(name = "keyStoreFile.keyStoreType")
    String DEFAULT_KEYSTORE_TYPE = java.security.KeyStore.getDefaultType();

    @ManagedContextDefault(name = "keyStoreFile.keyManagerFactoryAlgorithm")
    String DEFAULT_KEY_MANAGER_FACTORY_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();

    @Override
    @ManagedAttribute(defaultValue = "${this:path}")
    String getDescription();

    @ManagedAttribute(  mandatory = true, secure = true, oversize = true, oversizedAltText = OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT, secureValueFilter = "^data\\:.*")
    String getStoreUrl();

    @DerivedAttribute
    String getPath();

    @ManagedAttribute
    String getCertificateAlias();

    @ManagedAttribute( defaultValue = "${keyStoreFile.keyManagerFactoryAlgorithm}" , initialization = materialize)
    String getKeyManagerFactoryAlgorithm();

    @ManagedAttribute( defaultValue = "${keyStoreFile.keyStoreType}", initialization = materialize)
    String getKeyStoreType();

    @ManagedAttribute( secure = true, mandatory = true )
    String getPassword();

    @ManagedAttribute( defaultValue = "true", description = "Use SNI server name from the SSL handshake to select the most appropriate certificate for the indicated hostname")
    boolean isUseHostNameMatching();

    @ManagedOperation( description = "Reloads keystore.", changesConfiguredObjectState = true)
    void reload();

}
