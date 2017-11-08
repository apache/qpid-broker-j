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
package org.apache.qpid.server.transport.network.security.ssl;

import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidServerX509KeyManager extends X509ExtendedKeyManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidServerX509KeyManager.class);

    private X509ExtendedKeyManager _delegate;
    private String _alias;

    public QpidServerX509KeyManager(String alias, URL keyStoreUrl, String keyStoreType,
                                    String keyStorePassword, String keyManagerFactoryAlgorithmName) throws GeneralSecurityException, IOException
    {
        this._alias = alias;
        KeyStore ks = SSLUtil.getInitializedKeyStore(keyStoreUrl,keyStorePassword,keyStoreType);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithmName);
        kmf.init(ks, keyStorePassword.toCharArray());
        this._delegate = (X509ExtendedKeyManager)kmf.getKeyManagers()[0];
    }


    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket)
    {
        return _alias == null ? _delegate.chooseClientAlias(keyType, issuers, socket) : _alias;
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket)
    {
        LOGGER.debug("chooseServerAlias:Returning alias {}", _alias);
        return _alias;
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias)
    {
        return _delegate.getCertificateChain(alias);
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers)
    {
        return _delegate.getClientAliases(keyType, issuers);
    }

    @Override
    public PrivateKey getPrivateKey(String alias)
    {
        return _delegate.getPrivateKey(alias);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers)
    {
        LOGGER.debug("getServerAliases:Returning alias {}", _alias);
        return new String[]{_alias};
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine)
    {
        return _alias == null ? _delegate.chooseEngineClientAlias(keyType, issuers, engine) : _alias;
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine)
    {
        LOGGER.debug("chooseEngineServerAlias:Returning alias {}", _alias);
        return _alias;
    }
}
