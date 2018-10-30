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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidBestFitX509KeyManager extends X509ExtendedKeyManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidBestFitX509KeyManager.class);
    private static final long SIX_HOURS = 6L * 60L * 60L * 1000L;

    private final X509ExtendedKeyManager _delegate;
    private final String _defaultAlias;
    private final List<String> _aliases;

    public QpidBestFitX509KeyManager(String defaultAlias,
                                     URL keyStoreUrl, String keyStoreType,
                                     String keyStorePassword, String keyManagerFactoryAlgorithmName) throws GeneralSecurityException, IOException
    {
        KeyStore ks = SSLUtil.getInitializedKeyStore(keyStoreUrl,keyStorePassword,keyStoreType);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithmName);
        kmf.init(ks, keyStorePassword.toCharArray());
        List<String> aliases = new ArrayList<>();
        for(String alias : Collections.list(ks.aliases()))
        {
            if(ks.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
            {
                aliases.add(alias);
            }
        }
        _aliases = Collections.unmodifiableList(aliases);
        _delegate = (X509ExtendedKeyManager)kmf.getKeyManagers()[0];
        _defaultAlias = defaultAlias;
    }

    

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket)
    {
        return  _defaultAlias == null ? _delegate.chooseClientAlias(keyType, issuers, socket) : _defaultAlias;
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket)
    {
        return _delegate.chooseServerAlias(keyType, issuers, socket);
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
        return _delegate.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine)
    {
        return _defaultAlias == null ? _delegate.chooseEngineClientAlias(keyType, issuers, engine) : _defaultAlias;
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine)
    {
        Date currentDate = new Date();
        final List<SNIServerName> serverNames = engine.getSSLParameters().getServerNames();
        if(serverNames == null || serverNames.isEmpty())
        {
            return getDefaultServerAlias(keyType, issuers, engine);
        }
        else
        {
            List<String> validAliases = new ArrayList<>();
            List<String> invalidAliases = new ArrayList<>();

            for(SNIServerName serverName : engine.getSSLParameters().getServerNames())
            {
                if(serverName instanceof SNIHostName)
                {
                    for(String alias : _aliases)
                    {
                        if(keyType.equalsIgnoreCase(getPrivateKey(alias).getAlgorithm()))
                        {
                            final X509Certificate[] certChain = getCertificateChain(alias);
                            X509Certificate cert = certChain[0];
                            if (SSLUtil.checkHostname(((SNIHostName) serverName).getAsciiName(), cert))
                            {
                                if (currentDate.after(cert.getNotBefore()) && currentDate.before(cert.getNotAfter()))
                                {
                                    validAliases.add(alias);
                                }
                                else
                                {
                                    invalidAliases.add(alias);
                                }
                            }
                        }
                    }
                }
            }

            if(validAliases.isEmpty())
            {
                if(invalidAliases.isEmpty())
                {
                    return getDefaultServerAlias(keyType, issuers, engine);
                }
                else
                {
                    // all invalid, we'll just pick one
                    return invalidAliases.get(0);
                }
            }
            else
            {
                if(validAliases.size() > 1)
                {
                    // return the first alias which has at least six hours validity before / after the current time
                    for(String alias : validAliases)
                    {
                        final X509Certificate cert = getCertificateChain(alias)[0];
                        if((currentDate.getTime() - cert.getNotBefore().getTime() > SIX_HOURS)
                           && (cert.getNotAfter().getTime() - currentDate.getTime() > SIX_HOURS))
                        {
                            return alias;
                        }
                    }

                }
                return validAliases.get(0);
            }
        }
    }

    private String getDefaultServerAlias(final String keyType, final Principal[] issuers, final SSLEngine engine)
    {
        if(_defaultAlias != null)
        {
            return _defaultAlias;
        }
        else
        {
            return _delegate.chooseEngineServerAlias(keyType, issuers, engine);
        }
    }
}
