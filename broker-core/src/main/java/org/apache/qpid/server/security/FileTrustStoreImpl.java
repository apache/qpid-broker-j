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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public class FileTrustStoreImpl extends AbstractTrustStore<FileTrustStoreImpl> implements FileTrustStore<FileTrustStoreImpl>
{

    @ManagedAttributeField
    private volatile String _trustStoreType;
    @ManagedAttributeField
    private volatile String _trustManagerFactoryAlgorithm;
    @ManagedAttributeField(afterSet = "postSetStoreUrl")
    private volatile String _storeUrl;
    private volatile String _path;
    @ManagedAttributeField
    private volatile boolean _peersOnly;
    @ManagedAttributeField
    private volatile String _password;

    private volatile TrustManager[] _trustManagers;
    private volatile Certificate[] _certificates;

    static
    {
        Handler.register();
    }

    @ManagedObjectFactoryConstructor
    public FileTrustStoreImpl(Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateTrustStore(this);
        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
    {
        initializeExpiryChecking();
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        FileTrustStore updated = (FileTrustStore) proxyForValidation;
        if (changedAttributes.contains(TrustStore.DESIRED_STATE) && updated.getDesiredState() == State.DELETED)
        {
            return;
        }
        validateTrustStore(updated);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        initialize();
    }

    @Override
    protected void changeAttributes(final Map<String, Object> attributes)
    {
        super.changeAttributes(attributes);
        if (attributes.containsKey(STORE_URL)
            || attributes.containsKey(PASSWORD)
            || attributes.containsKey(TRUST_STORE_TYPE)
            || attributes.containsKey(TRUST_MANAGER_FACTORY_ALGORITHM)
            || attributes.containsKey(PEERS_ONLY))
        {
            initialize();
        }
    }

    private static KeyStore initializeKeyStore(final FileTrustStore trustStore)
            throws GeneralSecurityException, IOException
    {
        URL trustStoreUrl = getUrlFromString(trustStore.getStoreUrl());
        return SSLUtil.getInitializedKeyStore(trustStoreUrl, trustStore.getPassword(), trustStore.getTrustStoreType());
    }

    private static void validateTrustStore(FileTrustStore trustStore)
    {
        final String loggableStoreUrl = StringUtil.elideDataUrl(trustStore.getStoreUrl());
        try
        {
            KeyStore keyStore = initializeKeyStore(trustStore);

            final Enumeration<String> aliasesEnum = keyStore.aliases();
            boolean certificateFound = false;
            while (aliasesEnum.hasMoreElements())
            {
                String alias = aliasesEnum.nextElement();
                if (keyStore.isCertificateEntry(alias))
                {
                    certificateFound = true;
                    break;
                }
            }
            if (!certificateFound)
            {
                throw new IllegalConfigurationException(String.format(
                        "Trust store '%s' must contain at least one certificate.", loggableStoreUrl));
            }
        }
        catch (UnrecoverableKeyException e)
        {
            String message = String.format("Check trust store password. Cannot instantiate trust store from '%s'.", loggableStoreUrl);
            throw new IllegalConfigurationException(message, e);
        }
        catch (IOException | GeneralSecurityException e)
        {
            final String message = String.format("Cannot instantiate trust store from '%s'.", loggableStoreUrl);
            throw new IllegalConfigurationException(message, e);
        }

        try
        {
            TrustManagerFactory.getInstance(trustStore.getTrustManagerFactoryAlgorithm());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalConfigurationException(String.format("Unknown trustManagerFactoryAlgorithm '%s'",
                                                                  trustStore.getTrustManagerFactoryAlgorithm()));
        }
    }

    @Override
    public String getStoreUrl()
    {
        return _storeUrl;
    }

    @Override
    public String getPath()
    {
        return _path;
    }

    @Override
    public String getTrustManagerFactoryAlgorithm()
    {
        return _trustManagerFactoryAlgorithm;
    }

    @Override
    public String getTrustStoreType()
    {
        return _trustStoreType;
    }

    @Override
    public boolean isPeersOnly()
    {
        return _peersOnly;
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    @Override
    public void setPassword(String password)
    {
        _password = password;
    }

    @Override
    public void reload()
    {
        initialize();
    }

    @Override
    protected TrustManager[] getTrustManagersInternal()
    {
        TrustManager[] trustManagers = _trustManagers;
        if (trustManagers == null || trustManagers.length == 0)
        {
            throw new IllegalStateException("Truststore " + this + " defines no trust managers");
        }
        return Arrays.copyOf(trustManagers, trustManagers.length);
    }

    @Override
    public Certificate[] getCertificates()
    {
        Certificate[] certificates = _certificates;
        return certificates == null ? new Certificate[0] : Arrays.copyOf(certificates, certificates.length);
    }

    @SuppressWarnings(value = "unused")
    private void postSetStoreUrl()
    {
        if (_storeUrl != null && !_storeUrl.startsWith("data:"))
        {
            _path = _storeUrl;
        }
        else
        {
            _path = null;
        }
    }

    protected void initialize()
    {
        try
        {
            KeyStore ts = initializeKeyStore(this);
            TrustManager[] trustManagers = createTrustManagers(ts);
            Certificate[] certificates = createCertificates(ts);
            _trustManagers = trustManagers;
            _certificates = certificates;
        }
        catch (Exception e)
        {
            throw new IllegalConfigurationException(String.format("Cannot instantiate trust store '%s'", getName()), e);
        }
    }

    private TrustManager[] createTrustManagers(final KeyStore ts) throws KeyStoreException
    {
        final TrustManager[] delegateManagers = getTrustManagers(ts);
        if (delegateManagers.length == 0)
        {
            throw new IllegalStateException("Truststore " + this + " defines no trust managers");
        }
        else if (delegateManagers.length == 1)
        {
            if (_peersOnly  && delegateManagers[0] instanceof X509TrustManager)
            {
                return new TrustManager[] {new QpidPeersOnlyTrustManager(ts, ((X509TrustManager) delegateManagers[0]))};
            }
            else
            {
                return delegateManagers;
            }
        }
        else
        {
            final Collection<TrustManager> trustManagersCol = new ArrayList<>();
            final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
            for (TrustManager tm : delegateManagers)
            {
                if (tm instanceof X509TrustManager)
                {
                    if (_peersOnly)
                    {
                        mulTrustManager.addTrustManager(new QpidPeersOnlyTrustManager(ts, (X509TrustManager) tm));
                    }
                    else
                    {
                        mulTrustManager.addTrustManager((X509TrustManager) tm);
                    }
                }
                else
                {
                    trustManagersCol.add(tm);
                }
            }
            if (! mulTrustManager.isEmpty())
            {
                trustManagersCol.add(mulTrustManager);
            }
            return trustManagersCol.toArray(new TrustManager[trustManagersCol.size()]);
        }
    }

    private Certificate[] createCertificates(final KeyStore ts) throws KeyStoreException
    {
        final Collection<Certificate> certificates = SSLUtil.getCertificates(ts);

        return certificates.toArray(new Certificate[certificates.size()]);
    }
}
