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
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.TrustManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

@ManagedObject( category = false )
public class NonJavaTrustStoreImpl
        extends AbstractTrustStore<NonJavaTrustStoreImpl> implements NonJavaTrustStore<NonJavaTrustStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonJavaTrustStoreImpl.class);

    static
    {
        Handler.register();
    }

    @ManagedAttributeField( afterSet = "initialize" )
    private String _certificatesUrl;

    private volatile TrustManager[] _trustManagers = new TrustManager[0];

    private X509Certificate[] _certificates;

    @ManagedObjectFactoryConstructor
    public NonJavaTrustStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    public String getCertificatesUrl()
    {
        return _certificatesUrl;
    }

    @Override
    protected TrustManager[] getTrustManagersInternal() throws GeneralSecurityException
    {
        TrustManager[] trustManagers = _trustManagers;
        if (trustManagers == null || trustManagers.length == 0)
        {
            throw new IllegalStateException("Truststore " + this + " defines no trust managers");
        }
        return Arrays.copyOf(trustManagers, trustManagers.length);
    }

    @Override
    public Certificate[] getCertificates() throws GeneralSecurityException
    {
        X509Certificate[] certificates = _certificates;
        return certificates == null ? new X509Certificate[0] : certificates;
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateTrustStoreAttributes(this);
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
        NonJavaTrustStore changedStore = (NonJavaTrustStore) proxyForValidation;
        validateTrustStoreAttributes(changedStore);
    }

    private void validateTrustStoreAttributes(NonJavaTrustStore<?> keyStore)
    {
        try
        {
            SSLUtil.readCertificates(getUrlFromString(keyStore.getCertificatesUrl()));
        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalArgumentException("Cannot validate certificate(s):" + e, e);
        }
    }

    @SuppressWarnings("unused")
    protected void initialize()
    {
        try
        {
            if (_certificatesUrl != null)
            {
                X509Certificate[] certs = SSLUtil.readCertificates(getUrlFromString(_certificatesUrl));
                java.security.KeyStore inMemoryKeyStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

                inMemoryKeyStore.load(null, null);
                int i = 1;
                for(Certificate cert : certs)
                {
                    inMemoryKeyStore.setCertificateEntry(String.valueOf(i++), cert);
                }

                _trustManagers = getTrustManagers(inMemoryKeyStore);
                _certificates = certs;
            }

        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Cannot load certificate(s) :" + e, e);
        }
    }
}
