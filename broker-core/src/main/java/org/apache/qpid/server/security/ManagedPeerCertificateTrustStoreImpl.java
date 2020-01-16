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

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;

@ManagedObject( category = false )
public class ManagedPeerCertificateTrustStoreImpl
        extends AbstractTrustStore<ManagedPeerCertificateTrustStoreImpl> implements ManagedPeerCertificateTrustStore<ManagedPeerCertificateTrustStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedPeerCertificateTrustStoreImpl.class);

    private volatile TrustManager[] _trustManagers = new TrustManager[0];

    @ManagedAttributeField(afterSet = "initialize")
    private final List<Certificate> _storedCertificates = new ArrayList<>();

    @ManagedObjectFactoryConstructor
    public ManagedPeerCertificateTrustStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
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
        List<Certificate> storedCertificates = new ArrayList<>(_storedCertificates);
        return storedCertificates.toArray(new Certificate[storedCertificates.size()]);
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
    {
        initializeExpiryChecking();
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @SuppressWarnings("unused")
    protected void initialize()
    {
        try
        {
            java.security.KeyStore inMemoryKeyStore =
                    java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

            inMemoryKeyStore.load(null, null);
            int i = 1;
            for (Certificate cert : _storedCertificates)
            {
                inMemoryKeyStore.setCertificateEntry(String.valueOf(i++), cert);
            }

            final Collection<TrustManager> trustManagersCol = new ArrayList<>();
            final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
            final TrustManager[] delegateManagers = getTrustManagers(inMemoryKeyStore);
            for (final TrustManager tm : delegateManagers)
            {
                if (tm instanceof X509TrustManager)
                {
                    // truststore is supposed to trust only clients which peers certificates
                    // are directly in the store. CA signing will not be considered.
                    mulTrustManager.addTrustManager(new QpidPeersOnlyTrustManager(inMemoryKeyStore, (X509TrustManager) tm));

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

            if (trustManagersCol.isEmpty())
            {
                _trustManagers = null;
            }
            else
            {
                _trustManagers = trustManagersCol.toArray(new TrustManager[trustManagersCol.size()]);
            }
        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Cannot load certificate(s) :" + e, e);
        }
    }

    @Override
    public List<Certificate> getStoredCertificates()
    {
        return _storedCertificates;
    }

    @Override
    public void addCertificate(final Certificate cert)
    {
        final Set<Certificate> certificates = new LinkedHashSet<>(_storedCertificates);
        if (certificates.add(cert))
        {
            setAttributes(Collections.singletonMap("storedCertificates", certificates));
        }
    }

    @Override
    public void removeCertificates(final List<CertificateDetails> certs)
    {
        final Map<String, Set<BigInteger>> certsToRemove = new HashMap<>();
        for (CertificateDetails cert : certs)
        {
            if (!certsToRemove.containsKey(cert.getIssuerName()))
            {
                certsToRemove.put(cert.getIssuerName(), new HashSet<>());
            }
            certsToRemove.get(cert.getIssuerName()).add(new BigInteger(cert.getSerialNumber()));
        }

        boolean updated = false;
        Set<Certificate> currentCerts = new LinkedHashSet<>(_storedCertificates);
        Iterator<Certificate> iter = currentCerts.iterator();
        while (iter.hasNext())
        {
            Certificate cert = iter.next();
            if (cert instanceof X509Certificate)
            {
                X509Certificate x509Certificate = (X509Certificate) cert;
                String issuerName = x509Certificate.getIssuerX500Principal().getName();
                if(certsToRemove.containsKey(issuerName) && certsToRemove.get(issuerName).contains(x509Certificate.getSerialNumber()))
                {
                    iter.remove();
                    updated = true;
                }
            }
        }

        if (updated)
        {
            setAttributes(Collections.singletonMap("storedCertificates", currentCerts));
        }
    }

}
