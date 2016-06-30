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
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.TrustStoreMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedAttributeValue;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.transport.network.security.ssl.QpidPeersOnlyTrustManager;

@ManagedObject( category = false )
public class ManagedPeerCertificateTrustStoreImpl
        extends AbstractConfiguredObject<ManagedPeerCertificateTrustStoreImpl> implements ManagedPeerCertificateTrustStore<ManagedPeerCertificateTrustStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedPeerCertificateTrustStoreImpl.class);

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private boolean _exposedAsMessageSource;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _includedVirtualHostNodeMessageSources;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _excludedVirtualHostNodeMessageSources;

    private volatile TrustManager[] _trustManagers = new TrustManager[0];

    @ManagedAttributeField( afterSet = "updateTrustManagers")
    private final List<Certificate> _storedCertificates = new ArrayList<>();

    @ManagedObjectFactoryConstructor
    public ManagedPeerCertificateTrustStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);
        _broker = broker;
        _eventLogger = _broker.getEventLogger();
        _eventLogger.message(TrustStoreMessages.CREATE(getName()));
    }

    @Override
    public TrustManager[] getTrustManagers()
    {
        if (_trustManagers == null || _trustManagers.length == 0)
        {
            throw new IllegalStateException("Truststore " + this + " defines no trust managers");
        }
        return _trustManagers;
    }

    @Override
    public Certificate[] getCertificates()
    {
        return _storedCertificates.toArray(new Certificate[_storedCertificates.size()]);
    }

    @StateTransition(currentState = {State.ACTIVE, State.ERRORED}, desiredState = State.DELETED)
    protected ListenableFuture<Void> doDelete()
    {
        // verify that it is not in use
        String storeName = getName();

        Collection<Port<?>> ports = new ArrayList<>(_broker.getPorts());
        for (Port port : ports)
        {
            Collection<TrustStore> trustStores = port.getTrustStores();
            if(trustStores != null)
            {
                for (TrustStore store : trustStores)
                {
                    if(storeName.equals(store.getAttribute(TrustStore.NAME)))
                    {
                        throw new IntegrityViolationException("Trust store '"
                                + storeName
                                + "' can't be deleted as it is in use by a port: "
                                + port.getName());
                    }
                }
            }
        }

        Collection<AuthenticationProvider> authenticationProviders = new ArrayList<AuthenticationProvider>(_broker.getAuthenticationProviders());
        for (AuthenticationProvider authProvider : authenticationProviders)
        {
            if(authProvider.getAttributeNames().contains(SimpleLDAPAuthenticationManager.TRUST_STORE))
            {
                Object attributeType = authProvider.getAttribute(AuthenticationProvider.TYPE);
                Object attributeValue = authProvider.getAttribute(SimpleLDAPAuthenticationManager.TRUST_STORE);
                if (SimpleLDAPAuthenticationManager.PROVIDER_TYPE.equals(attributeType)
                        && storeName.equals(attributeValue))
                {
                    throw new IntegrityViolationException("Trust store '"
                            + storeName
                            + "' can't be deleted as it is in use by an authentication manager: "
                            + authProvider.getName());
                }
            }
        }
        deleted();
        setState(State.DELETED);
        _eventLogger.message(TrustStoreMessages.DELETE(getName()));
        return Futures.immediateFuture(null);
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }


    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        ManagedPeerCertificateTrustStore<?> changedStore = (ManagedPeerCertificateTrustStore) proxyForValidation;
        if (changedAttributes.contains(NAME) && !getName().equals(changedStore.getName()))
        {
            throw new IllegalConfigurationException("Changing the key store name is not allowed");
        }
    }


    @SuppressWarnings("unused")
    private void updateTrustManagers()
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


            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(inMemoryKeyStore);

            final Collection<TrustManager> trustManagersCol = new ArrayList<TrustManager>();
            final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
            TrustManager[] delegateManagers = tmf.getTrustManagers();
            for (TrustManager tm : delegateManagers)
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
    public boolean isExposedAsMessageSource()
    {
        return _exposedAsMessageSource;
    }

    @Override
    public List<VirtualHostNode<?>> getIncludedVirtualHostNodeMessageSources()
    {
        return _includedVirtualHostNodeMessageSources;
    }

    @Override
    public List<VirtualHostNode<?>> getExcludedVirtualHostNodeMessageSources()
    {
        return _excludedVirtualHostNodeMessageSources;
    }

    @Override
    public List<Certificate> getStoredCertificates()
    {
        return _storedCertificates;
    }

    @Override
    public void addCertificate(final Certificate cert)
    {
        final Map<String, Object> updateMap = new HashMap<>();

        doAfter(doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
                                    {
                                        @Override
                                        public ListenableFuture<Void> execute()
                                        {
                                            Set<Certificate> certs = new HashSet<>(_storedCertificates);
                                            if(certs.add(cert))
                                            {
                                                updateMap.put("storedCertificates", new ArrayList<>(certs));
                                            }
                                            return Futures.immediateFuture(null);
                                        }

                                        @Override
                                        public String getObject()
                                        {
                                            return ManagedPeerCertificateTrustStoreImpl.this.toString();
                                        }

                                        @Override
                                        public String getAction()
                                        {
                                            return "add certificate";
                                        }

                                        @Override
                                        public String getArguments()
                                        {
                                            return String.valueOf(cert);
                                        }
                                    }),
                 new Callable<ListenableFuture<Void>>()
                    {
                        @Override
                        public ListenableFuture<Void> call() throws Exception
                        {
                            if(updateMap.isEmpty())
                            {
                                return Futures.immediateFuture(null);
                            }
                            else
                            {
                                return setAttributesAsync(updateMap);
                            }
                        }

                    });

    }

    @Override
    public List<CertificateDetails> getCertificateDetails()
    {
        List<CertificateDetails> details = new ArrayList<>();
        for(Certificate cert : _storedCertificates)
        {
            if(cert instanceof X509Certificate)
            {
                details.add(new CertificateDetailsImpl((X509Certificate)cert));
            }
        }
        return details;
    }


    @Override
    public void removeCertificates(final List<CertificateDetails> certs)
    {
        final Map<String,Set<BigInteger>> certsToRemove = new HashMap<>();
        for(CertificateDetails cert : certs)
        {
            if(!certsToRemove.containsKey(cert.getIssuerName()))
            {
                certsToRemove.put(cert.getIssuerName(), new HashSet<BigInteger>());
            }
            certsToRemove.get(cert.getIssuerName()).add(new BigInteger(cert.getSerialNumber()));
        }

        final Map<String, Object> updateMap = new HashMap<>();

        doAfter(doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
                {
                    @Override
                    public ListenableFuture<Void> execute()
                    {

                        Set<Certificate> certs = new HashSet<>(_storedCertificates);

                        boolean updated = false;
                        Iterator<Certificate> iter = certs.iterator();
                        while(iter.hasNext())
                        {
                            Certificate cert = iter.next();
                            if(cert instanceof X509Certificate)
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


                        if(updated)
                        {
                            updateMap.put("storedCertificates", new ArrayList<>(certs));
                        }
                        return Futures.immediateFuture(null);
                    }

                    @Override
                    public String getObject()
                    {
                        return ManagedPeerCertificateTrustStoreImpl.this.toString();
                    }

                    @Override
                    public String getAction()
                    {
                        return "remove certificates";
                    }

                    @Override
                    public String getArguments()
                    {
                        return String.valueOf(certs);
                    }
                }),
                new Callable<ListenableFuture<Void>>()
                {
                    @Override
                    public ListenableFuture<Void> call() throws Exception
                    {
                        if(updateMap.isEmpty())
                        {
                            return Futures.immediateFuture(null);
                        }
                        else
                        {
                            return setAttributesAsync(updateMap);
                        }
                    }

                });
    }

    public static class CertificateDetailsImpl implements CertificateDetails, ManagedAttributeValue
    {
        private final X509Certificate _x509cert;

        public CertificateDetailsImpl(final X509Certificate x509cert)
        {
            _x509cert = x509cert;
        }

        @Override
        public String getSerialNumber()
        {
            return _x509cert.getSerialNumber().toString();
        }

        @Override
        public int getVersion()
        {
            return _x509cert.getVersion();
        }

        @Override
        public String getSignatureAlgorithm()
        {
            return _x509cert.getSigAlgName();
        }

        @Override
        public String getIssuerName()
        {
            return _x509cert.getIssuerX500Principal().getName();
        }

        @Override
        public String getSubjectName()
        {
            return _x509cert.getSubjectX500Principal().getName();
        }

        @Override
        public List<String> getSubjectAltNames()
        {
            try
            {
                List<String> altNames = new ArrayList<String>();
                final Collection<List<?>> altNameObjects = _x509cert.getSubjectAlternativeNames();
                if(altNameObjects != null)
                {
                    for (List<?> entry : altNameObjects)
                    {
                        final int type = (Integer) entry.get(0);
                        if (type == 1 || type == 2)
                        {
                            altNames.add(entry.get(1).toString().trim());
                        }

                    }
                }
                return altNames;
            }
            catch (CertificateParsingException e)
            {

                return Collections.emptyList();
            }
        }

        @Override
        public Date getValidFrom()
        {
            return _x509cert.getNotBefore();
        }

        @Override
        public Date getValidUntil()
        {
            return _x509cert.getNotAfter();
        }
    }
}
