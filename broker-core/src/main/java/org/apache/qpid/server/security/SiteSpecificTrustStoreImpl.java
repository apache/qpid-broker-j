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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.DatatypeConverter;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.TrustStoreMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.transport.util.Functions;
import org.apache.qpid.util.Strings;

@ManagedObject( category = false )
public class SiteSpecificTrustStoreImpl
        extends AbstractConfiguredObject<SiteSpecificTrustStoreImpl> implements SiteSpecificTrustStore<SiteSpecificTrustStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SiteSpecificTrustStoreImpl.class);

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private String _siteUrl;
    @ManagedAttributeField
    private boolean _exposedAsMessageSource;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _includedVirtualHostNodeMessageSources;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _excludedVirtualHostNodeMessageSources;

    private volatile TrustManager[] _trustManagers = new TrustManager[0];


    private volatile X509Certificate _x509Certificate;

    @ManagedObjectFactoryConstructor
    public SiteSpecificTrustStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);
        _broker = broker;
        _eventLogger = _broker.getEventLogger();
        _eventLogger.message(TrustStoreMessages.CREATE(getName()));
    }

    @Override
    public String getSiteUrl()
    {
        return _siteUrl;
    }

    @Override
    protected void postResolve()
    {
        super.postResolve();
        if(getActualAttributes().containsKey(CERTIFICATE))
        {
            decodeCertificate();
        }
    }

    @Override
    public String getCertificate()
    {
        if (_x509Certificate != null)
        {
            try
            {
                return DatatypeConverter.printBase64Binary(_x509Certificate.getEncoded());
            }
            catch (CertificateEncodingException e)
            {
                throw new IllegalConfigurationException("Unable to encode certificate");
            }
        }
        return null;
    }

    @Override
    public TrustManager[] getTrustManagers() throws GeneralSecurityException
    {
        if (_trustManagers == null || _trustManagers.length == 0)
        {
            throw new IllegalStateException("Truststore " + this + " defines no trust managers");
        }
        return _trustManagers;
    }

    @Override
    public Certificate[] getCertificates() throws GeneralSecurityException
    {
        return new Certificate[]{_x509Certificate};
    }

    @StateTransition(currentState = {State.ACTIVE, State.ERRORED}, desiredState = State.DELETED)
    protected ListenableFuture<Void> doDelete()
    {
        // verify that it is not in use
        String storeName = getName();

        Collection<Port<?>> ports = new ArrayList<Port<?>>(_broker.getPorts());
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
        if(_x509Certificate == null)
        {
            downloadCertificate();
        }
        if(_x509Certificate != null)
        {
            generateTrustManagers();

            setState(State.ACTIVE);
        }
        else
        {
            setState(State.ERRORED);
        }
        return Futures.immediateFuture(null);
    }

    private void downloadCertificate()
    {
        try
        {

            URL url = new URL(getSiteUrl());
            SSLContext sslContext = SSLUtil.tryGetSSLContext();
            sslContext.init(new KeyManager[0], new TrustManager[] {new AlwaysTrustManager()}, null);

            final int port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
            try(SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket(url.getHost(), port))
            {
                socket.startHandshake();
                final Certificate[] certificateChain = socket.getSession().getPeerCertificates();
                if (certificateChain != null && certificateChain.length != 0 && certificateChain[0] instanceof X509Certificate)
                {
                    runTask(new Task<Void, RuntimeException>()
                    {
                        @Override
                        public Void execute() throws RuntimeException
                        {
                            _x509Certificate = (X509Certificate) certificateChain[0];

                            final String certificate = getCertificate();
                            attributeSet(CERTIFICATE, certificate, certificate);
                            return null;
                        }

                        @Override
                        public String getObject()
                        {
                            return SiteSpecificTrustStoreImpl.this.getName();
                        }

                        @Override
                        public String getAction()
                        {
                            return "downloadCertificate";
                        }

                        @Override
                        public String getArguments()
                        {
                            return null;
                        }
                    });
                }
                else
                {
                    LOGGER.info("No valid certificates available from " + getSiteUrl());
                }
            }

        }
        catch (GeneralSecurityException | IOException e)
        {
            LOGGER.info("Unable to download certificate from " + getSiteUrl(), e);
        }
    }


    private void decodeCertificate()
    {
        byte[] certificateEncoded = Strings.decodeBase64((String) getActualAttributes().get(CERTIFICATE));


        try(ByteArrayInputStream input = new ByteArrayInputStream(certificateEncoded))
        {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            _x509Certificate = (X509Certificate) cf.generateCertificate(input);
        }
        catch (CertificateException | IOException e)
        {
            throw new IllegalConfigurationException("Could not decode certificate", e);
        }

    }

    private void generateTrustManagers()
    {
        try
        {
            java.security.KeyStore inMemoryKeyStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

            inMemoryKeyStore.load(null, null);
            inMemoryKeyStore.setCertificateEntry("1", _x509Certificate);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(inMemoryKeyStore);
            _trustManagers = tmf.getTrustManagers();

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
    public String getCertificateIssuer()
    {
        return _x509Certificate == null ? null : _x509Certificate.getIssuerX500Principal().toString();
    }


    @Override
    public String getCertificateSubject()
    {
        return _x509Certificate == null ? null : _x509Certificate.getSubjectX500Principal().toString();
    }

    @Override
    public String getCertificateSerialNumber()
    {
        return _x509Certificate == null ? null : _x509Certificate.getSerialNumber().toString();
    }

    @Override
    public String getCertificateSignature()
    {
        return _x509Certificate == null ? null : Functions.hex(_x509Certificate.getSignature(),4096, " ");
    }

    @Override
    public Date getCertificateValidFromDate()
    {
        return _x509Certificate == null ? null : _x509Certificate.getNotBefore();
    }


    @Override
    public Date getCertificateValidUntilDate()
    {
        return _x509Certificate == null ? null :_x509Certificate.getNotAfter();
    }

    @Override
    public void refreshCertificate()
    {
        downloadCertificate();
    }

    private static class AlwaysTrustManager implements X509TrustManager
    {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType)
                throws CertificateException
        {

        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType)
                throws CertificateException
        {

        }

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }


    @Override
    protected void logOperation(final String operation)
    {
        _broker.getEventLogger().message(TrustStoreMessages.OPERATION(operation));
    }
}
