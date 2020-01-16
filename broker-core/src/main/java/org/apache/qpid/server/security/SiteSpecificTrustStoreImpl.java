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
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.Strings;

@ManagedObject( category = false )
public class SiteSpecificTrustStoreImpl
        extends AbstractTrustStore<SiteSpecificTrustStoreImpl> implements SiteSpecificTrustStore<SiteSpecificTrustStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SiteSpecificTrustStoreImpl.class);

    @ManagedAttributeField
    private String _siteUrl;

    private volatile TrustManager[] _trustManagers = new TrustManager[0];

    private volatile int _connectTimeout;
    private volatile int _readTimeout;
    private volatile X509Certificate _x509Certificate;

    @ManagedObjectFactoryConstructor
    public SiteSpecificTrustStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    protected void initialize()
    {
        generateTrustManagers();
    }

    @Override
    public String getSiteUrl()
    {
        return _siteUrl;
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _connectTimeout = getContextValue(Integer.class, TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT);
        _readTimeout = getContextValue(Integer.class, TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT);
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
    protected void validateOnCreate()
    {
        super.validateOnCreate();

        try
        {
            URL url = new URL(_siteUrl);

            if (url.getHost() == null || (url.getPort() == -1 && url.getDefaultPort() == -1))
            {
                throw new IllegalConfigurationException(String.format("URL '%s' does not provide a hostname and port number", _siteUrl));
            }
        }
        catch (MalformedURLException e)
        {
            throw new IllegalConfigurationException(String.format("'%s' is not a valid URL", _siteUrl));
        }
    }

    @Override
    public String getCertificate()
    {
        if (_x509Certificate != null)
        {
            try
            {
                return Base64.getEncoder().encodeToString(_x509Certificate.getEncoded());
            }
            catch (CertificateEncodingException e)
            {
                throw new IllegalConfigurationException("Unable to encode certificate");
            }
        }
        return null;
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
        X509Certificate x509Certificate = _x509Certificate;
        return x509Certificate == null ? new Certificate[0] : new Certificate[]{x509Certificate};
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
    {
        initializeExpiryChecking();

        final SettableFuture<Void> result = SettableFuture.create();
        if(_x509Certificate == null)
        {
            final String currentCertificate = getCertificate();

            final ListenableFuture<X509Certificate> certFuture = downloadCertificate(getSiteUrl());
            addFutureCallback(certFuture, new FutureCallback<X509Certificate>()
            {
                @Override
                public void onSuccess(final X509Certificate cert)
                {
                    _x509Certificate = cert;
                    attributeSet(CERTIFICATE, currentCertificate, _x509Certificate);
                    generateTrustAndSetState(result);
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    _trustManagers = new TrustManager[0];
                    setState(State.ERRORED);
                    result.setException(t);

                }
            }, getTaskExecutor());
            return result;
        }
        else
        {
            generateTrustAndSetState(result);
        }
        return result;
    }

    private void generateTrustAndSetState(final SettableFuture<Void> result)
    {
        State state = State.ERRORED;
        try
        {
            generateTrustManagers();
            state = State.ACTIVE;
            result.set(null);
        }
        catch(IllegalConfigurationException e)
        {
            result.setException(e);
        }
        finally
        {
            setState(state);
            result.set(null);
        }
    }

    private ListenableFuture<X509Certificate> downloadCertificate(final String url)
    {
        final ListeningExecutorService workerService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                getThreadFactory("download-certificate-worker-" + getName())));
        try
        {
            return workerService.submit(new Callable<X509Certificate>()
            {

                @Override
                public X509Certificate call()
                {
                    try
                    {
                        final URL siteUrl = new URL(url);
                        final int port = siteUrl.getPort() == -1 ? siteUrl.getDefaultPort() : siteUrl.getPort();
                        SSLContext sslContext = SSLUtil.tryGetSSLContext();
                        sslContext.init(new KeyManager[0], new TrustManager[]{new AlwaysTrustManager()}, null);
                        try (SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket())
                        {
                            socket.setSoTimeout(_readTimeout);
                            socket.connect(new InetSocketAddress(siteUrl.getHost(), port), _connectTimeout);
                            socket.startHandshake();
                            final Certificate[] certificateChain = socket.getSession().getPeerCertificates();
                            if (certificateChain != null
                                && certificateChain.length != 0
                                && certificateChain[0] instanceof X509Certificate)
                            {
                                final X509Certificate x509Certificate = (X509Certificate) certificateChain[0];
                                LOGGER.debug("Successfully downloaded X509Certificate with DN {} certificate from {}",
                                             x509Certificate.getSubjectDN(), url);
                                return x509Certificate;
                            }
                            else
                            {
                                throw new IllegalConfigurationException(String.format("TLS handshake for '%s' from '%s' "
                                                                                      + "did not provide a X509Certificate",
                                                                                     getName(),
                                                                                     url));
                            }
                        }
                    }
                    catch (IOException | GeneralSecurityException e)
                    {
                        throw new IllegalConfigurationException(String.format("Unable to get certificate for '%s' from '%s'",
                                                                              getName(),
                                                                              url), e);
                    }
                }
            });
        }
        finally
        {
            workerService.shutdown();
        }
    }

    private void decodeCertificate()
    {
        byte[] certificateEncoded = Strings.decodeBase64((String) getActualAttributes().get(CERTIFICATE));


        try(ByteArrayInputStream input = new ByteArrayInputStream(certificateEncoded))
        {
            _x509Certificate = (X509Certificate) SSLUtil.getCertificateFactory().generateCertificate(input);
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

            _trustManagers = getTrustManagers(inMemoryKeyStore);;

        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Cannot load certificate(s) :" + e, e);
        }
    }

    @Override
    public void refreshCertificate()
    {
        logOperation("refreshCertificate");
        final String currentCertificate = getCertificate();
        final ListenableFuture<X509Certificate> certFuture = downloadCertificate(getSiteUrl());
        final ListenableFuture<Void> modelFuture = doAfter(certFuture, new CallableWithArgument<ListenableFuture<Void>, X509Certificate>()
        {
            @Override
            public ListenableFuture<Void> call(final X509Certificate cert) throws Exception
            {
                _x509Certificate = cert;
                attributeSet(CERTIFICATE, currentCertificate, _x509Certificate);
                return Futures.immediateFuture(null);
            }
        });
        doSync(modelFuture);
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

    private ThreadFactory getThreadFactory(final String name)
    {
        return runnable ->
        {

            final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            if (!thread.isDaemon())
            {
                thread.setDaemon(true);
            }
            thread.setName(name);
            return thread;
        };
    }
}
