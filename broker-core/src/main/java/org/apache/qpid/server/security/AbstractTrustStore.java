/*
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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertPathParameters;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.TrustStoreMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostNode;

public abstract class AbstractTrustStore<X extends AbstractTrustStore<X>>
        extends AbstractConfiguredObject<X> implements TrustStore<X>
{
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractTrustStore.class);

    protected static final long ONE_DAY = 24L * 60L * 60L * 1000L;

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private boolean _exposedAsMessageSource;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _includedVirtualHostNodeMessageSources;
    @ManagedAttributeField
    private List<VirtualHostNode<?>> _excludedVirtualHostNodeMessageSources;
    @ManagedAttributeField
    private boolean _trustAnchorValidityEnforced;
    @ManagedAttributeField
    private boolean _certificateRevocationCheckEnabled;
    @ManagedAttributeField
    private boolean _certificateRevocationCheckOfOnlyEndEntityCertificates;
    @ManagedAttributeField
    private boolean _certificateRevocationCheckWithPreferringCertificateRevocationList;
    @ManagedAttributeField
    private boolean _certificateRevocationCheckWithNoFallback;
    @ManagedAttributeField
    private boolean _certificateRevocationCheckWithIgnoringSoftFailures;
    @ManagedAttributeField(afterSet = "postSetCertificateRevocationListUrl")
    private volatile String _certificateRevocationListUrl;
    private volatile String _certificateRevocationListPath;

    private ScheduledFuture<?> _checkExpiryTaskFuture;

    AbstractTrustStore(Map<String, Object> attributes, Broker<?> broker)
    {
        super(broker, attributes);

        _broker = broker;
        _eventLogger = broker.getEventLogger();
        _eventLogger.message(TrustStoreMessages.CREATE(getName()));
    }

    public final Broker<?> getBroker()
    {
        return _broker;
    }

    final EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    protected abstract void initialize();

    @Override
    protected void changeAttributes(final Map<String, Object> attributes)
    {
        super.changeAttributes(attributes);
        if (attributes.containsKey(CERTIFICATE_REVOCATION_LIST_URL))
        {
            initialize();
        }
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        getCRLs();
    }

    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if (changedAttributes.contains(CERTIFICATE_REVOCATION_LIST_URL))
        {
            getCRLs((String) proxyForValidation.getAttribute(CERTIFICATE_REVOCATION_LIST_URL));
        }
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        onCloseOrDelete();
        return Futures.immediateFuture(null);
    }

    private void onCloseOrDelete()
    {
        if(_checkExpiryTaskFuture != null)
        {
            _checkExpiryTaskFuture.cancel(false);
            _checkExpiryTaskFuture = null;
        }
    }

    @Override
    protected void logOperation(final String operation)
    {
        _broker.getEventLogger().message(TrustStoreMessages.OPERATION(operation));
    }

    void initializeExpiryChecking()
    {
        int checkFrequency = getCertificateExpiryCheckFrequency();
        if(getBroker().getState() == State.ACTIVE)
        {
            _checkExpiryTaskFuture = getBroker().scheduleHouseKeepingTask(checkFrequency, TimeUnit.DAYS,
                                                                          this::checkCertificateExpiry);
        }
        else
        {
            final int frequency = checkFrequency;
            getBroker().addChangeListener(new AbstractConfigurationChangeListener()
            {
                @Override
                public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
                {
                    if (newState == State.ACTIVE)
                    {
                        _checkExpiryTaskFuture =
                                getBroker().scheduleHouseKeepingTask(frequency, TimeUnit.DAYS,
                                                                     () -> checkCertificateExpiry());
                        getBroker().removeChangeListener(this);
                    }
                }
            });
        }
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        onCloseOrDelete();
        _eventLogger.message(TrustStoreMessages.DELETE(getName()));
        return super.onDelete();
    }

    private void checkCertificateExpiry()
    {
        int expiryWarning = getCertificateExpiryWarnPeriod();
        if(expiryWarning > 0)
        {
            long currentTime = System.currentTimeMillis();
            Date expiryTestDate = new Date(currentTime + (ONE_DAY * (long) expiryWarning));

            try
            {
                Certificate[] certificatesInternal = getCertificates();
                if (certificatesInternal.length > 0)
                {
                    Arrays.stream(certificatesInternal)
                          .filter(cert -> cert instanceof X509Certificate)
                          .forEach(x509cert -> checkCertificateExpiry(currentTime, expiryTestDate, (X509Certificate) x509cert));
                }
            }
            catch (GeneralSecurityException e)
            {
                LOGGER.debug("Unexpected exception whilst checking certificate expiry", e);
            }
        }
    }

    private void checkCertificateExpiry(final long currentTime,
                                          final Date expiryTestDate,
                                          final X509Certificate cert)
    {
        try
        {
            cert.checkValidity(expiryTestDate);
        }
        catch(CertificateExpiredException e)
        {
            long timeToExpiry = cert.getNotAfter().getTime() - currentTime;
            int days = Math.max(0,(int)(timeToExpiry / (ONE_DAY)));

            getEventLogger().message(TrustStoreMessages.EXPIRING(getName(), String.valueOf(days), cert.getSubjectDN().toString()));
        }
        catch(CertificateNotYetValidException e)
        {
            // ignore
        }
    }

    @Override
    public final TrustManager[] getTrustManagers() throws GeneralSecurityException
    {
        if (isTrustAnchorValidityEnforced())
        {
            final Set<Certificate> trustManagerCerts = Sets.newHashSet(getCertificates());
            final Set<TrustAnchor> trustAnchors = new HashSet<>();
            final Set<Certificate> otherCerts = new HashSet<>();
            for (Certificate certs : trustManagerCerts)
            {
                if (certs instanceof X509Certificate && isSelfSigned((X509Certificate) certs))
                {
                    trustAnchors.add(new TrustAnchor((X509Certificate) certs, null));
                }
                else
                {
                    otherCerts.add(certs);
                }
            }

            TrustManager[] trustManagers = getTrustManagersInternal();
            TrustManager[] wrappedTrustManagers = new TrustManager[trustManagers.length];

            for (int i = 0; i < trustManagers.length; i++)
            {
                final TrustManager trustManager = trustManagers[i];
                if (trustManager instanceof X509TrustManager)
                {
                    wrappedTrustManagers[i] = new TrustAnchorValidatingTrustManager(getName(),
                                                                                    (X509TrustManager) trustManager,
                                                                                    trustAnchors,
                                                                                    otherCerts);
                }
                else
                {
                    wrappedTrustManagers[i] = trustManager;
                }
            }
            return wrappedTrustManagers;
        }
        else
        {
            return getTrustManagersInternal();
        }
    }

    protected abstract TrustManager[] getTrustManagersInternal() throws GeneralSecurityException;

    protected TrustManager[] getTrustManagers(KeyStore ts)
    {
        try
        {
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(new CertPathTrustManagerParameters(getParameters(ts)));
            return tmf.getTrustManagers();
        }
        catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e)
        {
            throw new IllegalConfigurationException("Cannot create trust manager factory for truststore '" +
                    getName() + "' :" + e, e);
        }
    }

    private CertPathParameters getParameters(KeyStore trustStore)
    {
        try
        {
            final PKIXBuilderParameters parameters = new PKIXBuilderParameters(trustStore, new X509CertSelector());
            parameters.setRevocationEnabled(_certificateRevocationCheckEnabled);
            if (_certificateRevocationCheckEnabled)
            {
                if (_certificateRevocationListUrl != null)
                {
                    parameters.addCertStore(
                            CertStore.getInstance("Collection", new CollectionCertStoreParameters(getCRLs())));
                }
                final PKIXRevocationChecker revocationChecker = (PKIXRevocationChecker) CertPathBuilder
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm()).getRevocationChecker();
                final Set<PKIXRevocationChecker.Option> options = new HashSet<>();
                if (_certificateRevocationCheckOfOnlyEndEntityCertificates)
                {
                    options.add(PKIXRevocationChecker.Option.ONLY_END_ENTITY);
                }
                if (_certificateRevocationCheckWithPreferringCertificateRevocationList)
                {
                    options.add(PKIXRevocationChecker.Option.PREFER_CRLS);
                }
                if (_certificateRevocationCheckWithNoFallback)
                {
                    options.add(PKIXRevocationChecker.Option.NO_FALLBACK);
                }
                if (_certificateRevocationCheckWithIgnoringSoftFailures)
                {
                    options.add(PKIXRevocationChecker.Option.SOFT_FAIL);
                }
                revocationChecker.setOptions(options);
                parameters.addCertPathChecker(revocationChecker);
            }
            return parameters;
        }
        catch (NoSuchAlgorithmException | KeyStoreException | InvalidAlgorithmParameterException e)
        {
            throw new IllegalConfigurationException("Cannot create trust manager factory parameters for truststore '" +
                    getName() + "' :" + e, e);
        }
    }

    private Collection<? extends CRL> getCRLs()
    {
        return getCRLs(_certificateRevocationListUrl);
    }

    /**
     * Load the collection of CRLs.
     */
    private Collection<? extends CRL> getCRLs(String crlUrl)
    {
        Collection<? extends CRL> crls = Collections.emptyList();
        if (crlUrl != null)
        {
            try (InputStream is = getUrlFromString(crlUrl).openStream())
            {
                crls = SSLUtil.getCertificateFactory().generateCRLs(is);
            }
            catch (IOException | CRLException e)
            {
                throw new IllegalConfigurationException("Unable to load certificate revocation list '" + crlUrl +
                        "' for truststore '" + getName() + "' :" + e, e);
            }
        }
        return crls;
    }

    protected static URL getUrlFromString(String urlString) throws MalformedURLException
    {
        URL url;
        try
        {
            url = new URL(urlString);
        }
        catch (MalformedURLException e)
        {
            final File file = new File(urlString);
            url = file.toURI().toURL();
        }
        return url;
    }

    @Override
    public final int getCertificateExpiryWarnPeriod()
    {
        try
        {
            return getContextValue(Integer.class, CERTIFICATE_EXPIRY_WARN_PERIOD);
        }
        catch (NullPointerException | IllegalArgumentException e)
        {
            LOGGER.warn("The value of the context variable '{}' for truststore {} cannot be converted to an integer. The value {} will be used as a default", CERTIFICATE_EXPIRY_WARN_PERIOD, getName(), DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD);
            return DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD;
        }
    }

    @Override
    public int getCertificateExpiryCheckFrequency()
    {
        int checkFrequency;
        try
        {
            checkFrequency = getContextValue(Integer.class, CERTIFICATE_EXPIRY_CHECK_FREQUENCY);
        }
        catch (IllegalArgumentException | NullPointerException e)
        {
            LOGGER.warn("Cannot parse the context variable {} ", CERTIFICATE_EXPIRY_CHECK_FREQUENCY, e);
            checkFrequency = DEFAULT_CERTIFICATE_EXPIRY_CHECK_FREQUENCY;
        }
        return checkFrequency;
    }

    @Override
    public boolean isTrustAnchorValidityEnforced()
    {
        return _trustAnchorValidityEnforced;
    }

    @Override
    public boolean isCertificateRevocationCheckEnabled()
    {
        return _certificateRevocationCheckEnabled;
    }

    @Override
    public boolean isCertificateRevocationCheckOfOnlyEndEntityCertificates()
    {
        return _certificateRevocationCheckOfOnlyEndEntityCertificates;
    }

    @Override
    public boolean isCertificateRevocationCheckWithPreferringCertificateRevocationList()
    {
        return _certificateRevocationCheckWithPreferringCertificateRevocationList;
    }

    @Override
    public boolean isCertificateRevocationCheckWithNoFallback()
    {
        return _certificateRevocationCheckWithNoFallback;
    }

    @Override
    public boolean isCertificateRevocationCheckWithIgnoringSoftFailures()
    {
        return _certificateRevocationCheckWithIgnoringSoftFailures;
    }

    @Override
    public String getCertificateRevocationListUrl()
    {
        return _certificateRevocationListUrl;
    }

    @Override
    public String getCertificateRevocationListPath()
    {
        return _certificateRevocationListPath;
    }

    @SuppressWarnings(value = "unused")
    private void postSetCertificateRevocationListUrl()
    {
        if (_certificateRevocationListUrl != null && !_certificateRevocationListUrl.startsWith("data:"))
        {
            _certificateRevocationListPath = _certificateRevocationListUrl;
        }
        else
        {
            _certificateRevocationListPath = null;
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
    public List<CertificateDetails> getCertificateDetails()
    {
        try
        {
            Certificate[] certificatesInternal = getCertificates();
            if (certificatesInternal.length > 0)
            {
                return Arrays.stream(certificatesInternal)
                             .filter(cert -> cert instanceof X509Certificate)
                             .map(x509cert -> new CertificateDetailsImpl((X509Certificate) x509cert))
                             .collect(Collectors.toList());
            }
            return Collections.emptyList();
        }
        catch (GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Failed to extract certificate details", e);
        }
    }

    private boolean isSelfSigned(X509Certificate cert) throws GeneralSecurityException
    {
        try
        {
            PublicKey key = cert.getPublicKey();
            cert.verify(key);
            return true;
        }
        catch (SignatureException | InvalidKeyException e)
        {
            return false;
        }
    }
}
