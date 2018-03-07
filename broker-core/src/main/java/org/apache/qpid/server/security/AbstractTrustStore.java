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

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
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

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.TrustStoreMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;

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

    @Override
    protected ListenableFuture<Void> onClose()
    {
        if(_checkExpiryTaskFuture != null)
        {
            _checkExpiryTaskFuture.cancel(false);
            _checkExpiryTaskFuture = null;
        }
        return Futures.immediateFuture(null);
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
