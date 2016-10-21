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

import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;

public abstract class AbstractKeyStore<X extends AbstractKeyStore<X>>
        extends AbstractConfiguredObject<X> implements KeyStore<X>
{
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractKeyStore.class);

    protected static final long ONE_DAY = 24l * 60l * 60l * 1000l;

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    private ScheduledFuture<?> _checkExpiryTaskFuture;


    public AbstractKeyStore(Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);

        _broker = broker;
        _eventLogger = broker.getEventLogger();
        _eventLogger.message(KeyStoreMessages.CREATE(getName()));
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
    protected void onClose()
    {
        super.onClose();
        if(_checkExpiryTaskFuture != null)
        {
            _checkExpiryTaskFuture.cancel(false);
            _checkExpiryTaskFuture = null;
        }
    }

    protected void initializeExpiryChecking()
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
        if(getBroker().getState() == State.ACTIVE)
        {
            _checkExpiryTaskFuture = getBroker().scheduleHouseKeepingTask(checkFrequency, TimeUnit.DAYS, new Runnable()
            {
                @Override
                public void run()
                {
                    checkCertificateExpiry();
                }
            });
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
                                getBroker().scheduleHouseKeepingTask(frequency, TimeUnit.DAYS, new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        checkCertificateExpiry();
                                    }
                                });
                        getBroker().removeChangeListener(this);
                    }
                }

            });
        }
    }

    protected final ListenableFuture<Void> deleteIfNotInUse()
    {
        // verify that it is not in use
        String storeName = getName();

        Collection<Port> ports = new ArrayList<Port>(getBroker().getPorts());
        for (Port port : ports)
        {
            if (port.getKeyStore() == this)
            {
                throw new IntegrityViolationException("Key store '" + storeName + "' can't be deleted as it is in use by a port:" + port.getName());
            }
        }
        deleted();
        setState(State.DELETED);
        getEventLogger().message(KeyStoreMessages.DELETE(getName()));
        return Futures.immediateFuture(null);
    }

    protected abstract void checkCertificateExpiry();

    protected void checkCertificatesExpiry(final long currentTime,
                                           final Date expiryTestDate,
                                           final X509Certificate[] chain)
    {
        if(chain != null)
        {
            for(X509Certificate cert : chain)
            {
                try
                {
                    cert.checkValidity(expiryTestDate);
                }
                catch(CertificateExpiredException e)
                {
                    long timeToExpiry = cert.getNotAfter().getTime() - currentTime;
                    int days = Math.max(0,(int)(timeToExpiry / (ONE_DAY)));

                    getEventLogger().message(KeyStoreMessages.EXPIRING(getName(), String.valueOf(days), cert.getSubjectDN().toString()));
                }
                catch(CertificateNotYetValidException e)
                {
                    // ignore
                }
            }
        }
    }

    protected final int getCertificateExpiryWarnPeriod()
    {
        try
        {
            return getContextValue(Integer.class, CERTIFICATE_EXPIRY_WARN_PERIOD);
        }
        catch (NullPointerException | IllegalArgumentException e)
        {
            LOGGER.warn("The value of the context variable '{}' for keystore {} cannot be converted to an integer. The value {} will be used as a default", CERTIFICATE_EXPIRY_WARN_PERIOD, getName(), DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD);
            return DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD;
        }
    }
}
