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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.transport.network.security.ssl.QpidClientX509KeyManager;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

@ManagedObject( category = false )
public class FileKeyStoreImpl extends AbstractConfiguredObject<FileKeyStoreImpl> implements FileKeyStore<FileKeyStoreImpl>
{
    private static Logger LOGGER = LoggerFactory.getLogger(FileKeyStoreImpl.class);

    private static final long ONE_DAY = 24l * 60l * 60l * 1000l;
    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private String _type;
    @ManagedAttributeField
    private String _keyStoreType;
    @ManagedAttributeField
    private String _certificateAlias;
    @ManagedAttributeField
    private String _keyManagerFactoryAlgorithm;
    @ManagedAttributeField(afterSet = "postSetStoreUrl")
    private String _storeUrl;
    private String _path;

    @ManagedAttributeField
    private String _password;

    static
    {
        Handler.register();
    }

    private ScheduledFuture<?> _checkExpiryTaskFuture;

    @ManagedObjectFactoryConstructor
    public FileKeyStoreImpl(Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);

        _broker = broker;
        _eventLogger = _broker.getEventLogger();
        _eventLogger.message(KeyStoreMessages.CREATE(getName()));
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

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateKeyStoreAttributes(this);
    }

    @StateTransition(currentState = {State.ACTIVE, State.ERRORED}, desiredState = State.DELETED)
    protected ListenableFuture<Void> doDelete()
    {
        // verify that it is not in use
        String storeName = getName();

        Collection<Port> ports = new ArrayList<Port>(_broker.getPorts());
        for (Port port : ports)
        {
            if (port.getKeyStore() == this)
            {
                throw new IntegrityViolationException("Key store '" + storeName + "' can't be deleted as it is in use by a port:" + port.getName());
            }
        }
        deleted();
        setState(State.DELETED);
        _eventLogger.message(KeyStoreMessages.DELETE(getName()));
        return Futures.immediateFuture(null);
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
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
        if(_broker.getState() == State.ACTIVE)
        {
            _checkExpiryTaskFuture = _broker.scheduleHouseKeepingTask(checkFrequency, TimeUnit.DAYS, new Runnable()
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
            _broker.addChangeListener(new ConfigurationChangeListener()
            {
                @Override
                public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
                {
                    if (newState == State.ACTIVE)
                    {
                        _checkExpiryTaskFuture =
                                _broker.scheduleHouseKeepingTask(frequency, TimeUnit.DAYS, new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        checkCertificateExpiry();
                                    }
                                });
                        _broker.removeChangeListener(this);
                    }
                }

                @Override
                public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
                {

                }

                @Override
                public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
                {

                }

                @Override
                public void attributeSet(final ConfiguredObject<?> object,
                                         final String attributeName,
                                         final Object oldAttributeValue,
                                         final Object newAttributeValue)
                {

                }

                @Override
                public void bulkChangeStart(final ConfiguredObject<?> object)
                {

                }

                @Override
                public void bulkChangeEnd(final ConfiguredObject<?> object)
                {

                }
            });
        }
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        FileKeyStore changedStore = (FileKeyStore) proxyForValidation;
        if (changedAttributes.contains(KeyStore.DESIRED_STATE) && changedStore.getDesiredState() == State.DELETED)
        {
            return;
        }
        if(changedAttributes.contains(NAME) && !getName().equals(changedStore.getName()))
        {
            throw new IllegalConfigurationException("Changing the key store name is not allowed");
        }
        validateKeyStoreAttributes(changedStore);
    }

    private void validateKeyStoreAttributes(FileKeyStore<?> fileKeyStore)
    {
        java.security.KeyStore keyStore;
        try
        {
            URL url = getUrlFromString(fileKeyStore.getStoreUrl());
            String password = fileKeyStore.getPassword();
            String keyStoreType = fileKeyStore.getKeyStoreType();
            keyStore = SSLUtil.getInitializedKeyStore(url, password, keyStoreType);
        }
        catch (Exception e)
        {
            final String message;
            if (e instanceof IOException && e.getCause() != null && e.getCause() instanceof UnrecoverableKeyException)
            {
                message = "Check key store password. Cannot instantiate key store from '" + fileKeyStore.getStoreUrl() + "'.";
            }
            else
            {
                message = "Cannot instantiate key store from '" + fileKeyStore.getStoreUrl() + "'.";
            }

            throw new IllegalConfigurationException(message, e);
        }

        if (fileKeyStore.getCertificateAlias() != null)
        {
            Certificate cert;
            try
            {
                cert = keyStore.getCertificate(fileKeyStore.getCertificateAlias());
            }
            catch (KeyStoreException e)
            {
                // key store should be initialized above
                throw new ServerScopedRuntimeException("Key store has not been initialized", e);
            }
            if (cert == null)
            {
                throw new IllegalConfigurationException("Cannot find a certificate with alias '" + fileKeyStore.getCertificateAlias()
                        + "' in key store : " + fileKeyStore.getStoreUrl());
            }
        }

        try
        {
            KeyManagerFactory.getInstance(fileKeyStore.getKeyManagerFactoryAlgorithm());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalConfigurationException("Unknown keyManagerFactoryAlgorithm: "
                    + fileKeyStore.getKeyManagerFactoryAlgorithm());
        }

        if(!fileKeyStore.isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
        checkCertificateExpiry();
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
    public String getCertificateAlias()
    {
        return _certificateAlias;
    }

    @Override
    public String getKeyManagerFactoryAlgorithm()
    {
        return _keyManagerFactoryAlgorithm;
    }

    @Override
    public String getKeyStoreType()
    {
        return _keyStoreType;
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    public void setPassword(String password)
    {
        _password = password;
    }

    public KeyManager[] getKeyManagers() throws GeneralSecurityException
    {

        try
        {
            URL url = getUrlFromString(_storeUrl);
            if (_certificateAlias != null)
            {
                return new KeyManager[] {
                        new QpidClientX509KeyManager( _certificateAlias, url, _keyStoreType, getPassword(),
                                                      _keyManagerFactoryAlgorithm)
                                        };

            }
            else
            {
                final java.security.KeyStore ks = SSLUtil.getInitializedKeyStore(url, getPassword(), _keyStoreType);

                char[] keyStoreCharPassword = getPassword() == null ? null : getPassword().toCharArray();

                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(_keyManagerFactoryAlgorithm);

                kmf.init(ks, keyStoreCharPassword);

                return kmf.getKeyManagers();
            }
        }
        catch (IOException e)
        {
            throw new GeneralSecurityException(e);
        }
    }

    private static URL getUrlFromString(String urlString) throws MalformedURLException
    {
        URL url;
        try
        {
            url = new URL(urlString);
        }
        catch (MalformedURLException e)
        {
            File file = new File(urlString);
            url = file.toURI().toURL();

        }
        return url;
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

    private void checkCertificateExpiry()
    {
        try
        {

            int expiryWarning = getContextValue(Integer.class, CERTIFICATE_EXPIRY_WARN_PERIOD);
            if(expiryWarning > 0)
            {
                long currentTime = System.currentTimeMillis();
                Date expiryTestDate = new Date(currentTime + (ONE_DAY * (long)expiryWarning));

                try
                {
                    URL url = getUrlFromString(_storeUrl);
                    final java.security.KeyStore ks = SSLUtil.getInitializedKeyStore(url, getPassword(), _keyStoreType);

                    char[] keyStoreCharPassword = getPassword() == null ? null : getPassword().toCharArray();

                    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(_keyManagerFactoryAlgorithm);

                    kmf.init(ks, keyStoreCharPassword);


                    for (KeyManager km : kmf.getKeyManagers())
                    {
                        if (km instanceof X509KeyManager)
                        {
                            X509KeyManager x509KeyManager = (X509KeyManager) km;

                            for(String alias : Collections.list(ks.aliases()))
                            {
                                final X509Certificate[] chain =
                                        x509KeyManager.getCertificateChain(alias);
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

                                            _eventLogger.message(KeyStoreMessages.EXPIRING(getName(), String.valueOf(days), cert.getSubjectDN().toString()));
                                        }
                                        catch(CertificateNotYetValidException e)
                                        {
                                            // ignore
                                        }
                                    }
                                }
                            }

                        }
                    }
                }
                catch (GeneralSecurityException | IOException e)
                {

                }
            }
        }
        catch(IllegalArgumentException | NullPointerException e)
        {
        }
    }
}
