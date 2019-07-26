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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.transport.network.security.ssl.QpidBestFitX509KeyManager;
import org.apache.qpid.server.transport.network.security.ssl.QpidServerX509KeyManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

@ManagedObject( category = false )
public class FileKeyStoreImpl extends AbstractKeyStore<FileKeyStoreImpl> implements FileKeyStore<FileKeyStoreImpl>
{

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
    @ManagedAttributeField
    private boolean _useHostNameMatching;
    private String _path;

    @ManagedAttributeField
    private String _password;

    private volatile Collection<Certificate> _certificates;

    static
    {
        Handler.register();
    }


    @ManagedObjectFactoryConstructor
    public FileKeyStoreImpl(Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }


    @Override
    public void onValidate()
    {
        super.onValidate();
        validateKeyStoreAttributes(this);
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> doActivate()
    {
        initializeExpiryChecking();
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
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
            || attributes.containsKey(KEY_STORE_TYPE)
            || attributes.containsKey(KEY_MANAGER_FACTORY_ALGORITHM))
        {
            initialize();
        }
    }

    private void initialize()
    {
        Collection<Certificate> result;
        try
        {
            result = Collections.unmodifiableCollection(SSLUtil.getCertificates(getInitializedKeyStore(this)));
        }
        catch (GeneralSecurityException | IOException e)
        {
            throw new IllegalConfigurationException(String.format("Cannot instantiate keystore '%s'", getName()), e);
        }
        _certificates = result;
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
        final String loggableStoreUrl = StringUtil.elideDataUrl(fileKeyStore.getStoreUrl());
        try
        {
            java.security.KeyStore keyStore = getInitializedKeyStore(fileKeyStore);

            final String certAlias = fileKeyStore.getCertificateAlias();
            if (certAlias != null)
            {
                Certificate cert = keyStore.getCertificate(certAlias);

                if (cert == null)
                {
                    throw new IllegalConfigurationException(String.format(
                            "Cannot find a certificate with alias '%s' in key store '%s'.",
                            certAlias,
                            loggableStoreUrl));
                }

                if (!keyStore.entryInstanceOf(certAlias, java.security.KeyStore.PrivateKeyEntry.class))
                {
                    throw new IllegalConfigurationException(String.format(
                            "Alias '%s' in key store '%s' does not identify a private key.",
                            certAlias,
                            loggableStoreUrl));

                }
            }
            else if (!containsPrivateKey(keyStore))
            {
                throw new IllegalConfigurationException(String.format(
                        "Keystore '%s' must contain at least one private key.", loggableStoreUrl));
            }
        }
        catch (UnrecoverableKeyException e)
        {
            String message = String.format("Check key store password. Cannot instantiate key store from '%s'.", loggableStoreUrl);
            throw new IllegalConfigurationException(message, e);
        }
        catch (IOException | GeneralSecurityException e)
        {
            final String message = String.format("Cannot instantiate key store from '%s'.", loggableStoreUrl);
            throw new IllegalConfigurationException(message, e);
        }

        try
        {
            KeyManagerFactory.getInstance(fileKeyStore.getKeyManagerFactoryAlgorithm());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalConfigurationException(String.format("Unknown keyManagerFactoryAlgorithm: '%s'",
                                                                  fileKeyStore.getKeyManagerFactoryAlgorithm()));
        }

        if(!fileKeyStore.isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }

        checkCertificateExpiry();
    }

    private java.security.KeyStore getInitializedKeyStore(final FileKeyStore<?> fileKeyStore)
            throws GeneralSecurityException, IOException
    {
        URL url = getUrlFromString(fileKeyStore.getStoreUrl());
        String password = fileKeyStore.getPassword();
        String keyStoreType = fileKeyStore.getKeyStoreType();
        return SSLUtil.getInitializedKeyStore(url, password, keyStoreType);
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

    @Override
    public boolean isUseHostNameMatching()
    {
        return _useHostNameMatching;
    }

    @Override
    public void reload()
    {
        initialize();
    }

    public void setPassword(String password)
    {
        _password = password;
    }

    @Override
    public KeyManager[] getKeyManagers() throws GeneralSecurityException
    {

        try
        {
            URL url = getUrlFromString(_storeUrl);
            if(isUseHostNameMatching())
            {
                return new KeyManager[] {
                        new QpidBestFitX509KeyManager(_certificateAlias, url, _keyStoreType, getPassword(),
                                                      _keyManagerFactoryAlgorithm)
                };
            }
            else if (_certificateAlias != null)
            {
                return new KeyManager[] {
                        new QpidServerX509KeyManager(_certificateAlias, url, _keyStoreType, getPassword(),
                                                     _keyManagerFactoryAlgorithm)
                                        };

            }
            else
            {
                final java.security.KeyStore ks = SSLUtil.getInitializedKeyStore(url, getPassword(), _keyStoreType);

                char[] keyStoreCharPassword = getPassword() == null ? null : getPassword().toCharArray();

                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(_keyManagerFactoryAlgorithm);

                kmf.init(ks, keyStoreCharPassword);

                KeyManager[] keyManagers = kmf.getKeyManagers();
                return keyManagers == null ? new KeyManager[0] : Arrays.copyOf(keyManagers, keyManagers.length);
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

    @Override
    protected void checkCertificateExpiry()
    {
        int expiryWarning = getCertificateExpiryWarnPeriod();
        if(expiryWarning > 0)
        {
            long currentTime = System.currentTimeMillis();
            Date expiryTestDate = new Date(currentTime + (ONE_DAY * (long)expiryWarning));

            try
            {
                final java.security.KeyStore ks = getInitializedKeyStore(this);

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
                            checkCertificatesExpiry(currentTime, expiryTestDate,
                                                    x509KeyManager.getCertificateChain(alias));
                        }

                    }
                }
            }
            catch (GeneralSecurityException | IOException e)
            {

            }
        }

    }

    @Override
    protected Collection<Certificate> getCertificates()
    {
        final Collection<Certificate> certificates = _certificates;
        return certificates == null ? Collections.emptyList() : certificates;
    }

    private boolean containsPrivateKey(final java.security.KeyStore keyStore) throws KeyStoreException
    {
        final Enumeration<String> aliasesEnum = keyStore.aliases();
        boolean foundPrivateKey = false;
        while (aliasesEnum.hasMoreElements())
        {
            String alias = aliasesEnum.nextElement();
            if (keyStore.entryInstanceOf(alias, java.security.KeyStore.PrivateKeyEntry.class))
            {
                foundPrivateKey = true;
                break;
            }
        }
        return foundPrivateKey;
    }

}
