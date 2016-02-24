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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

@ManagedObject( category = false )
public class NonJavaKeyStoreImpl extends AbstractConfiguredObject<NonJavaKeyStoreImpl> implements NonJavaKeyStore<NonJavaKeyStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonJavaKeyStoreImpl.class);
    private static final long ONE_DAY = 24l * 60l * 60l * 1000l;

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private String _privateKeyUrl;
    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private String _certificateUrl;
    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private String _intermediateCertificateUrl;

    private volatile KeyManager[] _keyManagers = new KeyManager[0];

    private static final SecureRandom RANDOM = new SecureRandom();

    static
    {
        Handler.register();
    }

    private X509Certificate _certificate;

    private ScheduledFuture<?> _checkExpiryTaskFuture;
    private int _checkFrequency;

    @ManagedObjectFactoryConstructor
    public NonJavaKeyStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker), attributes);
        _broker = broker;
        _eventLogger = _broker.getEventLogger();
        _eventLogger.message(KeyStoreMessages.CREATE(getName()));
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        int checkFrequency;
        try
        {
            checkFrequency = getContextValue(Integer.class, CERTIFICATE_EXPIRY_CHECK_FREQUENCY);
        }
        catch(IllegalArgumentException | NullPointerException e)
        {
            LOGGER.warn("Cannot parse the context variable {} ", CERTIFICATE_EXPIRY_CHECK_FREQUENCY, e);
            checkFrequency = DEFAULT_CERTIFICATE_EXPIRY_CHECK_FREQUENCY;
        }

        _checkExpiryTaskFuture =
                _broker.scheduleHouseKeepingTask(checkFrequency, TimeUnit.DAYS, new Runnable()
                {
                    @Override
                    public void run()
                    {
                        checkCertificateExpiry();
                    }
                });
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
    public String getPrivateKeyUrl()
    {
        return _privateKeyUrl;
    }

    @Override
    public String getCertificateUrl()
    {
        return _certificateUrl;
    }

    @Override
    public String getIntermediateCertificateUrl()
    {
        return _intermediateCertificateUrl;
    }

    @Override
    public String getSubjectName()
    {
        if(_certificate != null)
        {
            try
            {
                String dn = _certificate.getSubjectX500Principal().getName();
                LdapName ldapDN = new LdapName(dn);
                String name = dn;
                for (Rdn rdn : ldapDN.getRdns())
                {
                    if (rdn.getType().equalsIgnoreCase("CN"))
                    {
                        name = String.valueOf(rdn.getValue());
                        break;
                    }
                }
                return name;
            }
            catch (InvalidNameException e)
            {
                LOGGER.error("Error getting subject name from certificate");
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    @Override
    public long getCertificateValidEnd()
    {
        return _certificate == null ? 0 : _certificate.getNotAfter().getTime();
    }

    @Override
    public long getCertificateValidStart()
    {
        return _certificate == null ? 0 : _certificate.getNotBefore().getTime();
    }


    @Override
    public KeyManager[] getKeyManagers() throws GeneralSecurityException
    {

        return _keyManagers;
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
                throw new IntegrityViolationException("Key store '"
                        + storeName
                        + "' can't be deleted as it is in use by a port:"
                        + port.getName());
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
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        NonJavaKeyStore changedStore = (NonJavaKeyStore) proxyForValidation;
        if (changedAttributes.contains(NAME) && !getName().equals(changedStore.getName()))
        {
            throw new IllegalConfigurationException("Changing the key store name is not allowed");
        }
        validateKeyStoreAttributes(changedStore);
    }

    private void validateKeyStoreAttributes(NonJavaKeyStore<?> keyStore)
    {
        try
        {
            SSLUtil.readPrivateKey(getUrlFromString(keyStore.getPrivateKeyUrl()));
            SSLUtil.readCertificates(getUrlFromString(keyStore.getCertificateUrl()));
            if(keyStore.getIntermediateCertificateUrl() != null)
            {
                SSLUtil.readCertificates(getUrlFromString(keyStore.getIntermediateCertificateUrl()));
            }
        }
        catch (IOException | GeneralSecurityException e )
        {
            throw new IllegalConfigurationException("Cannot validate private key or certificate(s):" + e, e);
        }
    }

    @SuppressWarnings("unused")
    private void updateKeyManagers()
    {
        try
        {
            if (_privateKeyUrl != null && _certificateUrl != null)
            {
                PrivateKey privateKey = SSLUtil.readPrivateKey(getUrlFromString(_privateKeyUrl));
                X509Certificate[] certs = SSLUtil.readCertificates(getUrlFromString(_certificateUrl));
                if(_intermediateCertificateUrl != null)
                {
                    List<X509Certificate> allCerts = new ArrayList<>(Arrays.asList(certs));
                    allCerts.addAll(Arrays.asList(SSLUtil.readCertificates(getUrlFromString(_intermediateCertificateUrl))));
                    certs = allCerts.toArray(new X509Certificate[allCerts.size()]);
                }
                checkCertificateExpiry(certs);
                java.security.KeyStore inMemoryKeyStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

                byte[] bytes = new byte[64];
                char[] chars = new char[64];
                RANDOM.nextBytes(bytes);
                StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(bytes)).get(chars);
                inMemoryKeyStore.load(null, chars);
                inMemoryKeyStore.setKeyEntry("1", privateKey, chars, certs);


                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(inMemoryKeyStore, chars);
                _keyManagers = kmf.getKeyManagers();
                _certificate = certs[0];
            }

        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Cannot load private key or certificate(s): " + e, e);
        }
    }

    private void checkCertificateExpiry()
    {
        try
        {
            if (_privateKeyUrl != null && _certificateUrl != null)
            {
                X509Certificate[] certs = SSLUtil.readCertificates(getUrlFromString(_certificateUrl));
                if (_intermediateCertificateUrl != null)
                {
                    List<X509Certificate> allCerts = new ArrayList<>(Arrays.asList(certs));
                    allCerts.addAll(Arrays.asList(SSLUtil.readCertificates(getUrlFromString(_intermediateCertificateUrl))));
                    certs = allCerts.toArray(new X509Certificate[allCerts.size()]);
                }
                checkCertificateExpiry(certs);
            }
        }
        catch (GeneralSecurityException | IOException e)
        {
            LOGGER.info("Unexpected exception while trying to check certificate validity", e);
        }
    }

    private void checkCertificateExpiry(final X509Certificate... certificates)
    {
        int expiryWarning = getContextValue(Integer.class, CERTIFICATE_EXPIRY_WARN_PERIOD);
        if(expiryWarning > 0)
        {
            long currentTime = System.currentTimeMillis();
            Date expiryTestDate = new Date(currentTime + (ONE_DAY * (long) expiryWarning));


            if (certificates != null)
            {
                for (X509Certificate cert : certificates)
                {
                    try
                    {
                        cert.checkValidity(expiryTestDate);
                    }
                    catch (CertificateExpiredException e)
                    {
                        long timeToExpiry = cert.getNotAfter().getTime() - currentTime;
                        int days = Math.max(0, (int) (timeToExpiry / (ONE_DAY)));

                        _eventLogger.message(KeyStoreMessages.EXPIRING(getName(),
                                                                       String.valueOf(days),
                                                                       cert.getSubjectDN().toString()));
                    }
                    catch (CertificateNotYetValidException e)
                    {
                        // ignore
                    }
                }
            }
        }
    }

    private URL getUrlFromString(String urlString) throws MalformedURLException
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


}
