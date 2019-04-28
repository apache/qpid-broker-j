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
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

@ManagedObject( category = false )
public class NonJavaKeyStoreImpl extends AbstractKeyStore<NonJavaKeyStoreImpl> implements NonJavaKeyStore<NonJavaKeyStoreImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonJavaKeyStoreImpl.class);

    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private volatile String _privateKeyUrl;
    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private volatile String _certificateUrl;
    @ManagedAttributeField( afterSet = "updateKeyManagers" )
    private volatile String _intermediateCertificateUrl;

    private volatile KeyManager[] _keyManagers = new KeyManager[0];

    private static final SecureRandom RANDOM = new SecureRandom();

    static
    {
        Handler.register();
    }

    private volatile X509Certificate _certificate;
    private volatile Collection<Certificate> _certificates;

    @ManagedObjectFactoryConstructor
    public NonJavaKeyStoreImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
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
    public Date getCertificateValidEnd()
    {
        return _certificate == null ? null : _certificate.getNotAfter();
    }

    @Override
    public Date getCertificateValidStart()
    {
        return _certificate == null ? null : _certificate.getNotBefore();
    }


    @Override
    public KeyManager[] getKeyManagers() throws GeneralSecurityException
    {
        KeyManager[] keyManagers = _keyManagers;
        return keyManagers == null ? new KeyManager[0] : Arrays.copyOf(keyManagers, keyManagers.length);
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
            final PrivateKey privateKey = SSLUtil.readPrivateKey(getUrlFromString(keyStore.getPrivateKeyUrl()));
            X509Certificate[] certs = SSLUtil.readCertificates(getUrlFromString(keyStore.getCertificateUrl()));
            final List<X509Certificate> allCerts = new ArrayList<>(Arrays.asList(certs));
            if(keyStore.getIntermediateCertificateUrl() != null)
            {
                allCerts.addAll(Arrays.asList(SSLUtil.readCertificates(getUrlFromString(keyStore.getIntermediateCertificateUrl()))));
                certs = allCerts.toArray(new X509Certificate[allCerts.size()]);
            }
            final PublicKey publicKey = certs[0].getPublicKey();
            if (privateKey instanceof RSAPrivateKey && publicKey instanceof RSAPublicKey)
            {
                final BigInteger privateModulus = ((RSAPrivateKey) privateKey).getModulus();
                final BigInteger publicModulus = ((RSAPublicKey)publicKey).getModulus();
                if (!Objects.equals(privateModulus, publicModulus))
                {
                    throw new IllegalConfigurationException("Private key does not match certificate");
                }
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
                List<X509Certificate> allCerts = new ArrayList<>(Arrays.asList(certs));
                if(_intermediateCertificateUrl != null)
                {
                    allCerts.addAll(Arrays.asList(SSLUtil.readCertificates(getUrlFromString(_intermediateCertificateUrl))));
                    certs = allCerts.toArray(new X509Certificate[allCerts.size()]);
                }
                checkCertificateExpiry(certs);
                java.security.KeyStore inMemoryKeyStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

                byte[] bytes = new byte[64];
                char[] chars = "".toCharArray();
                RANDOM.nextBytes(bytes);
                StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(bytes)).get(chars);
                inMemoryKeyStore.load(null, chars);
                inMemoryKeyStore.setKeyEntry("1", privateKey, chars, certs);


                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(inMemoryKeyStore, chars);
                _keyManagers = kmf.getKeyManagers();
                _certificate = certs[0];
                _certificates = Collections.unmodifiableCollection(allCerts);
            }

        }
        catch (IOException | GeneralSecurityException e)
        {
            throw new IllegalConfigurationException("Cannot load private key or certificate(s): " + e, e);
        }
    }

    @Override
    protected void checkCertificateExpiry()
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
        int expiryWarning = getCertificateExpiryWarnPeriod();
        if(expiryWarning > 0)
        {
            long currentTime = System.currentTimeMillis();
            Date expiryTestDate = new Date(currentTime + (ONE_DAY * (long) expiryWarning));

            checkCertificatesExpiry(currentTime, expiryTestDate, certificates);
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

    @Override
    protected Collection<Certificate> getCertificates()
    {
        final Collection<Certificate> certificates = _certificates;
        return certificates == null ? Collections.emptyList() : certificates;
    }
}
