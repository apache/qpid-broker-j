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
package org.apache.qpid.client.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.JMSException;
import javax.security.auth.x500.X500Principal;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.BasicMessageConsumer;
import org.apache.qpid.transport.ConnectionSettings;

public class MessageEncryptionHelper
{
    public static final String ENCRYPTION_ALGORITHM_PROPERTY = "x-qpid-encryption-algorithm";
    public static final String KEY_INIT_VECTOR_PROPERTY = "x-qpid-key-init-vector";
    public static final String ENCRYPTED_KEYS_PROPERTY = "x-qpid-encrypted-keys";
    public static final String ENCRYPT_HEADER = "x-qpid-encrypt";
    public static final String ENCRYPT_RECIPIENTS_HEADER = "x-qpid-encrypt-recipients";
    public static final String UNENCRYPTED_PROPERTIES_HEADER = "x-qpid-unencrypted-properties";
    static final int AES_KEY_SIZE_BITS = 256;
    public static final int AES_KEY_SIZE_BYTES = AES_KEY_SIZE_BITS / 8;
    public static final String AES_ALGORITHM = "AES";
    public static final String DEFAULT_MESSAGE_ENCRYPTION_CIPHER_NAME = "AES/CBC/PKCS5Padding";
    public static final int AES_INITIALIZATION_VECTOR_LENGTH = 16;
    private final AMQSession<?, ?> _session;

    private static final int KEY_TRANSPORT_RECIPIENT_INFO_TYPE = 1;
    public static final String DEFAULT_KEY_ENCRYPTION_ALGORITHM = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";


    private final Map<String, X509Certificate> _signingCertificateCache =
            Collections.synchronizedMap(new LinkedHashMap<String, X509Certificate>(16,0.75f,true)
                                        {
                                            @Override
                                            protected boolean removeEldestEntry(final Map.Entry<String, X509Certificate> eldest)
                                            {
                                                return size() > 128;
                                            }
                                        });

    private String _keyEncryptionAlgorithm = DEFAULT_KEY_ENCRYPTION_ALGORITHM;
    private String _messageEncryptionCipherName = DEFAULT_MESSAGE_ENCRYPTION_CIPHER_NAME;

    public MessageEncryptionHelper(final AMQSession<?, ?> session)
    {
        _session = session;
    }

    public String getKeyEncryptionAlgorithm()
    {
        return _keyEncryptionAlgorithm;
    }

    public void setKeyEncryptionAlgorithm(final String keyEncryptionAlgorithm)
    {
        _keyEncryptionAlgorithm = keyEncryptionAlgorithm;
    }

    public String getMessageEncryptionCipherName()
    {
        return _messageEncryptionCipherName;
    }

    public void setMessageEncryptionCipherName(final String messageEncryptionCipherName)
    {
        _messageEncryptionCipherName = messageEncryptionCipherName;
    }

    public KeyStore getSigningCertificateStore() throws GeneralSecurityException, IOException
    {
        return _session.getAMQConnection().getConnectionSettings().getEncryptionTrustStore(new ConnectionSettings.RemoteStoreFinder()
        {
            @Override
            public KeyStore getKeyStore(final String name) throws GeneralSecurityException, IOException
            {
                try
                {
                    return _session.getAMQConnection().getBrokerSuppliedTrustStore(name);
                }
                catch (JMSException e)
                {
                    throw new CertificateException("Could not load remote certificate store: '" + name  + "'", e);
                }
            }
        });

    }

    public interface KeyTransportRecipientInfo
    {
        int getType();
        String getKeyEncryptionAlgorithm();
        String getCertIssuerPrincipal();
        String getCertSerialNumber();
        byte[] getEncryptedKey();

        List<Object> asList();
    }

    public List<KeyTransportRecipientInfo> getKeyTransportRecipientInfo(List<String> recipients, SecretKeySpec secretKey)
        throws GeneralSecurityException, IOException
    {
        List<KeyTransportRecipientInfo> result = new ArrayList<>();
        final String keyEncryptionAlgorithm = getKeyEncryptionAlgorithm();
        for(String recipient : recipients)
        {
            X509Certificate cert = getSigningCertificate(recipient.trim());
            if(cert != null)
            {

                Cipher cipher = Cipher.getInstance(keyEncryptionAlgorithm);
                cipher.init(Cipher.ENCRYPT_MODE, cert.getPublicKey());
                final byte[] encryptedKey = cipher.doFinal(secretKey.getEncoded());

                final String issuePrincipal = cert.getIssuerX500Principal().getName(X500Principal.CANONICAL);
                final String serialNumber = cert.getSerialNumber().toString();

                result.add(new KeyTransportRecipientInfoImpl(keyEncryptionAlgorithm,
                                                             issuePrincipal,
                                                             serialNumber,
                                                             encryptedKey));

            }
            else
            {
                throw new CertificateException("Unable to find certificate for recipient '" + recipient +"'");
            }
        }
        return result;
    }

    public X509Certificate getSigningCertificate(final String name)
            throws GeneralSecurityException, IOException
    {
        X509Certificate returnVal = _signingCertificateCache.get(name);
        if(returnVal == null)
        {
            KeyStore certStore = getSigningCertificateStore();
            X500Principal requestedPrincipal;
            List<X509Certificate> potentialCerts = new ArrayList<>();
            try
            {
                requestedPrincipal = new X500Principal(name);
            }
            catch (IllegalArgumentException e)
            {
                requestedPrincipal = null;
            }

            for (String alias : Collections.list(certStore.aliases()))
            {
                Certificate cert = certStore.getCertificate(alias);
                if (cert instanceof X509Certificate)
                {
                    X509Certificate x509Cert = (X509Certificate) cert;
                    if (requestedPrincipal != null
                        && requestedPrincipal.equals(x509Cert.getSubjectX500Principal()))
                    {
                        potentialCerts.add(x509Cert);
                    }
                    else if (x509Cert.getSubjectAlternativeNames() != null)
                    {
                        for (List<?> entry : x509Cert.getSubjectAlternativeNames())
                        {
                            final int type = (Integer) entry.get(0);
                            if ((type == 1 || type == 2) && (entry.get(1).toString().trim().equals(name)))
                            {
                                potentialCerts.add(x509Cert);
                                break;
                            }
                        }
                    }


                }
            }

            for (X509Certificate cert : potentialCerts)
            {
                try
                {
                    cert.checkValidity();
                    if (returnVal == null || returnVal.getNotAfter().getTime() > cert.getNotAfter().getTime())
                    {
                        returnVal = cert;
                    }
                }
                catch (CertificateExpiredException | CertificateNotYetValidException e)
                {
                    // ignore the invalid cert
                }
            }
            if(returnVal != null)
            {
                _signingCertificateCache.put(name, returnVal);
            }
        }
        return returnVal;
    }

    public PrivateKey getEncryptionPrivateKey(final X500Principal issuer,
                                              final BigInteger serialNumber)
            throws GeneralSecurityException, IOException
    {

        final ConnectionSettings connectionSettings = _session.getAMQConnection().getConnectionSettings();
        KeyStore keyStore = connectionSettings.getEncryptionKeyStore();
        if(keyStore != null)
        {
            for (String alias : Collections.list(keyStore.aliases()))
            {
                try
                {

                    final KeyStore.Entry entry = keyStore.getEntry(alias,
                                                                   new KeyStore.PasswordProtection(connectionSettings.getEncryptionKeyStorePassword()
                                                                                                           .toCharArray()));
                    if (entry instanceof KeyStore.PrivateKeyEntry)
                    {
                        KeyStore.PrivateKeyEntry pkEntry = (KeyStore.PrivateKeyEntry) entry;
                        if (pkEntry.getCertificate() instanceof X509Certificate)
                        {
                            X509Certificate cert = (X509Certificate) pkEntry.getCertificate();
                            if (cert.getIssuerX500Principal().equals(issuer) && cert.getSerialNumber()
                                    .equals(serialNumber))
                            {
                                return pkEntry.getPrivateKey();
                            }
                        }
                    }
                }
                catch (UnsupportedOperationException e)
                {
                    // ignore
                }
            }
        }
        return null;
    }

    private SecureRandom _random;

    public SecretKeySpec createSecretKey()
    {
        byte[] key = new byte[AES_KEY_SIZE_BYTES];
        getRandomBytes(key);
        return new SecretKeySpec(key, AES_ALGORITHM);
    }

    private void getRandomBytes(final byte[] key)
    {
        synchronized (this)
        {

            if(_random == null)
            {
                _random = new SecureRandom();
            }
            _random.nextBytes(key);
        }
    }

    public byte[] getInitialisationVector()
    {
        byte[] ivbytes = new byte[AES_INITIALIZATION_VECTOR_LENGTH];
        getRandomBytes(ivbytes);
        return ivbytes;
    }

    public byte[] readFromCipherStream(final byte[] unencryptedBytes, int offset, int length, final Cipher cipher)
            throws IOException
    {
        final byte[] encryptedBytes;
        try (CipherInputStream cipherInputStream = new CipherInputStream(new ByteArrayInputStream(unencryptedBytes,
                                                                                                  offset,
                                                                                                  length), cipher))
        {
            byte[] buf = new byte[512];
            int pos = 0;
            int read;
            while ((read = cipherInputStream.read(buf, pos, buf.length - pos)) != -1)
            {
                pos += read;
                if (pos == buf.length)
                {
                    byte[] tmp = buf;
                    buf = new byte[buf.length + 512];
                    System.arraycopy(tmp, 0, buf, 0, tmp.length);
                }
            }
            encryptedBytes = new byte[pos];
            System.arraycopy(buf, 0, encryptedBytes, 0, pos);
        }
        return encryptedBytes;
    }

    public byte[] readFromCipherStream(final byte[] unencryptedBytes, final Cipher cipher, final AMQSession amqSession) throws IOException
    {
        return readFromCipherStream(unencryptedBytes, 0, unencryptedBytes.length, cipher);
    }

    public byte[] encrypt(SecretKeySpec secretKey,
                          final byte[] unencryptedBytes,
                          byte[] ivbytes)
    {
        try
        {
            Cipher cipher = Cipher.getInstance(getMessageEncryptionCipherName());
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(ivbytes));
            return readFromCipherStream(unencryptedBytes, cipher, _session);
        }
        catch (IOException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            throw new IllegalArgumentException("Unable to encrypt secret with secret key. Cipher: "
                                               + getMessageEncryptionCipherName()
                                               + " . Key of type " + secretKey.getAlgorithm()
                                               + " size " + secretKey.getEncoded().length, e);
        }
    }

    private static class KeyTransportRecipientInfoImpl implements KeyTransportRecipientInfo
    {
        private final String _keyEncryptionAlgorithm;
        private final String _issuePrincipal;
        private final String _serialNumber;
        private final byte[] _encryptedKey;

        public KeyTransportRecipientInfoImpl(final String keyEncryptionAlgorithm,
                                             final String issuePrincipal,
                                             final String serialNumber,
                                             final byte[] encryptedKey)
        {
            _keyEncryptionAlgorithm = keyEncryptionAlgorithm;
            _issuePrincipal = issuePrincipal;
            _serialNumber = serialNumber;
            _encryptedKey = encryptedKey;
        }

        @Override
        public int getType()
        {
            return KEY_TRANSPORT_RECIPIENT_INFO_TYPE;
        }

        @Override
        public String getKeyEncryptionAlgorithm()
        {
            return _keyEncryptionAlgorithm;
        }

        @Override
        public String getCertIssuerPrincipal()
        {
            return _issuePrincipal;
        }

        @Override
        public String getCertSerialNumber()
        {
            return _serialNumber;
        }

        @Override
        public byte[] getEncryptedKey()
        {
            return _encryptedKey;
        }

        @Override
        public List<Object> asList()
        {
            List<Object> result = new ArrayList<>();

            result.add(KEY_TRANSPORT_RECIPIENT_INFO_TYPE);
            result.add(_keyEncryptionAlgorithm);
            result.add(_issuePrincipal);
            result.add(_serialNumber);
            result.add(_encryptedKey);
            return result;
        }
    }
}
