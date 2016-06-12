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
package org.apache.qpid.transport;

import static org.apache.qpid.transport.LegacyClientProperties.AMQJ_HEARTBEAT_DELAY;
import static org.apache.qpid.transport.LegacyClientProperties.AMQJ_HEARTBEAT_TIMEOUT_FACTOR;
import static org.apache.qpid.transport.LegacyClientProperties.IDLE_TIMEOUT_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.QPID_HEARTBEAT_INTERVAL;
import static org.apache.qpid.configuration.ClientProperties.QPID_HEARTBEAT_INTERVAL_010_DEFAULT;
import static org.apache.qpid.configuration.ClientProperties.QPID_HEARTBEAT_TIMEOUT_FACTOR;
import static org.apache.qpid.configuration.ClientProperties.QPID_HEARTBEAT_TIMEOUT_FACTOR_DEFAULT;
import static org.apache.qpid.transport.LegacyClientProperties.AMQJ_TCP_NODELAY_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.QPID_SSL_KEY_MANAGER_FACTORY_ALGORITHM_PROP_NAME;
import static org.apache.qpid.transport.LegacyClientProperties.QPID_SSL_KEY_STORE_CERT_TYPE_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.QPID_SSL_TRUST_MANAGER_FACTORY_ALGORITHM_PROP_NAME;
import static org.apache.qpid.transport.LegacyClientProperties.QPID_SSL_TRUST_STORE_CERT_TYPE_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.QPID_TCP_NODELAY_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.RECEIVE_BUFFER_SIZE_PROP_NAME;
import static org.apache.qpid.configuration.ClientProperties.SEND_BUFFER_SIZE_PROP_NAME;
import static org.apache.qpid.transport.LegacyClientProperties.LEGACY_RECEIVE_BUFFER_SIZE_PROP_NAME;
import static org.apache.qpid.transport.LegacyClientProperties.LEGACY_SEND_BUFFER_SIZE_PROP_NAME;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.qpid.configuration.QpidProperty;
import org.apache.qpid.ssl.SSLContextFactory;
import org.apache.qpid.transport.network.security.ssl.QpidClientX509KeyManager;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;


/**
 * A ConnectionSettings object can only be associated with
 * one Connection object. I have added an assertion that will
 * throw an exception if it is used by more than on Connection
 *
 */
public class ConnectionSettings
{
    public static final String WILDCARD_ADDRESS = "*";

    private static final SecureRandom RANDOM = new SecureRandom();

    private String _transport = "tcp";
    private String host = "localhost";
    private String vhost;
    private String username;
    private String password;
    private int port = 5672;
    private boolean tcpNodelay = QpidProperty.booleanProperty(Boolean.TRUE, QPID_TCP_NODELAY_PROP_NAME, AMQJ_TCP_NODELAY_PROP_NAME).get();
    private int maxChannelCount = 32767;
    private int maxFrameSize = 65535;
    private Integer hearbeatIntervalLegacyMs = QpidProperty.intProperty(null, IDLE_TIMEOUT_PROP_NAME).get();
    private Integer heartbeatInterval = QpidProperty.intProperty(null, QPID_HEARTBEAT_INTERVAL, AMQJ_HEARTBEAT_DELAY).get();
    private float heartbeatTimeoutFactor = QpidProperty.floatProperty(QPID_HEARTBEAT_TIMEOUT_FACTOR_DEFAULT, QPID_HEARTBEAT_TIMEOUT_FACTOR, AMQJ_HEARTBEAT_TIMEOUT_FACTOR).get();
    private int connectTimeout = 30000;
    private int readBufferSize = QpidProperty.intProperty(65535, RECEIVE_BUFFER_SIZE_PROP_NAME, LEGACY_RECEIVE_BUFFER_SIZE_PROP_NAME).get();
    private int writeBufferSize = QpidProperty.intProperty(65535, SEND_BUFFER_SIZE_PROP_NAME, LEGACY_SEND_BUFFER_SIZE_PROP_NAME).get();;

    // SSL props
    private boolean useSSL;
    private String keyStorePath = System.getProperty("javax.net.ssl.keyStore");
    private String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    private String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType",KeyStore.getDefaultType());
    private String keyManagerFactoryAlgorithm = QpidProperty.stringProperty(KeyManagerFactory.getDefaultAlgorithm(), QPID_SSL_KEY_MANAGER_FACTORY_ALGORITHM_PROP_NAME, QPID_SSL_KEY_STORE_CERT_TYPE_PROP_NAME).get();
    private String trustManagerFactoryAlgorithm = QpidProperty.stringProperty(TrustManagerFactory.getDefaultAlgorithm(), QPID_SSL_TRUST_MANAGER_FACTORY_ALGORITHM_PROP_NAME, QPID_SSL_TRUST_STORE_CERT_TYPE_PROP_NAME).get();
    private String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
    private String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    private String trustStoreType = System.getProperty("javax.net.ssl.trustStoreType",KeyStore.getDefaultType());
    private String certAlias;
    private boolean verifyHostname;

    private String _clientCertificatePrivateKeyPath;
    private String _clientCertificatePath;
    private String _clientCertificateIntermediateCertsPath;
    private String _trustedCertificatesFile;

    private String _encryptionKeyStorePath = System.getProperty("javax.net.ssl.keyStore");
    private String _encryptionKeyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    private String _encryptionKeyStoreType = System.getProperty("javax.net.ssl.keyStoreType",KeyStore.getDefaultType());
    private String _encryptionKeyManagerFactoryAlgorithm = QpidProperty.stringProperty(KeyManagerFactory.getDefaultAlgorithm(), QPID_SSL_KEY_MANAGER_FACTORY_ALGORITHM_PROP_NAME, QPID_SSL_KEY_STORE_CERT_TYPE_PROP_NAME).get();
    private String _encryptionTrustManagerFactoryAlgorithm = QpidProperty.stringProperty(TrustManagerFactory.getDefaultAlgorithm(), QPID_SSL_TRUST_MANAGER_FACTORY_ALGORITHM_PROP_NAME, QPID_SSL_TRUST_STORE_CERT_TYPE_PROP_NAME).get();
    private String _encryptionTrustStorePath = System.getProperty("javax.net.ssl.trustStore");
    private String _encryptionTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    private String _encryptionTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType",KeyStore.getDefaultType());

    private String _encryptionRemoteTrustStoreName;

    // SASL props
    private String saslMechs = System.getProperty("qpid.sasl_mechs", null);
    private String saslProtocol = System.getProperty("qpid.sasl_protocol", "AMQP");
    private String saslServerName = System.getProperty("qpid.sasl_server_name", "localhost");
    private boolean useSASLEncryption;

    private Map<String, Object> _clientProperties;
    private KeyStore _encryptionTrustStore;
    private KeyStore _encryptionKeyStore;

    public boolean isTcpNodelay()
    {
        return tcpNodelay;
    }

    public void setTcpNodelay(boolean tcpNodelay)
    {
        this.tcpNodelay = tcpNodelay;
    }

    /**
     * Gets the heartbeat interval (seconds) for 0-8/9/9-1 protocols.
     * 0 means heartbeating is disabled.
     * null means use the broker-supplied value.
     * @return the heartbeat interval
     */
    public Integer getHeartbeatInterval08()
    {
        if (heartbeatInterval != null)
        {
            return heartbeatInterval;
        }
        else if (hearbeatIntervalLegacyMs != null)
        {
            return hearbeatIntervalLegacyMs / 1000;
        }
        else
        {
            return null;
        }
    }

    /**
     * Gets the heartbeat interval (seconds) for the 0-10 protocol.
     * 0 means heartbeating is disabled.
     * @return the heartbeat interval
     */
    public int getHeartbeatInterval010()
    {
        if (heartbeatInterval != null)
        {
            return heartbeatInterval;
        }
        else if (hearbeatIntervalLegacyMs != null)
        {
            return hearbeatIntervalLegacyMs / 1000;
        }
        else
        {
            return QPID_HEARTBEAT_INTERVAL_010_DEFAULT;
        }
    }

    public void setHeartbeatInterval(int heartbeatInterval)
    {
        this.heartbeatInterval = heartbeatInterval;
    }

    public float getHeartbeatTimeoutFactor()
    {
        return this.heartbeatTimeoutFactor;
    }

    public String getTransport()
    {
        return _transport;
    }

    public void setTransport(String transport)
    {
        _transport = transport;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getVhost()
    {
        return vhost;
    }

    public void setVhost(String vhost)
    {
        this.vhost = vhost;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public boolean isUseSSL()
    {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL)
    {
        this.useSSL = useSSL;
    }

    public boolean isUseSASLEncryption()
    {
        return useSASLEncryption;
    }

    public void setUseSASLEncryption(boolean useSASLEncryption)
    {
        this.useSASLEncryption = useSASLEncryption;
    }

    public String getSaslMechs()
    {
        return saslMechs;
    }

    public void setSaslMechs(String saslMechs)
    {
        this.saslMechs = saslMechs;
    }

    public String getSaslProtocol()
    {
        return saslProtocol;
    }

    public void setSaslProtocol(String saslProtocol)
    {
        this.saslProtocol = saslProtocol;
    }

    public String getSaslServerName()
    {
        return saslServerName;
    }

    public void setSaslServerName(String saslServerName)
    {
        this.saslServerName = saslServerName;
    }

    public int getMaxChannelCount()
    {
        return maxChannelCount;
    }

    public void setMaxChannelCount(int maxChannelCount)
    {
        this.maxChannelCount = maxChannelCount;
    }

    public int getMaxFrameSize()
    {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
    }

    public void setClientProperties(final Map<String, Object> clientProperties)
    {
        _clientProperties = clientProperties;
    }

    public Map<String, Object> getClientProperties()
    {
        return _clientProperties;
    }
    
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
    }

    public void setKeyStoreType(String keyStoreType)
    {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyStoreType()
    {
        return keyStoreType;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    public void setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
    }

    public String getCertAlias()
    {
        return certAlias;
    }

    public void setCertAlias(String certAlias)
    {
        this.certAlias = certAlias;
    }

    public boolean isVerifyHostname()
    {
        return verifyHostname;
    }

    public void setVerifyHostname(boolean verifyHostname)
    {
        this.verifyHostname = verifyHostname;
    }
    
    public String getKeyManagerFactoryAlgorithm()
    {
        return keyManagerFactoryAlgorithm;
    }

    public void setKeyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm)
    {
        this.keyManagerFactoryAlgorithm = keyManagerFactoryAlgorithm;
    }

    public String getTrustManagerFactoryAlgorithm()
    {
        return trustManagerFactoryAlgorithm;
    }

    public void setTrustManagerFactoryAlgorithm(String trustManagerFactoryAlgorithm)
    {
        this.trustManagerFactoryAlgorithm = trustManagerFactoryAlgorithm;
    }

    public String getTrustStoreType()
    {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType)
    {
        this.trustStoreType = trustStoreType;
    }

    public String getClientCertificatePrivateKeyPath()
    {
        return _clientCertificatePrivateKeyPath;
    }

    public void setClientCertificatePrivateKeyPath(final String clientCertificatePrivateKeyPath)
    {
        _clientCertificatePrivateKeyPath = clientCertificatePrivateKeyPath;
    }

    public String getClientCertificatePath()
    {
        return _clientCertificatePath;
    }

    public void setClientCertificatePath(final String clientCertificatePath)
    {
        _clientCertificatePath = clientCertificatePath;
    }

    public String getClientCertificateIntermediateCertsPath()
    {
        return _clientCertificateIntermediateCertsPath;
    }

    public void setClientCertificateIntermediateCertsPath(final String clientCertificateIntermediateCertsPath)
    {
        _clientCertificateIntermediateCertsPath = clientCertificateIntermediateCertsPath;
    }

    public String getTrustedCertificatesFile()
    {
        return _trustedCertificatesFile;
    }

    public void setTrustedCertificatesFile(final String trustedCertificatesFile)
    {
        _trustedCertificatesFile = trustedCertificatesFile;
    }

    public String getEncryptionKeyStorePath()
    {
        return _encryptionKeyStorePath;
    }

    public void setEncryptionKeyStorePath(final String encryptionKeyStorePath)
    {
        _encryptionKeyStorePath = encryptionKeyStorePath;
    }

    public String getEncryptionKeyStorePassword()
    {
        return _encryptionKeyStorePassword;
    }

    public void setEncryptionKeyStorePassword(final String encryptionKeyStorePassword)
    {
        _encryptionKeyStorePassword = encryptionKeyStorePassword;
    }

    public String getEncryptionKeyStoreType()
    {
        return _encryptionKeyStoreType;
    }

    public void setEncryptionKeyStoreType(final String encryptionKeyStoreType)
    {
        _encryptionKeyStoreType = encryptionKeyStoreType;
    }

    public String getEncryptionKeyManagerFactoryAlgorithm()
    {
        return _encryptionKeyManagerFactoryAlgorithm;
    }

    public void setEncryptionKeyManagerFactoryAlgorithm(final String encryptionKeyManagerFactoryAlgorithm)
    {
        _encryptionKeyManagerFactoryAlgorithm = encryptionKeyManagerFactoryAlgorithm;
    }

    public String getEncryptionTrustManagerFactoryAlgorithm()
    {
        return _encryptionTrustManagerFactoryAlgorithm;
    }

    public void setEncryptionTrustManagerFactoryAlgorithm(final String encryptionTrustManagerFactoryAlgorithm)
    {
        _encryptionTrustManagerFactoryAlgorithm = encryptionTrustManagerFactoryAlgorithm;
    }

    public String getEncryptionTrustStorePath()
    {
        return _encryptionTrustStorePath;
    }

    public void setEncryptionTrustStorePath(final String encryptionTrustStorePath)
    {
        _encryptionTrustStorePath = encryptionTrustStorePath;
    }

    public String getEncryptionTrustStorePassword()
    {
        return _encryptionTrustStorePassword;
    }

    public void setEncryptionTrustStorePassword(final String encryptionTrustStorePassword)
    {
        _encryptionTrustStorePassword = encryptionTrustStorePassword;
    }

    public String getEncryptionTrustStoreType()
    {
        return _encryptionTrustStoreType;
    }

    public void setEncryptionTrustStoreType(final String encryptionTrustStoreType)
    {
        _encryptionTrustStoreType = encryptionTrustStoreType;
    }

    public String getEncryptionRemoteTrustStoreName()
    {
        return _encryptionRemoteTrustStoreName;
    }

    public void setEncryptionRemoteTrustStoreName(final String encryptionRemoteTrustStoreName)
    {
        _encryptionRemoteTrustStoreName = encryptionRemoteTrustStoreName;
    }

    public int getConnectTimeout()
    {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout)
    {
        this.connectTimeout = connectTimeout;
    }

    public int getReadBufferSize()
    {
        return readBufferSize;
    }

    public void setReadBufferSize(int readBufferSize)
    {
        this.readBufferSize = readBufferSize;
    }

    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }

    public void setWriteBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
    }

    public KeyManager[] getKeyManagers()
            throws GeneralSecurityException, IOException
    {
        if(getKeyStorePath() != null)
        {
            return SSLContextFactory.getKeyManagers(getKeyStorePath(),
                                                    getKeyStorePassword(),
                                                    getKeyStoreType(),
                                                    getKeyManagerFactoryAlgorithm(),
                                                    getCertAlias());
        }
        else if(getClientCertificatePrivateKeyPath() != null)
        {
            return getKeyManagers(getClientCertificatePrivateKeyPath(), getClientCertificatePath(), getClientCertificateIntermediateCertsPath(), getKeyManagerFactoryAlgorithm());
        }
        else
        {
            return null;
        }
    }

    public TrustManager[] getTrustManagers()
            throws GeneralSecurityException, IOException
    {
        if(getTrustStorePath() != null)
        {
            return SSLContextFactory.getTrustManagers(getTrustStorePath(),
                                                      getTrustStorePassword(),
                                                      getTrustStoreType(),
                                                      getTrustManagerFactoryAlgorithm());
        }
        else if(getTrustedCertificatesFile() != null)
        {
            return getTrustManagers(getTrustedCertificatesFile());
        }
        else
        {
            return null;
        }

    }

    private KeyManager[] getKeyManagers(String privateKeyFile,
                                        String certFile,
                                        String intermediateFile,
                                        String keyManagerFactoryAlgorithm) throws GeneralSecurityException, IOException
    {
        try (FileInputStream privateKeyStream = new FileInputStream(privateKeyFile);
             FileInputStream certFileStream = new FileInputStream(certFile))
        {
            PrivateKey privateKey = SSLUtil.readPrivateKey(privateKeyStream);
            X509Certificate[] certs = SSLUtil.readCertificates(certFileStream);
            if (intermediateFile != null)
            {
                try (FileInputStream intermediateFileStream = new FileInputStream(intermediateFile))
                {
                    List<X509Certificate> allCerts = new ArrayList<>(Arrays.asList(certs));
                    allCerts.addAll(Arrays.asList(SSLUtil.readCertificates(intermediateFileStream)));
                    certs = allCerts.toArray(new X509Certificate[allCerts.size()]);
                }
            }
            java.security.KeyStore inMemoryKeyStore =
                    java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

            byte[] bytes = new byte[64];
            char[] chars = new char[64];
            RANDOM.nextBytes(bytes);
            StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(bytes)).get(chars);
            inMemoryKeyStore.load(null, chars);
            inMemoryKeyStore.setKeyEntry("1", privateKey, chars, certs);


            return new KeyManager[]{new QpidClientX509KeyManager("1",
                                                                 inMemoryKeyStore,
                                                                 new String(chars),
                                                                 keyManagerFactoryAlgorithm)};
        }
    }

    private TrustManager[] getTrustManagers(String certFile) throws GeneralSecurityException, IOException
    {
        try(FileInputStream input = new FileInputStream(certFile))
        {
            X509Certificate[] certs = SSLUtil.readCertificates(input);
            java.security.KeyStore inMemoryKeyStore =
                    java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

            inMemoryKeyStore.load(null, null);
            int i = 1;
            for (Certificate cert : certs)
            {
                inMemoryKeyStore.setCertificateEntry(String.valueOf(i++), cert);
            }


            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(inMemoryKeyStore);
            return tmf.getTrustManagers();
        }
    }

    public interface RemoteStoreFinder
    {
        public KeyStore getKeyStore(String name) throws GeneralSecurityException, IOException;
    }

    public synchronized KeyStore getEncryptionTrustStore(final RemoteStoreFinder storeFinder) throws GeneralSecurityException, IOException
    {
        if(_encryptionTrustStore == null)
        {
            if (_encryptionTrustStorePath != null)
            {
                _encryptionTrustStore = SSLUtil.getInitializedKeyStore(getEncryptionTrustStorePath(),
                                                                       getEncryptionTrustStorePassword(),
                                                                       getEncryptionTrustStoreType());
            }
            else if(_encryptionRemoteTrustStoreName != null)
            {
                return storeFinder.getKeyStore(_encryptionRemoteTrustStoreName);
            }
        }
        return _encryptionTrustStore;
    }


    public synchronized KeyStore getEncryptionKeyStore() throws GeneralSecurityException, IOException
    {
        if(_encryptionKeyStore == null && _encryptionKeyStorePath != null)
        {
            _encryptionKeyStore = SSLUtil.getInitializedKeyStore(getEncryptionKeyStorePath(), getEncryptionKeyStorePassword(), getEncryptionKeyStoreType());
        }
        return _encryptionKeyStore;
    }
}
