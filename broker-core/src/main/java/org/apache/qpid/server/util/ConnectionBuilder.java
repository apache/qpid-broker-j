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
 */

package org.apache.qpid.server.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.transport.TransportException;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

public class ConnectionBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionBuilder.class);
    private final URL _url;
    private int _connectTimeout;
    private int _readTimeout;
    private TrustManager[] _trustMangers;
    private List<String> _tlsProtocolWhiteList;
    private List<String> _tlsProtocolBlackList;
    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;


    public ConnectionBuilder(final URL url)
    {
        _url = url;
    }

    public ConnectionBuilder setConnectTimeout(final int timeout)
    {
        _connectTimeout = timeout;
        return this;
    }

    public ConnectionBuilder setReadTimeout(final int readTimeout)
    {
        _readTimeout = readTimeout;
        return this;
    }

    public ConnectionBuilder setTrustMangers(final TrustManager[] trustMangers)
    {
        _trustMangers = trustMangers;
        return this;
    }

    public ConnectionBuilder setTlsProtocolWhiteList(final List<String> tlsProtocolWhiteList)
    {
        _tlsProtocolWhiteList = tlsProtocolWhiteList;
        return this;
    }

    public ConnectionBuilder setTlsProtocolBlackList(final List<String> tlsProtocolBlackList)
    {
        _tlsProtocolBlackList = tlsProtocolBlackList;
        return this;
    }

    public ConnectionBuilder setTlsCipherSuiteWhiteList(final List<String> tlsCipherSuiteWhiteList)
    {
        _tlsCipherSuiteWhiteList = tlsCipherSuiteWhiteList;
        return this;
    }

    public ConnectionBuilder setTlsCipherSuiteBlackList(final List<String> tlsCipherSuiteBlackList)
    {
        _tlsCipherSuiteBlackList = tlsCipherSuiteBlackList;
        return this;
    }

    public HttpURLConnection build() throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) _url.openConnection();
        connection.setConnectTimeout(_connectTimeout);
        connection.setReadTimeout(_readTimeout);

        if (_trustMangers != null && _trustMangers.length > 0)
        {
            HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
            final SSLContext sslContext;
            try
            {
                sslContext = SSLUtil.tryGetSSLContext();
                sslContext.init(null, _trustMangers, null);
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }

            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            httpsConnection.setSSLSocketFactory(socketFactory);
            httpsConnection.setHostnameVerifier(new HostnameVerifier()
            {
                @Override
                public boolean verify(final String hostname, final SSLSession sslSession)
                {
                    try
                    {
                        final Certificate cert = sslSession.getPeerCertificates()[0];
                        if (cert instanceof X509Certificate)
                        {
                            final X509Certificate x509Certificate = (X509Certificate) cert;
                            SSLUtil.verifyHostname(hostname, x509Certificate);
                            return true;
                        }
                        else
                        {
                            LOGGER.warn("Cannot verify peer's hostname as peer does not present a X509Certificate. "
                                        + "Presented certificate : {}", cert);
                        }
                    }
                    catch (SSLPeerUnverifiedException | TransportException e)
                    {
                        LOGGER.warn("Failed to verify peer's hostname (connecting to host {})", hostname, e);
                    }

                    return false;
                }
            });
        }

        if ((_tlsProtocolWhiteList != null && !_tlsProtocolWhiteList.isEmpty()) ||
            (_tlsProtocolBlackList != null && !_tlsProtocolBlackList.isEmpty()) ||
            (_tlsCipherSuiteWhiteList != null && !_tlsCipherSuiteWhiteList.isEmpty()) ||
            (_tlsCipherSuiteBlackList != null && !_tlsCipherSuiteBlackList.isEmpty()))
        {
            HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
            SSLSocketFactory originalSocketFactory = httpsConnection.getSSLSocketFactory();
            httpsConnection.setSSLSocketFactory(new CipherSuiteAndProtocolRestrictingSSLSocketFactory(originalSocketFactory,
                                                                                                      _tlsCipherSuiteWhiteList,
                                                                                                      _tlsCipherSuiteBlackList,
                                                                                                      _tlsProtocolWhiteList,
                                                                                                      _tlsProtocolBlackList));
        }
        return connection;
    }

}
