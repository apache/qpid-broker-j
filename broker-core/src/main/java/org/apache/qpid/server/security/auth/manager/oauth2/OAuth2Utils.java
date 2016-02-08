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
package org.apache.qpid.server.security.auth.manager.oauth2;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class OAuth2Utils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Utils.class);

    public static void setTrustedCertificates(final HttpsURLConnection connection,
                                              final TrustStore trustStore)
    {
        final SSLContext sslContext;
        try
        {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustStore.getTrustManagers(), null);
        }
        catch (GeneralSecurityException e)
        {
            throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
        }

        final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        connection.setSSLSocketFactory(socketFactory);
        connection.setHostnameVerifier(new HostnameVerifier()
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
        // TODO respect the tls protocols/cipher suite settings
    }

    public static String buildRequestQuery(final Map<String, String> requestBodyParameters)
    {
        try
        {
            final String charset = StandardCharsets.UTF_8.name();
            StringBuilder bodyBuilder = new StringBuilder();
            Iterator<Map.Entry<String, String>> iterator = requestBodyParameters.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                bodyBuilder.append(URLEncoder.encode(entry.getKey(), charset));
                bodyBuilder.append("=");
                bodyBuilder.append(URLEncoder.encode(entry.getValue(), charset));
                if (iterator.hasNext())
                {
                    bodyBuilder.append("&");
                }
            }
            return bodyBuilder.toString();
        }
        catch (UnsupportedEncodingException e)
        {
            throw new ServerScopedRuntimeException("Failed to encode as UTF-8", e);
        }
    }
}
