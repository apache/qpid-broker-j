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
package org.apache.qpid.server.transport;

import java.security.KeyStore;
import java.security.cert.Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

final class TlsTestContext
{
    private TlsTestContext()
    {
        // utility class has private constructor
    }

    static SSLContext create() throws Exception
    {
        final KeyCertificatePair certificate = TlsResourceBuilder.createSelfSigned("CN=localhost");
        final char[] password = "test-password".toCharArray();
        final KeyStore keys = KeyStore.getInstance("PKCS12");
        keys.load(null, password);
        keys.setKeyEntry("server", certificate.privateKey(), password, new Certificate[]{certificate.certificate()});
        final KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keys, password);
        final TrustManagerFactory trustManagers = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(keys);
        final SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
        return context;
    }
}
