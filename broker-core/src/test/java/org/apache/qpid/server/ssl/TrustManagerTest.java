/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.qpid.server.ssl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

public class TrustManagerTest extends UnitTestBase
{
    private static final String DEFAULT_TRUST_MANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    private static final String TEST_ALIAS = "test";
    private static final String DN_CA = "CN=MyRootCA,O=ACME,ST=Ontario,C=CA";
    private static final String DN_APP1 = "CN=app1@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_APP2 = "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_UNTRUSTED = "CN=untrusted_client";

    private static X509Certificate _ca;
    private static X509Certificate _app1;
    private static X509Certificate _app2;
    private static X509Certificate _untrusted;

    @BeforeAll
    public static void setUp() throws Exception
    {
        final KeyCertificatePair caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyPair keyPair1 = TlsResourceBuilder.createRSAKeyPair();
        final KeyPair keyPair2 = TlsResourceBuilder.createRSAKeyPair();
        final KeyCertificatePair untrustedKeyCertPair = TlsResourceBuilder.createSelfSigned(DN_UNTRUSTED);

        _ca = caPair.getCertificate();
        _app1 = TlsResourceBuilder.createCertificateForClientAuthorization(keyPair1, caPair, DN_APP1);
        _app2 = TlsResourceBuilder.createCertificateForClientAuthorization(keyPair2, caPair, DN_APP2);
        _untrusted = untrustedKeyCertPair.getCertificate();
    }

    /**
     * Tests that the QpidPeersOnlyTrustManager gives the expected behaviour when loaded separately
     * with the peer certificate and CA root certificate.
     */
    @Test
    public void testQpidPeersOnlyTrustManager() throws Exception
    {
        // peer manager is supposed to trust only clients which peers certificates
        // are directly in the store. CA signing will not be considered.
        final X509TrustManager peerManager = createPeerManager(_app1);

        // since peer manager contains the client's app1 certificate, the check should succeed
        assertDoesNotThrow(() -> peerManager.checkClientTrusted(new X509Certificate[]{_app1, _ca }, "RSA"),
                "Trusted client's validation against the broker's peer store manager failed.");

        // since peer manager does not contain the client's app2 certificate, the check should fail
        assertThrows(CertificateException.class,
                () -> peerManager.checkClientTrusted(new X509Certificate[]{_app2, _ca }, "RSA"),
                "Untrusted client's validation against the broker's peer store manager succeeded.");

        // now let's check that peer manager loaded with the CA certificate fails because
        // it does not have the clients certificate in it (though it does have a CA-cert that
        // would otherwise trust the client cert when using the regular trust manager).
        final X509TrustManager caPeerManager = createPeerManager(_ca);

        // since trust manager doesn't contain the client's app1 certificate, the check should fail
        // despite the fact that the truststore does have a CA that would otherwise trust the cert
        assertThrows(CertificateException.class,
                () -> caPeerManager.checkClientTrusted(new X509Certificate[]{_app1, _ca }, "RSA"),
                "Client's validation against the broker's peer store manager didn't fail.");

        // since  trust manager doesn't contain the client's app2 certificate, the check should fail
        // despite the fact that the truststore does have a CA that would otherwise trust the cert
        assertThrows(CertificateException.class,
                () -> caPeerManager.checkClientTrusted(new X509Certificate[]{_app2, _ca }, "RSA"),
                "Client's validation against the broker's peer store manager didn't fail.");
    }

    /**
     * Tests that the QpidMultipleTrustManager gives the expected behaviour when wrapping a
     * regular CA root certificate.
     */
    @Test
    public void testQpidMultipleTrustManagerWithRegularTrustStore() throws Exception
    {
        final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
        final X509TrustManager tm = createTrustManager(_ca);
        assertNotNull(tm, "The regular trust manager for the trust store was not found");

        mulTrustManager.addTrustManager(tm);

        // verify the CA-trusted app1 cert (should succeed)
        assertDoesNotThrow(() -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app1, _ca }, "RSA"),
                "Trusted client's validation against the broker's multi store manager failed.");

        // verify the CA-trusted app2 cert (should succeed)
        assertDoesNotThrow(() -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app2, _ca }, "RSA"),
                "Trusted client's validation against the broker's multi store manager failed.");

        // verify the untrusted cert (should fail)
        assertThrows(CertificateException.class,
                () -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_untrusted}, "RSA"),
                "Untrusted client's validation against the broker's multi store manager unexpectedly passed.");
    }

    /**
     * Tests that the QpidMultipleTrustManager gives the expected behaviour when wrapping a
     * QpidPeersOnlyTrustManager against the peer certificate
     */
    @Test
    public void testQpidMultipleTrustManagerWithPeerStore() throws Exception
    {
        final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
        final KeyStore ps = createKeyStore(_app1);
        final X509TrustManager tm = getX509TrustManager(ps);
        assertNotNull(tm, "The regular trust manager for the trust store was not found");

        mulTrustManager.addTrustManager(new QpidPeersOnlyTrustManager(ps, tm));

        // verify the trusted app1 cert (should succeed as the key is in the peerstore)
        assertDoesNotThrow(() -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app1, _ca }, "RSA"),
                "Trusted client's validation against the broker's multi store manager failed.");

        // verify the untrusted app2 cert (should fail as the key is not in the peerstore)
        assertThrows(CertificateException.class,
                () -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app2, _ca }, "RSA"),
                "Untrusted client's validation against the broker's multi store manager unexpectedly passed.");

        // verify the untrusted cert (should fail as the key is not in the peerstore)
        assertThrows(CertificateException.class,
                () -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_untrusted }, "RSA"),
                "Untrusted client's validation against the broker's multi store manager unexpectedly passed.");
    }

    /**
     * Tests that the QpidMultipleTrustManager gives the expected behaviour when wrapping a
     * QpidPeersOnlyTrustManager against the peer certificate, a regular TrustManager
     * against the CA root certificate.
     */
    @Test
    public void testQpidMultipleTrustManagerWithTrustAndPeerStores() throws Exception
    {
        final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();
        final KeyStore ts = createKeyStore(_ca);
        final X509TrustManager tm = getX509TrustManager(ts);
        assertNotNull(tm, "The regular trust manager for the trust store was not found");

        mulTrustManager.addTrustManager(tm);

        final KeyStore ps = createKeyStore(_app1);
        final X509TrustManager tm2 = getX509TrustManager(ts);
        assertNotNull(tm2, "The regular trust manager for the peer store was not found");
        mulTrustManager.addTrustManager(new QpidPeersOnlyTrustManager(ps, tm2));

        // verify the CA-trusted app1 cert (should succeed)
        assertDoesNotThrow(() -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app1, _ca }, "RSA"),
                "Trusted client's validation against the broker's multi store manager failed.");

        // verify the CA-trusted app2 cert (should succeed)
        assertDoesNotThrow(() -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_app2, _ca }, "RSA"),
                "Trusted client's validation against the broker's multi store manager failed.");

        // verify the untrusted cert (should fail)
        assertThrows(CertificateException.class,
                () -> mulTrustManager.checkClientTrusted(new X509Certificate[]{_untrusted }, "RSA"),
                "Untrusted client's validation against the broker's multi store manager unexpectedly passed.");
    }

    private KeyStore createKeyStore(final X509Certificate certificate) throws Exception
    {
        return TlsResourceHelper.createKeyStore(KeyStore.getDefaultType(), new char[]{},
                new CertificateEntry(TEST_ALIAS, certificate));
    }

    private X509TrustManager createTrustManager(final X509Certificate certificate) throws Exception
    {
        return getX509TrustManager(createKeyStore(certificate));
    }

    private X509TrustManager getX509TrustManager(final KeyStore ps) throws Exception
    {
        final TrustManagerFactory pmf = TrustManagerFactory.getInstance(DEFAULT_TRUST_MANAGER_ALGORITHM);
        pmf.init(ps);
        final TrustManager[] delegateTrustManagers = pmf.getTrustManagers();
        X509TrustManager trustManager = null;
        for (final TrustManager tm : delegateTrustManagers)
        {
            if (tm instanceof X509TrustManager)
            {
                trustManager = (X509TrustManager) tm;
            }
        }
        return trustManager;
    }

    private X509TrustManager createPeerManager(final X509Certificate certificate) throws Exception
    {
        final KeyStore ps = createKeyStore(certificate);
        final X509TrustManager tm = createTrustManager(certificate);
        return new QpidPeersOnlyTrustManager(ps, tm);
    }
}
