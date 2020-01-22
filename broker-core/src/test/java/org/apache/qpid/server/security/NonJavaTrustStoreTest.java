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

package org.apache.qpid.server.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.UnitTestBase;

public class NonJavaTrustStoreTest extends UnitTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String NAME = "myTestTrustStore";
    private static final String NON_JAVA_TRUST_STORE = "NonJavaTrustStore";
    private static final String DN_FOO = "CN=foo";
    private static final String DN_CA = "CN=CA";
    private static final String DN_BAR = "CN=bar";
    private static final String NOT_A_CRL = "/not/a/crl";

    @Test
    public void testCreationOfTrustStoreWithoutCRL() throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(DN_FOO);
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsPem(keyCertPair.getCertificate());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, certificateFile.toFile().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);

        TrustStore<?> trustStore = createTestTrustStore(attributes);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }


    @Test
    public void testCreationOfTrustStoreFromValidCertificate() throws Exception
    {
        final CertificateAndCrl<File> data = generateCertificateAndCrl();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, data.getCertificate().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, data.getCrl().getAbsolutePath());

        TrustStore<?> trustStore = createTestTrustStore(attributes);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testChangeOfCrlInTrustStoreFromValidCertificate() throws Exception
    {
        final CertificateAndCrl<File> data = generateCertificateAndCrl();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, data.getCertificate().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, data.getCrl().getAbsolutePath());

        TrustStore<?> trustStore = createTestTrustStore(attributes);

        try
        {
            trustStore.setAttributes(Collections.singletonMap(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                                                              NOT_A_CRL));
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                       message.contains(String.format(
                               "Unable to load certificate revocation list '%s' for truststore '%s'",
                               NOT_A_CRL,
                               NAME)));
        }

        assertEquals("Unexpected CRL path value after failed change",
                     data.getCrl().getAbsolutePath(), trustStore.getCertificateRevocationListUrl());

        final Path emptyCrl = TLS_RESOURCE.createCrl(data.getCa());
        trustStore.setAttributes(Collections.singletonMap(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                                                          emptyCrl.toFile().getAbsolutePath()));

        assertEquals("Unexpected CRL path value after change that is expected to be successful",
                     emptyCrl.toFile().getAbsolutePath(), trustStore.getCertificateRevocationListUrl());
    }

    @Test
    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        final KeyCertificatePair keyCertPair = createExpiredCertificate();
        final Path certificatePath = TLS_RESOURCE.saveCertificateAsPem(keyCertPair.getCertificate());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, certificatePath.toFile().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);

        TrustStore<?> trustStore = createTestTrustStore(attributes);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        try
        {
            trustManager.checkClientTrusted(new X509Certificate[]{keyCertPair.getCertificate()}, "NULL");
            fail("Exception not thrown");
        }
        catch (CertificateException e)
        {
            if (e instanceof CertificateExpiredException || "Certificate expired".equals(e.getMessage()))
            {
                // IBMJSSE2 does not throw CertificateExpiredException, it throws a CertificateException
                // PASS
            }
            else
            {
                throw e;
            }
        }
    }

    @Test
    public void testCreationOfTrustStoreWithoutCertificate() throws Exception
    {
        final CertificateAndCrl<File> data = generateCertificateAndCrl();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, data.getCrl().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                                                                      "Cannot load certificate(s)");
    }

    @Test
    public void testCreationOfTrustStoreFromValidCertificate_MissingCrlFile() throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(DN_FOO);
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsPem(keyCertPair.getCertificate());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, NAME);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, certificateFile.toFile().getAbsolutePath());
        attributes.put(NonJavaTrustStore.TYPE, NON_JAVA_TRUST_STORE);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, NOT_A_CRL);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                                                                      String.format(
                                                                              "Unable to load certificate revocation list '%s' for truststore '%s'",
                                                                              NOT_A_CRL,
                                                                              NAME));
    }

    private KeyCertificatePair createExpiredCertificate() throws Exception
    {
        final Instant from = Instant.now().minus(10, ChronoUnit.DAYS);
        final Instant to = Instant.now().minus(5, ChronoUnit.DAYS);
        return TlsResourceBuilder.createSelfSigned(DN_FOO, from, to);
    }

    @SuppressWarnings("unchecked")
    private NonJavaTrustStore<?> createTestTrustStore(final Map<String, Object> attributes)
    {
        return (NonJavaTrustStore<?>) FACTORY.create(TrustStore.class, attributes, BROKER);
    }

    private CertificateAndCrl<File> generateCertificateAndCrl() throws Exception
    {
        final KeyCertificatePair caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyCertificatePair keyCertPair1 = TlsResourceBuilder.createKeyPairAndCertificate(DN_FOO, caPair);
        final KeyCertificatePair keyCertPair2 = TlsResourceBuilder.createKeyPairAndCertificate(DN_BAR, caPair);
        final Path clrFile =
                TLS_RESOURCE.createCrl(caPair, keyCertPair1.getCertificate(), keyCertPair2.getCertificate());
        final Path caCertificateFile = TLS_RESOURCE.saveCertificateAsPem(caPair.getCertificate());
        return new CertificateAndCrl<>(caCertificateFile.toFile(), clrFile.toFile(), caPair);
    }

    private static class CertificateAndCrl<T>
    {
        private T _certificate;
        private T _crl;
        private KeyCertificatePair _ca;

        private CertificateAndCrl(final T certificate, final T crl, KeyCertificatePair ca)
        {
            _certificate = certificate;
            _crl = crl;
            _ca = ca;
        }

        T getCertificate()
        {
            return _certificate;
        }

        T getCrl()
        {
            return _crl;
        }

        KeyCertificatePair getCa()
        {
            return _ca;
        }
    }
}
