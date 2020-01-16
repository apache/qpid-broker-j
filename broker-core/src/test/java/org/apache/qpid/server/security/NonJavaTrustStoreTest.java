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

import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.test.utils.TestSSLConstants;

public class NonJavaTrustStoreTest extends UnitTestBase
{
    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();

    @Test
    public void testCreationOfTrustStoreFromValidCertificate() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, TestSSLConstants.BROKER_CRT);
        attributes.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);

        TrustStore trustStore = (TrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testChangeOfCrlInTrustStoreFromValidCertificate()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, TestSSLConstants.BROKER_CRT);
        attributes.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);

        TrustStore trustStore = (TrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, "/not/a/crl");

            trustStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                    message.contains("Unable to load certificate revocation list '/not/a/crl' for truststore 'myTestTrustStore'"));
        }

        assertEquals("Unexpected CRL path value after failed change",
                TestSSLConstants.CA_CRL, trustStore.getCertificateRevocationListUrl());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL_EMPTY);

        trustStore.setAttributes(changedAttributes);

        assertEquals("Unexpected CRL path value after change that is expected to be successful",
                TestSSLConstants.CA_CRL_EMPTY, trustStore.getCertificateRevocationListUrl());
    }

    @Test
    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, TestSSLConstants.CLIENT_EXPIRED_CRT);
        attributes.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");

        TrustStore trustStore = (TrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = SSLUtil.getInitializedKeyStore(TestSSLConstants.CLIENT_EXPIRED_KEYSTORE,
                                                              TestSSLConstants.PASSWORD,
                                                              TestSSLConstants.JAVA_KEYSTORE_TYPE);
        String alias = clientStore.aliases().nextElement();
        X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        try
        {
            trustManager.checkClientTrusted(new X509Certificate[] {certificate}, "NULL");
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
    public void testCreationOfTrustStoreFromNonCertificate()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, TestSSLConstants.BROKER_CSR);
        attributes.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Cannot load certificate(s)");
    }

    @Test
    public void testCreationOfTrustStoreFromValidCertificate_MissingCrlFile()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaTrustStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATES_URL, TestSSLConstants.BROKER_CRT);
        attributes.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(NonJavaTrustStore.CERTIFICATE_REVOCATION_LIST_URL, "/not/a/crl");

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Unable to load certificate revocation list '/not/a/crl' for truststore 'myTestTrustStore'");
    }
}
