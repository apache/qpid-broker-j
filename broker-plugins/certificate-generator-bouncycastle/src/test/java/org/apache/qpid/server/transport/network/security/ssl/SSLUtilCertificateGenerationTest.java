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
package org.apache.qpid.server.transport.network.security.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.certificate.CertificateGenerators;
import org.apache.qpid.server.security.certificate.CertificateGeneratorsTestHelper;
import org.apache.qpid.server.security.certificate.bouncycastle.BouncyCastleCertificateGeneratorFactory;
import org.apache.qpid.test.utils.UnitTestBase;

/** Certificate generation through the {@link SSLUtil} facade, with the Bouncy Castle generator installed. */
public class SSLUtilCertificateGenerationTest extends UnitTestBase
{
    @BeforeEach
    public void setUp()
    {
        CertificateGeneratorsTestHelper.reset();
    }

    @AfterEach
    public void tearDown()
    {
        CertificateGeneratorsTestHelper.reset();
    }

    @Test
    public void generatorSelectedByProperty() throws Exception
    {
        setTestSystemProperty(CertificateGenerators.PROPERTY, BouncyCastleCertificateGeneratorFactory.TYPE);

        CertificateGenerators.initialise();

        assertTrue(SSLUtil.canGenerateCerts());
        assertEquals("BouncyCastleCertificateGenerator", CertificateGenerators.get().getClass().getSimpleName());
    }

    @Test
    public void canGenerateCerts()
    {
        assertTrue(SSLUtil.canGenerateCerts());
    }

    @Test
    public void generateSelfSignedCertificate() throws Exception
    {
        final String keyAlgorithm = "RSA";
        final String signatureAlgorithm = "SHA256withRSA";
        final int keyLength = 2048;

        final Set<InetAddress> addresses = Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                .flatMap(networkInterface -> networkInterface.getInterfaceAddresses().stream())
                .map(InterfaceAddress::getAddress)
                .collect(Collectors.toSet());

        final Set<String> dnsNames = addresses.stream()
                .map(address -> (address.getHostName() != null) ? address.getHostName()
                                                                : address.getCanonicalHostName())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        final long startTime = System.currentTimeMillis();
        final long endTime = Instant.ofEpochMilli(startTime).plus(365, ChronoUnit.DAYS).toEpochMilli();

        final SSLUtil.KeyCertPair keyCertPair = SSLUtil.generateSelfSignedCertificate(keyAlgorithm,
                signatureAlgorithm, keyLength, startTime, endTime, "CN=Qpid", dnsNames, addresses);

        assertNotNull(keyCertPair);
        assertNotNull(keyCertPair.getCertificate());
        assertNotNull(keyCertPair.getPrivateKey());

        assertEquals(keyAlgorithm, keyCertPair.getPrivateKey().getAlgorithm());
        assertTrue(signatureAlgorithm.equalsIgnoreCase(keyCertPair.getCertificate().getSigAlgName()));
    }

    @Test
    public void generateSelfSignedCertificateWithoutNames() throws Exception
    {
        final long startTime = System.currentTimeMillis();
        final long endTime = Instant.ofEpochMilli(startTime).plus(1, ChronoUnit.DAYS).toEpochMilli();

        final SSLUtil.KeyCertPair keyCertPair = SSLUtil.generateSelfSignedCertificate("RSA", "SHA256withRSA", 2048,
                startTime, endTime, "CN=Qpid", null, null);

        assertNull(keyCertPair.getCertificate().getSubjectAlternativeNames());
    }

    @Test
    public void invalidParametersReportedAsGeneralSecurityException()
    {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 1000;

        assertThrows(GeneralSecurityException.class, () -> SSLUtil.generateSelfSignedCertificate("RSA",
                "SHA256withRSA", 2048, startTime, endTime, "not an X.500 name", Set.of(), Set.of()));
        assertThrows(GeneralSecurityException.class, () -> SSLUtil.generateSelfSignedCertificate("RSA",
                "SHA256withRSA", 2048, endTime, startTime, "CN=Qpid", Set.of(), Set.of()));
        assertThrows(GeneralSecurityException.class, () -> SSLUtil.generateSelfSignedCertificate("RSA",
                "SHA256withRSA", 0, startTime, endTime, "CN=Qpid", Set.of(), Set.of()));
        final GeneralSecurityException nullName = assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.generateSelfSignedCertificate("RSA", "SHA256withRSA", 2048, startTime, endTime, null,
                                                            Set.of(), Set.of()));
        assertTrue(nullName.getMessage().endsWith("is null"), nullName.getMessage());
    }
}
