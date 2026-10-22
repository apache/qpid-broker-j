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

package org.apache.qpid.server.security.certificate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Behaviour expected from every {@link CertificateGenerator}, as described by
 * {@link CertificateGenerator#generate(CertificateRequest)}. Generator implementations extend this class and provide
 * the generator under test.
 */
public abstract class AbstractCertificateGeneratorContractTest extends UnitTestBase
{
    private static final String BASIC_CONSTRAINTS_OID = "2.5.29.19";
    private static final String SUBJECT_ALTERNATIVE_NAME_OID = "2.5.29.17";
    private static final int SAN_DNS_NAME = 2;
    private static final int SAN_IP_ADDRESS = 7;
    private static final String X500_NAME = "CN=Qpid";
    private static final String BOUNCY_CASTLE_PROVIDER = "BC";
    // RFC 5280 limits serial numbers to 20 octets, whose most significant bit is the sign bit, so a positive serial
    // number has at most 159 bits
    private static final int MAX_SERIAL_NUMBER_BITS = 159;

    /**
     * @return the generator under test
     * @throws GeneralSecurityException if the generator cannot be created
     */
    protected abstract CertificateGenerator createGenerator() throws GeneralSecurityException;

    @Test
    void generatedPairIsComplete() throws Exception
    {
        final GeneratedCertificate pair = createGenerator().generate(request("RSA", "SHA256withRSA", 2048));

        assertNotNull(pair);
        assertNotNull(pair.privateKey());
        assertNotNull(pair.certificate());
    }

    @Test
    void rsaCertificate() throws Exception
    {
        final GeneratedCertificate pair = createGenerator().generate(request("RSA", "SHA256withRSA", 2048));
        final X509Certificate certificate = pair.certificate();

        assertEquals("RSA", pair.privateKey().getAlgorithm());
        assertInstanceOf(RSAPublicKey.class, certificate.getPublicKey());
        assertEquals(2048, ((RSAPublicKey) certificate.getPublicKey()).getModulus().bitLength());
        assertTrue("SHA256withRSA".equalsIgnoreCase(certificate.getSigAlgName()), certificate.getSigAlgName());
        assertKeyPairMatches(pair, "SHA256withRSA");
    }

    @Test
    void ecCertificate() throws Exception
    {
        final GeneratedCertificate pair = createGenerator().generate(request("EC", "SHA256withECDSA", 256));
        final X509Certificate certificate = pair.certificate();

        assertEquals("EC", pair.privateKey().getAlgorithm());
        assertInstanceOf(ECPublicKey.class, certificate.getPublicKey());
        assertEquals(256, ((ECPublicKey) certificate.getPublicKey()).getParams().getCurve().getField().getFieldSize());
        assertTrue("SHA256withECDSA".equalsIgnoreCase(certificate.getSigAlgName()), certificate.getSigAlgName());
        assertKeyPairMatches(pair, "SHA256withECDSA");
    }

    @Test
    void requestedKeyLength() throws Exception
    {
        // The default key lengths depend on the provider and, for the JDK ones, on the JDK version (2048-bit RSA and
        // 256-bit EC before JDK 19, 3072 and 384 from it), so lengths that are no provider's default show a
        // generator ignoring the key length. Generating other keys with the same generator in between catches state
        // left from an earlier request.
        final CertificateGenerator generator = createGenerator();
        generator.generate(request("RSA", "SHA256withRSA", 2048));

        final GeneratedCertificate ec = generator.generate(request("EC", "SHA512withECDSA", 521));
        final GeneratedCertificate rsa = generator.generate(request("RSA", "SHA256withRSA", 4096));

        final X509Certificate ecCertificate = ec.certificate();
        assertInstanceOf(ECPublicKey.class, ecCertificate.getPublicKey());
        assertEquals(521, ((ECPublicKey) ecCertificate.getPublicKey()).getParams().getCurve().getField()
                .getFieldSize());
        assertTrue("SHA512withECDSA".equalsIgnoreCase(ecCertificate.getSigAlgName()), ecCertificate.getSigAlgName());
        ecCertificate.verify(ecCertificate.getPublicKey());
        assertKeyPairMatches(ec, "SHA512withECDSA");
        assertInstanceOf(RSAPublicKey.class, rsa.certificate().getPublicKey());
        assertEquals(4096, ((RSAPublicKey) rsa.certificate().getPublicKey()).getModulus().bitLength());
    }

    @Test
    void selfSigned() throws Exception
    {
        final X509Certificate certificate = createGenerator().generate(request("RSA", "SHA256withRSA", 2048))
                .certificate();

        assertEquals(new X500Principal(X500_NAME), certificate.getSubjectX500Principal());
        assertEquals(new X500Principal(X500_NAME), certificate.getIssuerX500Principal());
        certificate.verify(certificate.getPublicKey());
        assertEquals(3, certificate.getVersion());
    }

    @Test
    void validity() throws Exception
    {
        final Instant notBefore = Instant.now();
        final Instant notAfter = notBefore.plus(30, ChronoUnit.DAYS);
        final CertificateRequest request = requestBuilder("RSA", "SHA256withRSA", 2048)
                .validity(notBefore, notAfter)
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertEquals(request.getNotBefore(), certificate.getNotBefore().toInstant());
        assertEquals(request.getNotAfter(), certificate.getNotAfter().toInstant());
    }

    @Test
    void validityAfter2049() throws Exception
    {
        // X.509 encodes dates from 2050 as GeneralizedTime instead of UTCTime
        final Instant notBefore = Instant.parse("2049-12-31T23:59:59Z");
        final Instant notAfter = Instant.parse("2060-01-01T00:00:00Z");
        final CertificateRequest request = requestBuilder("RSA", "SHA256withRSA", 2048)
                .validity(notBefore, notAfter)
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertEquals(notBefore, certificate.getNotBefore().toInstant());
        assertEquals(notAfter, certificate.getNotAfter().toInstant());
    }

    @Test
    void basicConstraintsEndEntityAndCritical() throws Exception
    {
        final X509Certificate certificate = createGenerator().generate(request("RSA", "SHA256withRSA", 2048))
                .certificate();

        assertEquals(-1, certificate.getBasicConstraints(), "Certificate must not be a CA certificate");
        assertTrue(certificate.getCriticalExtensionOIDs().contains(BASIC_CONSTRAINTS_OID));
        assertFalse(certificate.hasUnsupportedCriticalExtension(),
                    "Certificate must not have critical extensions the JDK does not support");
    }

    @Test
    void subjectAlternativeNames() throws Exception
    {
        final Set<InetAddress> addresses = Set.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1"));
        final CertificateRequest request = requestBuilder("RSA", "SHA256withRSA", 2048)
                .dnsNames(Set.of("localhost", "broker.example.com", "not a name", "-invalid"))
                .addresses(addresses)
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertTrue(certificate.getNonCriticalExtensionOIDs().contains(SUBJECT_ALTERNATIVE_NAME_OID));
        assertEquals(Set.of("localhost", "broker.example.com"), sanDnsNames(certificate));
        assertEquals(addresses, sanAddresses(certificate));
    }

    @Test
    void jcaStandardSignatureAlgorithmNames() throws Exception
    {
        // JCA standard RSA signature names, which every generator must accept
        final CertificateGenerator generator = createGenerator();
        for (final String signatureAlgorithm : List.of("SHA256withRSA", "SHA384withRSA", "SHA512withRSA"))
        {
            final X509Certificate certificate = generator.generate(request("RSA", signatureAlgorithm, 2048))
                    .certificate();
            assertTrue(signatureAlgorithm.equalsIgnoreCase(certificate.getSigAlgName()), certificate.getSigAlgName());
        }
    }

    @Test
    void scopedIpv6AddressIncluded() throws Exception
    {
        final byte[] linkLocal = InetAddress.getByName("fe80::1").getAddress();
        final InetAddress scoped = Inet6Address.getByAddress(null, linkLocal, 1);
        final CertificateRequest request = requestBuilder("RSA", "SHA256withRSA", 2048)
                .addresses(Set.of(scoped))
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertEquals(Set.of(InetAddress.getByAddress(linkLocal)), sanAddresses(certificate));
    }

    @Test
    void noSubjectAlternativeNameExtensionWithoutNames() throws Exception
    {
        final CertificateRequest request = requestBuilder("RSA", "SHA256withRSA", 2048)
                .dnsNames(Set.of("not a name"))
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertNull(certificate.getSubjectAlternativeNames());
    }

    @Test
    void parsesWithJdkCertificateFactory() throws Exception
    {
        final X509Certificate certificate = createGenerator().generate(request("RSA", "SHA256withRSA", 2048))
                .certificate();

        final X509Certificate parsed = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(certificate.getEncoded()));

        assertArrayEquals(certificate.getEncoded(), parsed.getEncoded());
    }

    @Test
    void uniqueKeysAndSerialNumbers() throws Exception
    {
        final CertificateGenerator generator = createGenerator();
        final GeneratedCertificate first = generator.generate(request("RSA", "SHA256withRSA", 2048));
        final GeneratedCertificate second = generator.generate(request("RSA", "SHA256withRSA", 2048));

        assertNotEquals(first.certificate().getSerialNumber(), second.certificate().getSerialNumber());
        assertValidSerialNumber(first.certificate().getSerialNumber());
        assertValidSerialNumber(second.certificate().getSerialNumber());
        assertNotEquals(first.certificate().getPublicKey(), second.certificate().getPublicKey());
    }

    @Test
    void unsupportedKeyAlgorithm() throws Exception
    {
        final CertificateGenerator generator = createGenerator();
        final CertificateRequest request = request("NOPE", "SHA256withRSA", 2048);

        final NoSuchAlgorithmException thrown =
                assertThrows(NoSuchAlgorithmException.class, () -> generator.generate(request));
        assertMessageContains(thrown, "NOPE");
    }

    @Test
    void unsupportedKeyLength() throws Exception
    {
        // no EC curve has this size, so every provider rejects it, the JDK and Bouncy Castle ones unchecked with
        // InvalidParameterException; the smallest RSA key length differs between providers
        final CertificateGenerator generator = createGenerator();
        final CertificateRequest request = request("EC", "SHA256withECDSA", 100);

        assertThrows(GeneralSecurityException.class, () -> generator.generate(request));
    }

    @Test
    void unsupportedSignatureAlgorithm() throws Exception
    {
        final CertificateGenerator generator = createGenerator();
        final CertificateRequest request = request("RSA", "NOPEwithRSA", 2048);

        final NoSuchAlgorithmException thrown =
                assertThrows(NoSuchAlgorithmException.class, () -> generator.generate(request));
        assertMessageContains(thrown, "NOPEwithRSA");
    }

    @Test
    void signatureAlgorithmNotMatchingKeyAlgorithm() throws Exception
    {
        final CertificateGenerator generator = createGenerator();
        final CertificateRequest request = request("RSA", "SHA256withECDSA", 2048);

        assertThrows(GeneralSecurityException.class, () -> generator.generate(request));
    }

    @Test
    void doesNotRegisterSecurityProviders() throws Exception
    {
        // Test utilities register Bouncy Castle for the whole JVM, which would hide a generator registering it.
        // Only registration while the generator is created or used is detected: registration in a static
        // initializer happens in whichever test loads the generator class first.
        final Provider[] original = Security.getProviders();
        Security.removeProvider(BOUNCY_CASTLE_PROVIDER);
        try
        {
            final List<String> before = providerNames();

            createGenerator().generate(request("RSA", "SHA256withRSA", 2048));

            assertEquals(before, providerNames());
        }
        finally
        {
            // restores the providers exactly, so that a generator changing them does not affect other tests
            providerNames().forEach(Security::removeProvider);
            Arrays.stream(original).forEach(Security::addProvider);
        }
    }

    private static void assertValidSerialNumber(final BigInteger serialNumber)
    {
        assertTrue(serialNumber.signum() > 0, "Serial number must be positive: " + serialNumber);
        assertTrue(serialNumber.bitLength() <= MAX_SERIAL_NUMBER_BITS,
                   "Serial number must fit in 20 octets: " + serialNumber);
    }

    private static void assertMessageContains(final Exception exception, final String expected)
    {
        final String message = exception.getMessage();
        assertTrue(message != null && message.contains(expected),
                   "Message should name '" + expected + "': " + message);
    }

    private static List<String> providerNames()
    {
        return Arrays.stream(Security.getProviders()).map(Provider::getName).toList();
    }

    private static void assertKeyPairMatches(final GeneratedCertificate pair, final String signatureAlgorithm)
            throws GeneralSecurityException
    {
        final byte[] data = "qpid".getBytes(StandardCharsets.UTF_8);
        final Signature signer = Signature.getInstance(signatureAlgorithm);
        signer.initSign(pair.privateKey());
        signer.update(data);
        final byte[] signature = signer.sign();

        final Signature verifier = Signature.getInstance(signatureAlgorithm);
        verifier.initVerify(pair.certificate().getPublicKey());
        verifier.update(data);
        assertTrue(verifier.verify(signature), "Private key does not match the certificate public key");
    }

    private static Set<String> sanDnsNames(final X509Certificate certificate) throws Exception
    {
        final Set<String> names = new HashSet<>();
        for (final List<?> entry : subjectAlternativeNames(certificate))
        {
            if ((Integer) entry.get(0) == SAN_DNS_NAME)
            {
                names.add((String) entry.get(1));
            }
        }
        return names;
    }

    private static Set<InetAddress> sanAddresses(final X509Certificate certificate) throws Exception
    {
        final Set<InetAddress> addresses = new HashSet<>();
        for (final List<?> entry : subjectAlternativeNames(certificate))
        {
            if ((Integer) entry.get(0) == SAN_IP_ADDRESS)
            {
                addresses.add(InetAddress.getByName((String) entry.get(1)));
            }
        }
        return addresses;
    }

    private static Collection<List<?>> subjectAlternativeNames(final X509Certificate certificate) throws Exception
    {
        final Collection<List<?>> names = certificate.getSubjectAlternativeNames();
        return (names == null) ? List.of() : names;
    }

    private static CertificateRequest request(final String keyAlgorithm,
                                              final String signatureAlgorithm,
                                              final int keyLength)
    {
        return requestBuilder(keyAlgorithm, signatureAlgorithm, keyLength).dnsNames(Set.of("localhost")).build();
    }

    private static CertificateRequest.Builder requestBuilder(final String keyAlgorithm,
                                                             final String signatureAlgorithm,
                                                             final int keyLength)
    {
        final Instant now = Instant.now();
        return CertificateRequest.builder()
                .keyAlgorithm(keyAlgorithm, keyLength)
                .signatureAlgorithm(signatureAlgorithm)
                .subject(new X500Principal(X500_NAME))
                .validity(now, now.plus(365, ChronoUnit.DAYS));
    }
}
