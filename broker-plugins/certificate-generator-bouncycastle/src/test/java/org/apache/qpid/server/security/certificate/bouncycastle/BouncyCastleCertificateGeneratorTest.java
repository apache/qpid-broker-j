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
package org.apache.qpid.server.security.certificate.bouncycastle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.ProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.RuntimeOperatorException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.certificate.AbstractCertificateGeneratorContractTest;
import org.apache.qpid.server.security.certificate.CertificateGenerator;
import org.apache.qpid.server.security.certificate.CertificateGeneratorFactory;
import org.apache.qpid.server.security.certificate.CertificateRequest;

public class BouncyCastleCertificateGeneratorTest extends AbstractCertificateGeneratorContractTest
{
    private static final String BASIC_CONSTRAINTS_OID = "2.5.29.19";
    private static final String SUBJECT_ALTERNATIVE_NAME_OID = "2.5.29.17";

    @Override
    protected CertificateGenerator createGenerator()
    {
        return new BouncyCastleCertificateGeneratorFactory().create();
    }

    /** The contract allows further extensions, but this generator adds none, as before it was a plugin. */
    @Test
    public void onlyBasicConstraintsAndSubjectAlternativeNameExtensions() throws Exception
    {
        final X509Certificate certificate = createGenerator().generate(request(Set.of("localhost"))).certificate();

        assertEquals(Set.of(BASIC_CONSTRAINTS_OID), certificate.getCriticalExtensionOIDs());
        assertEquals(Set.of(SUBJECT_ALTERNATIVE_NAME_OID), certificate.getNonCriticalExtensionOIDs());
    }

    @Test
    public void onlyBasicConstraintsExtensionWithoutNames() throws Exception
    {
        final X509Certificate certificate = createGenerator().generate(request(Set.of())).certificate();

        assertEquals(Set.of(BASIC_CONSTRAINTS_OID), certificate.getCriticalExtensionOIDs());
        final Set<String> nonCritical = certificate.getNonCriticalExtensionOIDs();
        assertTrue(nonCritical == null || nonCritical.isEmpty(), "Unexpected non-critical extensions: " + nonCritical);
    }

    /** Bouncy Castle accepts signature algorithm aliases that the JCA providers do not, as before it was a plugin. */
    @Test
    public void signatureAlgorithmAlias() throws Exception
    {
        final Instant now = Instant.now();
        final CertificateRequest request = CertificateRequest.builder()
                .keyAlgorithm("RSA", 2048)
                .signatureAlgorithm("SHA256withRSAEncryption")
                .subject(new X500Principal("CN=Qpid"))
                .validity(now, now.plus(1, ChronoUnit.DAYS))
                .build();

        final X509Certificate certificate = createGenerator().generate(request).certificate();

        assertEquals("SHA256withRSA", certificate.getSigAlgName());
        certificate.verify(certificate.getPublicKey());
    }

    /** The JCA provider fails when the certificate is signed, after the signer is created. */
    @Test
    public void signingFailureReported()
    {
        final SignatureException failure = new SignatureException("signing failed");
        final CertificateGenerator generator = createGenerator();
        final ContentSigner signer = failingSigner(failure);
        try (final MockedConstruction<JcaContentSignerBuilder> ignored = mockConstruction(JcaContentSignerBuilder.class,
                (signerBuilder, context) -> when(signerBuilder.build(any())).thenReturn(signer)))
        {
            final SignatureException thrown =
                    assertThrows(SignatureException.class, () -> generator.generate(request(Set.of())));

            assertSame(failure, thrown);
        }
    }

    @Test
    public void certificateBuildFailureReported()
    {
        final CertIOException failure = new CertIOException("cannot encode extension");
        final CertificateGenerator generator = createGenerator();
        try (final MockedConstruction<JcaX509v3CertificateBuilder> ignored = failingCertificateBuilder(failure))
        {
            final CertificateException thrown =
                    assertThrows(CertificateException.class, () -> generator.generate(request(Set.of())));

            assertSame(failure, thrown.getCause());
        }
    }

    @Test
    public void incompatibleLibraryReported()
    {
        final NoSuchMethodError failure = new NoSuchMethodError("org.bouncycastle.cert.X509v3CertificateBuilder");
        final CertificateGenerator generator = createGenerator();
        try (final MockedConstruction<JcaX509v3CertificateBuilder> ignored = failingCertificateBuilder(failure))
        {
            final GeneralSecurityException thrown =
                    assertThrows(GeneralSecurityException.class, () -> generator.generate(request(Set.of())));

            assertSame(failure, thrown.getCause());
            assertTrue(thrown.getMessage().contains("incompatible"), thrown.getMessage());
        }
    }

    /** A JCA provider may report a failure unchecked, which the contract requires as GeneralSecurityException. */
    @Test
    public void providerFailureReported()
    {
        final ProviderException failure = new ProviderException("self-test failed");
        final CertificateGenerator generator = createGenerator();
        try (final MockedConstruction<JcaContentSignerBuilder> ignored = mockConstruction(JcaContentSignerBuilder.class,
                (signerBuilder, context) -> when(signerBuilder.build(any())).thenThrow(failure)))
        {
            final GeneralSecurityException thrown =
                    assertThrows(GeneralSecurityException.class, () -> generator.generate(request(Set.of())));

            assertSame(failure, thrown.getCause());
        }
    }

    /** The providers are the only record of the default JCA selection, so they are logged at info. */
    @Test
    public void logsProvidersAtInfo() throws Exception
    {
        final Logger logger = (Logger) LoggerFactory.getLogger(BouncyCastleCertificateGenerator.class);
        final Level level = logger.getLevel();
        final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try
        {
            assertNotNull(createGenerator().generate(request(Set.of())).certificate());

            final List<ILoggingEvent> events = appender.list.stream()
                    .filter(event -> event.getLevel() == Level.INFO)
                    .filter(event -> event.getMessage().startsWith("Generated self-signed certificate"))
                    .toList();
            assertEquals(1, events.size(), "Unexpected events: " + appender.list);
            final Object[] arguments = events.get(0).getArgumentArray();
            assertEquals(4, arguments.length);
            for (int i = 1; i < arguments.length; i++)
            {
                assertNotEquals("unknown", arguments[i], events.get(0).getFormattedMessage());
            }
        }
        finally
        {
            logger.setLevel(level);
            logger.detachAppender(appender);
        }
    }

    @Test
    public void securityExceptionUnwrapsJcaCause()
    {
        final SignatureException cause = new SignatureException("signing failed");

        assertSame(cause, BouncyCastleCertificateGenerator.securityException(
                new OperatorCreationException("cannot create signer", cause), "message"));
    }

    @Test
    public void securityExceptionWrapsOtherFailure()
    {
        final RuntimeOperatorException failure = new RuntimeOperatorException("signing failed");

        final GeneralSecurityException exception =
                BouncyCastleCertificateGenerator.securityException(failure, "Unable to sign certificate");

        assertEquals("Unable to sign certificate", exception.getMessage());
        assertSame(failure, exception.getCause());
    }

    @Test
    public void signatureProviderOfUnknownAlgorithm() throws Exception
    {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);

        assertEquals("unknown", BouncyCastleCertificateGenerator.signatureProvider("NoSuchSignatureAlgorithm",
                keyPairGenerator.generateKeyPair().getPrivate()));
    }

    /** The lookup only feeds a log message, so an unchecked failure must not escape either. */
    @Test
    public void signatureProviderUncheckedFailure() throws Exception
    {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);

        // Signature.getInstance throws NullPointerException for a null name
        assertEquals("unknown", BouncyCastleCertificateGenerator.signatureProvider(null,
                keyPairGenerator.generateKeyPair().getPrivate()));
    }

    @Test
    public void factoryIsDiscoverable()
    {
        final List<String> types = new ArrayList<>();
        new QpidServiceLoader().instancesOf(CertificateGeneratorFactory.class)
                               .forEach(factory -> types.add(factory.getType()));
        assertTrue(types.contains(BouncyCastleCertificateGeneratorFactory.TYPE),
                   "BouncyCastle factory not discovered: " + types);
        assertEquals(1, types.size(), "Unexpected factories: " + types);
    }

    /** A signer that fails as Bouncy Castle's JCA signer does when the provider cannot sign. */
    private static ContentSigner failingSigner(final SignatureException failure)
    {
        final ContentSigner signer = mock(ContentSigner.class);
        when(signer.getAlgorithmIdentifier())
                .thenReturn(new DefaultSignatureAlgorithmIdentifierFinder().find("SHA256withRSA"));
        when(signer.getOutputStream()).thenReturn(OutputStream.nullOutputStream());
        when(signer.getSignature()).thenThrow(new RuntimeOperatorException("exception obtaining signature", failure));
        return signer;
    }

    private static MockedConstruction<JcaX509v3CertificateBuilder> failingCertificateBuilder(final Throwable failure)
    {
        return mockConstruction(JcaX509v3CertificateBuilder.class, (builder, context) ->
                when(builder.addExtension(any(ASN1ObjectIdentifier.class), anyBoolean(), any(ASN1Encodable.class)))
                        .thenThrow(failure));
    }

    private static CertificateRequest request(final Set<String> dnsNames)
    {
        final Instant now = Instant.now();
        return CertificateRequest.builder()
                .keyAlgorithm("RSA", 2048)
                .signatureAlgorithm("SHA256withRSA")
                .subject(new X500Principal("CN=Qpid"))
                .validity(now, now.plus(1, ChronoUnit.DAYS))
                .dnsNames(dnsNames)
                .build();
    }
}
