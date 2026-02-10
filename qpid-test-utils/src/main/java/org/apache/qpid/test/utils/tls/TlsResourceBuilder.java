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
package org.apache.qpid.test.utils.tls;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.RFC4519Style;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.CRLNumber;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public class TlsResourceBuilder
{
    private static final int RSA_KEY_SIZE = 2048;
    private static final int VALIDITY_DURATION = 365;
    private static final String BC_PROVIDER = BouncyCastleProvider.PROVIDER_NAME;
    private static final String KEY_ALGORITHM_RSA = "RSA";
    private static final String SIGNATURE_ALGORITHM_SHA_512_WITH_RSA = "SHA512WithRSA";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    static
    {
        if (Security.getProvider(BC_PROVIDER) == null)
        {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private TlsResourceBuilder()
    {
        super();
    }

    public static KeyPair createRSAKeyPair()
    {
        KeyPairGenerator keyPairGenerator;
        try
        {
            keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM_RSA);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalStateException("RSA generator is not found", e);
        }

        keyPairGenerator.initialize(RSA_KEY_SIZE, SECURE_RANDOM);
        return keyPairGenerator.genKeyPair();
    }

    public static KeyCertificatePair createKeyPairAndRootCA(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createKeyPairAndRootCA(dn, createValidityPeriod());
    }

    public static KeyCertificatePair createKeyPairAndIntermediateCA(final String dn,
                                                                    final KeyCertificatePair rootCA,
                                                                    final String crlUri)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createKeyPairAndIntermediateCA(dn, createValidityPeriod(), rootCA, crlUri);
    }

    public static KeyCertificatePair createSelfSigned(final String dn,
                                                      final Instant validFrom,
                                                      final Instant validTo,
                                                      final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createSelfSigned(dn, ValidityPeriod.of(validFrom, validTo), alternativeName);
    }

    public static KeyCertificatePair createSelfSigned(final String dn, final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createSelfSigned(dn, createValidityPeriod(), alternativeName);
    }

    public static KeyCertificatePair createKeyPairAndCertificate(final String dn,
                                                                 final KeyCertificatePair ca,
                                                                 final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createKeyPairAndCertificate(dn, createValidityPeriod(), ca, alternativeName);
    }

    public static X509Certificate createCertificate(final KeyPair keyPair,
                                                    final KeyCertificatePair ca,
                                                    final String dn,
                                                    final Instant from,
                                                    final Instant to,
                                                    final AlternativeName... alternativeNames)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createCertificate(keyPair, ca, dn, ValidityPeriod.of(from, to), alternativeNames, createKeyUsageExtension());
    }


    public static X509Certificate createCertificateForClientAuthorization(final KeyPair keyPair,
                                                                          final KeyCertificatePair ca,
                                                                          final String dn,
                                                                          final AlternativeName... alternativeNames)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createCertificate(keyPair, ca, dn, createValidityPeriod(), alternativeNames,
                createExtendedUsageExtension(new ExtendedKeyUsage(new KeyPurposeId[]{KeyPurposeId.id_kp_clientAuth})),
                createAuthorityKeyExtension(ca.certificate().getPublicKey()),
                createSubjectKeyExtension(keyPair.getPublic()));
    }

    public static X509Certificate createCertificateForServerAuthorization(final KeyPair keyPair,
                                                                          final KeyCertificatePair ca,
                                                                          final String dn,
                                                                          final AlternativeName... alternativeNames)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createCertificate(keyPair, ca, dn, createValidityPeriod(), alternativeNames,
                createExtendedUsageExtension(new ExtendedKeyUsage(new KeyPurposeId[]{KeyPurposeId.id_kp_serverAuth})),
                createAuthorityKeyExtension(ca.certificate().getPublicKey()),
                createSubjectKeyExtension(keyPair.getPublic()));
    }

    public static X509Certificate createCertificateWithCrlDistributionPoint(final KeyPair keyPair,
                                                                            final KeyCertificatePair caPair,
                                                                            final String dn,
                                                                            final String crlUri)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createCertificate(keyPair, caPair, dn, createValidityPeriod(), null, createKeyUsageExtension(), createDistributionPointExtension(crlUri));
    }

    private static X509Certificate createCertificate(final KeyPair keyPair,
                                                     final KeyCertificatePair ca,
                                                     final String dn,
                                                     final ValidityPeriod validityPeriod,
                                                     final AlternativeName[] alternativeNames,
                                                     final Extension... extensions)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                ca.certificate(),
                generateSerialNumber(),
                Date.from(validityPeriod.from()),
                Date.from(validityPeriod.to()),
                x500Name(dn),
                keyPair.getPublic());

        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        for (Extension e : extensions)
        {
            builder.addExtension(e);
        }
        if (alternativeNames != null && alternativeNames.length > 0)
        {
            builder.addExtension(createAlternateNamesExtension(alternativeNames));
        }
        return buildX509Certificate(builder, ca.privateKey());
    }

    private static X509Certificate createSelfSignedCertificate(final KeyPair keyPair,
                                                               final String dn,
                                                               final ValidityPeriod period,
                                                               final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                x500Name(dn),
                generateSerialNumber(),
                Date.from(period.from()),
                Date.from(period.to()),
                x500Name(dn),
                keyPair.getPublic());
        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        builder.addExtension(createKeyUsageExtension());
        builder.addExtension(createSubjectKeyExtension(keyPair.getPublic()));
        if (alternativeName != null && alternativeName.length > 0)
        {
            builder.addExtension(createAlternateNamesExtension(alternativeName));
        }
        return buildX509Certificate(builder, keyPair.getPrivate());
    }

    static X509CRL createCertificateRevocationList(final KeyCertificatePair ca, X509Certificate... certificate)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final X500Name issuerName = X500Name.getInstance(RFC4519Style.INSTANCE,
                                                         ca.certificate()
                                                           .getSubjectX500Principal()
                                                           .getEncoded());

        final Instant nextUpdate = Instant.now().plus(10, ChronoUnit.DAYS);

        final Date now = new Date();
        final X509v2CRLBuilder crlBuilder = new X509v2CRLBuilder(issuerName, now);
        crlBuilder.setNextUpdate(Date.from(nextUpdate));

        for (X509Certificate c : certificate)
        {
            crlBuilder.addCRLEntry(c.getSerialNumber(), now, CRLReason.privilegeWithdrawn);
        }

        crlBuilder.addExtension(createAuthorityKeyExtension(ca.certificate().getPublicKey()));
        crlBuilder.addExtension(Extension.cRLNumber, false, new CRLNumber(generateSerialNumber()));

        final ContentSigner contentSigner = createContentSigner(ca.privateKey());
        final X509CRLHolder crl = crlBuilder.build(contentSigner);

        return new JcaX509CRLConverter().getCRL(crl);
    }

    private static X509Certificate createRootCACertificate(final KeyPair keyPair,
                                                           final String dn,
                                                           final ValidityPeriod validityPeriod)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                x500Name(dn),
                generateSerialNumber(),
                Date.from(validityPeriod.from()),
                Date.from(validityPeriod.to()),
                x500Name(dn),
                keyPair.getPublic());

        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
        builder.addExtension(createSubjectKeyExtension(keyPair.getPublic()));
        builder.addExtension(createAuthorityKeyExtension(keyPair.getPublic()));
        return buildX509Certificate(builder, keyPair.getPrivate());
    }

    private static X509Certificate generateIntermediateCertificate(final KeyPair keyPair,
                                                                   final KeyCertificatePair rootCA,
                                                                   final String dn,
                                                                   final ValidityPeriod validityPeriod,
                                                                   final String crlUri)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                rootCA.certificate(),
                generateSerialNumber(),
                Date.from(validityPeriod.from()),
                Date.from(validityPeriod.to()),
                x500Name(dn),
                keyPair.getPublic());
        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));
        builder.addExtension(createSubjectKeyExtension(keyPair.getPublic()));
        builder.addExtension(createAuthorityKeyExtension(rootCA.certificate().getPublicKey()));
        if (crlUri != null)
        {
            builder.addExtension(createDistributionPointExtension(crlUri));
        }

        return buildX509Certificate(builder, rootCA.privateKey());
    }

    private static KeyCertificatePair createKeyPairAndRootCA(final String dn,
                                                             final ValidityPeriod validityPeriod)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final KeyPair keyPair = createRSAKeyPair();
        final X509Certificate rootCA = createRootCACertificate(keyPair, dn, validityPeriod);
        return new KeyCertificatePair(keyPair.getPrivate(), rootCA);
    }

    private static KeyCertificatePair createKeyPairAndIntermediateCA(final String dn,
                                                                     final ValidityPeriod validityPeriod,
                                                                     final KeyCertificatePair rootCA,
                                                                     final String crlUri)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final KeyPair keyPair = createRSAKeyPair();
        final X509Certificate intermediateCA = generateIntermediateCertificate(keyPair, rootCA, dn, validityPeriod, crlUri);
        return new KeyCertificatePair(keyPair.getPrivate(), intermediateCA);
    }

    private static KeyCertificatePair createKeyPairAndCertificate(final String dn,
                                                                  final ValidityPeriod validityPeriod,
                                                                  final KeyCertificatePair ca,
                                                                  final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final KeyPair keyPair = createRSAKeyPair();
        final X509Certificate certificate = createCertificate(keyPair, ca, dn, validityPeriod, alternativeName);
        return new KeyCertificatePair(keyPair.getPrivate(), certificate);
    }

    private static X509Certificate createCertificate(final KeyPair keyPair,
                                                     final KeyCertificatePair ca,
                                                     final String dn,
                                                     final ValidityPeriod validityPeriod,
                                                     final AlternativeName... alternativeNames)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        return createCertificate(keyPair, ca, dn, validityPeriod, alternativeNames, createKeyUsageExtension());
    }

    private static KeyCertificatePair createSelfSigned(final String dn,
                                                       final ValidityPeriod validityPeriod,
                                                       final AlternativeName... alternativeName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        final KeyPair keyPair = createRSAKeyPair();
        final X509Certificate certificate = createSelfSignedCertificate(keyPair, dn, validityPeriod, alternativeName);
        return new KeyCertificatePair(keyPair.getPrivate(), certificate);
    }

    private static ValidityPeriod createValidityPeriod()
    {
        return ValidityPeriod.fromYesterday(Duration.ofDays(VALIDITY_DURATION));
    }

    private static Extension createAuthorityKeyExtension(final PublicKey publicKey)
            throws GeneralSecurityException, IOException
    {
        return new Extension(Extension.authorityKeyIdentifier,
                             false,
                             new JcaX509ExtensionUtils().createAuthorityKeyIdentifier(publicKey).getEncoded());
    }

    private static Extension createSubjectKeyExtension(final PublicKey publicKey)
            throws GeneralSecurityException, IOException
    {
        return new Extension(Extension.subjectKeyIdentifier,
                             false,
                             new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey).getEncoded());
    }

    private static Extension createExtendedUsageExtension(final ExtendedKeyUsage extendedKeyUsage) throws IOException
    {
        return new Extension(Extension.extendedKeyUsage, false, extendedKeyUsage.getEncoded());
    }

    private static Extension createKeyUsageExtension()
    {
        final int flags = KeyUsage.digitalSignature | KeyUsage.nonRepudiation | KeyUsage.keyEncipherment;
        final KeyUsage keyUsage = new KeyUsage(flags);
        return new Extension(Extension.keyUsage, false, keyUsage.getBytes());
    }

    private static Extension createDistributionPointExtension(final String crlUri) throws IOException
    {
        final GeneralName generalName = new GeneralName(GeneralName.uniformResourceIdentifier, crlUri);
        final DistributionPointName pointName = new DistributionPointName(new GeneralNames(generalName));
        final DistributionPoint[] points = new DistributionPoint[]{new DistributionPoint(pointName, null, null)};
        return new Extension(Extension.cRLDistributionPoints, false, new CRLDistPoint(points).getEncoded());
    }

    private static Extension createAlternateNamesExtension(final AlternativeName[] alternativeName) throws IOException
    {
        final GeneralName[] generalNames = Arrays.stream(alternativeName)
                .map(an -> new GeneralName(an.type().generalNameTag(), an.value()))
                .toArray(GeneralName[]::new);
        return new Extension(Extension.subjectAlternativeName, false, new GeneralNames(generalNames).getEncoded());
    }

    private static BigInteger generateSerialNumber()
    {
        return new BigInteger(64, SECURE_RANDOM);
    }

    private static X509Certificate buildX509Certificate(final X509v3CertificateBuilder builder, final PrivateKey pk)
            throws GeneralSecurityException, OperatorCreationException
    {
        final ContentSigner contentSigner = createContentSigner(pk);
        return new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner));
    }

    private static ContentSigner createContentSigner(final PrivateKey privateKey) throws OperatorCreationException
    {
        return new JcaContentSignerBuilder(SIGNATURE_ALGORITHM_SHA_512_WITH_RSA)
                .setProvider(BC_PROVIDER)
                .build(privateKey);
    }

    private static X500Name x500Name(final String dn)
    {
        return new X500Name(RFC4519Style.INSTANCE, dn);
    }
}
