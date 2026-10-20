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

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidParameterException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.stream.Stream;

import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.RuntimeOperatorException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.certificate.CertificateGenerator;
import org.apache.qpid.server.security.certificate.CertificateRequest;
import org.apache.qpid.server.security.certificate.GeneratedCertificate;

/**
 * Builds self-signed certificates with the Bouncy Castle certificate API.
 * <p>
 * Bouncy Castle is used only to assemble and encode the certificate. Key pair generation, signing, random numbers
 * and certificate parsing use the default JCA provider selection; no Bouncy Castle provider is created or registered.
 */
final class BouncyCastleCertificateGenerator implements CertificateGenerator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BouncyCastleCertificateGenerator.class);

    private static final boolean IS_CA = false;
    private static final boolean CRITICAL = true;

    /**
     * Uses the Bouncy Castle code the generator needs without performing any cryptographic operation.
     *
     * @throws LinkageError if a Bouncy Castle library is missing from the classpath or incompatible
     */
    static void checkLibraries()
    {
        new DefaultSignatureAlgorithmIdentifierFinder().find("SHA256withRSA");
        new GeneralNames(new GeneralName[] { new GeneralName(GeneralName.dNSName, "localhost"),
                                             new GeneralName(GeneralName.iPAddress, new DEROctetString(new byte[4])) });
        new BasicConstraints(false);
        new JcaContentSignerBuilder("SHA256withRSA");
        new JcaX509CertificateConverter();
        // loads the builder class without building a certificate
        LOGGER.debug("Bouncy Castle certificate builder {} is available", JcaX509v3CertificateBuilder.class.getName());
    }

    @Override
    public GeneratedCertificate generate(final CertificateRequest request) throws GeneralSecurityException
    {
        try
        {
            return generateCertificate(request);
        }
        catch (LinkageError e)
        {
            // a Bouncy Castle class is missing or incompatible with the one this plugin was built against
            throw new GeneralSecurityException("The Bouncy Castle libraries on the classpath are incomplete or "
                                               + "incompatible: " + e, e);
        }
    }

    private GeneratedCertificate generateCertificate(final CertificateRequest request) throws GeneralSecurityException
    {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(request.getKeyAlgorithm());
        try
        {
            keyPairGenerator.initialize(request.getKeyLength());
        }
        catch (InvalidParameterException e)
        {
            throw new InvalidAlgorithmParameterException(String.format("Unsupported key length %d for '%s'",
                    request.getKeyLength(), request.getKeyAlgorithm()), e);
        }

        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final PrivateKey privateKey = keyPair.getPrivate();

        final ContentSigner contentSigner = createContentSigner(request.getSignatureAlgorithm(), privateKey);

        final GeneralName[] subjectAlternativeNames = Stream.concat(request.getDnsNames().stream()
                    .map(dnsName -> new GeneralName(GeneralName.dNSName, dnsName)),
                request.getAddresses().stream()
                    // the raw bytes, because Bouncy Castle cannot parse the textual form of a scoped IPv6 address
                    // (fe80::1%1); the scope is local to the host and is not part of the certificate
                    .map(address -> new GeneralName(GeneralName.iPAddress, new DEROctetString(address.getAddress()))))
                    .toArray(GeneralName[]::new);

        // RFC 5280 requires a positive serial number
        final BigInteger serialNumber = new BigInteger(64, new SecureRandom()).add(BigInteger.ONE);

        final X509Certificate certificate;
        try
        {
            final X509v3CertificateBuilder certificateBuilder =
                    new JcaX509v3CertificateBuilder(request.getSubject(), serialNumber,
                                                    Date.from(request.getNotBefore()), Date.from(request.getNotAfter()),
                                                    request.getSubject(), keyPair.getPublic())
                        .addExtension(Extension.basicConstraints, CRITICAL, new BasicConstraints(IS_CA));
            // RFC 5280 does not allow an empty subject alternative name extension
            if (subjectAlternativeNames.length > 0)
            {
                certificateBuilder.addExtension(Extension.subjectAlternativeName, false,
                                                new GeneralNames(subjectAlternativeNames));
            }

            // no provider is set, so the certificate is parsed by the default JCA CertificateFactory
            certificate = new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));
        }
        catch (CertIOException e)
        {
            throw new CertificateException("Unable to build certificate", e);
        }
        catch (RuntimeOperatorException e)
        {
            // thrown when the JCA provider fails to sign the certificate
            throw securityException(e, "Unable to sign certificate");
        }

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Generated self-signed certificate '{}': key pair provider '{}', signature provider '{}', "
                         + "certificate factory provider '{}'",
                         request.getSubject().getName(),
                         keyPairGenerator.getProvider().getName(),
                         // the JCA name of the signature, as the requested name may be an alias the JCA does not know
                         signatureProvider(certificate.getSigAlgName(), privateKey),
                         CertificateFactory.getInstance("X.509").getProvider().getName());
        }

        return new GeneratedCertificate(privateKey, certificate);
    }

    private static ContentSigner createContentSigner(final String signatureAlgorithm, final PrivateKey privateKey)
            throws GeneralSecurityException
    {
        try
        {
            // Bouncy Castle resolves the name, so that aliases such as SHA256withRSAEncryption, which the JCA
            // providers may not know, are accepted. It does so in build(), which throws IllegalArgumentException
            // for an unknown name, so build() has to be inside this try.
            return new JcaContentSignerBuilder(signatureAlgorithm).build(privateKey);
        }
        catch (IllegalArgumentException e)
        {
            throw new NoSuchAlgorithmException("Unknown signature algorithm '" + signatureAlgorithm + "'", e);
        }
        catch (OperatorCreationException e)
        {
            throw securityException(e, "Unable to create signer for '" + signatureAlgorithm + "'");
        }
    }

    /**
     * The JCA exception a Bouncy Castle operator exception wraps, or a new one wrapping it if there is none.
     *
     * @param e Bouncy Castle operator exception
     * @param message message of the new exception, if one is created
     * @return the wrapped JCA exception, or a new exception wrapping {@code e}
     */
    // visible for testing
    static GeneralSecurityException securityException(final Exception e, final String message)
    {
        return (e.getCause() instanceof GeneralSecurityException cause)
                ? cause
                : new GeneralSecurityException(message, e);
    }

    /**
     * The provider the default JCA selection uses for this algorithm and key, for diagnostics only.
     *
     * @param signatureAlgorithm JCA name of the signature algorithm
     * @param privateKey key to sign with
     * @return provider name, or {@code unknown} if no provider supports the algorithm and key
     */
    // visible for testing
    static String signatureProvider(final String signatureAlgorithm, final PrivateKey privateKey)
    {
        try
        {
            final Signature signature = Signature.getInstance(signatureAlgorithm);
            signature.initSign(privateKey);
            return signature.getProvider().getName();
        }
        catch (GeneralSecurityException e)
        {
            return "unknown";
        }
    }
}
