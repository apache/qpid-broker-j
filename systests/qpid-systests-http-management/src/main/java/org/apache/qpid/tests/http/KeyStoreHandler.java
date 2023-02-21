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
package org.apache.qpid.tests.http;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public class KeyStoreHandler
{
    private static final char[] PASSWORD = "password".toCharArray();

    public static void addCertificate(String alias)
    {
        try (final FileInputStream fis = new FileInputStream(keystore()))
        {
            final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(fis, PASSWORD);
            final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
            final X509Certificate cert = createCertificate(keyPair, alias);
            keystore.setKeyEntry(alias, keyPair.getPrivate(), null, new Certificate[]{ cert });
            try (OutputStream writeStream = new FileOutputStream(keystore()))
            {
                keystore.store(writeStream, PASSWORD);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void removeCertificate(String alias)
    {
        try (final FileInputStream fis = new FileInputStream(keystore()))
        {
            final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(fis, PASSWORD);
            if (keystore.containsAlias(alias))
            {
                keystore.deleteEntry(alias);
            }
            else
            {
                throw new IllegalStateException("Alias " + alias + " not found in trust store");
            }
            try (OutputStream writeStream = new FileOutputStream(keystore()))
            {
                keystore.store(writeStream, PASSWORD);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String keystore()
    {
        return System.getProperty("user.dir") + "/src/main/resources/java_broker_keystore.jks";
    }

    private static X509Certificate createCertificate(KeyPair keyPair, String subjectDN)
            throws OperatorCreationException, CertificateException, IOException
    {
        final Provider bcProvider = new BouncyCastleProvider();
        Security.addProvider(bcProvider);

        final long now = System.currentTimeMillis();
        final Date startDate = new Date(now);

        final X500NameBuilder builder = new X500NameBuilder(BCStyle.INSTANCE);
        builder.addRDN(BCStyle.CN, subjectDN);
        final X500Name dnName = builder.build();
        final BigInteger certSerialNumber = new BigInteger(Long.toString(now)); // <-- Using the current timestamp as the certificate serial number

        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.add(Calendar.YEAR, 1); // <-- 1 Yr validity

        final Date endDate = calendar.getTime();

        final String signatureAlgorithm = "SHA256WithRSA"; // <-- Use appropriate signature algorithm based on your keyPair algorithm.

        final ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm).build(keyPair.getPrivate());

        final JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(dnName, certSerialNumber, startDate, endDate, dnName, keyPair.getPublic());

        // Extensions --------------------------

        certBuilder.addExtension(new ASN1ObjectIdentifier("2.5.29.19"), false, new BasicConstraints(false)); // Basic Constraints is usually marked as critical.
        certBuilder.addExtension(new ASN1ObjectIdentifier("2.5.29.37"), false, new DERSequence(KeyPurposeId.id_kp_serverAuth));
        certBuilder.addExtension(new ASN1ObjectIdentifier("2.5.29.14"), false, keyPair.getPublic().getEncoded());

        // -------------------------------------

        return new JcaX509CertificateConverter().setProvider(bcProvider).getCertificate(certBuilder.build(contentSigner));
    }
}
