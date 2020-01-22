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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class TlsResourceHelper
{
    private static final byte[] LINE_SEPARATOR = new byte[]{'\r', '\n'};
    private static final String BEGIN_X_509_CRL = "-----BEGIN X509 CRL-----";
    private static final String END_X_509_CRL = "-----END X509 CRL-----";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final int PEM_LINE_LENGTH = 76;

    public static KeyStore createKeyStore(final String keyStoreType, char[] secret, final KeyStoreEntry... entries)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException
    {
        final KeyStore ks = createKeyStoreOfType(keyStoreType);
        for (KeyStoreEntry e : entries)
        {
            e.addEntryToKeyStore(ks, secret);
        }
        return ks;
    }

    public static String createKeyStoreAsDataUrl(final String keyStoreType,  char[] secret, KeyStoreEntry... entries) throws Exception
    {
        final KeyStore ks = createKeyStore(keyStoreType, secret, entries);
        return toDataUrl(ks, secret);
    }

    public static KeyStore createKeyStoreOfType(final String keyStoreType)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final KeyStore ks = KeyStore.getInstance(keyStoreType);
        ks.load(null, null);
        return ks;
    }

    public static void saveKeyStoreIntoFile(final KeyStore ks, final char[] secret, final File storeFile)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
    {
        try (FileOutputStream fos = new FileOutputStream(storeFile))
        {
            ks.store(fos, secret);
        }
    }

    public static String toDataUrl(final KeyStore ks, char[] secret)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
    {
        final String result;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream())
        {
            ks.store(os, secret);
            result = getDataUrlForBytes(os.toByteArray());
        }
        return result;
    }

    public static String getDataUrlForBytes(final byte[] bytes)
    {
        return new StringBuilder("data:;base64,").append(Base64.getEncoder().encodeToString(bytes)).toString();
    }

    public static SecretKey createAESSecretKey() throws NoSuchAlgorithmException
    {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        return keyGen.generateKey();
    }

    public static void saveBytesAsPem(final byte[] bytes, final String header, final String footer, final OutputStream out)
            throws IOException
    {
        out.write(header.getBytes(UTF_8));
        out.write(LINE_SEPARATOR);
        out.write(Base64.getMimeEncoder(PEM_LINE_LENGTH, LINE_SEPARATOR).encode(bytes));
        out.write(LINE_SEPARATOR);
        out.write(footer.getBytes(UTF_8));
        out.write(LINE_SEPARATOR);
    }

    public static void saveCertificateAsPem(final OutputStream os, final X509Certificate... certificate) throws IOException,
                                                                                                         CertificateEncodingException
    {
        for (X509Certificate b : certificate)
        {
            saveBytesAsPem(b.getEncoded(), BEGIN_CERTIFICATE, END_CERTIFICATE, os);
        }
    }

    public static void savePrivateKeyAsPem(final OutputStream os, final PrivateKey key) throws IOException
    {
        saveBytesAsPem(key.getEncoded(), BEGIN_PRIVATE_KEY, END_PRIVATE_KEY, os);
    }

    public static void saveCrlAsPem(final OutputStream os, final X509CRL crl) throws CRLException, IOException
    {
        saveBytesAsPem(crl.getEncoded(), BEGIN_X_509_CRL, END_X_509_CRL, os);
    }


    public static String toPEM(final Certificate pub) throws CertificateEncodingException
    {
        return toPEM(pub.getEncoded(), BEGIN_CERTIFICATE, END_CERTIFICATE);
    }

    public static String toPEM(final PrivateKey key)
    {
        return toPEM(key.getEncoded(), BEGIN_PRIVATE_KEY, END_PRIVATE_KEY);
    }

    private static String toPEM(final byte[] bytes, final String header, final String footer)
    {
        final StringBuilder pem = new StringBuilder();
        pem.append(header).append(new String(LINE_SEPARATOR, UTF_8));
        pem.append(Base64.getMimeEncoder(PEM_LINE_LENGTH, LINE_SEPARATOR).encodeToString(bytes));
        pem.append(new String(LINE_SEPARATOR, UTF_8)).append(footer).append(new String(LINE_SEPARATOR, UTF_8));
        return pem.toString();
    }
}
