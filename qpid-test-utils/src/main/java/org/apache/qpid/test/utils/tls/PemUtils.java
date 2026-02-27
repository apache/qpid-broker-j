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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PemUtils
{
    private static final byte[] LINE_SEPARATOR_BYTES = new byte[]{'\r', '\n'};
    private static final String LINE_SEPARATOR = "\r\n";
    private static final String BEGIN_X_509_CRL = "-----BEGIN X509 CRL-----";
    private static final String END_X_509_CRL = "-----END X509 CRL-----";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final int PEM_LINE_LENGTH = 76;

    public static void saveCertificateAsPem(final Path path, final List<X509Certificate> certificates)
            throws IOException, CertificateEncodingException
    {
        final StringBuilder stringBuilder = new StringBuilder();
        for (final X509Certificate x509Certificate : certificates)
        {
            stringBuilder.append(toPEM(x509Certificate));
        }
        Files.writeString(path, stringBuilder.toString(), UTF_8);
    }

    public static void savePrivateKeyAsPem(final Path path, final PrivateKey key) throws IOException
    {
        Files.writeString(path, toPEM(key), UTF_8);
    }

    public static void saveCrlAsPem(final Path path, final X509CRL crl) throws IOException, CRLException
    {
        Files.writeString(path, toPEM(crl.getEncoded(), BEGIN_X_509_CRL, END_X_509_CRL), UTF_8);
    }

    public static String toPEM(final Certificate certificate) throws CertificateEncodingException
    {
        return toPEM(certificate.getEncoded(), BEGIN_CERTIFICATE, END_CERTIFICATE);
    }

    public static String toPEM(final PrivateKey key)
    {
        return toPEM(key.getEncoded(), BEGIN_PRIVATE_KEY, END_PRIVATE_KEY);
    }

    private static String toPEM(final byte[] bytes, final String header, final String footer)
    {
        return header + LINE_SEPARATOR +
                Base64.getMimeEncoder(PEM_LINE_LENGTH, LINE_SEPARATOR_BYTES).encodeToString(bytes) + LINE_SEPARATOR +
                footer + LINE_SEPARATOR;
    }
}
