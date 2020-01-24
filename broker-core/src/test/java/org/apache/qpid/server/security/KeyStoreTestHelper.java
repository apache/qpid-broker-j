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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KeyStoreTestHelper
{
    public static KeyStore saveKeyStore(final String alias,
                                        final X509Certificate certificate,
                                        final char[] pass,
                                        final File file)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final KeyStore ks = createEmptyKeyStore();
        ks.setCertificateEntry(alias, certificate);
        saveKeyStore(ks, pass, file);
        return ks;
    }

    public static KeyStore saveKeyStore(final SSLUtil.KeyCertPair keyCertPair,
                                        final String keyAlias,
                                        final String certificateAlias,
                                        final char[] pass,
                                        final File file)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final KeyStore ks = createKeyStore(keyCertPair, keyAlias, certificateAlias, pass);
        saveKeyStore(ks, pass, file);
        return ks;
    }


    public static SSLUtil.KeyCertPair generateSelfSigned(final String cn)
            throws IllegalAccessException, InvocationTargetException, InstantiationException
    {
        return SSLUtil.generateSelfSignedCertificate("RSA",
                                                     "SHA256WithRSA",
                                                     2048,
                                                     Instant.now()
                                                            .minus(1, ChronoUnit.DAYS)
                                                            .toEpochMilli(),
                                                     Duration.of(365, ChronoUnit.DAYS)
                                                             .getSeconds(),
                                                     cn,
                                                     Collections.emptySet(),
                                                     Collections.emptySet());
    }

    public static void checkExceptionThrownDuringKeyStoreCreation(ConfiguredObjectFactory factory, Broker broker,
                                                              Class keystoreClass, Map<String, Object> attributes,
                                                              String expectedExceptionMessage)
    {
        try
        {
            factory.create(keystoreClass, attributes, broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            final String message = e.getMessage();
            assertTrue("Exception text not as expected:" + message,
                    message.contains(expectedExceptionMessage));

        }
    }


    private static File saveKeyStore(final KeyStore ks, final char[] pass, final File storeFile)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
    {
        try (FileOutputStream fos = new FileOutputStream(storeFile))
        {
            ks.store(fos, pass);
        }
        return storeFile;
    }

    private static KeyStore createKeyStore(final SSLUtil.KeyCertPair keyCertPair,
                                           final String keyAlias,
                                           final String certificateAlias,
                                           final char[] pass)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final KeyStore ks = createEmptyKeyStore();
        ks.setCertificateEntry(certificateAlias, keyCertPair.getCertificate());
        ks.setKeyEntry(keyAlias,
                       keyCertPair.getPrivateKey(),
                       pass,
                       new X509Certificate[]{keyCertPair.getCertificate()});
        return ks;
    }

    private static KeyStore createEmptyKeyStore()
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        return ks;
    }
}
