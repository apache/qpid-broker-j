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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Comparator;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TlsResource extends ExternalResource
{
    private static final String PRIVATE_KEY_ALIAS = "private-key-alias";
    private static final String CERTIFICATE_ALIAS = "certificate-alias";
    private static final String SECRET = "secret";

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsResource.class);

    private Path _keystoreDirectory;

    private final String _privateKeyAlias;
    private final String _certificateAlias;
    private final String _secret;
    private final String _keyStoreType;

    public TlsResource()
    {
        this(PRIVATE_KEY_ALIAS, CERTIFICATE_ALIAS, SECRET, KeyStore.getDefaultType());
    }

    public TlsResource(final String privateKeyAlias,
                       final String certificateAlias,
                       final String secret,
                       final String defaultType)
    {
        _privateKeyAlias = privateKeyAlias;
        _certificateAlias = certificateAlias;
        _secret = secret;
        _keyStoreType = defaultType;
    }

    @Override
    public void before() throws Exception
    {
        final Path targetDir = FileSystems.getDefault().getPath("target");
        _keystoreDirectory = Files.createTempDirectory(targetDir, "test-tls-resources-");
        LOGGER.debug("Test keystore directory is created : '{}'", _keystoreDirectory);
    }

    @Override
    public void after()
    {
        try
        {
            Files.walk(_keystoreDirectory).sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(f -> {
                     if (!f.delete())
                     {
                         LOGGER.warn("Could not delete file at {}", f.getAbsolutePath());
                     }
                 });
        }
        catch (Exception e)
        {
            LOGGER.warn("Failure to clean up test resources", e);
        }
    }

    public String getSecret()
    {
        return _secret;
    }

    public char[] getSecretAsCharacters()
    {
        return _secret == null ? new char[]{} : _secret.toCharArray();
    }

    public String getPrivateKeyAlias()
    {
        return _privateKeyAlias;
    }

    public String getCertificateAlias()
    {
        return _certificateAlias;
    }


    public String getKeyStoreType()
    {
        return _keyStoreType;
    }


    public Path createKeyStore(KeyStoreEntry... entries) throws Exception
    {
        return createKeyStore(getKeyStoreType(), entries);
    }

    public Path createKeyStore(final String keyStoreType, final KeyStoreEntry... entries)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException
    {
        final KeyStore ks = TlsResourceHelper.createKeyStore(keyStoreType, getSecretAsCharacters(), entries);
        return saveKeyStore(keyStoreType, ks);
    }

    public String createKeyStoreAsDataUrl(KeyStoreEntry... entries) throws Exception
    {
        return TlsResourceHelper.createKeyStoreAsDataUrl(getKeyStoreType(), getSecretAsCharacters(), entries);
    }

    public Path createSelfSignedKeyStore(String dn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new PrivateKeyEntry(_privateKeyAlias,
                                                  keyCertPair.getPrivateKey(),
                                                  keyCertPair.getCertificate()));
    }

    public String createSelfSignedKeyStoreAsDataUrl(String dn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new PrivateKeyEntry(_privateKeyAlias,
                                                           keyCertPair.getPrivateKey(),
                                                           keyCertPair.getCertificate()));
    }

    public Path createSelfSignedTrustStore(final String dn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.getCertificate()));
    }

    public Path createSelfSignedTrustStore(final String dn, Instant from, Instant to) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn, from, to);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.getCertificate()));
    }

    public String createSelfSignedTrustStoreAsDataUrl(String dn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new CertificateEntry(_certificateAlias, keyCertPair.getCertificate()));
    }

    public Path createTrustStore(final String dn, KeyCertificatePair ca) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createKeyPairAndCertificate(dn, ca);
        final String keyStoreType = getKeyStoreType();
        return createKeyStore(keyStoreType, new CertificateEntry(_certificateAlias, keyCertPair.getCertificate()));
    }

    public Path createSelfSignedKeyStoreWithCertificate(final String dn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new PrivateKeyEntry(_privateKeyAlias,
                                                  keyCertPair.getPrivateKey(),
                                                  keyCertPair.getCertificate()),
                              new CertificateEntry(_certificateAlias, keyCertPair.getCertificate()));
    }

    public Path createCrl(final KeyCertificatePair caPair, final X509Certificate... certificate) throws CRLException
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);

        try
        {
            final Path pkFile = createFile(".crl");
            try (FileOutputStream out = new FileOutputStream(pkFile.toFile()))
            {
                TlsResourceHelper.saveCrlAsPem(out, crl);
            }
            return pkFile;
        }
        catch (IOException e)
        {
            throw new CRLException(e);
        }
    }

    public Path createCrlAsDer(final KeyCertificatePair caPair, final X509Certificate... certificate)
            throws CRLException, IOException
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        return saveBytes(crl.getEncoded(), ".crl");
    }

    public String createCrlAsDataUrl(final KeyCertificatePair caPair, final X509Certificate... certificate)
            throws CRLException
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        return TlsResourceHelper.getDataUrlForBytes(crl.getEncoded());
    }

    public Path savePrivateKeyAsPem(final PrivateKey privateKey) throws IOException
    {
        final Path pkFile = createFile(".pk.pem");
        try (FileOutputStream out = new FileOutputStream(pkFile.toFile()))
        {
            TlsResourceHelper.savePrivateKeyAsPem(out, privateKey);
        }
        return pkFile;
    }

    public Path saveCertificateAsPem(final X509Certificate... certificate)
            throws IOException, CertificateEncodingException
    {
        final Path certificateFile = createFile(".cer.pem");
        try (FileOutputStream out = new FileOutputStream(certificateFile.toFile()))
        {
            TlsResourceHelper.saveCertificateAsPem(out, certificate);
        }
        return certificateFile;
    }

    public Path savePrivateKeyAsDer(final PrivateKey privateKey) throws IOException
    {
        return saveBytes(privateKey.getEncoded(), ".pk.der");
    }

    public Path saveCertificateAsDer(final X509Certificate certificate) throws CertificateEncodingException, IOException
    {
        return saveBytes(certificate.getEncoded(), ".cer.der");
    }

    public Path createFile(String suffix) throws IOException
    {
        return Files.createTempFile(_keystoreDirectory, "tls", suffix);
    }

    private Path saveBytes(final byte[] bytes, final String extension) throws IOException
    {
        final Path pkFile = createFile(extension);
        try (FileOutputStream out = new FileOutputStream(pkFile.toFile()))
        {
            out.write(bytes);
        }
        return pkFile;
    }

    private Path saveKeyStore(final String keyStoreType, final KeyStore ks)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
    {
        final Path storeFile = createFile("." + keyStoreType);
        TlsResourceHelper.saveKeyStoreIntoFile(ks, getSecretAsCharacters(), storeFile.toFile());
        return storeFile;
    }
}
