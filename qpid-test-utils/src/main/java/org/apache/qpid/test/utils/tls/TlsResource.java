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
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.bouncycastle.operator.OperatorCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test helper that creates and manages TLS-related resources like key/trust stores and PEM/DER files.
 * Temporary files are created under a dedicated directory and removed on {@link #close()}.
 * After close, the instance must not be reused.
 */
public class TlsResource implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TlsResource.class);

    private static final String PRIVATE_KEY_ALIAS = "private-key-alias";
    private static final String CERTIFICATE_ALIAS = "certificate-alias";
    private static final String SECRET = "secret";

    private Path _keystoreDirectory;
    private final String _privateKeyAlias;
    private final String _certificateAlias;
    private final char[] _secret;
    private final String _keyStoreType;
    private boolean _closed;

    /**
     * Creates a {@link TlsResource} with default aliases, secret, and key store type.
     */
    public TlsResource()
    {
        this(PRIVATE_KEY_ALIAS, CERTIFICATE_ALIAS, SECRET, KeyStore.getDefaultType());
    }

    /**
     * Creates a {@link TlsResource} with custom aliases, secret, and key store type.
     */
    public TlsResource(final String privateKeyAlias,
                       final String certificateAlias,
                       final String secret,
                       final String defaultType)
    {
        _privateKeyAlias = privateKeyAlias;
        _certificateAlias = certificateAlias;
        _secret = secret == null ? null : secret.toCharArray();
        _keyStoreType = defaultType;
    }

    /**
     * Removes temporary files and clears the in-memory secret.
     */
    @Override
    public void close()
    {
        if (_closed)
        {
            return;
        }
        _closed = true;
        deleteFiles();
        clearSecret();
        _keystoreDirectory = null;
    }

    /**
     * Returns the secret as a string, or {@code null} if not set.
     */
    public String getSecret()
    {
        assertOpen();
        return _secret == null ? null : new String(_secret);
    }

    /**
     * Returns a defensive copy of the secret as {@code char[]}.
     */
    public char[] getSecretAsCharacters()
    {
        assertOpen();
        return _secret == null ? new char[]{} : _secret.clone();
    }

    /**
     * Returns the default private key alias.
     */
    public String getPrivateKeyAlias()
    {
        assertOpen();
        return _privateKeyAlias;
    }

    /**
     * Returns the default certificate alias.
     */
    public String getCertificateAlias()
    {
        assertOpen();
        return _certificateAlias;
    }

    /**
     * Returns the key store type used by default.
     */
    public String getKeyStoreType()
    {
        assertOpen();
        return _keyStoreType;
    }

    /**
     * Creates a key store file using the default key store type.
     */
    public Path createKeyStore(final KeyStoreEntry... entries) throws GeneralSecurityException, IOException
    {
        assertOpen();
        return createKeyStore(getKeyStoreType(), entries);
    }

    /**
     * Creates a key store file using the given key store type.
     */
    public Path createKeyStore(final String keyStoreType, final KeyStoreEntry... entries)
            throws GeneralSecurityException, IOException
    {
        assertOpen();
        final KeyStore keyStore = TlsResourceHelper.createKeyStore(keyStoreType, getSecretAsCharacters(), entries);
        return saveKeyStore(keyStoreType, keyStore);
    }

    /**
     * Creates a key store as a data URL using the default key store type.
     */
    public String createKeyStoreAsDataUrl(final KeyStoreEntry... entries)
            throws GeneralSecurityException, IOException
    {
        assertOpen();
        return TlsResourceHelper.createKeyStoreAsDataUrl(getKeyStoreType(), getSecretAsCharacters(), entries);
    }

    /**
     * Creates a self-signed key store for the given DN.
     */
    public Path createSelfSignedKeyStore(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new PrivateKeyEntry(_privateKeyAlias, keyCertPair));
    }

    /**
     * Creates a self-signed key store as a data URL for the given DN.
     */
    public String createSelfSignedKeyStoreAsDataUrl(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new PrivateKeyEntry(_privateKeyAlias, keyCertPair));
    }

    /**
     * Creates a self-signed trust store for the given DN.
     */
    public Path createSelfSignedTrustStore(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    /**
     * Creates a self-signed trust store for the given DN and validity period.
     */
    public Path createSelfSignedTrustStore(final String dn, final Instant from, final Instant to)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn, from, to);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    /**
     * Creates a self-signed trust store as a data URL for the given DN.
     */
    public String createSelfSignedTrustStoreAsDataUrl(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    /**
     * Creates a trust store signed by the given CA.
     */
    public Path createTrustStore(final String dn, final KeyCertificatePair ca)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createKeyPairAndCertificate(dn, ca);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    /**
     * Creates a key store containing both a private key and certificate.
     */
    public Path createSelfSignedKeyStoreWithCertificate(final String dn)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        final PrivateKeyEntry privateKeyEntry = new PrivateKeyEntry(_privateKeyAlias, keyCertPair);
        final CertificateEntry certificateEntry = new CertificateEntry(_certificateAlias, keyCertPair.certificate());
        return createKeyStore(privateKeyEntry, certificateEntry);
    }

    /**
     * Creates a CRL file in PEM format.
     */
    public Path createCrl(final KeyCertificatePair caPair, final X509Certificate... certificate)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        final Path pkFile = createFile(".crl");
        PemUtils.saveCrlAsPem(pkFile, crl);
        return pkFile;
    }

    /**
     * Creates a CRL file in DER format.
     */
    public Path createCrlAsDer(final KeyCertificatePair caPair, final X509Certificate... certificate)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        return saveBytes(crl.getEncoded(), ".crl");
    }

    /**
     * Creates a CRL as a data URL.
     */
    public String createCrlAsDataUrl(final KeyCertificatePair caPair, final X509Certificate... certificate)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        assertOpen();
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        return TlsResourceHelper.getDataUrlForBytes(crl.getEncoded());
    }

    /**
     * Saves a private key as PEM.
     */
    public Path savePrivateKeyAsPem(final PrivateKey privateKey) throws IOException
    {
        assertOpen();
        final Path pkFile = createFile(".pk.pem");
        PemUtils.savePrivateKeyAsPem(pkFile, privateKey);
        return pkFile;
    }

    /**
     * Saves a certificate as PEM.
     */
    public Path saveCertificateAsPem(final X509Certificate certificate)
            throws IOException, CertificateEncodingException
    {
        assertOpen();
        final Path certificateFile = createFile(".cer.pem");
        PemUtils.saveCertificateAsPem(certificateFile, List.of(certificate));
        return certificateFile;
    }

    /**
     * Saves certificates as PEM.
     */
    public Path saveCertificateAsPem(final List<X509Certificate> certificates)
            throws IOException, CertificateEncodingException
    {
        assertOpen();
        final Path certificateFile = createFile(".cer.pem");
        PemUtils.saveCertificateAsPem(certificateFile, certificates);
        return certificateFile;
    }

    /**
     * Saves a private key as DER.
     */
    public Path savePrivateKeyAsDer(final PrivateKey privateKey) throws IOException
    {
        assertOpen();
        return saveBytes(privateKey.getEncoded(), ".pk.der");
    }

    /**
     * Saves a certificate as DER.
     */
    public Path saveCertificateAsDer(final X509Certificate certificate) throws CertificateEncodingException, IOException
    {
        assertOpen();
        return saveBytes(certificate.getEncoded(), ".cer.der");
    }

    /**
     * Creates a temporary file under the TLS resource directory.
     */
    public Path createFile(final String suffix) throws IOException
    {
        assertOpen();
        return Files.createTempFile(ensureKeystoreDirectory(), "tls", suffix);
    }

    /**
     * Writes bytes to a temporary file with the given extension.
     */
    private Path saveBytes(final byte[] bytes, final String extension) throws IOException
    {
        final Path path = createFile(extension);
        Files.write(path, bytes);
        return path;
    }

    /**
     * Writes the key store to a temporary file.
     */
    private Path saveKeyStore(final String keyStoreType, final KeyStore ks)
            throws GeneralSecurityException, IOException
    {
        final Path storePath = createFile("." + keyStoreType);
        TlsResourceHelper.saveKeyStoreIntoFile(ks, getSecretAsCharacters(), storePath);
        return storePath;
    }

    /**
     * Deletes the temporary resource directory if it exists.
     */
    private void deleteFiles()
    {
        if (_keystoreDirectory == null)
        {
            return;
        }
        if (!Files.exists(_keystoreDirectory))
        {
            return;
        }
        try (final Stream<Path> stream = Files.walk(_keystoreDirectory))
        {
            stream.sorted(Comparator.reverseOrder()).forEach(path ->
            {
                try
                {
                    Files.deleteIfExists(path);
                }
                catch (IOException e)
                {
                    final String message = "Could not delete path at %s".formatted(path);
                    LOGGER.warn(message, e);
                }
            });
        }
        catch (Exception e)
        {
            LOGGER.warn("Failure to clean up test resources", e);
        }
    }

    /**
     * Clears the in-memory secret.
     */
    private void clearSecret()
    {
        if (_secret != null)
        {
            Arrays.fill(_secret, '\0');
        }
    }

    /**
     * Lazily initializes the resource directory.
     */
    private synchronized Path ensureKeystoreDirectory() throws IOException
    {
        if (_keystoreDirectory != null)
        {
            return _keystoreDirectory;
        }
        final Path targetDir = Path.of("target");
        Files.createDirectories(targetDir);
        _keystoreDirectory = Files.createTempDirectory(targetDir, "test-tls-resources-");
        LOGGER.debug("Test keystore directory is created : '{}'", _keystoreDirectory);
        return _keystoreDirectory;
    }

    private void assertOpen()
    {
        if (_closed)
        {
            throw new IllegalStateException("TlsResource is closed");
        }
    }
}
