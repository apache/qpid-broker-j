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
package org.apache.qpid.server.transport.network.security.ssl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.StandardConstants;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.IPAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.TransportException;
import org.apache.qpid.server.util.Strings;

public class SSLUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);

    private static final Integer DNS_NAME_TYPE = 2;
    private static final String[] TLS_PROTOCOL_PREFERENCES = new String[] { "TLSv1.3", "TLSv1.2", "TLSv1.1", "TLS", "TLSv1" };
    private static final Pattern DNS_NAME_PATTERN = Pattern.compile("[\\w&&[^\\d]][\\w\\d.-]*");

    private static final Provider PROVIDER = new BouncyCastleProvider();
    private static final boolean CA_SIGNED = false;
    private static final boolean CRITICAL = true;
    private static final CertificateFactory CERTIFICATE_FACTORY;

    static
    {
        CertificateFactory certificateFactory;
        try
        {
            certificateFactory = CertificateFactory.getInstance("X.509");
        }
        catch (CertificateException e)
        {
            certificateFactory = null;
        }
        CERTIFICATE_FACTORY = certificateFactory;
    }

    private SSLUtil()
    {
    }

    public static CertificateFactory getCertificateFactory()
    {
        if (CERTIFICATE_FACTORY == null)
        {
            throw new ServerScopedRuntimeException("Certificate factory is null");
        }
        return CERTIFICATE_FACTORY;
    }

    public static void verifyHostname(final SSLEngine engine, final String hostnameExpected)
    {
        try
        {
            final Certificate cert = engine.getSession().getPeerCertificates()[0];
            if (cert instanceof X509Certificate)
            {
                verifyHostname(hostnameExpected, (X509Certificate) cert);
            }
            else
            {
                throw new TransportException("Cannot verify peer's hostname as peer does not present a X509Certificate. "
                        + "Presented certificate : " + cert);
            }
        }
        catch (SSLPeerUnverifiedException e)
        {
            throw new TransportException("Failed to verify peer's hostname", e);
        }
    }

    public static void verifyHostname(final String hostnameExpected, final X509Certificate cert)
    {
        try
        {
            final SortedSet<String> names = getNamesFromCert(cert);

            if (names.isEmpty())
            {
                throw new TransportException("SSL hostname verification failed. Certificate for did not contain CN or DNS subjectAlt");
            }

            final boolean match = verifyHostname(hostnameExpected, names);
            if (!match)
            {
                throw new TransportException("SSL hostname verification failed. Expected : " +
                        hostnameExpected + " Found in cert : " + names);
            }

        }
        catch (InvalidNameException e)
        {
            final Principal p = cert.getSubjectX500Principal();
            final String dn = p.getName();
            throw new TransportException("SSL hostname verification failed. Could not parse name " + dn, e);
        }
        catch (CertificateParsingException e)
        {
            throw new TransportException("SSL hostname verification failed. Could not parse certificate:  " + e.getMessage(), e);
        }
    }

    public static boolean checkHostname(final String hostname, final X509Certificate cert)
    {
        try
        {
            return verifyHostname(hostname, getNamesFromCert(cert));
        }
        catch (InvalidNameException | CertificateParsingException e)
        {
            return false;
        }
    }

    private static boolean verifyHostname(final String hostnameExpected, final SortedSet<String> names)
    {
        boolean match = false;

        final String hostName = hostnameExpected.trim().toLowerCase();
        for (final String cn : names)
        {
            final boolean doWildcard = cn.startsWith("*.") &&
                    cn.lastIndexOf('.') >= 3 &&
                    !cn.matches("\\*\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");

            match = doWildcard
                    ? hostName.endsWith(cn.substring(1)) && hostName.indexOf(".") == (1 + hostName.length() - cn.length())
                    : hostName.equals(cn);

            if (match)
            {
                break;
            }

        }
        return match;
    }

    private static SortedSet<String> getNamesFromCert(final X509Certificate cert)
            throws InvalidNameException, CertificateParsingException
    {
        final Principal p = cert.getSubjectX500Principal();
        final String dn = p.getName();
        final SortedSet<String> names = new TreeSet<>();
        final LdapName ldapName = new LdapName(dn);
        for (final Rdn part : ldapName.getRdns())
        {
            if (part.getType().equalsIgnoreCase("CN"))
            {
                names.add(part.getValue().toString());
                break;
            }
        }

        if (cert.getSubjectAlternativeNames() != null)
        {
            for (final List<?> entry : cert.getSubjectAlternativeNames())
            {
                if (DNS_NAME_TYPE.equals(entry.get(0)))
                {
                    names.add((String) entry.get(1));
                }
            }
        }
        return names;
    }

    public static String getIdFromSubjectDN(final String dn)
    {
        String cnStr = null;
        StringBuilder dcStr = null;
        if (dn == null)
        {
            return "";
        }
        else
        {
            try
            {
                final LdapName ln = new LdapName(dn);
                for (final Rdn rdn : ln.getRdns())
                {
                    if ("CN".equalsIgnoreCase(rdn.getType()))
                    {
                        cnStr = rdn.getValue().toString();
                    }
                    else if ("DC".equalsIgnoreCase(rdn.getType()))
                    {
                        if (dcStr == null)
                        {
                            dcStr = new StringBuilder(rdn.getValue().toString());
                        }
                        else
                        {
                            dcStr.insert(0, rdn.getValue().toString() + '.');
                        }
                    }
                }
                return cnStr == null || cnStr.length()==0 ? "" : dcStr == null ? cnStr : cnStr + '@' + dcStr;
            }
            catch (InvalidNameException e)
            {
                LOGGER.warn("Invalid name: '{}'", dn);
                return "";
            }
        }
    }

    public static KeyStore getInitializedKeyStore(final String storePath,
                                                  final String storePassword,
                                                  final String keyStoreType)
            throws GeneralSecurityException, IOException
    {
        final KeyStore ks = KeyStore.getInstance(keyStoreType);
        InputStream in = null;
        try
        {
            final File f = new File(storePath);
            if (f.exists())
            {
                in = new FileInputStream(f);
            }
            else
            {
                in = Thread.currentThread().getContextClassLoader().getResourceAsStream(storePath);
            }
            if (in == null && !"PKCS11".equalsIgnoreCase(keyStoreType)) // PKCS11 will not require an explicit path
            {
                throw new IOException("Unable to load keystore resource: " + storePath);
            }

            final char[] storeCharPassword = storePassword == null ? null : storePassword.toCharArray();

            ks.load(in, storeCharPassword);
        }
        finally
        {
            if (in != null)
            {
                //noinspection EmptyCatchBlock
                try
                {
                    in.close();
                }
                catch (IOException ignored)
                {
                }
            }
        }
        return ks;
    }

    public static KeyStore getInitializedKeyStore(final URL storePath,
                                                  final String storePassword,
                                                  final String keyStoreType)
            throws GeneralSecurityException, IOException
    {
        final KeyStore ks = KeyStore.getInstance(keyStoreType);
        try (final InputStream in = storePath.openStream())
        {
            if (in == null && !"PKCS11".equalsIgnoreCase(keyStoreType)) // PKCS11 will not require an explicit path
            {
                throw new IOException("Unable to load keystore resource: " + storePath);
            }

            final char[] storeCharPassword = storePassword == null ? null : storePassword.toCharArray();

            ks.load(in, storeCharPassword);
        }
        catch (IOException ioe)
        {
            if (ioe.getCause() instanceof GeneralSecurityException)
            {
                throw ((GeneralSecurityException) ioe.getCause());
            }
            else
            {
                throw ioe;
            }
        }
        return ks;
    }

    public static X509Certificate[] readCertificates(final URL certFile) throws IOException, GeneralSecurityException
    {
        try (final InputStream is = certFile.openStream())
        {
            return readCertificates(is);
        }
    }

    public static X509Certificate[] readCertificates(final InputStream input) throws IOException, GeneralSecurityException
    {
        final List<X509Certificate> crt = new ArrayList<>();
        try
        {
            do
            {
                crt.add( (X509Certificate) getCertificateFactory().generateCertificate(input));
            } while (input.available() != 0);
        }
        catch (CertificateException e)
        {
            if (crt.isEmpty())
            {
                throw e;
            }
        }
        return crt.toArray(new X509Certificate[0]);
    }

    public static PrivateKey readPrivateKey(final URL url) throws IOException, GeneralSecurityException
    {
        try (final InputStream urlStream = url.openStream())
        {
            return readPrivateKey(urlStream);
        }
    }

    public static PrivateKey readPrivateKey(final InputStream input) throws IOException, GeneralSecurityException
    {
        byte[] content = toByteArray(input);
        final String contentAsString = new String(content, StandardCharsets.US_ASCII);
        if (contentAsString.contains("-----BEGIN ") && contentAsString.contains(" PRIVATE KEY-----"))
        {
            final BufferedReader lineReader = new BufferedReader(new StringReader(contentAsString));

            String line;
            do
            {
                line = lineReader.readLine();
            } while (line != null && !(line.startsWith("-----BEGIN ") && line.endsWith(" PRIVATE KEY-----")));

            if (line != null)
            {
                final StringBuilder keyBuilder = new StringBuilder();

                while ((line = lineReader.readLine()) != null)
                {
                    if (line.startsWith("-----END ") && line.endsWith(" PRIVATE KEY-----"))
                    {
                        break;
                    }
                    keyBuilder.append(line);
                }

                content = Strings.decodeBase64(keyBuilder.toString());
            }
        }
        return readPrivateKey(content, "RSA");
    }

    private static byte[] toByteArray(final InputStream input) throws IOException
    {
        try (final ByteArrayOutputStream buffer = new ByteArrayOutputStream())
        {
            final byte[] tmp = new byte[1024];
            int read;
            while ((read = input.read(tmp)) != -1)
            {
                buffer.write(tmp, 0, read);
            }
            return buffer.toByteArray();
        }
    }

    public static PrivateKey readPrivateKey(final byte[] content, final String algorithm)
            throws NoSuchAlgorithmException, InvalidKeySpecException
    {
        PrivateKey key;
        try
        {
            final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(content);
            final KeyFactory kf = KeyFactory.getInstance(algorithm);
            key = kf.generatePrivate(keySpec);
        }
        catch (InvalidKeySpecException e)
        {
            // not in PCKS#8 format - try parsing as PKCS#1
            final RSAPrivateCrtKeySpec keySpec = getRSAKeySpec(content);
            final KeyFactory kf = KeyFactory.getInstance(algorithm);
            try
            {
                key = kf.generatePrivate(keySpec);
            }
            catch (InvalidKeySpecException e2)
            {
                throw new InvalidKeySpecException("Cannot parse the provided key as either PKCS#1 or PCKS#8 format");
            }
        }
        return key;
    }

    @SuppressWarnings("unused")
    private static RSAPrivateCrtKeySpec getRSAKeySpec(final byte[] keyBytes) throws InvalidKeySpecException
    {
        ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
        try
        {
            // PKCS#1 is encoded as a DER sequence of:
            // (version, modulus, publicExponent, privateExponent, primeP, primeQ,
            //  primeExponentP, primeExponentQ, crtCoefficient)

            final int tag = ((int)buffer.get()) & 0xff;

            // check tag is that of a sequence
            if (((tag & 0x20) != 0x20) || ((tag & 0x1F) != 0x10))
            {
                throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
            }

            final int length = getLength(buffer);

            buffer = buffer.slice();
            buffer.limit(length);

            // first tlv is version - which we'll ignore
            final byte versionTag = buffer.get();
            final int versionLength = getLength(buffer);
            buffer.position(buffer.position()+versionLength);

            return new RSAPrivateCrtKeySpec(
                    getInteger(buffer), getInteger(buffer), getInteger(buffer), getInteger(buffer), getInteger(buffer),
                    getInteger(buffer), getInteger(buffer), getInteger(buffer));
        }
        catch (BufferUnderflowException e)
        {
            throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
        }
    }

    private static int getLength(final ByteBuffer buffer)
    {
        final int i = ((int) buffer.get()) & 0xff;

        // length 0 <= i <= 127 encoded as a single byte
        if ((i & ~0x7F) == 0)
        {
            return i;
        }

        // otherwise the first octet gives us the number of octets needed to read the length
        final byte[] bytes = new byte[i & 0x7f];
        buffer.get(bytes);

        return new BigInteger(1, bytes).intValue();
    }

    private static BigInteger getInteger(final ByteBuffer buffer) throws InvalidKeySpecException
    {
        final int tag = ((int) buffer.get()) & 0xff;
        // 0x02 indicates an integer type
        if((tag & 0x1f) != 0x02)
        {
            throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
        }
        final byte[] num = new byte[getLength(buffer)];
        buffer.get(num);
        return new BigInteger(num);
    }

    public static void updateEnabledTlsProtocols(final SSLEngine engine,
                                                 final List<String> protocolAllowList,
                                                 final List<String> protocolDenyList)
    {
        final String[] filteredProtocols = filterEnabledProtocols(engine.getEnabledProtocols(),
                engine.getSupportedProtocols(), protocolAllowList, protocolDenyList);
        engine.setEnabledProtocols(filteredProtocols);
    }

    public static void updateEnabledTlsProtocols(final SSLSocket socket,
                                                 final List<String> protocolAllowList,
                                                 final List<String> protocolDenyList)
    {
        final String[] filteredProtocols = filterEnabledProtocols(socket.getEnabledProtocols(),
                socket.getSupportedProtocols(), protocolAllowList, protocolDenyList);
        socket.setEnabledProtocols(filteredProtocols);
    }

    public static String[] filterEnabledProtocols(final String[] enabledProtocols,
                                                  final String[] supportedProtocols,
                                                  final List<String> protocolAllowList,
                                                  final List<String> protocolDenyList)
    {
        return filterEntries(enabledProtocols, supportedProtocols, protocolAllowList, protocolDenyList);
    }

    public static String[] filterEnabledCipherSuites(final String[] enabledCipherSuites,
                                                     final String[] supportedCipherSuites,
                                                     final List<String> cipherSuiteAllowList,
                                                     final List<String> cipherSuiteDenyList)
    {
        return filterEntries(enabledCipherSuites, supportedCipherSuites, cipherSuiteAllowList, cipherSuiteDenyList);
    }

    public static void updateEnabledCipherSuites(final SSLEngine engine,
                                                 final List<String> cipherSuitesAllowList,
                                                 final List<String> cipherSuitesDenyList)
    {
        final String[] filteredCipherSuites = filterEntries(engine.getEnabledCipherSuites(),
                engine.getSupportedCipherSuites(), cipherSuitesAllowList, cipherSuitesDenyList);
        engine.setEnabledCipherSuites(filteredCipherSuites);
    }

    public static void updateEnabledCipherSuites(final SSLSocket socket,
                                                 final List<String> cipherSuitesAllowList,
                                                 final List<String> cipherSuitesDenyList)
    {
        final String[] filteredCipherSuites = filterEntries(socket.getEnabledCipherSuites(),
                socket.getSupportedCipherSuites(), cipherSuitesAllowList, cipherSuitesDenyList);
        socket.setEnabledCipherSuites(filteredCipherSuites);
    }

    static String[] filterEntries(final String[] enabledEntries,
                                  final String[] supportedEntries,
                                  final List<String> allowList,
                                  final List<String> denyList)
    {
        List<String> filteredList;
        if (allowList != null && !allowList.isEmpty())
        {
            filteredList = new ArrayList<>();
            final List<String> supportedList = new ArrayList<>(Arrays.asList(supportedEntries));
            // the outer loop must be over the allow list to preserve its order
            for (final String allowListedRegEx : allowList)
            {
                final Iterator<String> supportedIter = supportedList.iterator();
                while (supportedIter.hasNext())
                {
                    final String supportedEntry = supportedIter.next();
                    if (supportedEntry.matches(allowListedRegEx))
                    {
                        filteredList.add(supportedEntry);
                        supportedIter.remove();
                    }
                }
            }
        }
        else
        {
            filteredList = new ArrayList<>(Arrays.asList(enabledEntries));
        }

        if (denyList != null && !denyList.isEmpty())
        {
            for (final String denyListedRegEx : denyList)
            {
                filteredList.removeIf(s -> s.matches(denyListedRegEx));
            }
        }

        return filteredList.toArray(new String[0]);
    }

    public static SSLContext tryGetSSLContext() throws NoSuchAlgorithmException
    {
        return tryGetSSLContext(TLS_PROTOCOL_PREFERENCES);
    }

    public static SSLContext tryGetSSLContext(final String[] protocols) throws NoSuchAlgorithmException
    {
        for (final String protocol : protocols)
        {
            try
            {
                return SSLContext.getInstance(protocol);
            }
            catch (NoSuchAlgorithmException e)
            {
                // pass and try the next protocol in the list
            }
        }
        throw new NoSuchAlgorithmException(String.format("Could not create SSLContext with one of the requested protocols: %s",
                Arrays.toString(protocols)));
    }

    public static boolean isSufficientToDetermineClientSNIHost(final QpidByteBuffer buffer)
    {
        if (buffer.remaining() < 6)
        {
            return false;
        }
        else if (looksLikeSSLv3ClientHello(buffer))
        {
            final int position = buffer.position();
            final int recordSize = 5 + (((buffer.get(position + 3) & 0xFF) << 8) | (buffer.get(position + 4) & 0xFF));
            return buffer.remaining() >= recordSize;
        }
        else
        {
            return true;
        }
    }

    private static boolean looksLikeSSLv3ClientHello(final QpidByteBuffer buffer)
    {
        final int contentType = buffer.get(buffer.position());
        final int majorVersion = buffer.get(buffer.position() + 1);
        final int minorVersion = buffer.get(buffer.position() + 2);
        final int messageType = buffer.get(buffer.position() + 5);

        return contentType == 22 && // SSL Handshake
                (majorVersion == 3 && // SSL 3.0 / TLS 1.x
                (minorVersion == 0 || // SSL 3.0
                minorVersion == 1 || // TLS 1.0
                minorVersion == 2 || // TLS 1.1
                minorVersion == 3)) && // TLS1.2
                (messageType == 1); // client_hello
    }

    @SuppressWarnings("unused")
    public static String getServerNameFromTLSClientHello(final QpidByteBuffer source)
    {
        try (final QpidByteBuffer input = source.duplicate())
        {
            // Do we have a complete header?
            if (!isSufficientToDetermineClientSNIHost(source))
            {
                throw new IllegalArgumentException("Source buffer does not contain enough data to determine the SNI name");
            }
            else if(!looksLikeSSLv3ClientHello(source))
            {
                return null;
            }

            final byte contentType = input.get();
            final byte majorVersion = input.get();
            final byte minorVersion = input.get();
            if (minorVersion != 0x00) // not supported for SSL 3.0
            {
                final int recordLength = input.getUnsignedShort();
                final int messageType = input.get();
                // 24-bit length field
                final int length = (input.getUnsignedByte() << 16) | (input.getUnsignedByte() << 8) | input.getUnsignedByte();
                if (input.remaining() < length)
                {
                    return null;
                }
                input.limit(length + input.position());

                input.position(input.position() + 34);  // hello minor/major version + random
                int skip = input.get(); // session-id
                input.position(input.position() + skip);
                skip = input.getUnsignedShort(); // cipher suites
                input.position(input.position() + skip);
                skip = input.get(); // compression methods
                input.position(input.position() + skip);

                if (input.hasRemaining())
                {
                    final int remaining = input.getUnsignedShort();

                    if (input.remaining() < remaining)
                    {
                        // invalid remaining length
                        return null;
                    }

                    input.limit(input.position()+remaining);
                    while (input.hasRemaining())
                    {
                        final int extensionType = input.getUnsignedShort();
                        final int extensionLength = input.getUnsignedShort();

                        if (extensionType == 0x00)
                        {
                            int extensionDataRemaining = extensionLength;
                            if (extensionDataRemaining >= 2)
                            {
                                final int listLength = input.getUnsignedShort();     // length of server_name_list
                                if (listLength + 2 != extensionDataRemaining)
                                {
                                    // invalid format
                                    return null;
                                }

                                extensionDataRemaining -= 2;
                                while (extensionDataRemaining > 0)
                                {
                                    final int code = input.get();
                                    final int serverNameLength = input.getUnsignedShort();
                                    if (serverNameLength > extensionDataRemaining)
                                    {
                                        // invalid format;
                                        return null;
                                    }
                                    final byte[] encoded = new byte[serverNameLength];
                                    input.get(encoded);

                                    if (code == StandardConstants.SNI_HOST_NAME)
                                    {
                                        return createSNIHostName(encoded).getAsciiName();
                                    }
                                    extensionDataRemaining -= serverNameLength + 3;
                                }
                            }
                            return null;
                        }
                        else
                        {
                            if (input.remaining() < extensionLength)
                            {
                                return null;
                            }
                            input.position(input.position() + extensionLength);
                        }
                    }
                }
            }
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    public static SSLContext createSslContext(final org.apache.qpid.server.model.KeyStore keyStore,
                                              final Collection<TrustStore> trustStores,
                                              final String portName)
    {
        SSLContext sslContext;
        try
        {
            sslContext = tryGetSSLContext();
            final KeyManager[] keyManagers = keyStore.getKeyManagers();

            TrustManager[] trustManagers;
            if (trustStores == null || trustStores.isEmpty())
            {
                trustManagers = null;
            }
            else if (trustStores.size() == 1)
            {
                trustManagers = trustStores.iterator().next().getTrustManagers();
            }
            else
            {
                final Collection<TrustManager> trustManagerList = new ArrayList<>();
                final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();

                for (final TrustStore<?> ts : trustStores)
                {
                    final TrustManager[] managers = ts.getTrustManagers();
                    if (managers != null)
                    {
                        for (final TrustManager manager : managers)
                        {
                            if (manager instanceof X509TrustManager)
                            {
                                mulTrustManager.addTrustManager((X509TrustManager)manager);
                            }
                            else
                            {
                                trustManagerList.add(manager);
                            }
                        }
                    }
                }
                if (!mulTrustManager.isEmpty())
                {
                    trustManagerList.add(mulTrustManager);
                }
                trustManagers = trustManagerList.toArray(new TrustManager[0]);
            }
            sslContext.init(keyManagers, trustManagers, null);
        }
        catch (GeneralSecurityException e)
        {
            throw new IllegalArgumentException(String.format("Cannot configure TLS on port '%s'", portName), e);
        }
        return sslContext;
    }

    public static boolean canGenerateCerts()
    {
        return true;
    }

    public static KeyCertPair generateSelfSignedCertificate(final String keyAlgorithm,
                                                            final String signatureAlgorithm,
                                                            final int keyLength,
                                                            final long startTime,
                                                            final long endTime,
                                                            final String x500Name,
                                                            final Set<String> dnsNames,
                                                            final Set<InetAddress> addresses)
            throws NoSuchAlgorithmException, OperatorCreationException, CertificateException, CertIOException
    {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(keyAlgorithm);
        keyPairGenerator.initialize(keyLength);

        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final PrivateKey privateKey = keyPair.getPrivate();

        final ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm).build(keyPair.getPrivate());

        final GeneralName[] sanLocalHost = Stream.concat(dnsNames.stream()
                    .filter(dnsName -> DNS_NAME_PATTERN.matcher(dnsName).matches())
                    .map(dnaName -> new GeneralName(GeneralName.dNSName, dnaName)),
                addresses.stream()
                    .map(InetAddress::getHostAddress)
                    .filter(address -> IPAddress.isValidIPv4(address) || IPAddress.isValidIPv4WithNetmask(address) ||
                            IPAddress.isValidIPv6(address) || IPAddress.isValidIPv6WithNetmask(address))
                    .map(address -> new GeneralName(GeneralName.iPAddress, address)))
                    .toArray(GeneralName[]::new);

        final X500Principal issuerDn = new X500Principal(x500Name);
        final X500Principal subjectDn = new X500Principal(x500Name);
        final BigInteger serialNumber = new BigInteger(64, new SecureRandom());
        final Date startDate = new Date(startTime);
        final Date endDate = new Date(endTime);
        final PublicKey publicKey = keyPair.getPublic();

        final X509v3CertificateBuilder certificateBuilder =
                new JcaX509v3CertificateBuilder(issuerDn, serialNumber, startDate, endDate, subjectDn, publicKey)
                    .addExtension(Extension.basicConstraints, CRITICAL, new BasicConstraints(CA_SIGNED))
                    .addExtension(Extension.subjectAlternativeName, false, new GeneralNames(sanLocalHost));

        final X509Certificate certificate = new JcaX509CertificateConverter()
                .setProvider(PROVIDER)
                .getCertificate(certificateBuilder.build(contentSigner));

        return new KeyCertPair()
        {
            @Override
            public PrivateKey getPrivateKey()
            {
                return privateKey;
            }

            @Override
            public X509Certificate getCertificate()
            {
                return certificate;
            }
        };
    }

    public static Map<String, Certificate> getCertificates(final KeyStore ks) throws KeyStoreException
    {
        final Map<String ,Certificate> certificates = new HashMap<>();
        final Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements())
        {
            final String alias = aliases.nextElement();
            if (ks.isCertificateEntry(alias))
            {
                certificates.put(alias, ks.getCertificate(alias));
            }
        }
        return certificates;
    }

    public static SNIHostName createSNIHostName(final String hostName)
    {
        try
        {
            return new SNIHostName(hostName);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConnectionScopedRuntimeException("Failed to create SNIHostName from string '" + hostName + "'", e);
        }
    }

    public static SNIHostName createSNIHostName(final byte[] hostName)
    {
        try
        {
            return new SNIHostName(hostName);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConnectionScopedRuntimeException("Failed to create SNIHostName from byte array '" + new String(hostName) + "'", e);
        }
    }

    public interface KeyCertPair
    {
        PrivateKey getPrivateKey();
        X509Certificate getCertificate();
    }
}
