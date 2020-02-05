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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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

import org.apache.qpid.server.util.ServerScopedRuntimeException;
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
    private static final String[] TLS_PROTOCOL_PREFERENCES = new String[]{"TLSv1.3", "TLSv1.2", "TLSv1.1", "TLS", "TLSv1"};


    private static final Constructor<?> CONSTRUCTOR;
    private static final Method GENERATE_METHOD;
    private static final Method GET_PRIVATE_KEY_METHOD;
    private static final Method GET_SELF_CERTIFICATE_METHOD;
    private static final Constructor<?> X500_NAME_CONSTRUCTOR;
    private static final Constructor<?> DNS_NAME_CONSTRUCTOR;
    private static final Constructor<?> IP_ADDR_NAME_CONSTRUCTOR;
    private static final Constructor<?> GENERAL_NAMES_CONSTRUCTOR;
    private static final Constructor<?> GENERAL_NAME_CONSTRUCTOR;
    private static final Method ADD_NAME_TO_NAMES_METHOD;
    private static final Constructor<?> ALT_NAMES_CONSTRUCTOR;
    private static final Constructor<?> CERTIFICATE_EXTENSIONS_CONSTRUCTOR;
    private static final Method SET_EXTENSION_METHOD;
    private static final Method EXTENSION_GET_NAME_METHOD;
    private static final boolean CAN_GENERATE_CERTS;
    private static final CertificateFactory CERTIFICATE_FACTORY;

    static
    {
        Constructor<?> constructor = null;
        Method generateMethod = null;
        Method getPrivateKeyMethod = null;
        Method getSelfCertificateMethod = null;
        Constructor<?> x500NameConstructor = null;
        Constructor<?> dnsNameConstructor = null;
        Constructor<?> ipAddrNameConstructor = null;
        Constructor<?> generalNamesConstructor = null;
        Constructor<?> generalNameConstructor = null;
        Method addNameToNamesMethod = null;
        Constructor<?> altNamesConstructor = null;
        Constructor<?> certificateExtensionsConstructor = null;
        Method setExtensionMethod = null;
        Method extensionGetNameMethod = null;
        boolean canGenerateCerts = false;
        CertificateFactory certificateFactory = null;

        try
        {
            certificateFactory = CertificateFactory.getInstance("X.509");
        }
        catch (CertificateException e)
        {
            // ignore
        }

        try
        {
            Class<?> certAndKeyGenClass;
            try
            {
                certAndKeyGenClass = Class.forName("sun.security.x509.CertAndKeyGen");
            }
            catch (ClassNotFoundException e)
            {
                certAndKeyGenClass = Class.forName("sun.security.tools.keytool.CertAndKeyGen");
            }

            final Class<?> x500NameClass = Class.forName("sun.security.x509.X500Name");
            final Class<?> certificateExtensionsClass = Class.forName("sun.security.x509.CertificateExtensions");
            final Class<?> generalNamesClass = Class.forName("sun.security.x509.GeneralNames");
            final Class<?> generalNameClass = Class.forName("sun.security.x509.GeneralName");
            final Class<?> extensionClass = Class.forName("sun.security.x509.SubjectAlternativeNameExtension");

            constructor = certAndKeyGenClass.getConstructor(String.class, String.class);
            generateMethod = certAndKeyGenClass.getMethod("generate", Integer.TYPE);
            getPrivateKeyMethod = certAndKeyGenClass.getMethod("getPrivateKey");
            getSelfCertificateMethod = certAndKeyGenClass.getMethod("getSelfCertificate", x500NameClass,
                                                                    Date.class, Long.TYPE, certificateExtensionsClass);
            x500NameConstructor = x500NameClass.getConstructor(String.class);
            dnsNameConstructor = Class.forName("sun.security.x509.DNSName").getConstructor(String.class);
            ipAddrNameConstructor = Class.forName("sun.security.x509.IPAddressName").getConstructor(String.class);
            generalNamesConstructor = generalNamesClass.getConstructor();
            generalNameConstructor = generalNameClass.getConstructor(Class.forName("sun.security.x509.GeneralNameInterface"));
            addNameToNamesMethod = generalNamesClass.getMethod("add", generalNameClass);
            altNamesConstructor = extensionClass.getConstructor(generalNamesClass);
            certificateExtensionsConstructor = certificateExtensionsClass.getConstructor();
            setExtensionMethod = certificateExtensionsClass.getMethod("set", String.class, Object.class);
            extensionGetNameMethod = extensionClass.getMethod("getName");
            canGenerateCerts = true;
        }
        catch (ClassNotFoundException | LinkageError | NoSuchMethodException e)
        {
            // ignore
        }
        GET_SELF_CERTIFICATE_METHOD = getSelfCertificateMethod;
        CONSTRUCTOR = constructor;
        GENERATE_METHOD = generateMethod;
        GET_PRIVATE_KEY_METHOD = getPrivateKeyMethod;
        X500_NAME_CONSTRUCTOR = x500NameConstructor;
        DNS_NAME_CONSTRUCTOR = dnsNameConstructor;
        IP_ADDR_NAME_CONSTRUCTOR = ipAddrNameConstructor;
        GENERAL_NAMES_CONSTRUCTOR = generalNamesConstructor;
        GENERAL_NAME_CONSTRUCTOR = generalNameConstructor;
        ADD_NAME_TO_NAMES_METHOD = addNameToNamesMethod;
        ALT_NAMES_CONSTRUCTOR = altNamesConstructor;
        CERTIFICATE_EXTENSIONS_CONSTRUCTOR = certificateExtensionsConstructor;
        SET_EXTENSION_METHOD = setExtensionMethod;
        EXTENSION_GET_NAME_METHOD = extensionGetNameMethod;
        CAN_GENERATE_CERTS = canGenerateCerts;
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

    public static void verifyHostname(SSLEngine engine,String hostnameExpected)
    {
        try
        {
            Certificate cert = engine.getSession().getPeerCertificates()[0];
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
        catch(SSLPeerUnverifiedException e)
        {
            throw new TransportException("Failed to verify peer's hostname", e);
        }
    }

    public static void verifyHostname(final String hostnameExpected, final X509Certificate cert)
    {

        try
        {
            SortedSet<String> names = getNamesFromCert(cert);

            if (names.isEmpty())
            {
                throw new TransportException("SSL hostname verification failed. Certificate for did not contain CN or DNS subjectAlt");
            }

            boolean match = verifyHostname(hostnameExpected, names);
            if (!match)
            {
                throw new TransportException("SSL hostname verification failed." +
                                             " Expected : " + hostnameExpected +
                                             " Found in cert : " + names);
            }

        }
        catch (InvalidNameException e)
        {
            Principal p = cert.getSubjectDN();
            String dn = p.getName();
            throw new TransportException("SSL hostname verification failed. Could not parse name " + dn, e);
        }
        catch (CertificateParsingException e)
        {
            throw new TransportException("SSL hostname verification failed. Could not parse certificate:  " + e.getMessage(), e);
        }
    }

    public static boolean checkHostname(String hostname, X509Certificate cert)
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
        for (String cn : names)
        {

            boolean doWildcard = cn.startsWith("*.") &&
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
        Principal p = cert.getSubjectDN();
        String dn = p.getName();
        SortedSet<String> names = new TreeSet<>();
        LdapName ldapName = new LdapName(dn);
        for (Rdn part : ldapName.getRdns())
        {
            if (part.getType().equalsIgnoreCase("CN"))
            {
                names.add(part.getValue().toString());
                break;
            }
        }

        if(cert.getSubjectAlternativeNames() != null)
        {
            for (List<?> entry : cert.getSubjectAlternativeNames())
            {
                if (DNS_NAME_TYPE.equals(entry.get(0)))
                {
                    names.add((String) entry.get(1));
                }
            }
        }
        return names;
    }

    public static String getIdFromSubjectDN(String dn)
    {
        String cnStr = null;
        String dcStr = null;
        if(dn == null)
        {
            return "";
        }
        else
        {
            try
            {
                LdapName ln = new LdapName(dn);
                for(Rdn rdn : ln.getRdns())
                {
                    if("CN".equalsIgnoreCase(rdn.getType()))
                    {
                        cnStr = rdn.getValue().toString();
                    }
                    else if("DC".equalsIgnoreCase(rdn.getType()))
                    {
                        if(dcStr == null)
                        {
                            dcStr = rdn.getValue().toString();
                        }
                        else
                        {
                            dcStr = rdn.getValue().toString() + '.' + dcStr;
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


    public static String retrieveIdentity(SSLEngine engine)
    {
        String id = "";
        Certificate cert = engine.getSession().getLocalCertificates()[0];
        Principal p = ((X509Certificate)cert).getSubjectDN();
        String dn = p.getName();
        try
        {
            id = SSLUtil.getIdFromSubjectDN(dn);
        }
        catch (Exception e)
        {
            LOGGER.info("Exception received while trying to retrieve client identity from SSL cert", e);
        }
        LOGGER.debug("Extracted Identity from client certificate : {}", id);
        return id;
    }

    public static KeyStore getInitializedKeyStore(String storePath, String storePassword, String keyStoreType) throws GeneralSecurityException, IOException
    {
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        InputStream in = null;
        try
        {
            File f = new File(storePath);
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

            char[] storeCharPassword = storePassword == null ? null : storePassword.toCharArray();

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

    public static KeyStore getInitializedKeyStore(URL storePath, String storePassword, String keyStoreType) throws GeneralSecurityException, IOException
    {
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        try(InputStream in = storePath.openStream())
        {
            if (in == null && !"PKCS11".equalsIgnoreCase(keyStoreType)) // PKCS11 will not require an explicit path
            {
                throw new IOException("Unable to load keystore resource: " + storePath);
            }

            char[] storeCharPassword = storePassword == null ? null : storePassword.toCharArray();

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

    public static X509Certificate[] readCertificates(URL certFile)
            throws IOException, GeneralSecurityException
    {
        try (InputStream is = certFile.openStream())
        {
            return readCertificates(is);
        }
    }

    public static X509Certificate[] readCertificates(InputStream input)
            throws IOException, GeneralSecurityException
    {
        List<X509Certificate> crt = new ArrayList<>();
        try
        {
            do
            {
                crt.add( (X509Certificate) getCertificateFactory().generateCertificate(input));
            } while(input.available() != 0);
        }
        catch(CertificateException e)
        {
            if(crt.isEmpty())
            {
                throw e;
            }
        }
        return crt.toArray(new X509Certificate[crt.size()]);
    }

    public static PrivateKey readPrivateKey(final URL url)
            throws IOException, GeneralSecurityException
    {
        try (InputStream urlStream = url.openStream())
        {
            return readPrivateKey(urlStream);
        }
    }

    public static PrivateKey readPrivateKey(InputStream input)
            throws IOException, GeneralSecurityException
    {
        byte[] content = toByteArray(input);
        String contentAsString = new String(content, StandardCharsets.US_ASCII);
        if(contentAsString.contains("-----BEGIN ") && contentAsString.contains(" PRIVATE KEY-----"))
        {
            BufferedReader lineReader = new BufferedReader(new StringReader(contentAsString));

            String line;
            do
            {
                line = lineReader.readLine();
            } while(line != null && !(line.startsWith("-----BEGIN ") && line.endsWith(" PRIVATE KEY-----")));

            if(line != null)
            {
                StringBuilder keyBuilder = new StringBuilder();

                while((line = lineReader.readLine()) != null)
                {
                    if(line.startsWith("-----END ") && line.endsWith(" PRIVATE KEY-----"))
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
        try(ByteArrayOutputStream buffer = new ByteArrayOutputStream())
        {

            byte[] tmp = new byte[1024];
            int read;
            while((read=input.read(tmp))!=-1)

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
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(content);
            KeyFactory kf = KeyFactory.getInstance(algorithm);
            key = kf.generatePrivate(keySpec);
        }
        catch(InvalidKeySpecException e)
        {
            // not in PCKS#8 format - try parsing as PKCS#1
            RSAPrivateCrtKeySpec keySpec = getRSAKeySpec(content);
            KeyFactory kf = KeyFactory.getInstance(algorithm);
            try
            {
                key = kf.generatePrivate(keySpec);
            }
            catch(InvalidKeySpecException e2)
            {
                throw new InvalidKeySpecException("Cannot parse the provided key as either PKCS#1 or PCKS#8 format");
            }

        }
        return key;
    }

    private static RSAPrivateCrtKeySpec getRSAKeySpec(byte[] keyBytes) throws InvalidKeySpecException
    {

        ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
        try
        {
            // PKCS#1 is encoded as a DER sequence of:
            // (version, modulus, publicExponent, privateExponent, primeP, primeQ,
            //  primeExponentP, primeExponentQ, crtCoefficient)

            int tag = ((int)buffer.get()) & 0xff;

            // check tag is that of a sequence
            if(((tag & 0x20) != 0x20) || ((tag & 0x1F) != 0x10))
            {
                throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
            }

            int length = getLength(buffer);

            buffer = buffer.slice();
            buffer.limit(length);

            // first tlv is version - which we'll ignore
            byte versionTag = buffer.get();
            int versionLength = getLength(buffer);
            buffer.position(buffer.position()+versionLength);


            RSAPrivateCrtKeySpec keySpec = new RSAPrivateCrtKeySpec(
                    getInteger(buffer), getInteger(buffer), getInteger(buffer), getInteger(buffer), getInteger(buffer),
                    getInteger(buffer), getInteger(buffer), getInteger(buffer));

            return keySpec;
        }
        catch(BufferUnderflowException e)
        {
            throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
        }
    }

    private static int getLength(ByteBuffer buffer)
    {

        int i = ((int) buffer.get()) & 0xff;

        // length 0 <= i <= 127 encoded as a single byte
        if ((i & ~0x7F) == 0)
        {
            return i;
        }

        // otherwise the first octet gives us the number of octets needed to read the length
        byte[] bytes = new byte[i & 0x7f];
        buffer.get(bytes);

        return new BigInteger(1, bytes).intValue();
    }

    private static BigInteger getInteger(ByteBuffer buffer) throws InvalidKeySpecException
    {
        int tag = ((int) buffer.get()) & 0xff;
        // 0x02 indicates an integer type
        if((tag & 0x1f) != 0x02)
        {
            throw new InvalidKeySpecException("Unable to parse key as PKCS#1 format");
        }
        byte[] num = new byte[getLength(buffer)];
        buffer.get(num);
        return new BigInteger(num);
    }

    public static void updateEnabledTlsProtocols(final SSLEngine engine,
                                                 final List<String> protocolWhiteList,
                                                 final List<String> protocolBlackList)
    {
        String[] filteredProtocols = filterEnabledProtocols(engine.getEnabledProtocols(),
                                                            engine.getSupportedProtocols(),
                                                            protocolWhiteList,
                                                            protocolBlackList);
        engine.setEnabledProtocols(filteredProtocols);
    }

    public static void updateEnabledTlsProtocols(final SSLSocket socket,
                                             final List<String> protocolWhiteList,
                                             final List<String> protocolBlackList)
    {
        String[] filteredProtocols = filterEnabledProtocols(socket.getEnabledProtocols(),
                                                            socket.getSupportedProtocols(),
                                                            protocolWhiteList,
                                                            protocolBlackList);
        socket.setEnabledProtocols(filteredProtocols);
    }

    public static String[] filterEnabledProtocols(final String[] enabledProtocols,
                                                  final String[] supportedProtocols,
                                                  final List<String> protocolWhiteList,
                                                  final List<String> protocolBlackList)
    {
        return filterEntries(enabledProtocols, supportedProtocols, protocolWhiteList, protocolBlackList);
    }

    public static String[] filterEnabledCipherSuites(final String[] enabledCipherSuites,
                                                     final String[] supportedCipherSuites,
                                                     final List<String> cipherSuiteWhiteList,
                                                     final List<String> cipherSuiteBlackList)
    {
        return filterEntries(enabledCipherSuites, supportedCipherSuites, cipherSuiteWhiteList, cipherSuiteBlackList);
    }


    public static void updateEnabledCipherSuites(final SSLEngine engine,
                                                 final List<String> cipherSuitesWhiteList,
                                                 final List<String> cipherSuitesBlackList)
    {
        String[] filteredCipherSuites = filterEntries(engine.getEnabledCipherSuites(),
                                                      engine.getSupportedCipherSuites(),
                                                      cipherSuitesWhiteList,
                                                      cipherSuitesBlackList);
        engine.setEnabledCipherSuites(filteredCipherSuites);
    }

    public static void updateEnabledCipherSuites(final SSLSocket socket,
                                                 final List<String> cipherSuitesWhiteList,
                                                 final List<String> cipherSuitesBlackList)
    {
        String[] filteredCipherSuites = filterEntries(socket.getEnabledCipherSuites(),
                                                      socket.getSupportedCipherSuites(),
                                                      cipherSuitesWhiteList,
                                                      cipherSuitesBlackList);
        socket.setEnabledCipherSuites(filteredCipherSuites);
    }

    static String[] filterEntries(final String[] enabledEntries,
                                  final String[] supportedEntries,
                                  final List<String> whiteList,
                                  final List<String> blackList)
    {
        List<String> filteredList;
        if (whiteList != null && !whiteList.isEmpty())
        {
            filteredList = new ArrayList<>();
            List<String> supportedList = new ArrayList<>(Arrays.asList(supportedEntries));
            // the outer loop must be over the white list to preserve its order
            for (String whiteListedRegEx : whiteList)
            {
                Iterator<String> supportedIter = supportedList.iterator();
                while (supportedIter.hasNext())
                {
                    String supportedEntry = supportedIter.next();
                    if (supportedEntry.matches(whiteListedRegEx))
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

        if (blackList != null && !blackList.isEmpty())
        {
            for (String blackListedRegEx : blackList)
            {
                Iterator<String> entriesIter = filteredList.iterator();
                while (entriesIter.hasNext())
                {
                    if (entriesIter.next().matches(blackListedRegEx))
                    {
                        entriesIter.remove();
                    }
                }
            }
        }

        return filteredList.toArray(new String[filteredList.size()]);
    }

    public static SSLContext tryGetSSLContext() throws NoSuchAlgorithmException
    {
        return tryGetSSLContext(TLS_PROTOCOL_PREFERENCES);
    }

    public static SSLContext tryGetSSLContext(final String[] protocols) throws NoSuchAlgorithmException
    {
        for (String protocol : protocols)
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

    public static boolean isSufficientToDetermineClientSNIHost(QpidByteBuffer buffer)
    {
        if(buffer.remaining() < 6)
        {
            return false;
        }
        else if(looksLikeSSLv3ClientHello(buffer))
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

    private static boolean looksLikeSSLv3ClientHello(QpidByteBuffer buffer)
    {
        int contentType = buffer.get(buffer.position()+0);
        int majorVersion = buffer.get(buffer.position()+1);
        int minorVersion = buffer.get(buffer.position()+2);
        int messageType = buffer.get(buffer.position()+5);

        return contentType == 22 && // SSL Handshake
                   (majorVersion == 3 && // SSL 3.0 / TLS 1.x
                    (minorVersion == 0 || // SSL 3.0
                     minorVersion == 1 || // TLS 1.0
                     minorVersion == 2 || // TLS 1.1
                     minorVersion == 3)) && // TLS1.2
                   (messageType == 1); // client_hello
    }

    public final static String getServerNameFromTLSClientHello(QpidByteBuffer source)
    {
        try (QpidByteBuffer input = source.duplicate())
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

            byte contentType = input.get();
            byte majorVersion = input.get();
            byte minorVersion = input.get();
            if (minorVersion != 0x00) // not supported for SSL 3.0
            {

                int recordLength = input.getUnsignedShort();
                int messageType = input.get();
                // 24-bit length field
                int length = (input.getUnsignedByte() << 16) | (input.getUnsignedByte() << 8) | input.getUnsignedByte();
                if(input.remaining() < length)
                {
                    return null;
                }
                input.limit(length + input.position());

                input.position(input.position() + 34);  // hello minor/major version + random
                int skip = (int) input.get(); // session-id
                input.position(input.position() + skip);
                skip = input.getUnsignedShort(); // cipher suites
                input.position(input.position() + skip);
                skip = (int) input.get(); // compression methods
                input.position(input.position() + skip);

                if (input.hasRemaining())
                {

                    int remaining = input.getUnsignedShort();

                    if(input.remaining() < remaining)
                    {
                        // invalid remaining length
                        return null;
                    }

                    input.limit(input.position()+remaining);
                    while (input.hasRemaining())
                    {
                        int extensionType = input.getUnsignedShort();

                        int extensionLength = input.getUnsignedShort();

                        if (extensionType == 0x00)
                        {

                            int extensionDataRemaining = extensionLength;
                            if (extensionDataRemaining >= 2)
                            {
                                int listLength = input.getUnsignedShort();     // length of server_name_list
                                if (listLength + 2 != extensionDataRemaining)
                                {
                                    // invalid format
                                    return null;
                                }

                                extensionDataRemaining -= 2;
                                while (extensionDataRemaining > 0)
                                {
                                    int code = input.get();
                                    int serverNameLength = input.getUnsignedShort();
                                    if (serverNameLength > extensionDataRemaining)
                                    {
                                        // invalid format;
                                        return null;
                                    }
                                    byte[] encoded = new byte[serverNameLength];
                                    input.get(encoded);

                                    if (code == StandardConstants.SNI_HOST_NAME)
                                    {
                                        return new SNIHostName(encoded).getAsciiName();
                                    }
                                    extensionDataRemaining -= serverNameLength + 3;
                                }
                            }
                            return null;
                        }
                        else
                        {
                            if(input.remaining() < extensionLength)
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

    public static SSLContext createSslContext(final org.apache.qpid.server.model.KeyStore keyStore,
                                              final Collection<TrustStore> trustStores,
                                              final String portName)
    {
        SSLContext sslContext;
        try
        {
            sslContext = tryGetSSLContext();
            KeyManager[] keyManagers = keyStore.getKeyManagers();

            TrustManager[] trustManagers;
            if(trustStores == null || trustStores.isEmpty())
            {
                trustManagers = null;
            }
            else if(trustStores.size() == 1)
            {
                trustManagers = trustStores.iterator().next().getTrustManagers();
            }
            else
            {
                Collection<TrustManager> trustManagerList = new ArrayList<>();
                final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();

                for(TrustStore ts : trustStores)
                {
                    TrustManager[] managers = ts.getTrustManagers();
                    if(managers != null)
                    {
                        for(TrustManager manager : managers)
                        {
                            if(manager instanceof X509TrustManager)
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
                if(!mulTrustManager.isEmpty())
                {
                    trustManagerList.add(mulTrustManager);
                }
                trustManagers = trustManagerList.toArray(new TrustManager[trustManagerList.size()]);
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
        return CAN_GENERATE_CERTS;
    }

    public static KeyCertPair generateSelfSignedCertificate(final String keyAlgorithm,
                                                            final String signatureAlgorithm,
                                                            final int keyLength,
                                                            long startTime,
                                                            long duration,
                                                            String x500Name,
                                                            Set<String> dnsNames,
                                                            Set<InetAddress> addresses)
            throws IllegalAccessException, InvocationTargetException, InstantiationException
    {
        Object certAndKeyGen = CONSTRUCTOR.newInstance(keyAlgorithm, signatureAlgorithm);
        GENERATE_METHOD.invoke(certAndKeyGen, keyLength);
        final PrivateKey _privateKey = (PrivateKey) GET_PRIVATE_KEY_METHOD.invoke(certAndKeyGen);

        Object generalNames = GENERAL_NAMES_CONSTRUCTOR.newInstance();

        for(String dnsName : dnsNames)
        {
            if(dnsName.matches("[\\w&&[^\\d]][\\w\\d.-]*"))
            {
                ADD_NAME_TO_NAMES_METHOD.invoke(generalNames,
                                                GENERAL_NAME_CONSTRUCTOR.newInstance(DNS_NAME_CONSTRUCTOR.newInstance(
                                                        dnsName)));
            }
        }

        for(InetAddress inetAddress : addresses)
        {
            ADD_NAME_TO_NAMES_METHOD.invoke(generalNames, GENERAL_NAME_CONSTRUCTOR.newInstance(IP_ADDR_NAME_CONSTRUCTOR.newInstance(inetAddress.getHostAddress())));
        }
        Object certificateExtensions;
        if(dnsNames.isEmpty() && addresses.isEmpty())
        {
            certificateExtensions = null;
        }
        else
        {
            Object altNamesExtension = ALT_NAMES_CONSTRUCTOR.newInstance(generalNames);
            certificateExtensions = CERTIFICATE_EXTENSIONS_CONSTRUCTOR.newInstance();
            SET_EXTENSION_METHOD.invoke(certificateExtensions,
                                        EXTENSION_GET_NAME_METHOD.invoke(altNamesExtension),
                                        altNamesExtension);
        }

        final X509Certificate _certificate = (X509Certificate) GET_SELF_CERTIFICATE_METHOD.invoke(certAndKeyGen,
                                                                                                  X500_NAME_CONSTRUCTOR
                                                                                                          .newInstance(x500Name),
                                                                                                  new Date(startTime),
                                                                                                  duration,
                                                                                                  certificateExtensions);

        return new KeyCertPair()
        {
            @Override
            public PrivateKey getPrivateKey()
            {
                return _privateKey;
            }

            @Override
            public X509Certificate getCertificate()
            {
                return _certificate;
            }
        };

    }

    public static Collection<Certificate> getCertificates(final KeyStore ks) throws KeyStoreException
    {
        List<Certificate> certificates = new ArrayList<>();
        Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements())
        {
            String alias = aliases.nextElement();
            if (ks.isCertificateEntry(alias))
            {
                certificates.add(ks.getCertificate(alias));
            }
        }
        return certificates;
    }

    public interface KeyCertPair
    {
        PrivateKey getPrivateKey();
        X509Certificate getCertificate();
    }
}
