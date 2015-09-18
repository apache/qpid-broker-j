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
package org.apache.qpid.transport.network.security.ssl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.net.URL;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.transport.TransportException;

public class SSLUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);

    private static final Integer DNS_NAME_TYPE = 2;
    public static final String SSLV3_PROTOCOL = "SSLv3";

    private SSLUtil()
    {
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
        Principal p = cert.getSubjectDN();

        SortedSet<String> names = new TreeSet<>();
        String dn = p.getName();
        try
        {
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

            if (names.isEmpty())
            {
                throw new TransportException("SSL hostname verification failed. Certificate for did not contain CN or DNS subjectAlt");
            }

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
            if (!match)
            {
                throw new TransportException("SSL hostname verification failed." +
                                             " Expected : " + hostnameExpected +
                                             " Found in cert : " + names);
            }

        }
        catch (InvalidNameException e)
        {
            throw new TransportException("SSL hostname verification failed. Could not parse name " + dn, e);
        }
        catch (CertificateParsingException e)
        {
            throw new TransportException("SSL hostname verification failed. Could not parse certificate:  " + e.getMessage(), e);
        }
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
        return ks;
    }

    public static X509Certificate[] readCertificates(URL certFile)
            throws IOException, GeneralSecurityException
    {
        try (InputStream is = certFile.openStream())
        {
            return readCertificates(certFile.openStream());
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
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                crt.add( (X509Certificate) cf.generateCertificate(input));
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
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        byte[] tmp = new byte[1024];
        int read;
        while ((read = input.read(tmp)) != -1)
        {
            buffer.write(tmp, 0, read);
        }

        byte[] content = buffer.toByteArray();
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

                content = DatatypeConverter.parseBase64Binary(keyBuilder.toString());
            }
        }
        return readPrivateKey(content, "RSA");
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

    private static interface SSLEntity
    {
        String[] getEnabledCipherSuites();

        void setEnabledCipherSuites(String[] strings);

        String[] getEnabledProtocols();

        void setEnabledProtocols(String[] protocols);

        String[] getSupportedCipherSuites();

        String[] getSupportedProtocols();
    }

    private static SSLEntity asSSLEntity(final Object object, final Class<?> clazz)
    {
        return (SSLEntity) Proxy.newProxyInstance(SSLEntity.class.getClassLoader(), new Class[] { SSLEntity.class }, new InvocationHandler()
        {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
            {
                Method delegateMethod = clazz.getMethod(method.getName(), method.getParameterTypes());
                return delegateMethod.invoke(object, args);
            }
        })   ;
    }

    private static void removeSSLv3Support(final SSLEntity engine)
    {
        List<String> enabledProtocols = Arrays.asList(engine.getEnabledProtocols());
        if(enabledProtocols.contains(SSLV3_PROTOCOL))
        {
            List<String> allowedProtocols = new ArrayList<>(enabledProtocols);
            allowedProtocols.remove(SSLV3_PROTOCOL);
            engine.setEnabledProtocols(allowedProtocols.toArray(new String[allowedProtocols.size()]));
        }
    }

    public static void removeSSLv3Support(final SSLEngine engine)
    {
        removeSSLv3Support(asSSLEntity(engine, SSLEngine.class));
    }

    public static void removeSSLv3Support(final SSLSocket socket)
    {
        removeSSLv3Support(asSSLEntity(socket, SSLSocket.class));
    }

    public static void removeSSLv3Support(final SSLServerSocket socket)
    {
        removeSSLv3Support(asSSLEntity(socket, SSLServerSocket.class));
    }

    private static void updateEnabledCipherSuites(final SSLEntity entity,
                                                  final Collection<String> enabledCipherSuites,
                                                  final Collection<String> disabledCipherSuites)
    {
        if(enabledCipherSuites != null && !enabledCipherSuites.isEmpty())
        {
            final Set<String> supportedSuites =
                    new HashSet<>(Arrays.asList(entity.getSupportedCipherSuites()));
            supportedSuites.retainAll(enabledCipherSuites);
            entity.setEnabledCipherSuites(supportedSuites.toArray(new String[supportedSuites.size()]));
        }

        if(disabledCipherSuites != null && !disabledCipherSuites.isEmpty())
        {
            final Set<String> enabledSuites = new HashSet<>(Arrays.asList(entity.getEnabledCipherSuites()));
            enabledSuites.removeAll(disabledCipherSuites);
            entity.setEnabledCipherSuites(enabledSuites.toArray(new String[enabledSuites.size()]));
        }

    }


    public static void updateEnabledCipherSuites(final SSLEngine engine,
                                                 final Collection<String> enabledCipherSuites,
                                                 final Collection<String> disabledCipherSuites)
    {
        updateEnabledCipherSuites(asSSLEntity(engine, SSLEngine.class), enabledCipherSuites, disabledCipherSuites);
    }

    public static void updateEnabledCipherSuites(final SSLServerSocket socket,
                                                 final Collection<String> enabledCipherSuites,
                                                 final Collection<String> disabledCipherSuites)
    {
        updateEnabledCipherSuites(asSSLEntity(socket, SSLServerSocket.class), enabledCipherSuites, disabledCipherSuites);
    }

    public static void updateEnabledCipherSuites(final SSLSocket socket,
                                                 final Collection<String> enabledCipherSuites,
                                                 final Collection<String> disabledCipherSuites)
    {
        updateEnabledCipherSuites(asSSLEntity(socket, SSLSocket.class), enabledCipherSuites, disabledCipherSuites);
    }
}
