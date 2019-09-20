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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Test;

import org.apache.qpid.server.transport.TransportException;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.test.utils.UnitTestBase;

public class SSLUtilTest extends UnitTestBase
{
    @Test
    public void testFilterEntries_empty()
    {
        String[] enabled = {};
        String[] supported = {};
        List<String> whiteList = Arrays.asList();
        List<String> blackList = Arrays.asList();
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertEquals("filtered list is not empty", (long) 0, (long) result.length);
    }

    @Test
    public void testFilterEntries_whiteListNotEmpty_blackListEmpty()
    {
        List<String> whiteList = Arrays.asList("TLSv1\\.[0-9]+");
        List<String> blackList = Collections.emptyList();
        String[] enabled = {"TLS", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
        String[] expected = {"TLSv1.1", "TLSv1.2", "TLSv1.3"};
        String[] supported = {"SSLv3", "TLS", "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
    }

    @Test
    public void testFilterEntries_whiteListEmpty_blackListNotEmpty()
    {
        List<String> whiteList = Arrays.asList();
        List<String> blackList = Arrays.asList("TLSv1\\.[0-9]+");
        String[] enabled = {"TLS", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
        String[] expected = {"TLS"};
        String[] supported = {"SSLv3", "TLS", "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
    }

    @Test
    public void testFilterEntries_respectOrder()
    {
        List<String> whiteList = Arrays.asList("b", "c", "a");
        List<String> blackList = Collections.emptyList();
        String[] enabled = {"x"};
        String[] expected = {"b", "c", "a"};
        String[] supported = {"x", "c", "a", "xx", "b", "xxx"};
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
        // change order to make sure order was not correct by coincidence
        whiteList = Arrays.asList("c", "b", "a");
        expected = new String[]{"c", "b", "a"};
        result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
    }

    @Test
    public void testFilterEntries_blackListAppliesToWhiteList()
    {
        List<String> whiteList = Arrays.asList("a", "b");
        List<String> blackList = Arrays.asList("a");
        String[] enabled = {"a", "b", "c"};
        String[] expected = {"b"};
        String[] supported = {"a", "b", "c", "x"};
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
    }

    @Test
    public void testFilterEntries_whiteListIgnoresEnabled()
    {
        List<String> whiteList = Arrays.asList("b");
        List<String> blackList = Collections.emptyList();
        String[] enabled = {"a"};
        String[] expected = {"b"};
        String[] supported = {"a", "b", "x"};
        String[] result = SSLUtil.filterEntries(enabled, supported, whiteList, blackList);
        assertTrue("unexpected filtered list: expected " + Arrays.toString(expected) + " actual " + Arrays.toString(
                result), Arrays.equals(expected, result));
    }

    @Test
    public void testGetIdFromSubjectDN()
    {
        // "normal" dn
        assertEquals("user@somewhere.example.org",
                            SSLUtil.getIdFromSubjectDN("cn=user,dc=somewhere,dc=example,dc=org"));

        // quoting of values, case of types, spacing all ignored
        assertEquals("user2@somewhere.example.org",
                            SSLUtil.getIdFromSubjectDN("DC=somewhere, dc=example,cn=\"user2\",dc=org"));
        // only first cn is used
        assertEquals("user@somewhere.example.org",
                            SSLUtil.getIdFromSubjectDN("DC=somewhere, dc=example,cn=\"user\",dc=org, cn=user2"));
        // no cn, no Id
        assertEquals("", SSLUtil.getIdFromSubjectDN("DC=somewhere, dc=example,dc=org"));
        // cn in value is ignored
        assertEquals("", SSLUtil.getIdFromSubjectDN("C=CZ,O=Scholz,OU=\"JAKUB CN=USER1\""));
        // cn with no dc gives just user
        assertEquals("someone", SSLUtil.getIdFromSubjectDN("ou=someou, CN=\"someone\""));
        // null results in empty string
        assertEquals("", SSLUtil.getIdFromSubjectDN(null));
        // invalid name results in empty string
        assertEquals("", SSLUtil.getIdFromSubjectDN("ou=someou, ="));
        // component containing whitespace
        assertEquals("me@example.com",
                            SSLUtil.getIdFromSubjectDN("CN=me,DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB"));
        // empty CN
        assertEquals("", SSLUtil.getIdFromSubjectDN("CN=,DC=somewhere, dc=example,dc=org"));
    }

    @Test
    public void testWildCardAndSubjectAltNameMatchingWorks() throws Exception
    {
        doNameMatchingTest(KEYSTORE_1,
                           Arrays.asList("amqp.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "*.example.com"));

        doNameMatchingTest(KEYSTORE_2,
                           Arrays.asList("amqp.example.com", "amqp1.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "*.example.com"));

        doNameMatchingTest(KEYSTORE_3,
                           Arrays.asList("amqp.example.com", "amqp1.example.com", "amqp2.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "*.example.com"));

        doNameMatchingTest(KEYSTORE_4,
                           Arrays.asList("amqp.example.com", "amqp1.example.com", "amqp2.example.com", "foo.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "foo.bar.example.com"));

        doNameMatchingTest(KEYSTORE_5,
                           Arrays.asList("amqp.example.com", "foo.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "foo.bar.example.com", "foo.org"));

        doNameMatchingTest(KEYSTORE_6,
                           Arrays.asList("amqp.example.com"),
                           Arrays.asList("amqp.example.net", "example.com", "foo.bar.example.com", "foo.org", "foo"));

        doNameMatchingTest(KEYSTORE_7,
                           Arrays.asList("amqp.example.org", "amqp1.example.org", "amqp2.example.org"),
                           Arrays.asList("amqp.example.net", "example.com", "foo.bar.example.com", "foo.org", "foo"));

        doNameMatchingTest(KEYSTORE_8,
                           Arrays.asList("amqp.example.org", "example.org"),
                           Arrays.asList("amqp1.example.org", "example.com", "foo.bar.example.com", "foo.org", "foo"));

        doNameMatchingTest(KEYSTORE_9,
                           Arrays.asList("amqp.example.org"),
                           Arrays.asList("amqp1.example.org", "example.org", "*.example.org"));

        doNameMatchingTest(KEYSTORE_10,
                           Arrays.asList("amqp.example.org", "amqp1.example.org"),
                           Arrays.asList("example.org", "a.mqp.example.org"));

        doNameMatchingTest(KEYSTORE_11,
                           Collections.<String>emptyList(),
                           Arrays.asList("example.org", "a.mqp.example.org", "org"));

        doNameMatchingTest(KEYSTORE_12,
                           Collections.<String>emptyList(),
                           Arrays.asList("example.org", "a.mqp.example.org", "org"));

        doNameMatchingTest(KEYSTORE_13,
                           Collections.<String>emptyList(),
                           Arrays.asList("example.org", "a.mqp.example.org", "org"));
    }

    @Test
    public void testReadCertificates() throws Exception
    {
        Certificate certificate = getTestCertificate();

        assertNotNull("Certificate is not found", certificate);

        URL certificateURL = new URL(null, DataUrlUtils.getDataUrlForBytes(certificate.getEncoded()), new Handler());
        X509Certificate[] certificates = SSLUtil.readCertificates(certificateURL);

        assertEquals("Unexpected number of certificates", 1, certificates.length);
        assertEquals("Unexpected certificate", certificate, certificates[0]);
    }

    private Certificate getTestCertificate()
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new ByteArrayInputStream(TRUSTSTORE), "password".toCharArray());

        Enumeration<String> aliases = trustStore.aliases();
        Certificate certificate = null;
        while (aliases.hasMoreElements())
        {
            String alias = aliases.nextElement();
            if (trustStore.isCertificateEntry(alias))
            {
                certificate = trustStore.getCertificate(alias);
                break;
            }
        }
        return certificate;
    }

    private void doNameMatchingTest(byte[] keystoreBytes, List<String> validAddresses, List<String> invalidAddresses) throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new ByteArrayInputStream(keystoreBytes), "password".toCharArray());


        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new ByteArrayInputStream(TRUSTSTORE), "password".toCharArray());

        for(String validAddress : validAddresses)
        {
            try
            {
                SSLUtil.verifyHostname(getSSLEngineAfterHandshake(keyStore, trustStore, validAddress, 5672),
                                       validAddress);
            }
            catch(TransportException e)
            {
                fail("The address " + validAddress + " should validate but does not");
            }
        }

        for(String invalidAddress : invalidAddresses)
        {
            try
            {
                SSLUtil.verifyHostname(getSSLEngineAfterHandshake(keyStore, trustStore, invalidAddress, 5672),
                                       invalidAddress);
                fail("The address " + invalidAddress + " should not validate but it does");
            }
            catch(TransportException e)
            {
                // pass
            }
        }
    }

    private SSLEngine getSSLEngineAfterHandshake(final KeyStore keyStore,
                                                 final KeyStore trustStore,
                                                 String host,
                                                 int port)
            throws Exception
    {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        KeyManagerFactory keyManager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManager.init(keyStore, "password".toCharArray());
        sslContext.init(keyManager.getKeyManagers(), null,null);

        SSLEngine serverEngine = sslContext.createSSLEngine();
        serverEngine.setUseClientMode(false);


        SSLContext clientContext = SSLContext.getInstance("TLS");
        TrustManagerFactory trustManager = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManager.init(trustStore);

        clientContext.init(null, trustManager.getTrustManagers(), null);

        SSLEngine clientEngine = clientContext.createSSLEngine(host, port);

        clientEngine.setUseClientMode(true);
        clientEngine.beginHandshake();

        byte[] clientInput = new byte[0];
        byte[] clientOutput = new byte[0];

        SSLEngineResult.HandshakeStatus clientStatus;
        while((clientStatus = clientEngine.getHandshakeStatus()) != SSLEngineResult.HandshakeStatus.FINISHED
              && clientStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
        {
            switch (clientStatus)
            {
                case NEED_TASK:
                    clientEngine.getDelegatedTask().run();
                    break;
                case NEED_WRAP:
                    ByteBuffer dst = ByteBuffer.allocate(1024*1024);
                    clientEngine.wrap(ByteBuffer.allocate(0), dst);
                    dst.flip();
                    byte[] output = new byte[clientOutput.length+dst.remaining()];
                    System.arraycopy(clientOutput,0,output,0,clientOutput.length);
                    dst.get(output, clientOutput.length, dst.remaining());
                    clientOutput = output;
                    break;
                case NEED_UNWRAP:
                    ByteBuffer unwrapDst = ByteBuffer.allocate(1024*1024);
                    ByteBuffer src = ByteBuffer.wrap(clientInput);
                    clientEngine.unwrap(src, unwrapDst);
                    byte[] input = new byte[src.remaining()];
                    src.get(input,0,src.remaining());
                    clientInput = input;
                default:
                    break;
            }

            SSLEngineResult.HandshakeStatus serverStatus = serverEngine.getHandshakeStatus();
            switch (serverStatus)
            {
                case NEED_TASK:
                    serverEngine.getDelegatedTask().run();
                    break;
                case NEED_WRAP:
                    ByteBuffer dst = ByteBuffer.allocate(1024*1024);
                    serverEngine.wrap(ByteBuffer.allocate(0), dst);
                    dst.flip();
                    byte[] serverOutput = new byte[clientInput.length+dst.remaining()];
                    System.arraycopy(clientInput,0,serverOutput,0,clientInput.length);
                    dst.get(serverOutput, clientInput.length, dst.remaining());
                    clientInput = serverOutput;
                    break;

                case NOT_HANDSHAKING:
                case NEED_UNWRAP:
                    ByteBuffer unwrapDst = ByteBuffer.allocate(1024*1024);
                    ByteBuffer src = ByteBuffer.wrap(clientOutput);
                    serverEngine.unwrap(src, unwrapDst);
                    byte[] input = new byte[src.remaining()];
                    src.get(input,0,src.remaining());
                    clientOutput = input;
            }
        }
        return clientEngine;
    }

    private static byte[] TRUSTSTORE = Base64.getDecoder().decode("/u3+7QAAAAIAAAANAAAAAgAPa2V5c3RvcmUyLWFsaWFzAAABVutBZIkABVguNTA5AAAGHzCCBhsw"
                                                                  + "ggQDoAMCAQICCQCrOvhXap7bYTANBgkqhkiG9w0BAQUFADBcMQswCQYDVQQGEwJVUzEQMA4GA1UE"
                                                                  + "CBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFt"
                                                                  + "cXAuZXhhbXBsZS5jb20wHhcNMTYwOTAyMTQxNTE1WhcNMjYwNzEyMTQxNTE1WjBcMQswCQYDVQQG"
                                                                  + "EwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkx"
                                                                  + "GTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC"
                                                                  + "AQCwc7RdXSWaFrtNp7RQ1YEh0n2S8VjtBosY5FB6BoNe2B14LyIVQqA/bmbgZJAfn2RTKnEcxyHL"
                                                                  + "qM1PZThj2lyb9/mtvt2k4gRviP4/ZYbcrtByParZPERu7gmxe7eaJn7ghpqVY5zaJ96XQFSiSzNK"
                                                                  + "6jBNswx2zMhMnLEzegXFbLL125K8B/++1dJNK2gB3o/M9692mygrJSvGwuPmDYcWQnzsyLPTYx0/"
                                                                  + "Y+eNtnaBx+4NjsSLCvlp9G7pKqHiKb4agatLHNPyMubt600eV56xWeZ4ujvZgPuPmNhO0ogtZpFW"
                                                                  + "tF7NrPSCbEXEMVhSgh9mrR1dyR5amEFWCvs23kSDtQZl895Z5CXm2GRAc10HYu5NJym4UE1utsAP"
                                                                  + "nRhcJ7lOl/lnMfXG+rbn0fnBbh5zoXi32UcCkldNLbXn9fBSn17hRZ5TmXmGOpBxa7By8k+GRkGD"
                                                                  + "ntQrWHIJdalI73c5Jne4W9NOkWKvTw5wKOUB9HGispvbrOXH9/Qfx/techw9qlK6WL3v7h9VE5w0"
                                                                  + "+DXiDy4CGq19g9L+XAQq73AvROOTruiDFsPg5rqi4cZVEAhZbHAfe+s59ZOzGIgU5BXVtsmIyiK3"
                                                                  + "wqQxOlsi6NNpdpv6FM8pQaOnq3tQr67R2xFmRQX6VBD+8X5xrpHNXVUR5VUJC3bc8d98J6Khi3RK"
                                                                  + "uQIDAQABo4HfMIHcMB0GA1UdDgQWBBQdgOTsHaTNceb+faB5aBhus4mDyDCBjgYDVR0jBIGGMIGD"
                                                                  + "gBQdgOTsHaTNceb+faB5aBhus4mDyKFgpF4wXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZh"
                                                                  + "dGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1w"
                                                                  + "bGUuY29tggkAqzr4V2qe22EwDAYDVR0TBAUwAwEB/zAcBgNVHREEFTATghFhbXFwMS5leGFtcGxl"
                                                                  + "LmNvbTANBgkqhkiG9w0BAQUFAAOCAgEAMR8mKY7ml9KrhnSMbvTauvuXNXT5hp0NRpmQc6Wt/Vyw"
                                                                  + "V4BPVAPOz/KCmMj0tkz/LOSk5MbLxXfDDhQHA1zKPxYLM4DfObUhbJcsNo+HlC2EQ8vN4srqgNFv"
                                                                  + "rY8yvfIgTILDUv02381njrz+GOLClSbLB7hcXvrIILENb72BwMv4QTIvXxYaJRa++s89I1OWe4f6"
                                                                  + "CzseEIBQ2ezMsU4Qjgv6tfvgsn6K4tfpVLT4jeJkv7xZ6WAW6XKgEcDreVGm8E0/7B0E5IBFgfA3"
                                                                  + "VOs78s5BGDccz/EFcnh5Knkhnj666Cbn4rhvI/CB+TMj5Qae18Qr3cV6j7pMpCNYwwHUT2/Aoygq"
                                                                  + "/BxrKgDX0b8xlyiDqEgy4vHYdb1980FOkdK23z5Q2xVeTeCJDFNPa7oNwHj4d3znbR6QRGBIQHKU"
                                                                  + "v7iKcWNdmtVjYV9MQvMM9BVcYxbg3KDpV9GWXpz19ZWYchfZJBGUCENPE55YKh7iyj9yAZ7opPDx"
                                                                  + "JlyvDcEwwyl/N9I6KlhqubuI1i8arsFY+ouAaNNfElBMPeoU7ws8cq3C9+ek+vs8BT4p6Dkj7cx9"
                                                                  + "kwugSW4mDKdlLwLDyfzEpIEpg/rjBtSE2DRLNfpr05MKcXsZX5RB33g0IpXVCBGLqRWFHLgNnUkv"
                                                                  + "tT+ptmkwvMXQehAbwvWtelKQWr6tft8AAAACABBrZXlzdG9yZTEyLWFsaWFzAAABVutBb3EABVgu"
                                                                  + "NTA5AAAF5DCCBeAwggPIoAMCAQICCQD5mUaCZSGVOzANBgkqhkiG9w0BAQUFADBTMQswCQYDVQQG"
                                                                  + "EwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkx"
                                                                  + "EDAOBgNVBAMUByouKi5vcmcwHhcNMTYwOTAyMTQxNTQyWhcNMjYwNzEyMTQxNTQyWjBTMQswCQYD"
                                                                  + "VQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNp"
                                                                  + "dHkxEDAOBgNVBAMUByouKi5vcmcwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDNP8p1"
                                                                  + "69NWypvWS5mwSYsEHL4ITE7p8Ym26bZTUT66yoY1gxH/sXE0VTSs0wea1Jf+VUrxk6hsMjtu9Z77"
                                                                  + "0zXGrqpBS0KLrJcgAnLRatd8ZAGxakNeESXEIrVBly9MK4NrMtyDXlo/vZdsyTMMMyllPjTGvFcV"
                                                                  + "4zZdH3MGo0Zh6pZjnXPlvDCII5w0m6oairCVpH73LmO6CfcIncYRgx94dQNLMR0tuxCuTZyvwyjd"
                                                                  + "y2c/KQbNt+FIKQZJBozwyXPnSEEO7L3r8FqFw/fK1dWpyo5sc6M3tGjgNfCSpTJXy4qxiJPDi8RE"
                                                                  + "87oEeQ97VEdzmsooMhLMnlCagJxO3nMtM28S/ahc5fjUQd9Gsw74G8bMAWvv5Dkt3QTRbHlQ4Mdl"
                                                                  + "AMPF0117o4THujZpkSm0rCdvKCGFv7lZIyf+0p4HL5JwKjBZjHc8uXKp7CQtPh3UnZyHcqey48E6"
                                                                  + "mQm3uv3YHPIzUTcWYDCEAyPchZnWoYZE2N5B5bzuPrRyckgTS3pOS7WiYUgUVE77stOgYcOsA/qJ"
                                                                  + "44xqEXzPCR3OXPRLMCacRsnB/At+SnlZxzz5Gx9QOZPCibW7Q0kEHpf/Ct10aq2wLzNgqDx93xTx"
                                                                  + "fcNc1glgH0ao+6lUyxX9q8jFJTtqzx00p/0yApFAVz/9/nKpHGLF7KqNgFhcHQiqIs1b5QIDAQAB"
                                                                  + "o4G2MIGzMB0GA1UdDgQWBBRuPq5dd25CnI+IGzefyksqvejPEDCBgwYDVR0jBHwweoAUbj6uXXdu"
                                                                  + "QpyPiBs3n8pLKr3ozxChV6RVMFMxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYD"
                                                                  + "VQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEQMA4GA1UEAxQHKi4qLm9yZ4IJAPmZRoJlIZU7"
                                                                  + "MAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggIBAIpiGjC1weE8zVxaSWLVc5yu+YZuGSvI"
                                                                  + "NOJJRsyfotWVPMBBNmwYscTiYyjiweNqfHYTK/nNxmdm4qPeoyMpI8U11MUILTt6AL+JM6sR16qY"
                                                                  + "Ij9PbXzOCJB8mLyZVFygKftPivfCM4xGsOvsH22uHCKYBbtQJJLjP9yIxeI6YAURz1goEivLzk9o"
                                                                  + "dzFyxkOKe4uGzwEBqgU6fHrOC9WFIk4/pa/52o1dKly+ls14Nbq3wiGPOVZVVnbJGaQMNvibCUPv"
                                                                  + "vHq2yeqNScpPzcoZyNeKdVXA75TIG/PltkS0k/KPX2fCSD99CnD98g2L+bGU9PFXG8MYaTOCnnKc"
                                                                  + "Qp0j1Z9lYtQiXATfkfGr+IAnbLTBfvwzPlPT0j+4lBBjBwuLgZCYHVRs4JAVx92SUuLDoDl9akN5"
                                                                  + "usuuhuh+thokwaDWRITAWX+r9aLLqyUmEydTL9RUe5WBWklO992cKack1UhQJzeNmVO1na5y/BIy"
                                                                  + "O5touRVxmKDW39eXZ8vwmzTTSjqeqlMPGRe1Ll+L/LVVT9SD4XSVthJsUBAlhdW+a73iCGEJ+BZR"
                                                                  + "o5CeE9V7GpSF6rrMN1o+4jZt3VCuOasUbvsRvKSuHiuyKoeG+OeNrINE8gOyPp6n+t1KQx5fRpQR"
                                                                  + "s9naYIz5fC7sKye7N88QnVvWpA8Jq1S0nj9eur6RcUmrAAAAAgAPa2V5c3RvcmU0LWFsaWFzAAAB"
                                                                  + "VutBZqYABVguNTA5AAAGLjCCBiowggQSoAMCAQICCQDb5QsXfWUWdDANBgkqhkiG9w0BAQUFADBc"
                                                                  + "MQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNV"
                                                                  + "BAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb20wHhcNMTYwOTAyMTQxNTIwWhcNMjYw"
                                                                  + "NzEyMTQxNTIwWjBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJv"
                                                                  + "dmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb20wggIiMA0GCSqG"
                                                                  + "SIb3DQEBAQUAA4ICDwAwggIKAoICAQDFvK1eaYndkaHMzGeSfJ+Q8kI0yoh7hFWaQ7DaGBZuRQsi"
                                                                  + "mcesWBcV6bPnMnnpDVgM73GlcP54NmghNa7jZXInLF4/HVrr4uYbeeN5idU4bm39FdctffxTQVnN"
                                                                  + "qxUOOBkH3hIhTJvIuTJS11P3x7U6FhnkTkkS38pNtqLaedX+fGHte/J0K1YTDcjE27pp0rIVf/to"
                                                                  + "9q3PEsjcRGUWx+aENml9ldLSzTn5PJZnnoPGljeaR2zvTIhh8OiOTDlXXwtuQvP8EYQvtV1KJn0w"
                                                                  + "qYPaepOyDub0dlWRQ7RO73rsgktfdSEad4bKOvAyViGtaXSIS5TQ9UExWRFb54xFfW4Szjp8TBDE"
                                                                  + "zrEJHzzUSMJ3PY9wGKtYqDHi5W3ic28dIus17uBuUSvcRka0cpYeWAeR8imI56AFsiCom/VmJZs0"
                                                                  + "IbCMZghWKNnCiUPzfuRrS5rB7ph0iMzfZfoNW/UCN/xLbZfNQtYqyWexee23q03hhIgTkh27vgH9"
                                                                  + "qWJFRYb2GeusIkRBif1Ih1SsG6+f4KHqcf0OnYVP0kInq11CzXAMZeuafhlnUy0ofQ1L0Bqz7g+b"
                                                                  + "LhZg3NYhio1U+en5bRJPy8cavazeXmK52DVyqwByOcV9sM/myujtnUapVFwtFytBd3jnUWtpv3B/"
                                                                  + "DqDKX0/Sl781QHBBIhc8laSQ278gnQIDAQABo4HuMIHrMB0GA1UdDgQWBBRiaZMkFmQQvhV94s8p"
                                                                  + "gfTqTYkwjzCBjgYDVR0jBIGGMIGDgBRiaZMkFmQQvhV94s8pgfTqTYkwj6FgpF4wXDELMAkGA1UE"
                                                                  + "BhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5"
                                                                  + "MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUuY29tggkA2+ULF31lFnQwDAYDVR0TBAUwAwEB/zArBgNV"
                                                                  + "HREEJDAighFhbXFwMS5leGFtcGxlLmNvbYINKi5leGFtcGxlLmNvbTANBgkqhkiG9w0BAQUFAAOC"
                                                                  + "AgEAqa2GUD8L6P2roEoE1R0y8EzmIjjqQOLrHG89PFIRjj9jJbzSNKqVP9T8qUUepIF8Df2PLCKB"
                                                                  + "jhCV/t1+q8nVBV1gX/x8Mz905Vda1XdxKTYJp88OuoRl1FFDpXZBskaH4X9ynKx6GKifqofR/7RD"
                                                                  + "r7swguZN2xDVnPVMZnTSI5eYGnrYJH8c9Kbmz40KJbF8Codk/L/3i3uhjGgLVp/TqYSYoTCn0zxa"
                                                                  + "5rHFMq2HaWPyoj7ms9Be9v8DmoQ4n4bsSLMEVaXIPfuBYChZwblT+qp8bGCJGFBXf41Ng5/CNYqB"
                                                                  + "Uo8ZrhU5tvAGl5hd6AlhtUEN/ldZFGqp9OipdEqfOeT3Akm3xot0EHOhqzf1ckWV3nUa4aPVtetm"
                                                                  + "sFN2LHsy8xq/PPH2hFjZw2OUiycI+BQdM77r4dGWPNolFzKsTOBre1lTWKxoO+oZicZ1HfQbftvJ"
                                                                  + "Z+c3iXzQwoEC6eKkWriJbn2VKzrqx3an3hWk9YFCid3HgM/FbFMcJ2yW7YRDVmosNNmIEbeUebXW"
                                                                  + "ds6EnQ95X3R8a9zQJYQ3XwKtzFpV87yhrwIGIW5EYC6PQHF1yKlYZASFdHVpdt92LpZSfITTyYMj"
                                                                  + "gPYmC82HAaT9qnwHK96pa1nMhAmKpE6VIwhW2rNa5HvJ6xY6/D1GNbp0FW3Pgs67rDnEoFwnX+N/"
                                                                  + "lQIAAAACABBrZXlzdG9yZTEwLWFsaWFzAAABVutBbSUABVguNTA5AAAF+DCCBfQwggPcoAMCAQIC"
                                                                  + "CQCnthdI64QDMjANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                  + "ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNVBAMUDSouZXhhbXBsZS5v"
                                                                  + "cmcwHhcNMTYwOTAyMTQxNTM2WhcNMjYwNzEyMTQxNTM2WjBZMQswCQYDVQQGEwJVUzEQMA4GA1UE"
                                                                  + "CBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNVBAMUDSou"
                                                                  + "ZXhhbXBsZS5vcmcwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDHZRqoukGFLZeBPhr0"
                                                                  + "G2i8SVCN9d5j6/p6nRFb5CW3/+zgs5+mC5aSKV3v+P7tztDLdePK3iGe/koR6F3olRmi1KZEJS9K"
                                                                  + "E1fYuI5bd+vLIcOCfPSyjmYdXI/lD0p8Ii+lHlaZOG4wYAWwrnsLKCEm9+jR3Ba+qt5ubuW5gWlL"
                                                                  + "pJxfAljQ2MDCMYXRWAgYgmhAiA6LiFEU/2vK9pfCIgMQuCnI/qDlyxUtllcgRrdUCSTPJldogoTk"
                                                                  + "S3eDrT1sDqabA//EOWF8NCcSExAkVnw3+SumcOg3PevmIx5ul5F8re4kclmHzUOlTO0Tax+nDNmx"
                                                                  + "mbYqUe9w23/FD2edzS/6Wrv6fAqexyblkXiIc/sMhts3chdAi51PsmQ6xaokrWrFpK1MTaUnH/jD"
                                                                  + "ylpG3gxnvqjZDrpV3+feap2LHVQAHfXYkMUamyxCQ8P14BjfHbhwQeCr2g/Z4vxelQgcj14iej+B"
                                                                  + "G5yRWMhSMAbNQJ0CpRBf7Y1bO1Fbu+FLHiwjygtxTQyNT3mPoVPQHys73HYl2pI2us72AzBAgFTb"
                                                                  + "6qqzvbwhGuv/CXPI1P7kcM8x/fh5BE8ZQ0ixCIJMM4MP2Nnx6hxZFuGH8GP2sg1C8Kz3HK9DRF5N"
                                                                  + "V6dH2OYIx7I3aQYVucW/IGJ9/zW7mkZS8Tb9WsLa6N6uo/PEVGh6nXZpgwIDAQABo4G+MIG7MB0G"
                                                                  + "A1UdDgQWBBTssUOh+PFW+dL0Eh1THwBmZ4FvYDCBiwYDVR0jBIGDMIGAgBTssUOh+PFW+dL0Eh1T"
                                                                  + "HwBmZ4FvYKFdpFswWTELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHBy"
                                                                  + "b3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRYwFAYDVQQDFA0qLmV4YW1wbGUub3JnggkAp7YXSOuEAzIw"
                                                                  + "DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAgEAxk80rlNZiOk/p7bDW+DyMIz5HBQU1WqJ"
                                                                  + "71eskKs8uccQ9tlFMuehd7GBw8ums5FIBWw/tCMHMhR1gcLvmQ8GX4iWHQwec8W6KXYzS/1Punje"
                                                                  + "2D6Akiv1UjwWBYjUDr1tWpAAqdy647PhK4k1I+FELba1x9JB3yQTunjTyVTrzy2lGs41ImKloe5C"
                                                                  + "fYh4rQLPP/jjeNYbgfUaXhufwv2qq6k/WnjmM3S67boC53P9vNgdz5EtizNusnhx4D83ecQ5SS8I"
                                                                  + "G8PQZmN75jUg+xKaBtxr03AblGQRDoJQZdVyDGvjyX9cgOJ0lDzP77Ca6bmOj7qB6a6X2NWiF/pr"
                                                                  + "Wc9fWF9Qehjs5xPmUxKfctTOZ2PEPvPGb7GrHK82arHCSnSu4/nL5b7mBPInp1gsb0mbo+gdwrwb"
                                                                  + "6iBXTynXil1Y/fqGFGbNwOyteaqueEbRCdINyi4hCcatQSTLv8oAU6GEzXCelkP+iTx0Y9CEp1Rn"
                                                                  + "qTOTiW++vBDTDxXp4XLmQuX2viU1fwpsb6hE2F2d3uqTBbYnVxA5T7VvMDL4B2r9wKzzXUfMsC1q"
                                                                  + "m4hquq1YOmF0lQy5kFZvHePhFMWoMxuNM/PfScotvr0YoiZD4hKw2l3bqxSukbG9fEXZ1kM+3pXx"
                                                                  + "hAZ6Tjt6B7GIub89FYqvCryWvKE2JE7v2MrjaiKY/UAAAAACAA9rZXlzdG9yZTYtYWxpYXMAAAFW"
                                                                  + "60FoywAFWC41MDkAAAYPMIIGCzCCA/OgAwIBAgIJAPtyzK3J1z5DMA0GCSqGSIb3DQEBBQUAMFwx"
                                                                  + "CzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UE"
                                                                  + "ChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxlLmNvbTAeFw0xNjA5MDIxNDE1MjRaFw0yNjA3"
                                                                  + "MTIxNDE1MjRaMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92"
                                                                  + "aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxlLmNvbTCCAiIwDQYJKoZI"
                                                                  + "hvcNAQEBBQADggIPADCCAgoCggIBAMP/qhHCCKyIF76WM0sFip5BvbJmZ0Tpxec9B4myGPT6TVBY"
                                                                  + "X7zJhnBMYKwCdorwh93yL8Y7LakiVM1svbtlgO+blRewSQri0yv0bLSeTy0KDnQBVXzW4QA8o3jL"
                                                                  + "CjFPFr9jP4dpr/pWGrBpgsF2/MYeVd9z6K0knSbGzb0tTjivYpffsKG68tmjmNyuB/8Cw5YyHKeU"
                                                                  + "eIQNNygEekKF1Z/2D96NjcZSBVvImY/nSDcPa1joihhRWb0e7Tw8j0v5VMY8J6NDp9ShP9Z+ilGf"
                                                                  + "SrzPkNrnyt+I+ULv07JS8b0Z2lr8WXsOEWt/38vO/58Rk0H4izE5T8LHs4fhwgyz0b79LOZO6NaJ"
                                                                  + "ZVmYk2GeTFEcC0Bgdv0oJT957l6LwTHb59CczaXIQTAytp7QgqQGKiM7JmMFUJAUWj9bosp/Xjkq"
                                                                  + "T/fiHv96nIXVCWex04vW44HsvS8V0Ylm6oZb1mghRx/3m1LLUvsG9UPaV2v4CeMLqhn7yENpCuot"
                                                                  + "Pd6yofUyrKhj0vemVFIK0MAinaeAr2b13WRFZGM31eM5pccmBBjXiApCtfeONY+FZGcAl3RiQ3aR"
                                                                  + "qqEWcMyMtc3gLU9AL9yJN5zQTggl/RtCuIWw7lIRwgANDBgrMNcEMfL61Z3y0Yvtzk2jT6Xj1epp"
                                                                  + "dgREYpDcdtuxRX458bpYsIgAikZBAgMBAAGjgc8wgcwwHQYDVR0OBBYEFP72SKdmLnfWkWklsgZh"
                                                                  + "f8M3R63UMIGOBgNVHSMEgYYwgYOAFP72SKdmLnfWkWklsgZhf8M3R63UoWCkXjBcMQswCQYDVQQG"
                                                                  + "EwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkx"
                                                                  + "GTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb22CCQD7csytydc+QzAMBgNVHRMEBTADAQH/MAwGA1Ud"
                                                                  + "EQQFMAOCASowDQYJKoZIhvcNAQEFBQADggIBAERcEnWfm4Hkgptqoxityf41rzhhXVZx9wkx+rEA"
                                                                  + "eAPBbtgNMlkahAvAEmEKCVziFAMCWtxCLKXE62Jq/VSfzuUI3ZoNAlnrplnZ17KQBq7eXcVA/jzH"
                                                                  + "XM54KigkHSr5rAJPWDmLBdmr8dIzt3m+DSA5cFOfeiMadXWppP+hcWTtiKuPXSIRj5UKG05p5PnZ"
                                                                  + "F4jaJJKS1++wNYswIw+SXPvC95kfi7deWyu7JnTLdj+C3wP51yWY8anOQEjR+ZEtegxsOOb/Lh3c"
                                                                  + "/tQyO2tnqZyNG8emuO+zBPBhcIlRq64B0O+5QhMCEZYSov49ru/gCSQlEZX22zA++TcqLMkSAy2S"
                                                                  + "7cfRPl0DxLJR0OKQZk0PVK2f78ZkhEArwg/ucO+3QE6GAAnYWO8PLdc8bh4BhmucJ5zOcsfYptMC"
                                                                  + "CZA9aCDA+Vu1rpn84+JaOeLisNJkWha24ij7AMzzwu8uspPdujFthQbZ84cxaQtPZc3UN+X4EAVV"
                                                                  + "RciN6++j9q1UXKjwvpv+3Fb5w6tjccaONGaMMAtxD6NsAC7r6qmomuTW9kqOlvcUV4Q5TM4/JJfW"
                                                                  + "mQxixhMEGv9O08cGafCvOa+mlIyhQsxyAP9d+iEWwvMt/2m2uJDM2sLEQv8rIPf2nVfvewBT4Qik"
                                                                  + "AGdK43vRq8eGXpJnCRCue4jBbxDGhY63ktsxAAAAAgAPa2V5c3RvcmU4LWFsaWFzAAABVutBavYA"
                                                                  + "BVguNTA5AAAF+zCCBfcwggPfoAMCAQICCQDnwtsZrUgpuzANBgkqhkiG9w0BAQUFADBNMQswCQYD"
                                                                  + "VQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNp"
                                                                  + "dHkxCjAIBgNVBAMUASowHhcNMTYwOTAyMTQxNTMyWhcNMjYwNzEyMTQxNTMyWjBNMQswCQYDVQQG"
                                                                  + "EwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkx"
                                                                  + "CjAIBgNVBAMUASowggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCpeguzUe0G+A/9FDCk"
                                                                  + "TmxiDCkaiczYuIdXYXI9+llRwzR1UCYWYb7oVkcVsEPmx5+egDR4ceLDNPWiVK3OCPm6mlMIsFTL"
                                                                  + "Wqn51q2bFOSR515ON3PmltpaLyWOjsCS3JxUemSDO+R30koavTIJU6x7UI1/jBJCHadx40oCxgh/"
                                                                  + "kwz6FafUXxzEwOmeOT9xc4fY9cMhHXDtmH5VXoFOp3HkEmRvIrWvnnf/2Py5+hX9CtrkZLLmS7YD"
                                                                  + "ZkYTKfyIK4WzKFA/pN1tUzWovC6P2HtpQ1mgDAJ1+xV/k2FC/ZKwBoz4bpr/aEBt3q2C1J8lMMQW"
                                                                  + "YtyZSRnZMYilU5CITwhpFPJiDv8expe/JTwpFOMoZyj/pK8ZOzE+/XqdeE0VlFLtlqLc/wumc3KX"
                                                                  + "xTtt8EFRdr8VxhrHn8Mt4eVBZJuU7Kw2rynRq6V+Cj8iQfyDUO4LJ1+aTQ42y+q8QwF15ISLP78g"
                                                                  + "5vu82jB4A9Y/8+qdbU2jvG8vbraMJg380dbCqmDyc957UVH5CrbjYI9ji6romUtCXHzDdAkXTTKJ"
                                                                  + "yXvjSgzFlPj3jkdY0TdLGiLpGmIU+1wznDN+UgufXEkUP+aoA9SF68jhaVHmZbQn2n45QgBbNkKf"
                                                                  + "qREptcdEBPdu9527LlUmMTjCrY6Fcll2WcGgjS7Q/aAkjcS58Nouu5X9PwIDAQABo4HZMIHWMB0G"
                                                                  + "A1UdDgQWBBTsWSVhrBtavY0Ssr4c7P0rNeSuFzB9BgNVHSMEdjB0gBTsWSVhrBtavY0Ssr4c7P0r"
                                                                  + "NeSuF6FRpE8wTTELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3Zp"
                                                                  + "bmNlMQ0wCwYDVQQKEwRjaXR5MQowCAYDVQQDFAEqggkA58LbGa1IKbswDAYDVR0TBAUwAwEB/zAo"
                                                                  + "BgNVHREEITAfghBhbXFwLmV4YW1wbGUub3JnggtleGFtcGxlLm9yZzANBgkqhkiG9w0BAQUFAAOC"
                                                                  + "AgEAVkYBfdIAD9TmCTcV0FBTngsqNsZyl2RJ7wDeJddsY+4+MqHQgorq6a8m0XhVlFnEqFs0erbS"
                                                                  + "o/RC/4KLkfb+QyUp5/c26tGDmi8iAvfxMGU1d15S+tApVjVCd6BUqxJCY1ol/YUBkdtWFg1kXSzk"
                                                                  + "ukbFsoOVfDtfz4j7frv3xFxSTbn9QH63cQfEaEEaLPl1hW3BE6JcL6na3m15okQE0Is4nvedCfd/"
                                                                  + "hY+30YTFy3T/DFv4AwX2/WQqI+VRA/me9Tq/orEmU0K+VrkSCxsLAqQh/1Ue81NPY0VeEFAwEjlU"
                                                                  + "wOdaFm3oMOKxOxodS/Bt62ge/eDHvBAlh+d99n9qg6TQpH2FJHmxfX5ZEj9uclES5F0Yjih5tg+Z"
                                                                  + "E6U/bjkx2ChwhmuPYXObLs/iUgic54snKotMBA9H3nX6f4yZS7sZmhHmjhatB8WOEtuX6wv8Eqqi"
                                                                  + "O6pYoOth1wFKGNpmFm3xYSLriVApnHzrToxiaPS2N+bzguNrXveb3lLgku3w6Z/j2Inhi9kMS1/o"
                                                                  + "lOIUCHYJb7vfDr0D4ELmS6LF/3Q4j3Pjf/InaoSSCr44N/MR4zJvWvZYBz3FuoT7Ov5sekgihisv"
                                                                  + "Io5s7kRSnWkCHxV7b3i1HuyBhCWvtsr5PwlRu62aGHDx/zcFO45CHhA7nTg/H4LNYUKCXexuui6w"
                                                                  + "tVQAAAACAA9rZXlzdG9yZTEtYWxpYXMAAAFW60FjggAFWC41MDkAAAYeMIIGGjCCBAKgAwIBAgIJ"
                                                                  + "AN3n6RoioRdnMA0GCSqGSIb3DQEBBQUAMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                  + "MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxl"
                                                                  + "LmNvbTAeFw0xNjA5MDIxNDE1MTNaFw0yNjA3MTIxNDE1MTNaMFwxCzAJBgNVBAYTAlVTMRAwDgYD"
                                                                  + "VQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQ"
                                                                  + "YW1xcC5leGFtcGxlLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAKBCPc++0AiG"
                                                                  + "TX01xBv7qh7o7XP9BGpmFJbduuCul5J4/5XyPVQTsUSQtPfi1uvoLDEyi/04OyGFqSbthWRxidUC"
                                                                  + "BjWFANJeA/TPASPqsRH3NVNU1VtZq3B4Re87uP5jlFI07lsGnJcE93SKOP9LDsD8VwsbWgbn9BMz"
                                                                  + "w++oKfb4640cqSTe/5ta628JKJ5jnrb4j/UndpQX2lBVyaYZ4yzgu/a5DZqPSO15fFUNK7kJfFuB"
                                                                  + "LqMzIOcQwIe130Zh0lgbclIrHHuo1TC6LJg0HUCDdPjjEnRUqARV8NokawcXEGgknkiVHkm/FSfr"
                                                                  + "UjK9GC3uKDkCpWw/2+r4uh1FFzhv0WUJV6byMXmsStaRW2Nwfe07vE/m9VpuKF+UVlXmJ5JlSInm"
                                                                  + "PdaW2IzFUucOc5LMcjpCeYspKmQceSZgwKxM51ilc95FLmJgzKXsN63dwn8KPZh9QIRPy0p875C6"
                                                                  + "o7ZjZb+K0kq9isS9avltSriojmDqe46LJASyxu9N++sAENVjUD+4FpZi72o4R8iDv1prIQULYyhw"
                                                                  + "sWh/sfKdie5r7/wj3SJhiKYVE3veEvA/hevMJZn0byF1P/x0ofAKvcpl/sR3iAngDcV0L5NfH147"
                                                                  + "hvsskxsOfy4YZXR7GB0H0zCETeGuKftTGLipQDRNegIIT5l570bQHAb4GoejWJdXAgMBAAGjgd4w"
                                                                  + "gdswHQYDVR0OBBYEFIfno5y3gDsCxSLnZmJRxxGqds3pMIGOBgNVHSMEgYYwgYOAFIfno5y3gDsC"
                                                                  + "xSLnZmJRxxGqds3poWCkXjBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UE"
                                                                  + "BxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb22CCQDd"
                                                                  + "5+kaIqEXZzAMBgNVHRMEBTADAQH/MBsGA1UdEQQUMBKCEGFtcXAuZXhhbXBsZS5jb20wDQYJKoZI"
                                                                  + "hvcNAQEFBQADggIBAD2ZOIJPqaNQsobtSqB7oUTExHTJgMKaybjhiWfUYU/pv4EaDWckRFbo+fmz"
                                                                  + "Dsi6Bmg8kWfPrbBBk3w0dVVtXK2mKGm3gv8URLmrpXMpbNcFqDNjNIbeM/VO5xgTQ1zJi1UtbBUC"
                                                                  + "zA86E+ABm2PdssFqDz+TvsNt8gEqrSFfnmXGp2tohpoRvWN9gPK8BD1u/D3Jpj/TqG/MrZHF0ZDd"
                                                                  + "WmtnCoQJq3j6kIEgHm87nrMABkrpUV8dB1Qw6/5pA2R9azjqa6/O/7AP8txBWVJpgdkMgpfqXMki"
                                                                  + "UNiJUhxXxWnLNGNsJk2cFDR8haQGVmcG3B/dmk7G2N7XuEFq8jwstDXlRUDbTc/yieGdee5PYULr"
                                                                  + "9cfdd9ljXQLYi/3uDdIOgBQArp4tPNqa/fgViJRRrtecJ6UEIt/FtFRapKyXLyiHEJgLIeT02dea"
                                                                  + "v/DfecIPgQ9oS9SuAiRPWWh1MbY5I/QLa5P8kwDf4V5Dz0yU2vOkF37xBQmky/0gw0mRc4+RsxCO"
                                                                  + "mc2oChaKxr4c1wqKChjEX/wOFeQM50JyGI37ln+2ma5ymSOc3nYTqJMBNApoctHKeGTMzT0xMCi7"
                                                                  + "+j+F/sje6VtiEnpY6jMNtwDjSHjoigjhNeBAcJBm3YL53u+j8Nlj1l/pVCrTEwIkUzWDEXxseHaJ"
                                                                  + "Pw3DC7mJ5QTzSRmgAAAAAgAQa2V5c3RvcmUxMS1hbGlhcwAAAVbrQW5UAAVYLjUwOQAABd4wggXa"
                                                                  + "MIIDwqADAgECAgkA9QTGOPFAnvkwDQYJKoZIhvcNAQEFBQAwUTELMAkGA1UEBhMCVVMxEDAOBgNV"
                                                                  + "BAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MQ4wDAYDVQQDFAUq"
                                                                  + "Lm9yZzAeFw0xNjA5MDIxNDE1MzlaFw0yNjA3MTIxNDE1MzlaMFExCzAJBgNVBAYTAlVTMRAwDgYD"
                                                                  + "VQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEOMAwGA1UEAxQF"
                                                                  + "Ki5vcmcwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCtKNS3OeXHsVOdxdgsRoY0QcrK"
                                                                  + "O6AdzYrlRaiU+i1CzV25o/5Km9gAzleDUxLhZ4tVEn0DK5WQA5OB3qChnp6nujcRw4hd3hpjoIk9"
                                                                  + "+GFFFsq4nmsG9dYU7wgOpd1nYADPlu3GXjep/Vnvk0OgsPtyQFQJcTb6nnKE3gen/L+XZplE4USo"
                                                                  + "83RPwwMJZSE0JMLzyjS3FuKMbjOyeBeO3C2kRT/NB/KuFLsL/7iwEnnC5UAKynOJUNLvbMGHp2h6"
                                                                  + "l9jHRw6Sa6pYIDy5nsvqN6LPcYNLH/UXz/ZzqqXeL5jBz3cVIdcweccTSpRI3+KWNYrPq8rkorJv"
                                                                  + "iRUSXDHDMCmFop0YiJEtvDtgWz+ORNGtkdHuaqenkIfaPmVzikpTOvDWwOYrEuJ1IDETmDa7+MWl"
                                                                  + "ikuScAbi8Ch/5cQ8Va7M5Zgr3sypsAjrrF51v4/ClP2bJ5ixQXqwFdJdKyNSnGIrX8CniwqqQ6FQ"
                                                                  + "KPSve8LbTyW4AEGfvzR2TpqDztWGp9Ae1Jc5mHeOOn+6LE6f00CFyv1spdYtqBvfYeGeMXNXL1eU"
                                                                  + "4ax3d4ODjgO8zow+fxsfhmmyQS1rvABF+NpuRS+B7ML7txOBgF+Ge2znHA/xawntGeEOz6Lqh3/V"
                                                                  + "PLNvIq3ORDlc5baZ55oe0l4JIPBl3kUjy3Is1idGcKJXWV5D7wIDAQABo4G0MIGxMB0GA1UdDgQW"
                                                                  + "BBQ6FKLROz5fTG10zxsxEzocTqGwPDCBgQYDVR0jBHoweIAUOhSi0Ts+X0xtdM8bMRM6HE6hsDyh"
                                                                  + "VaRTMFExCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTEN"
                                                                  + "MAsGA1UEChMEY2l0eTEOMAwGA1UEAxQFKi5vcmeCCQD1BMY48UCe+TAMBgNVHRMEBTADAQH/MA0G"
                                                                  + "CSqGSIb3DQEBBQUAA4ICAQAvTuj+4fDSofGF/3puBhWPQfOn+HPL4IqYxzMsTfQ8o/k3SjMSQox4"
                                                                  + "XvL13TxZNeZInrC+1NEUyPt+FtiVF/9XkHL1emActL7J4hyp/yTKi49+O5k+3Jl2kkXQHZQqrbTj"
                                                                  + "Ja01muFFY+yZfakUHpDys7SKSC4xwrL8zVVoZ9gAy1fpTz8ERchdLe+B9QYLbeCYmX5jrP2NdtzG"
                                                                  + "jPs3P1p+VhD+ankIgDgQksKuwfRHWsxK4/Yy2nM3Aj1Uxwa9xWT1abrF+q5zEiA8/uB4m9dM1+Un"
                                                                  + "DNfh4TOdPvPrt8wE4uURTW+Jy2ahIIEx3V/vq5MFaO2IaHOoM94L51vbBS7ubg0z5HDXDVcKLoY+"
                                                                  + "1Rfor7A8LNDbPB51N0539PiQfl8ilqxamSmFbpLLkMdum8rW1GXv05Z4boyjlCfUkNJGBPXLA/sf"
                                                                  + "Kl7S1kBm4p2cl3H+b2QHaphKTNGSqq+5teSpebrh2L7F+FXjKxxwW0mfZNxWS1a7UKElD2Ova/Vn"
                                                                  + "JNTjOJyCUjKx264PYbWmDW3pLYHlCD+crja6+yevgU1A+mndEL3tlhMAyVjewtu6fbtaZ3Dh9F8U"
                                                                  + "ed5O4y1x/6I0OTC3a8AxEMsJDVb2PVJ2brqRw+faUdhMGG6VyhUbGa2PsTMfUQv30o+VFwai2bz7"
                                                                  + "hoFWQqkbwhx4Z+eJcABwhAAAAAIAD2tleXN0b3JlNy1hbGlhcwAAAVbrQWneAAVYLjUwOQAABhQw"
                                                                  + "ggYQMIID+KADAgECAgkA1F2rvQSQ7cwwDQYJKoZIhvcNAQEFBQAwTTELMAkGA1UEBhMCVVMxEDAO"
                                                                  + "BgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MQowCAYDVQQD"
                                                                  + "FAEqMB4XDTE2MDkwMjE0MTUyN1oXDTI2MDcxMjE0MTUyN1owTTELMAkGA1UEBhMCVVMxEDAOBgNV"
                                                                  + "BAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MQowCAYDVQQDFAEq"
                                                                  + "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAyHvSXxGgaT6RxGJ6hitvXcdFFrKVYB3G"
                                                                  + "ad9kJ+yYEdDyDzZj6ONLWKGoMDWHinFTDBv5VE7+/WGhJ/MamVV8q3ri1D+QIOT2EI05d8dw+2KX"
                                                                  + "qlFThGOdX5wgJ6mf+Kk+/stDzVGyH7DOGmSPyFF8hs0d+3kYlKiPxxu00SrYBCisuCTohKBYZ9O/"
                                                                  + "YlKOBeW8+fzxxHb3RrcqEvJchf6gGD6/786iHnbMae5m3QM/bILNtiOPAtmFQRSlHscl4HDzp59w"
                                                                  + "Xh8l1nrfTzFpWRbro330cQqLdNAUoWCdQT3dnGTCuYjGdtIDiYrIkn2rM36/8rNkWJ7xauXKtaft"
                                                                  + "LVAzzEcGEyh2bpUY1em3hdvt02BPpAj6DndE2Df3MsI+UNXO8VF3Ixo8xtw895j5iDFRfdn3NJdJ"
                                                                  + "Shvvb+HO2WxOVBlOFI3jVE58MBOqXwf9VrDL69CU5gTlLu0IHAzZQS3+la6/lh0CMxHl5GRCaU6w"
                                                                  + "xj42Dy2iNktYDN4h7C5bp/a4V791uep0Hf68ZHujQ1AELeuwo+m7y2xW9jiPaPZeGc3BCIGStD53"
                                                                  + "wiqPJ6OYbnKOyu7UePkfmukX1oqHsVWF6bJuOXOiTPUDZ5x/xXckMHTO2sPZVslfALuzSJX2Xzy5"
                                                                  + "ce1azOxeUWPdI5JTEecFn6TVFUaSdnmEv4chG9/SHiUCAwEAAaOB8jCB7zAdBgNVHQ4EFgQUktNt"
                                                                  + "P40v3mCli8ZwjCPKA8xqtIswfQYDVR0jBHYwdIAUktNtP40v3mCli8ZwjCPKA8xqtIuhUaRPME0x"
                                                                  + "CzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UE"
                                                                  + "ChMEY2l0eTEKMAgGA1UEAxQBKoIJANRdq70EkO3MMAwGA1UdEwQFMAMBAf8wQQYDVR0RBDowOIIQ"
                                                                  + "YW1xcC5leGFtcGxlLm9yZ4IRYW1xcDEuZXhhbXBsZS5vcmeCEWFtcXAyLmV4YW1wbGUub3JnMA0G"
                                                                  + "CSqGSIb3DQEBBQUAA4ICAQBIzOZuOHYdlFTfe3PePMgMZ80FSws/7MDmrpsgDKa07ocN3PY0D84y"
                                                                  + "0W6rXsD1kACqg7y5+gw2qz81kU8rYGtKRoB+1oFPz0Dpi/pIYq+nzyT/k3gRa68ef/CyX9BIa1xu"
                                                                  + "zzMAaIEvgvjAhwmIdrkBaE5MHgxCYRQy6/M6zwONelZHyCNs2ryPsE61FuYHbgRFr9kZuT6cRlDM"
                                                                  + "EJOzvbHX5iJCRJixSt+/yvdKtXkz/nSEGS+SHLrjggK3fKzIoYTRpQWDKXAHZUyebk8eShhpumEA"
                                                                  + "UPGZNnYiiyJdfcpkFAzAZVzIrnvB/FiyFkfof56DhV4Gnq0r33n2eK1mJ6J1MCRyaMtEjVvujemr"
                                                                  + "KmD16x4nZUI3zUtMHrbORuzjbmxl3apXdp96UyJKfbifnvoJ4N2yppiByWsHDkkmC/gbzmugaN4p"
                                                                  + "v4QYQrMIBauAY8+QKoBRy25EkfZZ5hOqZQV0Odb4COfnhvUM2oBvGTz7xID8L0RyqfEdZ7gEgNOa"
                                                                  + "C7EyzztJT8lXdu0Mm17JaisueFak9kt4aXYCUKM+GNniwj7Zu9njffzwmgCMiRZ/33hysih5iElN"
                                                                  + "PNoN1OU5NHyqp3Z/YIbqp9TWTMEbs1BUEsnyDu5iTbVvvlLhfnHBipPAUdzL6aGIlfNMQ7U9wHaM"
                                                                  + "0EtQgZ7nnQL7eQoW/I2/WwAAAAIAD2tleXN0b3JlMy1hbGlhcwAAAVbrQWWRAAVYLjUwOQAABjIw"
                                                                  + "ggYuMIIEFqADAgECAgkAkXA0Ujw8haYwDQYJKoZIhvcNAQEFBQAwXDELMAkGA1UEBhMCVVMxEDAO"
                                                                  + "BgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQD"
                                                                  + "ExBhbXFwLmV4YW1wbGUuY29tMB4XDTE2MDkwMjE0MTUxOFoXDTI2MDcxMjE0MTUxOFowXDELMAkG"
                                                                  + "A1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRj"
                                                                  + "aXR5MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUuY29tMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIIC"
                                                                  + "CgKCAgEAz4LAVVvnf2ViPH9rqe2t5IpfmDRlERi0qsvR7nFQtQO2KBVp/8xSAujZeMA0H9RHLeWZ"
                                                                  + "PnfuCLDPJyLoPofGHnI2O5Dr9+ru+Lw+5KxgAr6ZWcHVNtfwbGCvjYIDmuChNUuV2PEJBZmmgdTB"
                                                                  + "iOyrVKPNV30TOkqFkTviOgDfwFcAUC7RNO9GEmZa8s7emyJkWmogeDlcHoCAtydqLy/1nA7tq8f6"
                                                                  + "KowrwxFftU9CsvmchQaqQiFtBFClZu/GVT2s7mKdI1VBzmY1NPxelzlc1GNxzQh8Ckj91n9dXz00"
                                                                  + "MFhMI8OnvEWdKue5zfteoV+pmObwX5e6W24MjA42Urco6S8JoaOFuOsFAhara4Xr7JiKzMH8AEyz"
                                                                  + "CnBQj+FOOIfEkEn0gpWsJMts83w+9EFkMNgNziG3icLt0//kZaUuE2qms5OYN+GebqVbaa6KhKon"
                                                                  + "hOgDgS9uPxz2xTpU53k27kI7ZJe60YwcJb7jr+lmIeUjpwLFd6oCMfdjvYBCiUYXhzTuv1CKQByv"
                                                                  + "iLgRT+6OYZzUk27rsqxR96KVIB1cW+7HBAfYmBUhA/s3jawKxGCb4XTramN53ONfWxOJaWjQ5B5z"
                                                                  + "u9xtwY3451xtMW7jfjL6ggAkJks+AyZ4XXVdJTfgJiaTFOeiPq4Q++ioWwLVQVxv2ZXQKts1cpFz"
                                                                  + "t7LJhYcCAwEAAaOB8jCB7zAdBgNVHQ4EFgQUvCJ0sdBYkSRtfVkttqS2baHLgtMwgY4GA1UdIwSB"
                                                                  + "hjCBg4AUvCJ0sdBYkSRtfVkttqS2baHLgtOhYKReMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdw"
                                                                  + "cml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5l"
                                                                  + "eGFtcGxlLmNvbYIJAJFwNFI8PIWmMAwGA1UdEwQFMAMBAf8wLwYDVR0RBCgwJoIRYW1xcDEuZXhh"
                                                                  + "bXBsZS5jb22CEWFtcXAyLmV4YW1wbGUuY29tMA0GCSqGSIb3DQEBBQUAA4ICAQAfMx8vAkjnLQFc"
                                                                  + "GePtafiI57+M/arx3Me2upoNuaAYK/WMkxvWksqF2+rOjPIvXp95O9gakKlqhvOuFUHzPv5QQzOp"
                                                                  + "wKIicsXnztNoSwBM9WGTe2uyxNqbStUX2VND21GvrjmsB8dU9ZyShkHfNlMY8HXLeWKA5nvv14wK"
                                                                  + "ymg6MWQbIiR9yqdhxTv/KKX0ryT/uLabM+2zImCjhSRfYR+8YrSxf89otmPKoS29XJ47gMEtQveL"
                                                                  + "rA8k/5l1DAGoDx04lsnbT486Hbj6cSaKdtJOQQwLLA9xEnwcReeXJ5eigyvKIreQ4bnwG04CQ35k"
                                                                  + "efdtcOYCS44IeA4AwJLDukoiFu84NH1CegNbQModa8heVN3wOos0LMijmpKYfUf8by8C7V3Yq3B0"
                                                                  + "OSo3NkzTHpuhpkiMSutpGaNRBRmGvHbSzuf6WWKxPNChyTtIjQ4Z5Y2ogBvoqL1atE+Zhwu5mPHo"
                                                                  + "DvZTd8G46k8PFTTDYQ7MKBGgRdIVffBOEy0Xn31mqkgjSGwJuHB2pkYVRZ8mB8Y8mDgchmYtbhY/"
                                                                  + "fyiXYKX/HYzAVqbZm4OzxsfwkhavdTSbynt7/hP0dT8gC0XxqHSeWg8wvYYXzb20ennUEcovf3YH"
                                                                  + "8J73vbfSXjm36P3vs/dqKUhazXpsAIujkhRrk+EMAdi024AKvpHQr4T5UQpWuwAAAAIAEGtleXN0"
                                                                  + "b3JlMTMtYWxpYXMAAAFW60FwnQAFWC41MDkAAAXRMIIFzTCCA7WgAwIBAgIJAK0AE8CfFEluMA0G"
                                                                  + "CSqGSIb3DQEBBQUAME0xCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhw"
                                                                  + "cm92aW5jZTENMAsGA1UEChMEY2l0eTEKMAgGA1UEAxQBKjAeFw0xNjA5MDIxNDE1NDVaFw0yNjA3"
                                                                  + "MTIxNDE1NDVaME0xCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92"
                                                                  + "aW5jZTENMAsGA1UEChMEY2l0eTEKMAgGA1UEAxQBKjCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCC"
                                                                  + "AgoCggIBALJfiVCXHtyDg+yTZVp/x4FnGvDCAIEBzq3WnziCUF6paMyWlHWctM57Xab63v+A2A68"
                                                                  + "ey6K1RNhQX38ESU3rdmG+Kq+nNaMRnynkbEFN26XSq5xV2sRq4KH/cEosfU76RsNUXdPDx+XPjG/"
                                                                  + "itr6kuHUCKMGSXlDqiyU3uH+v2e67JM4+Fu/RY8pHLCTO5IrSjDAkHPSvItN2WEz9xqHqTzZQ8ob"
                                                                  + "97UMCQsK5xCF897vrQfX0rS5izk1Hsd5Psz0vtotIMzi3R2A8JVaoKryjaoCndxc6IswX96W3Bez"
                                                                  + "pUTUIIpiAfXDeNfvYSHfKHr83WCIsU5+Gu62YuQbZMJX9dDdmlAOFRdfla8oePTkiO2+5xwt6412"
                                                                  + "E1K7P4guoDA191qwXj/UIIwoA4hFnmPtCkOEf5HPytxvGdP3ssZ+Wx+ZFx3vaBAw2HtBaDrUW1Wf"
                                                                  + "BgOGtOYjivDho7Vkt8Ap3mToIGqkJrwWc+oEOu9926KZFKaMzE7ve0fIpAXKhYMIZZGOmr/wi1v2"
                                                                  + "eqD5H4gXS+XYoKjKV/39NsvL7nJnSxaqrqvW8Ja/SbN4ps+nRvpDQC2/rLG8ZUN5J0IcYSGdXsyx"
                                                                  + "9yvYGicS78vzdYjwyJzCJuINhfFRJDVXitgSpD2AjJHlrb3XSJ4ypj3auZ6KIUXhdq7cS+XNABwu"
                                                                  + "rIodnQU7AgMBAAGjga8wgawwHQYDVR0OBBYEFDUXFvr89Rzmep0MGf81gNIXINXyMH0GA1UdIwR2"
                                                                  + "MHSAFDUXFvr89Rzmep0MGf81gNIXINXyoVGkTzBNMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJp"
                                                                  + "dmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNVBAMUASqCCQCtABPA"
                                                                  + "nxRJbjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4ICAQBlkCIxCpbTRfoo7u49oduXCj6f"
                                                                  + "gtEo1H7U8hkwxrh9SvH58D8kRb9Siw6UW/zfF4kSlCETEVTegtH8KDGcyWN7Ef6zMVFK5ZpPpTtE"
                                                                  + "rGlDyXw4Sasy1xNuYeHAFOWo8haCUybFXrykatkJf6p8KX6dfEA+p6nWSoCPY4Bh5gKFLxvTNc/B"
                                                                  + "FtsfZQsIZKWQxL/qYGrn/TlmsDgyaSt5OMZQr4FQLEVGZ2Z0NN7AlTPDydUH58y7lbJv21sddK+S"
                                                                  + "OUBIGErWjV83bFx4DX4FPHpOtK89/CYOSQccqmYLswmRPSjCINJ4UybxW118DnGp9v47kKx6+bna"
                                                                  + "zOo4te//kjl0nv3ka0dMlH7gG/Q22fb7zL0aEI/HjHUsNoEtw9kQxW+V3Vx/ZdW8NI4cdRF4v/av"
                                                                  + "5p1qM1/TVW93Vm2VnO+lSRB4MXE9jpc9AxSVSNX1Knyuso/rKxbwFmJkxy1gcfSCQnfP3LvNdgVV"
                                                                  + "voq40bbRd1P28RSrGTOGSgILNGk7MGE7Fki2SL6RP9SRLE/5vdUyDHg8XUmpR+PlkN/MQvUdANZ2"
                                                                  + "LyDObsKUdN4JvQarugCZ1vYicd4VjPY2IBHRRCIFlkeqgQ2PBR3NwJ4ppkby/384ojyEK9T/rcix"
                                                                  + "pBPpDEQroikXliqWNGggU6kPPNLoUsoGUttt/fuDoao2JVU6uwAAAAIAD2tleXN0b3JlOS1hbGlh"
                                                                  + "cwAAAVbrQWwDAAVYLjUwOQAABgEwggX9MIID5aADAgECAgkA/fDYO6N4ERQwDQYJKoZIhvcNAQEF"
                                                                  + "BQAwXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0w"
                                                                  + "CwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUub3JnMB4XDTE2MDkwMjE0MTUzNVoX"
                                                                  + "DTI2MDcxMjE0MTUzNVowXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcT"
                                                                  + "CHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUub3JnMIICIjAN"
                                                                  + "BgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA0BRKztgdwAfnvwCarRJA1jPCL7VE7HKAvH+efmT4"
                                                                  + "7/kqCptnPpPOcm7wRDgtNSA+IGqgxKhxLbCnl0cv58YCu1a9MJQLIqvDApSdKpPtCCCPQ8VYRCTm"
                                                                  + "pY14m4EemKPbkSV+WluFK6tEQoYLz1yM5lAOP0A3q7sKxDv+IMOWKu4felcxo/gI3GWg50azh665"
                                                                  + "O/mJobF/0RX0pxVziqbxJrmkQpHeYrBJlwqkhQQWFq1RtTCVYCn370An2w0ylhz8Z7J1RKOIV+Q8"
                                                                  + "CDyFSPwV4Jy7PIDNvklJDPUDhuXW7xGdcaJuGTzq8LJywUhN8UAOK/mNkxVZDnmBJ806uhmhUglm"
                                                                  + "9zIH0xt7G55EAk8gCTkMCn1iAK5DdK3KrgE64xjrb55MQZp2K+zgdGQFNGDSCPCTc2PDieTU4Y01"
                                                                  + "AAAP6Ih86RRDuitCI7dsSIRnt7/aUAAm65dz1IwZX9AK+plSxpWuZ5ByZVEz2v7jqoAhV4zy9uqz"
                                                                  + "FuyPC/JeSZMWtIZ0ZJ4QjOjle8RRDsdKcXsDa+Xy9we5OFK13k0azm4lT6uBE0OQcKLUG2HP6rWi"
                                                                  + "C8DwPKo6RqkjFhkFd+9OGdsKgb5tSIkIfR/DIqDIyhr+LGnwRzHXnMNZXHgmKdiUXXpR60ZRilPq"
                                                                  + "QEyFp0VBM8hukq5GJ6suZNcPdKL5iyJByFECAwEAAaOBwTCBvjAdBgNVHQ4EFgQU6xXwdAL2OYyF"
                                                                  + "GNI8gvEFQ8xAIgswgY4GA1UdIwSBhjCBg4AU6xXwdAL2OYyFGNI8gvEFQ8xAIguhYKReMFwxCzAJ"
                                                                  + "BgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChME"
                                                                  + "Y2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxlLm9yZ4IJAP3w2DujeBEUMAwGA1UdEwQFMAMBAf8w"
                                                                  + "DQYJKoZIhvcNAQEFBQADggIBAGrk/bfEx5v4bkAtwLrajrI2ZUfM7pe3FOdxdtuWqCUgOuHovhmc"
                                                                  + "AhPcitgBexMaGRySYi2XZFhCw3gAt/eGgb6J/DeEU7QaYU29QiVJKV7xi31OIuld8ZWYIIrUYpem"
                                                                  + "L5QKYPi9Rp4TyJM2NOaxGmmQyMlRfSh8KgzGfd990z8Ebqv31bIGAVGKCZZzHNLok1xFF5qRclri"
                                                                  + "twBqaID8yTFGxxQ9BJNN0cX0FjzAg6ZvpGYE7BDSS2suQT5aO5rGKfY/sGbm+lzCis2V2ggwQSSw"
                                                                  + "cNkCuSNgytaHc6BD7n5l7wm0m9ZoAM9AG9/iWBD8NwW/fDKfiAxXNdvxeGpLrJwZmyvf+YziagWg"
                                                                  + "U/R+Q2/pxVVWxT5ON3vpOcv4YA/DCXbppiEAHx59h2cI+z4BUUlRmyFwSJZPD1oYWT2Y+wJjbIqK"
                                                                  + "ixGm5YpaFzOa7og0IzlZICn4Kea/r1Y2no4ZGHL6IcIjftgAPbF7XGZqrDMdte9veA8Cc9T+U60v"
                                                                  + "TMyUvyolIJsBtztyICj8WaZWdmd0iCHA3IefepwdW9zG5qcIxBhWuVTrYjloSl/fdoKCSZkQYxph"
                                                                  + "TVK4ngqvl9Z5N96/rIjmTKwTmJ2XV/Slp3OKrpp7gGv47G0C05umNvrEoAoOPY6GJNdD/uJahuH7"
                                                                  + "dC6g1c2tRd3UU1D5VjvP4Td2AAAAAgAPa2V5c3RvcmU1LWFsaWFzAAABVutBZ7YABVguNTA5AAAG"
                                                                  + "HTCCBhkwggQBoAMCAQICCQC6gQnNeaWpszANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJVUzEQ"
                                                                  + "MA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNV"
                                                                  + "BAMUDSouZXhhbXBsZS5jb20wHhcNMTYwOTAyMTQxNTIxWhcNMjYwNzEyMTQxNTIxWjBZMQswCQYD"
                                                                  + "VQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNp"
                                                                  + "dHkxFjAUBgNVBAMUDSouZXhhbXBsZS5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC"
                                                                  + "AQDJQHx065CB4RrCR4ivSxdoIswDTQ2MFdcsP9wYZeMyUB23qsvRhNtmX02CPeYqOZsTJSOWjg/x"
                                                                  + "e1XP8QsO939b7vqhS3w1JJQdCTlYTuPSoUhAnkUjkSpruukM/IzB2dPl3x5ZVwYHCqHFDnXNYjhm"
                                                                  + "kCH9Dgsge7h1MnsSOIoSyamJl61Cfj5M7VZMAXyZchZbfc6MPXEsaxDnzVb7n/r6NuaPRpmgs8O2"
                                                                  + "WrjQ5/BaPTHkF5/7dsQH1j6v/XurwF5/yPYBzvzHFBEZwwiZCXXXi24EeUqjS3MBK2k55qVZwEn8"
                                                                  + "UWOHPksAN/dp/LT2QKNQAG30nLFlJ7XtBbEVpDqEhpAmTfzP/ri8XfTBBiG7xGrTay1cLeeudSWl"
                                                                  + "CgU+hjQWkGeGu41oW6G/2mi4TIZw91pI+F0+8P7cufW/tK0nXVHLseCXPXW54455ieH7Z4SkdD0m"
                                                                  + "6ykrOXpRTBlKmAWEGHHAQmmtTWZkHWqrFR8wiwYi3dvrIXqbcPe85+wxh6o8fW/v3JhkLnH25A4r"
                                                                  + "TFl8xh+zzMxcmD9WEXItS3AAsUnQ8ALBv6D3gkGt8UZSc3IyrJZu5gRpW/e7QFEzhySot9WUDlG0"
                                                                  + "TdeKIxJm8gm1GDuwNCVFYmBXpFeplwPGZHZQORuAe7HolPqmTkdJ8qdN6UevSDghM6Jk9CwjljV5"
                                                                  + "ZwIDAQABo4HjMIHgMB0GA1UdDgQWBBRISQpA420Ivhyq/i5TI+wAWPPvZjCBiwYDVR0jBIGDMIGA"
                                                                  + "gBRISQpA420Ivhyq/i5TI+wAWPPvZqFdpFswWTELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZh"
                                                                  + "dGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRYwFAYDVQQDFA0qLmV4YW1wbGUu"
                                                                  + "Y29tggkAuoEJzXmlqbMwDAYDVR0TBAUwAwEB/zAjBgNVHREEHDAaghFhbXFwMS5leGFtcGxlLm5l"
                                                                  + "dIIFKi5vcmcwDQYJKoZIhvcNAQEFBQADggIBAKurURdQkpsdKaijtUZQPCz/YrpDiHocSseJ9jwj"
                                                                  + "iHS2pkT4fweK2gVP9S52fzNyA03pBdXUg96nWQo8EKaRjV3pT53Yx7zW85QBYqfwd2qvVSKJhJYY"
                                                                  + "Qvjx3lZa9wGksb4rkhms60hCL2A3Lf7x/tWhoAuyYrSCC8JjXVKEWJ5zNq/iKG7DJz0YcY9qgh6a"
                                                                  + "qacTm9SapdyFxjyjRF2/GhC3U4OZPajThOYS5o441ShHrJO/I2IvY5C0Of+uWqCG6Qiw/Ob0zCjh"
                                                                  + "/xNKzgj8IInVKC6YGj+Jonnn46/q3JvQmg6vhKqDZjJ+ias8vjHHYncNzxIs992+hW6XQwH2cZ4p"
                                                                  + "TUwf7gBYdgti0w1+u/IEezEHH2O0+3GT5MaoEO51Zn9jyMqLbhqwqTlHNF2bCnX7cET3NS03fjf2"
                                                                  + "NZZ5K9vEmhlMktNxdpuF1ld8Vbpj0I5jVKgWj5HHgdp0kB+8TEmF/kqiWWSLDR7xnzOs5A1V+9N7"
                                                                  + "JRCNrWM8veSG/hQT74pBufplXlmBo2yJgl3DlS/R8IYsT0O6mY2MWISvFn1ALuKyl7ZPhNIooQ5p"
                                                                  + "Dt+NVSxHZy1cXtvviEjF6AeKv1uldyyuwHaeTUsLb7IlHKu1Vjm9AMVGvsP2YPjU3HKlqBrWSRIx"
                                                                  + "OkWni+D4VHu4dmpXokhHLRVDJmEMsf+hfdV3LJlvrOsAMkijCJBQWanfjR2IriY="
                                                                 );


    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp.example.com

    private static byte[] KEYSTORE_1 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQOSYAAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJbpiFtanggVFjHUoiw5SBLNdnZpIbHao5oqJ6X7ra2IYE5pAYc8lA00P8GAt8ZfVFU/r1"
                                                                           +"iSj26gWQzbnOqgICfLEe1UNPzMmutxCKHUkZIqlXgvc/Ga7eNhZFxLYoJcjSHcH3rXjfiLihR7fi"
                                                                           +"ig7OavC88c3qnZld9SHcVSGquUQf65UlMDcLz5ro4JCWojxP0HqmaysTfVW0qGNSeGZcLM4F8siO"
                                                                           +"x/wDjDXaVV+4nCmHDnKM5WulZScq8xiMUhSSBEU9Pa8qXIdnr5NRWGIpm1APsZzhpd64mt64Nq4b"
                                                                           +"sHmwNEP8tIVNR+0N8zbgzsV0int1frqy9+pbXZE1khmnB+g6nrJw4SAQRcsG9BC0DVYfJsioASOK"
                                                                           +"EG+ypcJ7a7IsBK+6ltJSU5hlwUV363hMqi0pPOsB9pch5Q2amr+fbfD/FEktE5I4ibfsm2Bw9xSj"
                                                                           +"Yz6CpxSFM66SwLVZ2qeOcJfS/LOHHzEllNXZlWqIGc40v11Y5pnO8aUOJd++nYGW9oXmUvQZbChO"
                                                                           +"qr6aAABRTOWUXyEb3KoO/IGWj3O4pI36su3pDdMHDWOzjIx3+X2NmPnvg5zKsprXJ0mP/GVIcTqb"
                                                                           +"y/Revy9mLxEYGgIk18jHglvOV86lZjft3DI/loBi9W0+CH0SD42XXTGzQo8aCVGZLEE1yZ4MRFNG"
                                                                           +"W6kIKXmnOe/2ifoXLDqFU8QI+/FH18MrhXQTGDd0jtFsMl7k2CDJxQktE5olBQ3dVinBk2c0wVMJ"
                                                                           +"WmBPr06CBz4dtjTWTqZx1UNFfRsiErtqJrnoDNaONhWnvSXH34be1M6yMSPITbCoEuRx3j+WyJeW"
                                                                           +"kjhyapdU/TQn7qXsXEhG9P61uGAF8f8neD0nE00PLVFhzBoW5BCuJoG82132x/cjiCpayvOZ8ftE"
                                                                           +"YsFMm1+aVO68w7DIspY/V7ygDpun5+bkJ1PKoyD4bjjqAIOZAdEByIUAPzBpi+cTn/4TFrsKOtca"
                                                                           +"GAvHqw3fZKUbDfrkYyy8Awt/QF7JF2tXZLZbZfNQegqgUOb1YJ3CS0PONL7HqC2DHwpen29Wpw4A"
                                                                           +"9ilRfO6bkyokBfr8EJULFByMK5lWKu556hi+ngqHMdiG7WcrLOIZHDbfE2uz10b6Uc9tQZHwkct2"
                                                                           +"LfGJ2DqcrT7jUCaD4g/P2lHeuIF8DwviXwZmq3GvGUvl+E0EeFs4jQoEWnlp/K2NZo0JzaGTb3Mm"
                                                                           +"mhhrUhgwlpMo5/LKuhPuHfe0cWIEWzVzoL7SD90OyS4Qnod5M3Sw48KPYJ4VL2/GvKxSGbtZ3dX6"
                                                                           +"cwHYlIqMKgSFliYO4Gua9tBp+KywPjqP0DN0Z6cOz+JU3wFU8O/R6a5VXws6a/GhhfBLOK0Mbs8n"
                                                                           +"Dl9qdbybhD4Gni+1YWxk38fHgH+uKWhsJLTVAlXqWOGnB5mfxW8HzCPCAh6LUYGmjCy4Lb+nlWck"
                                                                           +"QtaAu+INIwo/WRyJDLRxbbGscNnWdHCxur3KXvV/SfPI0hMV+psWW5duGBhZZUndh3oSbXMolqhZ"
                                                                           +"mqn5BEtmM2NPeVMkuep5o1FIut1y7jfPkiT5fO/IBjsdX2t/Cv0htdLEtUcvcPlCyfYbnW260JeF"
                                                                           +"sgmYLzFS6sV667XgeZuZm5fVX5TxVXqr09/ouiLpO2bDw6/i6cs5hok2QZMQ4Ib5UovYdF9Va4nD"
                                                                           +"SFwET5lNI/SfOwRkbpgVcZH9I+OfW7yJ0DyYdgaGsDJEQ61+WMYPyeTnTJ/ELX2M7wgT/IFqq7t8"
                                                                           +"tvHUwBgIWZAtrcEJmgpZPJMFPlczRQkRP3wV97E3jmSVEcDltPmZTGBjKxKlmWBTdPbSLkfUcu8n"
                                                                           +"46ut4owb/4HEVsIFaZjNo0vUm+D0GgPoRuC5gHZ5tEbBg9oEUPJ9duRLnbmI7op0PYt/qC+ldD2H"
                                                                           +"0NpbPV77GiuGkWk4dz8vuEkd1naM6lMzB/noWBBTrkk1ECcBWHtCKp5cEGl67V4LRVHW69c99AK6"
                                                                           +"k/9TJIAxD1/Ed/MLnXV2FZzlzN9p33xBpPGgU7LmW1slii+vWClu/ZGcCxxd5453e1shdKBxjfSY"
                                                                           +"VkZpr7BSlUtl3HPUYWoih3t58EuBcR4CtJ16c2i0z6colzrIoQwUc27yHsSHhFHXyaXhyMcmn6Bz"
                                                                           +"NnQjKjuhSSPoHvh4ZINjLGOU1J5Gp9bZmGhiBpNeC+vPC/vUEgPf8YYdlr4Dgy9LDznNHZP6oehl"
                                                                           +"8nohY6v1Qg8Wd6ysMMMDPG2uiSf84rgaveKXmqb1ASDN5Mo2HbpVd0TRIFM3H26fqkjSV45TDeXg"
                                                                           +"VaObW8H6ccSgP70CK+0CufRbZzNVSEG+YtYy/L12kFTibGjlf+z/NxyUj6tzJe/gy0vdQGoCNv4Y"
                                                                           +"t4DZnkB7f+p3rohDd/RvnyhDVbJuje6M0CTW9LjoeEZqrAylpsbjIYoDDhMQ9emaqiPNp0RoDLVX"
                                                                           +"NWHmMV4Bjqo4zKQFHJnXzAW4L7NbZBu2XRNEAgGm0Z+Ea98PPDJla+FGCMyW1rwT8rUIT+EOhK11"
                                                                           +"qCJ7OaV/pBEnZvM09fEgea9gbnhq99xFPJNIDOCzq6Dl81PkJr1STp4HRxNdrL6NhjnoJ0DnrJEE"
                                                                           +"U8IX4XlgP29pcVJ30mZ9oJyjJ624BqfTTSB9SvoMmfUob0tMdk1rDGDc7qr7b/LsZTZRVlR7ncw5"
                                                                           +"CVhc4DNYGU3ktyBbxAznrLfW9MCWx2M/IeZjuWSDJI238hu8AVZTnWsc+mJZ1Do5JV3ZHZOGhiAY"
                                                                           +"ph+JavTUeuRSIyvVvYu2LeRpQdAqJ5wWGH8HZJ4kVq4N5+xSdPVHeAliS73k851Uv2ZJGxBHsYEp"
                                                                           +"zyQEtfqAkvV6LTpQ8iObhdglmVnm/gU9LqnySA0NxwGe2WtIyuGnOrwiN+mDBFYrlBOhl3nPeOD1"
                                                                           +"9ApL89DPsZWClvFuzaqgSY+iH9SvHPuyUa5T09O49kyy81Rd8YZ3ipjkErB4lEU1dTtGqJNLjP12"
                                                                           +"FxuDgtWZPEeNlVN89QBlp6dEA9Hdv3jt+xpBQlR9OuS+PUb5U6Eyws6UQw267aA1jpXaMygLXz+k"
                                                                           +"GFRkRXtK9h9vkuj1jwco3mwp9OhZZvNTVe/BdVB972S5hXZQlh34wzRBr2lY+jz13C0Xbhb6ptbe"
                                                                           +"HWtmYwi2IzaNbwr/DyNTqwaRZG8R2mps32Opg+WbWQXwKTMlg0YhnFpR5pYn8lMzNLjzmnDN+P1T"
                                                                           +"f6DLqcI6GsIcLieBXPik7jj4bDceTNaRYIx1AAAAAQAFWC41MDkAAAYeMIIGGjCCBAKgAwIBAgIJ"
                                                                           +"AN3n6RoioRdnMA0GCSqGSIb3DQEBBQUAMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                           +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxl"
                                                                           +"LmNvbTAeFw0xNjA5MDIxNDE1MTNaFw0yNjA3MTIxNDE1MTNaMFwxCzAJBgNVBAYTAlVTMRAwDgYD"
                                                                           +"VQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQ"
                                                                           +"YW1xcC5leGFtcGxlLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAKBCPc++0AiG"
                                                                           +"TX01xBv7qh7o7XP9BGpmFJbduuCul5J4/5XyPVQTsUSQtPfi1uvoLDEyi/04OyGFqSbthWRxidUC"
                                                                           +"BjWFANJeA/TPASPqsRH3NVNU1VtZq3B4Re87uP5jlFI07lsGnJcE93SKOP9LDsD8VwsbWgbn9BMz"
                                                                           +"w++oKfb4640cqSTe/5ta628JKJ5jnrb4j/UndpQX2lBVyaYZ4yzgu/a5DZqPSO15fFUNK7kJfFuB"
                                                                           +"LqMzIOcQwIe130Zh0lgbclIrHHuo1TC6LJg0HUCDdPjjEnRUqARV8NokawcXEGgknkiVHkm/FSfr"
                                                                           +"UjK9GC3uKDkCpWw/2+r4uh1FFzhv0WUJV6byMXmsStaRW2Nwfe07vE/m9VpuKF+UVlXmJ5JlSInm"
                                                                           +"PdaW2IzFUucOc5LMcjpCeYspKmQceSZgwKxM51ilc95FLmJgzKXsN63dwn8KPZh9QIRPy0p875C6"
                                                                           +"o7ZjZb+K0kq9isS9avltSriojmDqe46LJASyxu9N++sAENVjUD+4FpZi72o4R8iDv1prIQULYyhw"
                                                                           +"sWh/sfKdie5r7/wj3SJhiKYVE3veEvA/hevMJZn0byF1P/x0ofAKvcpl/sR3iAngDcV0L5NfH147"
                                                                           +"hvsskxsOfy4YZXR7GB0H0zCETeGuKftTGLipQDRNegIIT5l570bQHAb4GoejWJdXAgMBAAGjgd4w"
                                                                           +"gdswHQYDVR0OBBYEFIfno5y3gDsCxSLnZmJRxxGqds3pMIGOBgNVHSMEgYYwgYOAFIfno5y3gDsC"
                                                                           +"xSLnZmJRxxGqds3poWCkXjBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UE"
                                                                           +"BxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5jb22CCQDd"
                                                                           +"5+kaIqEXZzAMBgNVHRMEBTADAQH/MBsGA1UdEQQUMBKCEGFtcXAuZXhhbXBsZS5jb20wDQYJKoZI"
                                                                           +"hvcNAQEFBQADggIBAD2ZOIJPqaNQsobtSqB7oUTExHTJgMKaybjhiWfUYU/pv4EaDWckRFbo+fmz"
                                                                           +"Dsi6Bmg8kWfPrbBBk3w0dVVtXK2mKGm3gv8URLmrpXMpbNcFqDNjNIbeM/VO5xgTQ1zJi1UtbBUC"
                                                                           +"zA86E+ABm2PdssFqDz+TvsNt8gEqrSFfnmXGp2tohpoRvWN9gPK8BD1u/D3Jpj/TqG/MrZHF0ZDd"
                                                                           +"WmtnCoQJq3j6kIEgHm87nrMABkrpUV8dB1Qw6/5pA2R9azjqa6/O/7AP8txBWVJpgdkMgpfqXMki"
                                                                           +"UNiJUhxXxWnLNGNsJk2cFDR8haQGVmcG3B/dmk7G2N7XuEFq8jwstDXlRUDbTc/yieGdee5PYULr"
                                                                           +"9cfdd9ljXQLYi/3uDdIOgBQArp4tPNqa/fgViJRRrtecJ6UEIt/FtFRapKyXLyiHEJgLIeT02dea"
                                                                           +"v/DfecIPgQ9oS9SuAiRPWWh1MbY5I/QLa5P8kwDf4V5Dz0yU2vOkF37xBQmky/0gw0mRc4+RsxCO"
                                                                           +"mc2oChaKxr4c1wqKChjEX/wOFeQM50JyGI37ln+2ma5ymSOc3nYTqJMBNApoctHKeGTMzT0xMCi7"
                                                                           +"+j+F/sje6VtiEnpY6jMNtwDjSHjoigjhNeBAcJBm3YL53u+j8Nlj1l/pVCrTEwIkUzWDEXxseHaJ"
                                                                           +"Pw3DC7mJ5QTzSRmgkd/u4CjQAdsxRxvPQ9J/3Zx8YeA="
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp1.example.com
    private static byte[] KEYSTORE_2 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQOxJAAAJhzCCCYMwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJb7B1Wn+7tr/UMY9U5S1jldHUBVs55D+bq9mEnI+15JSYH0HOgTeG2GHUMFg5s+P9sMX6"
                                                                           +"OQL5awZrIrj/IBm3H1JGYvQ90tGNZpbIyPxx912QRfM9qwx4x9q8/EX4C4HqUYzHPrwhtdauGuM7"
                                                                           +"0v2QMKG6ZKTb9f8VSXhhrntVA0V3hmHpFozpIm1vFjFHQvKwOk0H/ig0ZBCPEBGT447lSn8DyM42"
                                                                           +"TvlMbA5n7HLpRP/26yNrk6lKiOS0GhmEl+IpraiXnoU5yl/+FzQ5s43pp6RTkSjaKFFuKlwrdTxL"
                                                                           +"PtnS6vQ7tcg1IdlAcrHuSGro3PmqzHxTVuugJzOubUv6His5vAkLzmnFNMOZb22bWV2Wx3DWLnXK"
                                                                           +"bRCAa9YTK7C0XH/+1NFxxBgPgp7xE675eMd8Aw/BdyCxhDctYXs/WKwQ0hyMTeWudFnCYZxFpeO0"
                                                                           +"2NKjJcxED0aL1uHJp1pf/WDZzvrXRxSRf1AGQ9Q5yDGNKj8VyBdD9DnKUqIqAOuBxmenYg0XLIGP"
                                                                           +"DPlkGzg70Cbe7+1Q9rNZdH5qaOhqShx/0ji/pfXwyzjvWTp7cF2u9IVVZ4NtWGUffLGMU1fjgIX+"
                                                                           +"GQ+DzBm1TN9Vjtl7/9F3V4uq+1stN9Zb6dDHnT1NdAVtt5FxkK4n+3ZBGpKTZiZqwOPvN9ZE+oAN"
                                                                           +"Pho9d3KJBjznnZLgLjoyO4QUh4//AknFaldlu6FdtOZG5puwsvJOMwifYi9gwhdbLI+JcNvCTss+"
                                                                           +"4kgDUMpKcfeGuhqwc841lvZcgcxcm7rinRwia5aAdbanxRUe/M2rdQlxXPudXNSs2B7vJtAOgsVW"
                                                                           +"8PR2jHFYUZMsrmZRuQ+PQJBztviIDNJr9MpemIz4FYF31G1P3p4b6u7SWk5mGUWL6ib7LjHan/nZ"
                                                                           +"Ri2DL9v3fHKBQ00jdlncnECVohAKIMTDUBVrzsI+zu1w3hKxedkDLVI8SckV2REVkUWFqLpElL5N"
                                                                           +"ymhrwnjcFlu3UZXSBgId07G1wF7d9UzPp7cCmfpGrnQJoSAptz+DvRUA0BZkHTN43W3QW4B+WRZs"
                                                                           +"qq/mVraOqLmm/+P+JCwwPhehfPN/jL4TbnDCks0u1fgVZnnoInNQRRIEggB37Litflvr318tbeZ9"
                                                                           +"wZICJEirkEn6yVQetK43XBeSCjTML0euS/lSsR7gjxT5HPJneeT23YY27yRyRM4iEpwRoXobc52k"
                                                                           +"eCn05WQCJMhLxWz5bC1pvXAF6fMlUHCmgnmu8qNS+uqdsBNm3q3v9r+9gwar2MV5buN4i72et5ot"
                                                                           +"aSeE9mhT5g07jceAYCK7zKU6/vU6+79DSUraqsQNMPCMiIH39MLM4MNjKFR2jJFn6SWIgO47/0MT"
                                                                           +"WfevtlhsoeGvB68mHBegI0IzvAuJ2RHW/qWAt11XzPvtaDtEUhttN0GG3HMCbSJAT8Egcuwphz5F"
                                                                           +"nw4NTYvhPwYPiCWQzxB5azOFlFRlWXsJYPJeMVNcv83SD9t34ORoJdRU/llRgQOfdhZuTG2wZs9C"
                                                                           +"yjxNdP0rDIPjHGe6dkHSl9FKb/nji+xcqJx9rQllgmVBG7CYde4BNwOeE+jyP0geUUfaqtlPkPSh"
                                                                           +"PT/+Y7sse1NTwfRB1ObgNmJwe9GDXrj9E1NXjnIcoq72qhvJz8pAsh1aO7rxO9Gl7C3oK2f5cMWf"
                                                                           +"vPIjoSOvOnyX04ztIEc9E1Nh7MbPW563vV3ony86/NxQ3/YyP7VXs7f3oGmifzV//kjBGfTQNvr6"
                                                                           +"hTLmcYb8bVZesh4i1qyBIyDwPVQo4cOiBgfJn1eOhE8RmHz5lyJyqEyii0yBegssuMCq6L4C4lCF"
                                                                           +"aTO61VDdOtWT7dE8NOXAiYw2nHCT5J680qJssnVUnxumLx1FuxYp9kLAvoRc/7RswxgS3xBbCKqT"
                                                                           +"rnz7BdauMCr+bHlgBQsTsFoERfKs109UjlaNaQqs6C6trDso2MkCvxR1o8aWHdP/qmqLIKQ9jtMi"
                                                                           +"zuXlKrgFzK6UHumWgGYkzGsW0ow13lKlVFEgmJj/DJd1mc31Dj9PH2ythNhJ1wAAWBq1bXwPGjlq"
                                                                           +"sHCrZ+xtbu9LXd1+s5sv4OHu/Q0nIqy2K9w9hOIrq4P6x83VWAZQLbbbNUdpukfN1P/xTSWxC32S"
                                                                           +"vTEXR6oyqUWggUMRoHoJs21nZSF1Af0NJeSYLIJTxZmM1WMhkl9iCvM2iNdjgERTQX2is8Frr1JV"
                                                                           +"x0/ytHI+TBaocbLpPZ7hFnQPLk1stKx6qd67wj4PUpDdHiw45HYPnNYp8zNFod9edMOufi9F4J/z"
                                                                           +"7P0qb3GXF8G2F/33AakjsRe4MOjYC1KvQ9Obw2OMKIUEx8vOAzbGbOaTWr6lHUaHri0TV9Apyub5"
                                                                           +"V/nFE8tKojdJLP9rf4L/lAbsRFniv01fWTB9fJ+nf0knOyrdjnO2lVd1sDVgKffAZReIfEWHHeF/"
                                                                           +"2smHLXu9E8PvseF95RDc1g+8W21T1pTE3OT9nFCEncgQgg35ZfS87/DjvvYdeRruytEOpEa79lZD"
                                                                           +"GIS9x+SEJmIGEXFSsPTUydV1vl0rnRKG9IZwrn/t/AIvwhgvPqinNjGDJi3HwmX9T0G7fhoilArT"
                                                                           +"ekGNxqdJRtO8ehthkbtGWegUGneHScyRAWcWQ4Ku9RBJI6JmxRbeW33qAUbgVKf4Gxuugdb8RjUz"
                                                                           +"AtE/nW7ZHcumx67Jg2joQBGxRMSa4phP8nbGiTL+f4up1WHlWpvT4PIUQLubof0T4mKuFjNcVCoI"
                                                                           +"qj1OVJzRR9+akHUp+enck6Ssh6rV/hPAYzTuJYT3aICHfyu35WkvxPG6NzVfPwHKblJpdsNLPfLC"
                                                                           +"h3bHdk3eptg9X+MN4GWeesPqy3lt630UfoeIv/TEiWGbqZmj6cSjAc1x7celFnxKkgNiZHSyV7on"
                                                                           +"sBg/dCicLMo+sPSJbEJM8QknKXDqTtombXdGoVYr9aC6NuhG6xpv44pQc75vfAzmbUwHNJW252oW"
                                                                           +"lxE+R/fGzqn5TUr3MD5g6efkEp14YYKa7fbKrsqoeBCJ0+kqF/gxI+k6j95hEEu+B3xw3+EeOAxm"
                                                                           +"JBS5l6JXLn7jpgfQaARRbyY3Z1vdl0GJhKP2yi6olEqRn7ozd1R1JUKBcTvNyCKm00MQdYnvYqbx"
                                                                           +"E+ekj1eN5uE4T1QJ0CYTWEn4Nc9vHHdLxS8xRytPaXyx499sVfWmBmzA9TfxIdbyC5iQdqy4qd9Q"
                                                                           +"xs1qOwVBWkcaMw/MOsEC3ykot8vxwY8+Cuz3ywAAAAEABVguNTA5AAAGHzCCBhswggQDoAMCAQIC"
                                                                           +"CQCrOvhXap7bYTANBgkqhkiG9w0BAQUFADBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                           +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBs"
                                                                           +"ZS5jb20wHhcNMTYwOTAyMTQxNTE1WhcNMjYwNzEyMTQxNTE1WjBcMQswCQYDVQQGEwJVUzEQMA4G"
                                                                           +"A1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMT"
                                                                           +"EGFtcXAuZXhhbXBsZS5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCwc7RdXSWa"
                                                                           +"FrtNp7RQ1YEh0n2S8VjtBosY5FB6BoNe2B14LyIVQqA/bmbgZJAfn2RTKnEcxyHLqM1PZThj2lyb"
                                                                           +"9/mtvt2k4gRviP4/ZYbcrtByParZPERu7gmxe7eaJn7ghpqVY5zaJ96XQFSiSzNK6jBNswx2zMhM"
                                                                           +"nLEzegXFbLL125K8B/++1dJNK2gB3o/M9692mygrJSvGwuPmDYcWQnzsyLPTYx0/Y+eNtnaBx+4N"
                                                                           +"jsSLCvlp9G7pKqHiKb4agatLHNPyMubt600eV56xWeZ4ujvZgPuPmNhO0ogtZpFWtF7NrPSCbEXE"
                                                                           +"MVhSgh9mrR1dyR5amEFWCvs23kSDtQZl895Z5CXm2GRAc10HYu5NJym4UE1utsAPnRhcJ7lOl/ln"
                                                                           +"MfXG+rbn0fnBbh5zoXi32UcCkldNLbXn9fBSn17hRZ5TmXmGOpBxa7By8k+GRkGDntQrWHIJdalI"
                                                                           +"73c5Jne4W9NOkWKvTw5wKOUB9HGispvbrOXH9/Qfx/techw9qlK6WL3v7h9VE5w0+DXiDy4CGq19"
                                                                           +"g9L+XAQq73AvROOTruiDFsPg5rqi4cZVEAhZbHAfe+s59ZOzGIgU5BXVtsmIyiK3wqQxOlsi6NNp"
                                                                           +"dpv6FM8pQaOnq3tQr67R2xFmRQX6VBD+8X5xrpHNXVUR5VUJC3bc8d98J6Khi3RKuQIDAQABo4Hf"
                                                                           +"MIHcMB0GA1UdDgQWBBQdgOTsHaTNceb+faB5aBhus4mDyDCBjgYDVR0jBIGGMIGDgBQdgOTsHaTN"
                                                                           +"ceb+faB5aBhus4mDyKFgpF4wXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNV"
                                                                           +"BAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUuY29tggkA"
                                                                           +"qzr4V2qe22EwDAYDVR0TBAUwAwEB/zAcBgNVHREEFTATghFhbXFwMS5leGFtcGxlLmNvbTANBgkq"
                                                                           +"hkiG9w0BAQUFAAOCAgEAMR8mKY7ml9KrhnSMbvTauvuXNXT5hp0NRpmQc6Wt/VywV4BPVAPOz/KC"
                                                                           +"mMj0tkz/LOSk5MbLxXfDDhQHA1zKPxYLM4DfObUhbJcsNo+HlC2EQ8vN4srqgNFvrY8yvfIgTILD"
                                                                           +"Uv02381njrz+GOLClSbLB7hcXvrIILENb72BwMv4QTIvXxYaJRa++s89I1OWe4f6CzseEIBQ2ezM"
                                                                           +"sU4Qjgv6tfvgsn6K4tfpVLT4jeJkv7xZ6WAW6XKgEcDreVGm8E0/7B0E5IBFgfA3VOs78s5BGDcc"
                                                                           +"z/EFcnh5Knkhnj666Cbn4rhvI/CB+TMj5Qae18Qr3cV6j7pMpCNYwwHUT2/Aoygq/BxrKgDX0b8x"
                                                                           +"lyiDqEgy4vHYdb1980FOkdK23z5Q2xVeTeCJDFNPa7oNwHj4d3znbR6QRGBIQHKUv7iKcWNdmtVj"
                                                                           +"YV9MQvMM9BVcYxbg3KDpV9GWXpz19ZWYchfZJBGUCENPE55YKh7iyj9yAZ7opPDxJlyvDcEwwyl/"
                                                                           +"N9I6KlhqubuI1i8arsFY+ouAaNNfElBMPeoU7ws8cq3C9+ek+vs8BT4p6Dkj7cx9kwugSW4mDKdl"
                                                                           +"LwLDyfzEpIEpg/rjBtSE2DRLNfpr05MKcXsZX5RB33g0IpXVCBGLqRWFHLgNnUkvtT+ptmkwvMXQ"
                                                                           +"ehAbwvWtelKQWr6tft912ikTZDtRHXxokM0vN1Vs4nAxcw=="
                                                                           +""
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp1.example.com, DNS:amqp2.example.com
    private static byte[] KEYSTORE_3 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQPpoAAAJiDCCCYQwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJcMesQe4dg1MYgHKVJCEUzCTpgJu3iTGwbnLOOhraGFn02YXh0Axbwws2hD0SH8XYF4h3"
                                                                           +"pVI+YbXCWHiQzzn0/5mbSEAQcNBqPR+UTQULdDpI5jGWlf3oPRdqRLP/zAXzgb5N2bmbtdLQ02NK"
                                                                           +"pvhRHNnLWmTBUokkBRUkh8kiUH3Xu8qIaJbK7ge0yglerFOK6kzic1PZwfvdzsoBxgb0CTHfzOK2"
                                                                           +"JhTBDunoV1lmZ0VpGc7gycu1FbUdykMbDwfq5+JdArSVlnxRXaNGne318OidwibNAjeY4emvdtzV"
                                                                           +"F6D04Xav7LUrGWsgSH0V8wqI4jrnvhTl9a5CJR0PkS5xynjSKMzOv3JOyr4jui1TATSm/S7gg5Yq"
                                                                           +"lhRanxwSHQIpJU0+VNIyGVax6KrbzcGn6c+O2iSlkix6DCK95PybsTgKzf0tSCqGAU7v/qDo1gKw"
                                                                           +"ypSxRYf8aSbqfMt5yX5LKDqNpPJwmlJl5GN2IB2qPo0nVJSlyfD3g9drWuqsDqLIMxF40BkCsN25"
                                                                           +"c2Le6c2ZIY659srIR21/ecJLVYxRSpgvUD0KqBTxKm4in/8jl/19yZF+V2fNwNgmuJXlxOxoYd+z"
                                                                           +"W26EMBZwlc1lQFxWWBOc1dZ1np4Q0QjzQTw8hPbarulR7l2P4ukCZuLWKomKZ5GeMqMApwz9S+ZJ"
                                                                           +"le6xZL27UIXyieJbgQIbnD+q9yVv7l8hsjScYXUQHgYEdd/MB+Mt+YFz+EByfQsmD+5eGb3yP/mU"
                                                                           +"BsX7DntmMINcx2PApEYZiGGBQL8DW6ZlMyBCmQeivPVkzXEeQYCnTH8ezna6xRirWZSN8GEFWD+5"
                                                                           +"JGmaCShE/4+8b1EIn9MkcikVldxHJxEGT4my1HRdm9chsZiYkLqxUTbb/SnoGkK+bnDbMNL+bBe8"
                                                                           +"P3ZYb2vGPxdOpzh/VCOHxXUV9G1VtEGyf63nX7GM6XF5PYihr6UDMZc54A432c7WZLdkVryo6TFv"
                                                                           +"40aTLO/D5bv/D0ql9QR9CK0hEMI5ceg/Q5fNQ+DpI52HC9xxDJBADfiYeNmj/ToOgpUH8OL/OMp8"
                                                                           +"eTg2kYdIWqjbWaejLW5OJt0QQ5FogHRhWnqZztf6BInzu0zY/ZrUBRqOG/wxfEYjKgN5+mXmv1w8"
                                                                           +"OXSIf52uE3WFPWdI3F5a6gf4icbXoeIH6RU8fZlSOyvZZB2lR7ljBl61TF8giLkJyyCEp9HRGyUx"
                                                                           +"ewpXCmZGMJmFKf6EJhuKEwPNIF8oyDRjZHQoJGgIoqGHrtWeOUsx4pHqRSMyY1sToayOkBiEVcf9"
                                                                           +"vt3gLvW2qyCnUnKYq0iPkW1w4OsmpILQ3fEtZbt1rqsWRucAw5WKzqsEE8R2goK3oL+2DHD8gQCy"
                                                                           +"c0lI/y39kuM1w/Up5/HRtrpWh6mCGt/QkmvFto0Z9PySUezi3s3wvJR9wa+VgAiQZQ5PlgJtZLly"
                                                                           +"WGkQJnGRnHW2nDdj3N+BbsMth6YHMwgjxoZjeEEHxzuUh66/WNLtNvNzxF2gXuylHI1uTcy73aW9"
                                                                           +"0hzv209rcSHdDuGuq6q8g8HbsADJRYfW1wY/l9ia7isaAbYQQ4iF1TV/4aal9sVuACMhEp6bmRFX"
                                                                           +"+kyBYaSOs8/vWBCxVT4KA4wf4weYDZuJmiXZ3mfdO9pES+3/4Ocmk+Xmv2NL1uF6uG1X6gzDd3v0"
                                                                           +"3JaUKud9L9Gb/xeRpLEHabFv4OM6aTBqsGfJABWlhL+Wtzq5LNsnMDmfPKmaYBg+M8OeczvnL5tj"
                                                                           +"hw+rOp3oPv+UBeQaMQy/f+7lTt/8PrGsUr+7YqFxJo8IdZ4SgxfmgZvwBHMs9PB5bqJfnultSQeg"
                                                                           +"TfgDJbv+GeteDOUeqTlvRYznOGpaxcOPTC6XKYneZFqLgmUp6+/VyyrnzmfrPhmVpB9dAjRtG3Yi"
                                                                           +"+tItGa6xzFXIaW64waakXhqs/auKxt3eTSFLW91O7k0hhvEB7gk+dxPjrGjHAA1FyQ6evfYt5nQi"
                                                                           +"kkU0tZMuzauRfX3wQKXKaGJeKk6vdCcGt4YSXNmmx33gzS+en2WBO7eNYGTDROqlOUUuhtxYlijy"
                                                                           +"GGF4aiBZcFBofTUIkv1IyVN/lMSeT9EyJBojeC6U4EtqFhUt7RCW+Ce6Ng2MOC5TyUnnM3eYFx29"
                                                                           +"zhg9ChZLAyWvJbPYJyvHP8t1pjJU13BlAHqBN33zu1WWFEd15KghNFyb7PjSaujT1dvBpq2pUcg6"
                                                                           +"M6S5HV1il0Uggq00HGTLq7Ix0G1lKwNyHfZFCIqRchtYx/m6PlqwFEv4ZkLkig2WBJQjXWO716bb"
                                                                           +"e/fSyv2R3UlqBMoCr3gR0Vhzdv6UZfNTv1U5F7L3e2loz7ZJ8A7Qrqf38PCGjmbo3/Z3SfXbRQ02"
                                                                           +"q3WAT874QdyJXBh45yf4uiuKO2IKPLeEkYpQcN6/ciTMdzkrI5HOdxZV0KUECKh/Q+VXVDTMaN/a"
                                                                           +"7YjYqT2gOHm4im+QtC8tt/0eSX3se0P1FVU9z7nhGqTwnMs832ECyr2cRHEAwPnTWnSRzFFhFHhU"
                                                                           +"Rd/YnJ6RCqXuxPxTFg/9OKzsXgexcUx4mbHx3z+wALYjjEbP9A6eKWjh3PdWXV6vpQTvbRvCaSbn"
                                                                           +"MB4gRgoW3AO3FW7w1TCSFqs/4msX9wWc/KdPfawbm6qWhm8iO2+fGjDJZg7dk2wX0rZGB2ZEZts8"
                                                                           +"Q1aEPfJF3ft1sKqRH7zc7LlzyZ3qXO1rWLclcvE/OP/NW7J1s/d9Xf2l74Lj60KknneOTovH61SF"
                                                                           +"VnQTTamhbwenCjLBZoDC/zDazGbvI7BNQ5gAsmZeBINyuYiEOxypD+RxxJxRqxtbpf7eKd0coTjf"
                                                                           +"oj3QW/RAqMWs3xllKEmUIE4t/WafJ4Tp30b4cZGsZmqEpqp9dZwo16RDfV62Bio3wTAzqaGl7v6g"
                                                                           +"Sr78AhGuxop13ktZGHUEY2XIr9LyDiiUQTGUyamyiE4hWuI++IRnXleTfwoAUwqthKjlbqSiSa/8"
                                                                           +"I9LFMGceuI9sDvfE8zbeEVQs+Fm15bA7AdI73EbE3u+vQG9gjlPyj10S9R/9VC0RERU5xN2FittI"
                                                                           +"0WM1IcKSQRAnFAHjuIkuZ4fqyrRd0w8UWxbgtWWYOlPjxf2T6zHFtmSQLczLFX0NXsYZtdAYAuKD"
                                                                           +"jn9FQlXx2TIv3h0xvDMaMds5XhHhJr9Agl1ShVBLUavjr5dd+tw9jNR55DCbyNFzUUiO4r+2qt5I"
                                                                           +"CEfurIMWSVvUScrBvDdkyRm628CXcJZ9rYOsxNoAAAABAAVYLjUwOQAABjIwggYuMIIEFqADAgEC"
                                                                           +"AgkAkXA0Ujw8haYwDQYJKoZIhvcNAQEFBQAwXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZh"
                                                                           +"dGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1w"
                                                                           +"bGUuY29tMB4XDTE2MDkwMjE0MTUxOFoXDTI2MDcxMjE0MTUxOFowXDELMAkGA1UEBhMCVVMxEDAO"
                                                                           +"BgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQD"
                                                                           +"ExBhbXFwLmV4YW1wbGUuY29tMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAz4LAVVvn"
                                                                           +"f2ViPH9rqe2t5IpfmDRlERi0qsvR7nFQtQO2KBVp/8xSAujZeMA0H9RHLeWZPnfuCLDPJyLoPofG"
                                                                           +"HnI2O5Dr9+ru+Lw+5KxgAr6ZWcHVNtfwbGCvjYIDmuChNUuV2PEJBZmmgdTBiOyrVKPNV30TOkqF"
                                                                           +"kTviOgDfwFcAUC7RNO9GEmZa8s7emyJkWmogeDlcHoCAtydqLy/1nA7tq8f6KowrwxFftU9Csvmc"
                                                                           +"hQaqQiFtBFClZu/GVT2s7mKdI1VBzmY1NPxelzlc1GNxzQh8Ckj91n9dXz00MFhMI8OnvEWdKue5"
                                                                           +"zfteoV+pmObwX5e6W24MjA42Urco6S8JoaOFuOsFAhara4Xr7JiKzMH8AEyzCnBQj+FOOIfEkEn0"
                                                                           +"gpWsJMts83w+9EFkMNgNziG3icLt0//kZaUuE2qms5OYN+GebqVbaa6KhKonhOgDgS9uPxz2xTpU"
                                                                           +"53k27kI7ZJe60YwcJb7jr+lmIeUjpwLFd6oCMfdjvYBCiUYXhzTuv1CKQByviLgRT+6OYZzUk27r"
                                                                           +"sqxR96KVIB1cW+7HBAfYmBUhA/s3jawKxGCb4XTramN53ONfWxOJaWjQ5B5zu9xtwY3451xtMW7j"
                                                                           +"fjL6ggAkJks+AyZ4XXVdJTfgJiaTFOeiPq4Q++ioWwLVQVxv2ZXQKts1cpFzt7LJhYcCAwEAAaOB"
                                                                           +"8jCB7zAdBgNVHQ4EFgQUvCJ0sdBYkSRtfVkttqS2baHLgtMwgY4GA1UdIwSBhjCBg4AUvCJ0sdBY"
                                                                           +"kSRtfVkttqS2baHLgtOhYKReMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYD"
                                                                           +"VQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxlLmNvbYIJ"
                                                                           +"AJFwNFI8PIWmMAwGA1UdEwQFMAMBAf8wLwYDVR0RBCgwJoIRYW1xcDEuZXhhbXBsZS5jb22CEWFt"
                                                                           +"cXAyLmV4YW1wbGUuY29tMA0GCSqGSIb3DQEBBQUAA4ICAQAfMx8vAkjnLQFcGePtafiI57+M/arx"
                                                                           +"3Me2upoNuaAYK/WMkxvWksqF2+rOjPIvXp95O9gakKlqhvOuFUHzPv5QQzOpwKIicsXnztNoSwBM"
                                                                           +"9WGTe2uyxNqbStUX2VND21GvrjmsB8dU9ZyShkHfNlMY8HXLeWKA5nvv14wKymg6MWQbIiR9yqdh"
                                                                           +"xTv/KKX0ryT/uLabM+2zImCjhSRfYR+8YrSxf89otmPKoS29XJ47gMEtQveLrA8k/5l1DAGoDx04"
                                                                           +"lsnbT486Hbj6cSaKdtJOQQwLLA9xEnwcReeXJ5eigyvKIreQ4bnwG04CQ35kefdtcOYCS44IeA4A"
                                                                           +"wJLDukoiFu84NH1CegNbQModa8heVN3wOos0LMijmpKYfUf8by8C7V3Yq3B0OSo3NkzTHpuhpkiM"
                                                                           +"SutpGaNRBRmGvHbSzuf6WWKxPNChyTtIjQ4Z5Y2ogBvoqL1atE+Zhwu5mPHoDvZTd8G46k8PFTTD"
                                                                           +"YQ7MKBGgRdIVffBOEy0Xn31mqkgjSGwJuHB2pkYVRZ8mB8Y8mDgchmYtbhY/fyiXYKX/HYzAVqbZ"
                                                                           +"m4OzxsfwkhavdTSbynt7/hP0dT8gC0XxqHSeWg8wvYYXzb20ennUEcovf3YH8J73vbfSXjm36P3v"
                                                                           +"s/dqKUhazXpsAIujkhRrk+EMAdi024AKvpHQr4T5UQpWu2okULxh+Oa7afT8c+lJkbkxOgv8"
                                                                           +""
                                                                          );


    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp1.example.com, DNS:*.example.com
    private static byte[] KEYSTORE_4 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQP7OAAAJiDCCCYQwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJcM11nToM9HiebwkA/+F9HTYV7bjlRO04+J09z8yiaerv82Iik4bIZjsl2V8IPIjUACxM"
                                                                           +"7ToGh+WiELoTTDtcjTrfAxk8CM1buiugzN8do8/VwTHpIyDmKQoEY+54Ma2S1r52mqpl5B4sJT3U"
                                                                           +"25g1ahx23Ytwti9TNKf5NGDLuKrVzbidoYNc2p8yFQe7gaktKVSmIyaZOjRrwm9quoopKNdB/73k"
                                                                           +"Q6geQGpYg0Wz7qh/oCEKG76tNLt76rbPITpISE0S+YF2dAyGMmQl/v/QEdyoq98+/BMmSBzuq+l2"
                                                                           +"wj6DzOdlC2WZAcsHk+99s2dOG4OgmZPxhI1pSZckTAJXGwxGeuKcMNO7n+P5XfMx0zqBqEi9AlLu"
                                                                           +"+k2GAp0kHQ1HJGmVVAAlX2DAdrxyIOSk7XEXPBbBVnIpuDBzwaZQi2yQxGCENjBp0TUSLB9SWQtZ"
                                                                           +"9yxIwZsYHH2hhVi+eywc//bvvbnfmlBMKcQL0ustCY1vVPl5GKzcEadaWZ4AmOL/SUa5W3i/1PQH"
                                                                           +"4ysBt22WWBY8u5HUBpfCx0YhiaZQzOtvlyhZmbGsz6+RoumWHkkOaHv0JGNelCzfGk3d3guwu3l6"
                                                                           +"7+yj2pkUFa7OQRv22hmAADZwdGYkiY9Qty2+jRl/AETVMBOo16vtDZBQpp+3Qtb4QuzZ8us2sWsy"
                                                                           +"TK8ZoKjGglZeFa7oQEyYGliWaGsRxb6EKGyOt3N1xbJBapwpHBnYUJ5HycsBYnje4trhjePwP9qk"
                                                                           +"QDNBz9pRJAuJyjqTF00NcmLcKb68WWOsXl/swWXMt2sipI1kZuiOoUyoHsd5jUqS0i1E3ToWsvof"
                                                                           +"CoIiyh1pZ48udYpWvEsMPtnoRKVhhSQ/Gj2DRfE37siDxPrimLKrtfvTa0FS496Upzgo8NHWIcva"
                                                                           +"f/geisCKtueqynStygn3MPhsyPcUXXpe3DQmLXFuNI0YhYIVrxUhJvDhKBqNLcBavbIdB1AaxSWb"
                                                                           +"Yo7GeVN6OsuI/wPaHenrQosWXV/ynvQ2xZbYhhPuNmpZHmaRZwoGb9cpNEbutKnPsJnKiWzLprFW"
                                                                           +"9QalAQCe66y8nJIvH4Dzpe/A4BXkpy118Xk+i1WxEGdtfrDhDODyNVgOh2daZ3OQ21Ad543C74RZ"
                                                                           +"Uss9Qht9AY/KImyvekKpbtHo+QSi95iRdW0MM1hjztXdlE91XxYM1V/Q6TvAtjthjG2scTu+rJN2"
                                                                           +"cb7u+OsPziQzooXqh4xwXKtrn3vPifna+udvpqRJvATmdcuyIf3WkcJ0UIcDVHYcwV/hBciFpLof"
                                                                           +"CmJEpHVcjJFj2z2LNO91xorjd47zcQuYWbTrLT92Zuu596Cn1bpm/s0u9Wybf8VWJJJkO2Vpb3jT"
                                                                           +"kWRtauNrbpa9YGZa1/deNfOYwrahglllzhGEwaC6lYS82QRrdQkppOWo4hQhFtoovZHATvuGVxTv"
                                                                           +"Yj0SXGwWfRor/dlATgRv1eZ9iDzYLkTVFL8VdrODJ5GVDaRCH6sFuiAEYndMhlqlMiB0T6jllA72"
                                                                           +"0u9a9jw5MfsrodgvpI7gq7ikTMwtaZ2UEbLwYoXlboBAu9mywrSH0oXOrgnu4C65SBxlyjIeVpLB"
                                                                           +"eEA5YImrA7MljMHyonW1YoZ4Erg5PtVibWz1sVaamD/KDlYbdYfGVNcuzsLIEfaztGXcFnaeBUyn"
                                                                           +"WK+bT6IOvlOlL4eV1Q+pGL013uED1qHp2hHQ4eKncoVGFSA1BI8cANMFUsIummGmBJ3/SJjc2cBR"
                                                                           +"dbxpEybUwRl8YwiVt4oB0Km3fl+0URiB3m4mY5UZH2rkS7qthoZQZ01IpbAlv2uQQM1pi3rl7gNd"
                                                                           +"SInlBvIVBVUkbjWrcjFWFdlseV21Yc1gAfSIz6Ur2DBgygrJvqs1qSiX+aUlxv3zBJG6qupnlXZt"
                                                                           +"YEMSvjx/Z/8NkUrvTfhG8S1E90SSlLMEjMBNA4SNO0EXbcbI6NahbrD2H6zpixsgBPSO1Dt/3VVx"
                                                                           +"p/B2DDdHKu1YCdvseKKLUABKqd50MYHuWECvDFKByzJKQj5FKRs0i37dSINy6oich4rRuefhsCdp"
                                                                           +"2QfeQ8EBZ9Fci+8aTYPxZpJucuNGMJmEkNUUKfdI3jQ6z4K3uB/DjLosIMSiYkIayT7XSOJbsBv0"
                                                                           +"ZIN73qGiEKlyy5v8HoIP/3eZ58XbkMafw0MwOQbgia6mlnB15rNmcV01GhWctDLT3BE1VWPoNahA"
                                                                           +"xWybxZ3VgBnuaZhSqQvbIG8C35OcvP3mhjQkDgyTmxWpoEfXDqm6p4Au+LrdN3GtNa4kBs9VfVwy"
                                                                           +"ltUll3CeXYXoP+7KYeNZaF6wn+Nci3i4us3faY8AIM4bjNMtuOTuCD6B8MCgzjfAJMBlCl/d003z"
                                                                           +"5ftjnW1FYaJGQnYMEi+zD+oKSfi01TExSXGrLQXKMny1xj5JVFe8kP4AVpSBwqAdNc0KxIJN6wJa"
                                                                           +"c99zPkmWCbeHDbHfJKVWr7z034rMDnRDNxr7rAMe5OUU22C/h3aUIeAxz5qT+ni3tCVQnX9wXlcj"
                                                                           +"Mc48dWu/oORJ9maxA5NQl0n+micUXMJxvkHrn0O2bP+mWn3Q5ig1gXQRRjDPqUKrhvejU7Om2xIY"
                                                                           +"/O3X/bOG72BdiByb8Rt/qgi4z/X65sqwouqOHQN+3zHiMJYRkXcrptZnCbvzrGoOKIEpLtevXFIc"
                                                                           +"9f7XBcakmNO/W3jpEEKrh2z8md8Lmkj18haUMaE+bVPJBtLnJUyf13WV0u50FGW5mFjxpklXY7wd"
                                                                           +"fM2FLUBVnIJ45UCBX9GkgvdFOv3Vncl/r1Hzkbasgi52uc0t2hv0l21zvOJu1Lh2BhtW+7qSzRjN"
                                                                           +"bO8SNZuMXlq6mr9G6bvS3IoGiI1TlOMEP3kdLsdXrahCK7m9UCsVY5g69Zorwcgut/z41qxdkadk"
                                                                           +"MtmpvImIwSGbeYyjGlGipsjFjbv+zhdtBWoZatNYCzxwvFXVLEXos9uKFS7XmigrMXo4Z3Nv/tk7"
                                                                           +"BearFUlwJYwN6ij9QuSZ8kbheA3opS2Xdu2F3pammfE9/RlAEllG9xnQqI7xhxlW9/uY1Loq4Q53"
                                                                           +"2YoulfWshiJ2uhlNyheZynkd/laGDc4sZPJUf0dbpND8+yUkxRLP062RhrPnocjmavP3u/P7lrvR"
                                                                           +"bzuavFTYHCgzBpDc1R1PQ7sZZAYMq+//tkb6ipR8FRJlu5y40xNEyYgEEHNZU2mq/34yp76pb4/S"
                                                                           +"WZxS3qjjWTDigMSh7qnFaxBmJXOvl3togiQNn74AAAABAAVYLjUwOQAABi4wggYqMIIEEqADAgEC"
                                                                           +"AgkA2+ULF31lFnQwDQYJKoZIhvcNAQEFBQAwXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZh"
                                                                           +"dGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1w"
                                                                           +"bGUuY29tMB4XDTE2MDkwMjE0MTUyMFoXDTI2MDcxMjE0MTUyMFowXDELMAkGA1UEBhMCVVMxEDAO"
                                                                           +"BgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQD"
                                                                           +"ExBhbXFwLmV4YW1wbGUuY29tMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxbytXmmJ"
                                                                           +"3ZGhzMxnknyfkPJCNMqIe4RVmkOw2hgWbkULIpnHrFgXFemz5zJ56Q1YDO9xpXD+eDZoITWu42Vy"
                                                                           +"JyxePx1a6+LmG3njeYnVOG5t/RXXLX38U0FZzasVDjgZB94SIUybyLkyUtdT98e1OhYZ5E5JEt/K"
                                                                           +"Tbai2nnV/nxh7XvydCtWEw3IxNu6adKyFX/7aPatzxLI3ERlFsfmhDZpfZXS0s05+TyWZ56DxpY3"
                                                                           +"mkds70yIYfDojkw5V18LbkLz/BGEL7VdSiZ9MKmD2nqTsg7m9HZVkUO0Tu967IJLX3UhGneGyjrw"
                                                                           +"MlYhrWl0iEuU0PVBMVkRW+eMRX1uEs46fEwQxM6xCR881EjCdz2PcBirWKgx4uVt4nNvHSLrNe7g"
                                                                           +"blEr3EZGtHKWHlgHkfIpiOegBbIgqJv1ZiWbNCGwjGYIVijZwolD837ka0uawe6YdIjM32X6DVv1"
                                                                           +"Ajf8S22XzULWKslnsXntt6tN4YSIE5Idu74B/aliRUWG9hnrrCJEQYn9SIdUrBuvn+Ch6nH9Dp2F"
                                                                           +"T9JCJ6tdQs1wDGXrmn4ZZ1MtKH0NS9Aas+4Pmy4WYNzWIYqNVPnp+W0ST8vHGr2s3l5iudg1cqsA"
                                                                           +"cjnFfbDP5sro7Z1GqVRcLRcrQXd451Frab9wfw6gyl9P0pe/NUBwQSIXPJWkkNu/IJ0CAwEAAaOB"
                                                                           +"7jCB6zAdBgNVHQ4EFgQUYmmTJBZkEL4VfeLPKYH06k2JMI8wgY4GA1UdIwSBhjCBg4AUYmmTJBZk"
                                                                           +"EL4VfeLPKYH06k2JMI+hYKReMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREwDwYD"
                                                                           +"VQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxlLmNvbYIJ"
                                                                           +"ANvlCxd9ZRZ0MAwGA1UdEwQFMAMBAf8wKwYDVR0RBCQwIoIRYW1xcDEuZXhhbXBsZS5jb22CDSou"
                                                                           +"ZXhhbXBsZS5jb20wDQYJKoZIhvcNAQEFBQADggIBAKmthlA/C+j9q6BKBNUdMvBM5iI46kDi6xxv"
                                                                           +"PTxSEY4/YyW80jSqlT/U/KlFHqSBfA39jywigY4Qlf7dfqvJ1QVdYF/8fDM/dOVXWtV3cSk2CafP"
                                                                           +"DrqEZdRRQ6V2QbJGh+F/cpysehion6qH0f+0Q6+7MILmTdsQ1Zz1TGZ00iOXmBp62CR/HPSm5s+N"
                                                                           +"CiWxfAqHZPy/94t7oYxoC1af06mEmKEwp9M8WuaxxTKth2lj8qI+5rPQXvb/A5qEOJ+G7EizBFWl"
                                                                           +"yD37gWAoWcG5U/qqfGxgiRhQV3+NTYOfwjWKgVKPGa4VObbwBpeYXegJYbVBDf5XWRRqqfToqXRK"
                                                                           +"nznk9wJJt8aLdBBzoas39XJFld51GuGj1bXrZrBTdix7MvMavzzx9oRY2cNjlIsnCPgUHTO+6+HR"
                                                                           +"ljzaJRcyrEzga3tZU1isaDvqGYnGdR30G37byWfnN4l80MKBAunipFq4iW59lSs66sd2p94VpPWB"
                                                                           +"Qondx4DPxWxTHCdslu2EQ1ZqLDTZiBG3lHm11nbOhJ0PeV90fGvc0CWEN18CrcxaVfO8oa8CBiFu"
                                                                           +"RGAuj0BxdcipWGQEhXR1aXbfdi6WUnyE08mDI4D2JgvNhwGk/ap8ByveqWtZzIQJiqROlSMIVtqz"
                                                                           +"WuR7yesWOvw9RjW6dBVtz4LOu6w5xKBcJ1/jf5UCCsfGrOCpBiI+lxKYpQCXbuqGiO4="
                                                                           +""
                                                                          );


    //        Subject: C=US, ST=private, L=province, O=city, CN=*.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp1.example.net, DNS:*.org
    private static byte[] KEYSTORE_5 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQQX7AAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJbhJZvU/boEm92Z+W8kebBumXt/3K9qwGBIntSvmLduW4HIVcQ6+W3Q5Kbd7JiQbAut7b"
                                                                           +"jicmydwnibrk7DimQAfCGsqud2ywj6eZwkXXa5ZNbuQKUQxP0me82awrQHYBkSaHJv2kwSdTQU7O"
                                                                           +"la3CoRhtps1pInt00GjVbBEtBERcUrCVG7GtLbxKaOnDEaixK/ewS+7FnG83SfjEKCc5yso/aKaG"
                                                                           +"UIJt6lo1XtYju6Mly5AkqR4CQ/iIQLzaDTo+SGw2zGnvdKnrE/ggW3eYKUZgzWSDtw/A6saF5Df9"
                                                                           +"14O6oQ3Dq+MYcu6giUgeTyeiBY4iK9kIX6xM7Sj7Y26pqnPgRojkO0mfbD3f185f6mJdJwA8F+Rx"
                                                                           +"rQ7r72HIQUv9IgOpTzQ493yyo9dWje2vF7uJ6vs14ebWEbFyF2HCHyv8txbIB7TnfF0COGsYIgaZ"
                                                                           +"hLP+a9SjQzq3JAyR2U6gxujVZ0lbljPEiUs4ZXQCtXOZwO5oYNM12YTlu7unlMUm7Wjcy3BGOC44"
                                                                           +"l/Bc+wgkofm+phReGyni4Y2OXrYOcF9WTAf93y3YK9JYHXpraG6kZfosKrfXUZ0Ub2iBCO+qGtHn"
                                                                           +"0vC4+PzClD/CFaWXo4z8lIEK0unQJszl/C0IzcJBXQpOJLR+fDx/T1gB60lTX2lHexRvvTwQqG28"
                                                                           +"2Rxeg7qCHlQVpPuHuepnm77gXyIgYFRN+t6K1Z6Vp4xrpWdJ3k2H1rVIyqpwSNb83xS+JudVemKf"
                                                                           +"wezHj4pXlwCqr5Mv2gzcZu3NnfFvCT3PEJCAmC+ZldKCAu5IkKSlikkaXc/WHe2E+kD6yHDluBux"
                                                                           +"uMMk8b7MkLcrsX4M8zkB1GsZYIZp6BUU3TMb0X4F8wnAA5I98GpysX7nZYC4VO0BIRmT3e86XbVy"
                                                                           +"J60gmtKaMgKpGmplcDgZZUw7hZRZWQYlzlYBR+Uvtiy4rB0z8qggvP6rVE8oIYEKb8NzDz3cZ4bN"
                                                                           +"AdsV1b8b0rv/Zi2WLjKagXKGVN2NskLHSpIGxUsxZFbHpiEpcfIpFmNlbdS0llBXXmZwBw6Rm3Vu"
                                                                           +"QSPU9w1k51B7JnqjqAfkotnZ62DiRoRuH6ppwBUKo5GnJLAJkBHARwSdc0Bpd/U3MZSXsZ8x4COH"
                                                                           +"4Nl6zdB4qyC/XicsO4+mGiecd3GS9oQ+ksEA/xYcu1h38Lij7mgAmunRYQ7GLxUdJmLxwQ9JaaYC"
                                                                           +"KPRrc1jkRCpcCnq1fE/bnOjxyiecW2kNjp3f8hp9kk9DLc5g0kZT8VqJsCpxAHtum0I8KWH3zVYK"
                                                                           +"MnRQopkOpQLTWYRSg+aNnhaMvjf/7DsZSvlkk0jD+g1wHIh0lSIXQ71juV4PIR+scgxrC7roDrLe"
                                                                           +"MnA2Ty9W1VsRp0Jw/R4xhqNV4kcYcpC97La/wvAtEZvdJb4gguA1gWox0XP62jpkoOVhCMB/IFQ2"
                                                                           +"mT8ilExULwqmu6BHjDBw9GLeLaXBMpbYvyc0dbjs0Gag+stlECu34jTXnLe19Ktl83pTALiariZD"
                                                                           +"XYJRmOnF6H20uCmURVYsfxS4rtm5QowyD9HJF28OAK42S3fJzSgrAYW3Vv/Ou1nTlt4gmy/8+BS7"
                                                                           +"gcAq9koL6KK/X5wlU6SHIMQJ4jmHzxn2tAPccazugnHdryMtFCGChYsk8BnjKG28yIezwZGbPyp9"
                                                                           +"pCcOm9HdwOg9jfuUljXQrY1WBkIrGtGNi/EhuQfnZoguQHhqJR8vlc2ZfyIVKXYDRhmBbcDVVR5T"
                                                                           +"2eqWE2IKox07yK0X1OxMskQf0lZdCG9C4Ch3kI3h98fYT5EplduaZd3goeMKD4MJERK33VngvjC0"
                                                                           +"oC3WPHB/BneL8QoFqzJDFxqiAjhVEEDzOFYyEfiY1N0l5w0mgQILshcSB9DTadaIWm+R16sDvDMt"
                                                                           +"gxhV/0mqkM5C5Ubw7CIh/182ELiPlrWI8l1/WP85R0wbfA6cCwIEJ6KGFyRqj28SfWopyVt1y15Y"
                                                                           +"s+RN7ZXcZK4E5qR3IAiwlZXVTQPWiBxTdiJZUyEvXMZSPAICTGS82ifwAg15yzK2dyDlRbvRExLO"
                                                                           +"gRBNgR+8uPemajpIMsRSaOin2bhTzVfZAxZWlCyWirauUTdTjwR+M0ERw2ZQcMbxlL1HxkbZML6k"
                                                                           +"0SK/t39iEIp6PI8sqe/0FZfg2sifiQv7PGUX+phMdcwxgxZ4ZV8PNLkb6ufyu9k0kOnqdFpNWNH6"
                                                                           +"i9arWakHGORusCRWj3XcIXeYmjNfsxuCfZRx4k0EF1qslH2w9gMShATz7gCL+cNPUYcd/Ewam77e"
                                                                           +"mpbDiPUzEFc9ELICOnJkIslX53ImjEM2Oz2WrB3BFErU5SrNc4X7bdKZTW2wT2HknNBdEwxvrh2v"
                                                                           +"4qBJiHMWpWQGMHMFyuaHrAqENwYfeuntuqlMhUlP+7JG1SVZqC0TjtLH/smAUCvPrjD9XkiAACL0"
                                                                           +"w357hyjiYQdd4KqIWbMF/IZy4LHGG7eb0BS3XzX0n7r7ROtb39E4lJnf7J5z1dNHPme8piVTjE2Y"
                                                                           +"Qp6KjWiKxyW+CF6BP3FhSCvXe3t9lKWzE42REiUViGbZukZjT1GUha2yQeIaGBcC9tCkW9rvpHG1"
                                                                           +"FAlS5OH+lOfY2eve6suQGERTu1L+Njlv51VfBWJReVsJb9WNOMYMseF3WFj7NM0pOxvj6LMje2HI"
                                                                           +"R9qTog+7Ij3pMe68pWD8h/oyoTz96wQyopo7XSxCpqI+1S57VFf0Th+DXUhtSJ4wA/NaxNs2YS72"
                                                                           +"FN1vWq24DwZCu6LtiVCtBFMQcXNXHUDbjwOrj86zjvtNb6dEb+M6sJLY8BdBRnHdOoIdR4arfYfY"
                                                                           +"v4f7HPyVwoma4nNA/TBSVWurhtHoLqYNp9mhsTRCjIGS90IQPHyITXCxgG6aEk08gqKPp7BNMojw"
                                                                           +"kMEJqI8ucqhoY0dt5vfCLZXPFoAAizhKVILO8zr+dgLiKp7PkSum5e7/INgkUKMXasX4cU2DnZeo"
                                                                           +"yN+GhrokutGVSSWYsE0byeb8fARVmqB0uGHMeG/WsgOTXEXiZUrPE490D0qpox6CDLVi3X2jIA0r"
                                                                           +"5vBPuCRLzqdRTxQHBjLesleVkRCFbX2+XxWkt2BOXwdknDmfBuzfe5p/KRE6w+S3ZQOiVctTDDxS"
                                                                           +"76aEttcrCJIvFChx1JctyYgNuR0TAQknX4TbzF2d19rroZ9aVX6+m4Kw8/Z+hxz5/CvfRY1uFZl6"
                                                                           +"jNBZPcSsjp+lJIX3ubmeEO8JKuoiga8zVRaoAAAAAQAFWC41MDkAAAYdMIIGGTCCBAGgAwIBAgIJ"
                                                                           +"ALqBCc15pamzMA0GCSqGSIb3DQEBBQUAMFkxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                           +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEWMBQGA1UEAxQNKi5leGFtcGxlLmNv"
                                                                           +"bTAeFw0xNjA5MDIxNDE1MjFaFw0yNjA3MTIxNDE1MjFaMFkxCzAJBgNVBAYTAlVTMRAwDgYDVQQI"
                                                                           +"Ewdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEWMBQGA1UEAxQNKi5l"
                                                                           +"eGFtcGxlLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMlAfHTrkIHhGsJHiK9L"
                                                                           +"F2gizANNDYwV1yw/3Bhl4zJQHbeqy9GE22ZfTYI95io5mxMlI5aOD/F7Vc/xCw73f1vu+qFLfDUk"
                                                                           +"lB0JOVhO49KhSECeRSORKmu66Qz8jMHZ0+XfHllXBgcKocUOdc1iOGaQIf0OCyB7uHUyexI4ihLJ"
                                                                           +"qYmXrUJ+PkztVkwBfJlyFlt9zow9cSxrEOfNVvuf+vo25o9GmaCzw7ZauNDn8Fo9MeQXn/t2xAfW"
                                                                           +"Pq/9e6vAXn/I9gHO/McUERnDCJkJddeLbgR5SqNLcwEraTnmpVnASfxRY4c+SwA392n8tPZAo1AA"
                                                                           +"bfScsWUnte0FsRWkOoSGkCZN/M/+uLxd9MEGIbvEatNrLVwt5651JaUKBT6GNBaQZ4a7jWhbob/a"
                                                                           +"aLhMhnD3Wkj4XT7w/ty59b+0rSddUcux4Jc9dbnjjnmJ4ftnhKR0PSbrKSs5elFMGUqYBYQYccBC"
                                                                           +"aa1NZmQdaqsVHzCLBiLd2+sheptw97zn7DGHqjx9b+/cmGQucfbkDitMWXzGH7PMzFyYP1YRci1L"
                                                                           +"cACxSdDwAsG/oPeCQa3xRlJzcjKslm7mBGlb97tAUTOHJKi31ZQOUbRN14ojEmbyCbUYO7A0JUVi"
                                                                           +"YFekV6mXA8ZkdlA5G4B7seiU+qZOR0nyp03pR69IOCEzomT0LCOWNXlnAgMBAAGjgeMwgeAwHQYD"
                                                                           +"VR0OBBYEFEhJCkDjbQi+HKr+LlMj7ABY8+9mMIGLBgNVHSMEgYMwgYCAFEhJCkDjbQi+HKr+LlMj"
                                                                           +"7ABY8+9moV2kWzBZMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJv"
                                                                           +"dmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNVBAMUDSouZXhhbXBsZS5jb22CCQC6gQnNeaWpszAM"
                                                                           +"BgNVHRMEBTADAQH/MCMGA1UdEQQcMBqCEWFtcXAxLmV4YW1wbGUubmV0ggUqLm9yZzANBgkqhkiG"
                                                                           +"9w0BAQUFAAOCAgEAq6tRF1CSmx0pqKO1RlA8LP9iukOIehxKx4n2PCOIdLamRPh/B4raBU/1LnZ/"
                                                                           +"M3IDTekF1dSD3qdZCjwQppGNXelPndjHvNbzlAFip/B3aq9VIomElhhC+PHeVlr3AaSxviuSGazr"
                                                                           +"SEIvYDct/vH+1aGgC7JitIILwmNdUoRYnnM2r+IobsMnPRhxj2qCHpqppxOb1Jql3IXGPKNEXb8a"
                                                                           +"ELdTg5k9qNOE5hLmjjjVKEesk78jYi9jkLQ5/65aoIbpCLD85vTMKOH/E0rOCPwgidUoLpgaP4mi"
                                                                           +"eefjr+rcm9CaDq+EqoNmMn6Jqzy+Mcdidw3PEiz33b6FbpdDAfZxnilNTB/uAFh2C2LTDX678gR7"
                                                                           +"MQcfY7T7cZPkxqgQ7nVmf2PIyotuGrCpOUc0XZsKdftwRPc1LTd+N/Y1lnkr28SaGUyS03F2m4XW"
                                                                           +"V3xVumPQjmNUqBaPkceB2nSQH7xMSYX+SqJZZIsNHvGfM6zkDVX703slEI2tYzy95Ib+FBPvikG5"
                                                                           +"+mVeWYGjbImCXcOVL9HwhixPQ7qZjYxYhK8WfUAu4rKXtk+E0iihDmkO341VLEdnLVxe2++ISMXo"
                                                                           +"B4q/W6V3LK7Adp5NSwtvsiUcq7VWOb0AxUa+w/Zg+NTccqWoGtZJEjE6RaeL4PhUe7h2aleiSEct"
                                                                           +"FUMmYQyx/6F91Xd3AywvZNk1AkcYkQWKknm9AggeXg=="
                                                                          );


    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.com
    //            X509v3 Subject Alternative Name:
    //                DNS:*
    private static byte[] KEYSTORE_6 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQQ/fAAAJhzCCCYMwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJb3fs2zcPchqg7CgqHGxwNehB9UogWD1nbgdXd5RtWrIg8Gifp05miLKUjPxQp1/L0D36"
                                                                           +"XVyqVO7hINZZE3wb9p7PR+bzo7DWMuPZ7+2YzcX2fSLRLcT2h88L3wHjblYH19Bk19/H44JI6j2H"
                                                                           +"NwVXbAJRAR/6gQJWDewRmkDmSwbiLZ+0pYGz6P5lucXWFt8T2h+pAqz0Ui400RxrpM49AadvCK6v"
                                                                           +"3vxZQw7ceuWdd8KLpgVCR3/IOoRj8gIwIC5jd+tP4psbqgGlDgDruICdcza27k2NX/WHuR2sx9u6"
                                                                           +"K6vCAQ7CcfWYabFYvyJKtjRPloEA2orj/4X+/vD/9joTTDuZRhgpRWnAU4OPfSHrosL7qB4nPmsO"
                                                                           +"sNXalvYTcnVIzrEFqSnAZz0CAEL3jo+BXXcu+m2XnvHt3ke66CHBynmnVex7f+Yk/JKnNqaxsvGs"
                                                                           +"65ka0mBxvI3Mi353NOiSruSh73XbWXzU8XCI5HMth22CgvtcMgT1xxkSqFEk4MxF/Zo5eDhtHdS2"
                                                                           +"skXt0oX2SCwbINYa6kJesG95bEUnJzlrz7ZI7mGmQPakG4kG9og80sNC+uM9x2P4+ChRfB1yM9m2"
                                                                           +"uarjGKbbJZUIHa1DZUVbLJ9bTvBDw6k3mnK9kV9kMiAfxWZO8sW1Nm6e6bwJNqZphS27TO5uJhAT"
                                                                           +"GiC0/MtmdhyJWRnuckZqKTdXo7zu3AV5mZqKyeXmXuFCW7cyrHM3d5d2xXG8LQUTEGhpNo1TD9Ir"
                                                                           +"QJekRnbsdrJzvHonOKcKJcQ1T3Ump+lglU/CUTkKD45n/LgtMe85i1LqJyBPjjC+m7iQuAENI1LS"
                                                                           +"pNpWtARtwkeWZJ7CZKOYEg+jUY8QMHIunMUPVQd4CSNle2HwqEs3LXiqYtYC1AmrboE32KjQCKoU"
                                                                           +"nLmFMY8dULi1oS9Z++oSVY9zqS3RnOdRmNYGktCCSzSTIRtQo2fq+GKO5phi//gRqM9675cRmxll"
                                                                           +"ED0ajBn9cE9IBG81o2d9dyHbw3ihPuKQsWmwGF0xX6vrzBsZxqfJvLikOcisN7UlWve8YXa0Jm0O"
                                                                           +"kFRt0NygDTVkeYqOmMaIwTlfb7jbfXw83IQGd/JUDUj/EZEOcQv5IvIPE90SG4w/qNA/CVtzNTC4"
                                                                           +"GFH2H0Qr2VyuSfgCT+cDhIIjWhbbkkTgh+Aolm1bqLx8NBptt+v6wQylDO6lhnIbOC64dONn9lJ4"
                                                                           +"AQofw5OqU1DJnPq2u+KeSIozz+ZTlHo6Dc03T9nnDYfXgrWC2bX285swhbWCy+l418IM1BlHUDxp"
                                                                           +"mFtlONN9YO0IpAoiv5o+uQ2/kU4gMUWJAQqJRRCoQ9M442LZSiKMuEIMwLMVx32XN9OSWIufGnNj"
                                                                           +"wk6Tcn835D7GGa63O7TbAqODCjL/S4QARxNithnMgZKT21kAaRWwmmz/nIHbEgEG4TXpavTGPU32"
                                                                           +"3wQCOYQWk8aMzszP8wbg7I9BwJ4YYTlXUllo+0rpIMOBFtMBFE5RQaumm+zbD3cz1PPQKJ2XcrPm"
                                                                           +"+tMwBgV1mYRXfhW5DvONFSZ8jxM6OgjkATznsHWyg06YKkgDTyKIxi9J2iKhM+ve73+S4cFNMtK3"
                                                                           +"u7JyaJf4fQjd6kIY7J4UYmkoTT+h509CzfZx26FMau1soU7UBbkp2Hc9Js0hCa9bNNpFCw31ctFw"
                                                                           +"rFrULcCaV7apTplVVzE0hk4HEnu44x6XdgbLv//lP/2JN8F4MA5l52nfzPEuSDmvO3Gg8nxoGxVP"
                                                                           +"M75JwmH4kNXVbUJvSk8yJcwLQrAiL+RusWg8lRbj58vjfagFs/hR6Sh8cYhcyai5Jss5S7UF6zT9"
                                                                           +"UOJvdWD07zNLIrLjng3I27aibkSQuE6JkDjzOIgWbiUDMdm9Iu954cp4xgJAx8ETBMcpL3sbtLyh"
                                                                           +"zTdURhvae53DBvu80MuScgGxbISTQrgocEh05JYU2+hQ0eL53pHlBwEHOA9LUoTA/Q3ohWJmt57i"
                                                                           +"CGL4wMj2LLrwkwczE5Wx1BkJLYkBNKvH59h/VfqV1/TFVIBO8eKm6O7DmO/8Oyu6GFQlDXc8lSon"
                                                                           +"89IU9rJNl0AI1aS9nHgwduxUnn6FtB4yP86mFJfP+zj+Trsq/HigjnTyjcVHzfYxkNrYxW+RlA5m"
                                                                           +"9E+XwegUmrcGjRw2qzHW+NMpVd6uw8Wyb0pt1SfON4Z9O+fml4jQHO2wI4jHlUxG27x9kTQYCkpO"
                                                                           +"oMV4OQowywWOW8BHhzU/D0mHYjTNZY2N8MrJIzwT+arpe22cNI0cYXDQHG74p5WJ9h3jn3Juw06t"
                                                                           +"PCUFAa0lDS+LEvM7ZpegG+kZXR+Y1aWpfeWQbMislN1nsNbHRckhRVLOq6UItVSqWXaWcKB6hrkL"
                                                                           +"HIg+EsFJdOGt56etNwpwNmyPSsgVqz5bjLZLKtJgXah/YziqePzmyQKrI8OCrMRuBaeXOp51cJoB"
                                                                           +"80gEGdFvUt8DCAU9Ae8mRQwdyELnUnjHLG1d8oFtwVK1FovGe3O8CiGevdmqYwrw0Nf4EzuWwF7y"
                                                                           +"eu6NJh/a3S0q1xSGKUbJscEs2QT9uyzY/Nk6ttTfs6hmXppznbMG0KUvwnp4npdF4gj5A74QJf/Z"
                                                                           +"uvWfNHE9GT3EbJO/XCiyzAhnwEEYTJjWmyYgg5Mn43Oqu569J+vLb59sXDmT7f0qHVhORE27kD1j"
                                                                           +"Hr3G8gnRjcfo4mILLJNZllFEIFohlLltSERXfcdmdwlLYSF84ZAz0D04JTwHbVMO3hHHt0Y0ws/p"
                                                                           +"CqSFFC7zx7j5j2k1cSp85cOI4BK7pJHhBSa0stT+N1LNB2CJaU97V9vc4p4sbBv2SGO6CE5Pgqco"
                                                                           +"Saq3J9S4SIoX6tXSrX8l8Zb+XtsXGNVS0yP8SRsMUNY1mYqvFqaOKgjehDL3gTAAAsmyckW69G89"
                                                                           +"t/9gswa7I67IQ7mF49ebRaeiKOWw7dhuWsVXEpwFn87J+rpN7bshPwfzuJ2PhgWwbeHazJIZRr5z"
                                                                           +"AmNnFlBhMDYU7h/QaWqY5mXCrTYNaUI8F1M0DgBvZ7URbKTzjbdj6rh3gkXjdmPJ2qPp2x7SIOHq"
                                                                           +"cWOa1P2PWqaQnvCp4gS+2U6/RtvA2cQyvcgpYtrjXDUUVCGKZNrUj83dxHLMb8Eyia9oyAnwAYQi"
                                                                           +"Z229x001dUQl3RSj7vL1iiKMgqkGAeae/XE4wgx4Bttw11EOEimDNZO/srF9Nd2Hg4PwR+rniM/0"
                                                                           +"ib3W/QuUUxWnckvaEfzGOMikZk6pDrYemTlGWQAAAAEABVguNTA5AAAGDzCCBgswggPzoAMCAQIC"
                                                                           +"CQD7csytydc+QzANBgkqhkiG9w0BAQUFADBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                           +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBs"
                                                                           +"ZS5jb20wHhcNMTYwOTAyMTQxNTI0WhcNMjYwNzEyMTQxNTI0WjBcMQswCQYDVQQGEwJVUzEQMA4G"
                                                                           +"A1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMT"
                                                                           +"EGFtcXAuZXhhbXBsZS5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDD/6oRwgis"
                                                                           +"iBe+ljNLBYqeQb2yZmdE6cXnPQeJshj0+k1QWF+8yYZwTGCsAnaK8Ifd8i/GOy2pIlTNbL27ZYDv"
                                                                           +"m5UXsEkK4tMr9Gy0nk8tCg50AVV81uEAPKN4ywoxTxa/Yz+Haa/6VhqwaYLBdvzGHlXfc+itJJ0m"
                                                                           +"xs29LU44r2KX37ChuvLZo5jcrgf/AsOWMhynlHiEDTcoBHpChdWf9g/ejY3GUgVbyJmP50g3D2tY"
                                                                           +"6IoYUVm9Hu08PI9L+VTGPCejQ6fUoT/WfopRn0q8z5Da58rfiPlC79OyUvG9Gdpa/Fl7DhFrf9/L"
                                                                           +"zv+fEZNB+IsxOU/Cx7OH4cIMs9G+/SzmTujWiWVZmJNhnkxRHAtAYHb9KCU/ee5ei8Ex2+fQnM2l"
                                                                           +"yEEwMrae0IKkBiojOyZjBVCQFFo/W6LKf145Kk/34h7/epyF1QlnsdOL1uOB7L0vFdGJZuqGW9Zo"
                                                                           +"IUcf95tSy1L7BvVD2ldr+AnjC6oZ+8hDaQrqLT3esqH1MqyoY9L3plRSCtDAIp2ngK9m9d1kRWRj"
                                                                           +"N9XjOaXHJgQY14gKQrX3jjWPhWRnAJd0YkN2kaqhFnDMjLXN4C1PQC/ciTec0E4IJf0bQriFsO5S"
                                                                           +"EcIADQwYKzDXBDHy+tWd8tGL7c5No0+l49XqaXYERGKQ3HbbsUV+OfG6WLCIAIpGQQIDAQABo4HP"
                                                                           +"MIHMMB0GA1UdDgQWBBT+9kinZi531pFpJbIGYX/DN0et1DCBjgYDVR0jBIGGMIGDgBT+9kinZi53"
                                                                           +"1pFpJbIGYX/DN0et1KFgpF4wXDELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNV"
                                                                           +"BAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRkwFwYDVQQDExBhbXFwLmV4YW1wbGUuY29tggkA"
                                                                           +"+3LMrcnXPkMwDAYDVR0TBAUwAwEB/zAMBgNVHREEBTADggEqMA0GCSqGSIb3DQEBBQUAA4ICAQBE"
                                                                           +"XBJ1n5uB5IKbaqMYrcn+Na84YV1WcfcJMfqxAHgDwW7YDTJZGoQLwBJhCglc4hQDAlrcQiylxOti"
                                                                           +"av1Un87lCN2aDQJZ66ZZ2deykAau3l3FQP48x1zOeCooJB0q+awCT1g5iwXZq/HSM7d5vg0gOXBT"
                                                                           +"n3ojGnV1qaT/oXFk7Yirj10iEY+VChtOaeT52ReI2iSSktfvsDWLMCMPklz7wveZH4u3XlsruyZ0"
                                                                           +"y3Y/gt8D+dclmPGpzkBI0fmRLXoMbDjm/y4d3P7UMjtrZ6mcjRvHprjvswTwYXCJUauuAdDvuUIT"
                                                                           +"AhGWEqL+Pa7v4AkkJRGV9tswPvk3KizJEgMtku3H0T5dA8SyUdDikGZND1Stn+/GZIRAK8IP7nDv"
                                                                           +"t0BOhgAJ2FjvDy3XPG4eAYZrnCecznLH2KbTAgmQPWggwPlbta6Z/OPiWjni4rDSZFoWtuIo+wDM"
                                                                           +"88LvLrKT3boxbYUG2fOHMWkLT2XN1Dfl+BAFVUXIjevvo/atVFyo8L6b/txW+cOrY3HGjjRmjDAL"
                                                                           +"cQ+jbAAu6+qpqJrk1vZKjpb3FFeEOUzOPySX1pkMYsYTBBr/TtPHBmnwrzmvppSMoULMcgD/Xfoh"
                                                                           +"FsLzLf9ptriQzNrCxEL/KyD39p1X73sAU+EIpABnSuN70avHhl6SZwkQrnuIwW8QxoWOt5LbMaud"
                                                                           +"mE+Y1GZRNkV1hx0gAqf2GX1s"
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp.example.org, DNS:amqp1.example.org, DNS:amqp2.example.org
    private static byte[] KEYSTORE_7 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQRyQAAAJhzCCCYMwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJb+vOaA9ERF3UR5QStjyDSVyHQneIIdOijZfbIxzvUHqZLXOZ6g03xiRs0mJ/RPgPPXo2"
                                                                           +"EzR8U19YIljUdVw37yro4LpsH1buo+tzPC5p6PhLS28jaMCsLWKpnabAQfWptCgjSoOvKJ26XdxB"
                                                                           +"NVJVPrBBscwP+ytgcwJ1wsAkkvOGazygincn7MJiBUNkd7HpJ2VjueeqgttwMKFKFifFdE390Has"
                                                                           +"NHhwm5fXF9VWBEqUJvg9CRVABgoEVyxNRp+R3xJF6E6DW/Mp5k1QyOzNipObgnugVhTwKaBx8Etn"
                                                                           +"AtZwEPwafcG79Sf4MOu+3H3jWnS9HxNfpVRIZp4UKhqHUPxULKiR144LeJmVGOP3PN37H94BrVeS"
                                                                           +"8ok0SpoYpDk/78jlTK0lFM7aHExlkvHZA//RdkNtzlG/nuOL3/4ggO2yjGTzLIVg8kF+OxtwI2AS"
                                                                           +"7r/D5inu5M/ry2gk8KRSdA0RGpfimJe/ndRa63+pBkZC8oiJAXGquWa2bkLbCXziSScaDgTPpn1n"
                                                                           +"c1OORrpl/fWtcWRfz037NRwDRuNrTNkEe/c5SPve7qU+msxPdY4DtMqPla2S8uJRubgujiejr8AL"
                                                                           +"fBNKh1mDWrHxeRhTHmjAIdlwjj+TLenqk9FM2BLvUZqJV+s9kEbCQZ1NPmZ7sLXivrTTlPJ6PFgZ"
                                                                           +"CfFq22rjh+TIKYPX3x0nStuFb0P4DNFFBpXFEqHu5DbiSETw/XWhMS3O6JUqw8UlEKIgToah+1yf"
                                                                           +"AbniuaLEpmXE76fdu5PH9AWUmGUgOzJQaLiLj1NOVyBL8c4WYigs+WayTuJaGKnMGIaf5WN2Qdx3"
                                                                           +"LoWoeSUzYYvAkw/oR1XX+hN1XtItP2OSL/RB8XgoCqqRPFHr30ZGToLnj3V7EOXk+gp9STcgR04T"
                                                                           +"sFlHMXSdNcRQ5rRic77lCgJbZ3DOERIZq0EMuKAjgw46y5bNcpOydVEUWmLhtLuT5OgPT3039Ovu"
                                                                           +"pGYyiqucx9OiMfHt18gG8Blalu/v7KiKLgGMJ0zyRkgbRJo5znq8QO0SzXbmVubD/lT5yotWaMHH"
                                                                           +"2l+5iBVBnael0Rvqf7vjzKAXq83NfhNlQjDuld1AyyzWa2/S+rtd8swtz9ce7pEdWYuFcI474k4y"
                                                                           +"OQzPoseGb8HkOujXyvEnOJvqxk41m1zwpmPttnoCwI1t6CZjduPZuJeGh81DGXI+YB81zRIBtX1e"
                                                                           +"jrzIm0z6RHhRX5ZJZ1R7hWQI1HPyDFf5B/KAG/Sm9thnwpLei8bqjvzqny3OiGzDInODYkPZlRHL"
                                                                           +"DgE0mlWC4OnM1AcqVKc//uJMybtQBU8OoX232A7Z9ds21JPnxtdvOjejG95v5GZm+jSkJnDVCh5k"
                                                                           +"6ClPmzbP4bqGZ6Xa6QL4pYy/4KvtOgzoxqRrE8u1q9ZqWP6Vt04xnuoARKVIQOUg5ZcNQKAym9Ev"
                                                                           +"NkImgeuOAcRqB6IPeosQXMw9IwgSxZzachyxpTa1EmbwwtHe65yj4tv9I/EuiwwmS8LncN1Mf1rV"
                                                                           +"E1JXGVy+recPGqWfOwzywY9tV1DQLzFmcpYA9yRIZXcpnJCklkptSZMV8dVlSE4ReRrM6mp1UmVU"
                                                                           +"GD90JKD5RQe6WljBIxcLlhuPIhxSzjODKEruZhVr9mU9g4JQr1bp0GsNZN6hZrWxEmT7mTWllFo/"
                                                                           +"syRkuhVYNxzhRA+/KeYsNgyRcfEMtpJAqOsfy4KeyQLKgwRp6d4j7U4UV9LKbDA0SWfy0/RWnj5i"
                                                                           +"LzN17pOaBAJEAtAwoLDaHIJIw+3N3jjy7omRaCiVezyBtS05w8+gkbjACHh6MhChsFOxMPQz3HSx"
                                                                           +"ZQj5ZXUKP/o8i7EDSCxY1XiMgwRUjzUqcmvCNrMk+KyCx3ts3Nhvt3xaTkcxUB8vumEKW2EExTAC"
                                                                           +"9c7jkjHWi0qLCtHS+raPpLeL/5lT+ssUBsdsumXiVaHj/mq+IBjXZGVJopg76twt0cKqB76R3Y46"
                                                                           +"J6cYyuuvHme0w/+pKCqtKD6lFAvr8nEnbYFJouANuuhK0DNbPxAOh0GMANlYgUhhqjbMbCRU1sAG"
                                                                           +"jAJBtvH7fsfglCA3TAuW1uqXnKKHhgNplZUWLVpn7R//7xD/rMrY+ZsfaDuuqpDcKa9ibnwEgwJB"
                                                                           +"Ao77xAuZnn3U10BtSyLOd54AiEmI0y3GuhO7Y1u/+uSDTyuZ60hC/Wx+kJq343qRfnMyZPvgunhn"
                                                                           +"hb0r37HJf/gs64AmUoMBoZTYHxad5ozPKjLDg6iylzxIDdM+yeYmT1MlDgaJ45EJht19RVrcQsh5"
                                                                           +"IwuB9cjFCUb7pnwKXjwnph8C3VkfYa4SyPDBNjkRYiJAKzZGnY5SzTG4qkuLGsy5SiYP7+UxIWaF"
                                                                           +"7Fyzx1xZSZaMmVa5lAzopRe/A9SmFNeZvgU03bhk3PUb42/b4OkLkv6i2qI2iedHg6ButA32esiQ"
                                                                           +"VkB2JATr0CXKWM0nPpnMp8nBQOjSDX4Lbpkcz6h65teq3md+abywG+th9Ku2yBgMsNmpYDLKfN3R"
                                                                           +"4wJKKPuMe2tjRLgeqghFcPTgGCSJfOqq86lS9jhlDiOKF+OTVMBe+KutmxQIioqPUdFdNQtw+Rfb"
                                                                           +"XpRkt560rxQ4Q/QOuvtSWW8+E/9K28ajZf2a+5l5fcTLUdahCNl9LpMVjpqfOTAVFpLGNnFxUfG3"
                                                                           +"ufy1co8LofTUtXxmDX7K4rxpfTODsVUookZfzFUiaFTOhluDfJcztxjrbchgjmLhXfoU85d/4D+w"
                                                                           +"w1kTLDTzXUEyDquRuM4L8auULj7VYeGWoNKKIW0fChw5tfRkIadHDyPj5C8RqO6XIuTTRbirWaXl"
                                                                           +"FPrf6JYDqcuV+AY++v9Ou48a4Mv/FwtaK+1XhPRyEZgMfnZ2q6R73Q74Idp+FN93nF1SObTmUA6k"
                                                                           +"pxWuokqhi8F6mFRngcfhzR17/XdCC5ujMyRVGvUaT2Vadj65j2IuRpgaEpEhnWBcc7lvytIEGd63"
                                                                           +"ztLF1MEiN5WuxD7XeTUphY+wCqKkwCbXHvJ8YKibu1K39Q9WFwTNNZjtBWGS8ukjP7uX8BEREniX"
                                                                           +"79zkHbirslyJUrs+GwSKlTXOLzHgUWHrayoaLE3WOj8JyF7/tthBESjJ9sHXxI/NxE4lvy9kV0sy"
                                                                           +"gdG2Ro6BilhwBlb+4dToYbWXhvqlbY7XNxWQg5ICXDxLLP9pN496337y3bjWB4WKFIPFSGEWsYnV"
                                                                           +"iTSsesCv8ucNMGIpvtL810BxHGVuFG2TCdCyagAAAAEABVguNTA5AAAGFDCCBhAwggP4oAMCAQIC"
                                                                           +"CQDUXau9BJDtzDANBgkqhkiG9w0BAQUFADBNMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                           +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNVBAMUASowHhcNMTYwOTAy"
                                                                           +"MTQxNTI3WhcNMjYwNzEyMTQxNTI3WjBNMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTER"
                                                                           +"MA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNVBAMUASowggIiMA0GCSqGSIb3"
                                                                           +"DQEBAQUAA4ICDwAwggIKAoICAQDIe9JfEaBpPpHEYnqGK29dx0UWspVgHcZp32Qn7JgR0PIPNmPo"
                                                                           +"40tYoagwNYeKcVMMG/lUTv79YaEn8xqZVXyreuLUP5Ag5PYQjTl3x3D7YpeqUVOEY51fnCAnqZ/4"
                                                                           +"qT7+y0PNUbIfsM4aZI/IUXyGzR37eRiUqI/HG7TRKtgEKKy4JOiEoFhn079iUo4F5bz5/PHEdvdG"
                                                                           +"tyoS8lyF/qAYPr/vzqIedsxp7mbdAz9sgs22I48C2YVBFKUexyXgcPOnn3BeHyXWet9PMWlZFuuj"
                                                                           +"ffRxCot00BShYJ1BPd2cZMK5iMZ20gOJisiSfaszfr/ys2RYnvFq5cq1p+0tUDPMRwYTKHZulRjV"
                                                                           +"6beF2+3TYE+kCPoOd0TYN/cywj5Q1c7xUXcjGjzG3Dz3mPmIMVF92fc0l0lKG+9v4c7ZbE5UGU4U"
                                                                           +"jeNUTnwwE6pfB/1WsMvr0JTmBOUu7QgcDNlBLf6Vrr+WHQIzEeXkZEJpTrDGPjYPLaI2S1gM3iHs"
                                                                           +"Llun9rhXv3W56nQd/rxke6NDUAQt67Cj6bvLbFb2OI9o9l4ZzcEIgZK0PnfCKo8no5huco7K7tR4"
                                                                           +"+R+a6RfWioexVYXpsm45c6JM9QNnnH/FdyQwdM7aw9lWyV8Au7NIlfZfPLlx7VrM7F5RY90jklMR"
                                                                           +"5wWfpNUVRpJ2eYS/hyEb39IeJQIDAQABo4HyMIHvMB0GA1UdDgQWBBSS020/jS/eYKWLxnCMI8oD"
                                                                           +"zGq0izB9BgNVHSMEdjB0gBSS020/jS/eYKWLxnCMI8oDzGq0i6FRpE8wTTELMAkGA1UEBhMCVVMx"
                                                                           +"EDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MQowCAYD"
                                                                           +"VQQDFAEqggkA1F2rvQSQ7cwwDAYDVR0TBAUwAwEB/zBBBgNVHREEOjA4ghBhbXFwLmV4YW1wbGUu"
                                                                           +"b3JnghFhbXFwMS5leGFtcGxlLm9yZ4IRYW1xcDIuZXhhbXBsZS5vcmcwDQYJKoZIhvcNAQEFBQAD"
                                                                           +"ggIBAEjM5m44dh2UVN97c948yAxnzQVLCz/swOaumyAMprTuhw3c9jQPzjLRbqtewPWQAKqDvLn6"
                                                                           +"DDarPzWRTytga0pGgH7WgU/PQOmL+khir6fPJP+TeBFrrx5/8LJf0EhrXG7PMwBogS+C+MCHCYh2"
                                                                           +"uQFoTkweDEJhFDLr8zrPA416VkfII2zavI+wTrUW5gduBEWv2Rm5PpxGUMwQk7O9sdfmIkJEmLFK"
                                                                           +"37/K90q1eTP+dIQZL5IcuuOCArd8rMihhNGlBYMpcAdlTJ5uTx5KGGm6YQBQ8Zk2diKLIl19ymQU"
                                                                           +"DMBlXMiue8H8WLIWR+h/noOFXgaerSvfefZ4rWYnonUwJHJoy0SNW+6N6asqYPXrHidlQjfNS0we"
                                                                           +"ts5G7ONubGXdqld2n3pTIkp9uJ+e+gng3bKmmIHJawcOSSYL+BvOa6Bo3im/hBhCswgFq4Bjz5Aq"
                                                                           +"gFHLbkSR9lnmE6plBXQ51vgI5+eG9QzagG8ZPPvEgPwvRHKp8R1nuASA05oLsTLPO0lPyVd27Qyb"
                                                                           +"XslqKy54VqT2S3hpdgJQoz4Y2eLCPtm72eN9/PCaAIyJFn/feHKyKHmISU082g3U5Tk0fKqndn9g"
                                                                           +"huqn1NZMwRuzUFQSyfIO7mJNtW++UuF+ccGKk8BR3MvpoYiV80xDtT3AdozQS1CBnuedAvt5Chb8"
                                                                           +"jb9bOenj9B8BmpqSrBhWOJ1R+VxIytg="
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*
    //            X509v3 Subject Alternative Name:
    //                DNS:amqp.example.org, DNS:example.org
    private static byte[] KEYSTORE_8 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQTD+AAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJbjpQpyUW3ZmF8P1gwRce6uQCTe3hN+4Mft8FrnZRnv4kx0YW1cMRfyig89HYONM8KsS0"
                                                                           +"BMoqT3xa5iCQ/oHBymb82OC4YWSAomHFNT2JoDWelNQfnMqjGt3UETBP7g06+ulDjEA5+DAsdcxy"
                                                                           +"Jtt3Tpfjpe0s0I5jY3blGPV8yQDCNkahhqET6Jvg3PJYsHBE3ssPkTNKI5rMjzbt36pai/HlmjdU"
                                                                           +"0BSuTeEcwkzHXhdn3snoV7XQYRBTNScbTpqs6g0LPz6AVqa8+wrdzELFb9C8yL+TUpsVH7xWBQcI"
                                                                           +"w2nz9q5syYGg6A4sghJeyvUdezmbcJPbch0+XG5RNZgM3PASoyS4Nl3L8d280Z35Eke69NBkoJ9u"
                                                                           +"0DdBGtPIczEh2jWfMs93qAT9Wh7iqvQhWAuRAj0IoDtwk2xZaLzTN1/8lHBvQlMtY1uqzM5IdZYA"
                                                                           +"org9xHwmRAhiD9NnKhXAhMtbujWLlA214VJqEnR7hRs4QQ6wy0d9dS5gh8ZDGyCzr5E25s2WVFsQ"
                                                                           +"SbcQFCIDmsrUXq9lJGHWFcMjQviQdwGbcMwx34FtqXZmM0mzixreUSlVv8GrlkC+/Mc67QuzRw2y"
                                                                           +"23Cu6Tw1R1tUltO2eeMZPo2tiZiEVTS3jo729ru7tvtFWOeR5TYqPtG+gbbTTMpARtPt3J65tBhi"
                                                                           +"7N6/jN3qa+SCS/PQFCNa6UqSSOmH0QuTSVuVL7nTKjk//y1injANEbe8n5yNQhKcxgtVST4ipcjx"
                                                                           +"eEVgelyGYf2xQnNt3oUiphMjekFH9T7r3drZHB77ERBmzXGhCILGoXe4q41JnKRGchorNCEDoVmI"
                                                                           +"702p3oEVczQKeW8KJYc3Iekzp+bSzlek3SlRFBzB52Jev5WqASH2VguE4Od2WclRjojEjdfbQqev"
                                                                           +"m530N9En/0qsy7Glm4dhbEviOad9riz8wgPi37WLh4iq7xIq4uJOsbzEsaSSmri7zB0ex0/ECVy+"
                                                                           +"B72BlTkt+Wj2tiL9blo3ujCAuhYg6IsxYkUBKGgbdb/qIlCD6ifqTf3g4CYfslPNZxlNVQrHJXMb"
                                                                           +"cb/VbaYJRHOdjgdxrQOK8QgRNk7KH6CRYF1szNsfeyL872eMVb82dKgwvtxcDX6LVIS8f4z/4NeI"
                                                                           +"EE6ZpKWOE4E4i9aJ3Bcizzqhnyt67p3Phv4wI7AXoptoqW6vBpRatKsCc1XX7M1LPV05hPgwLH62"
                                                                           +"DF9EFRj/hMBzmuWxDVGqvdj+VU3WrD+2LG4Hp0OZP2GHOU6jUGKqvNl4+d6R4/A/suSZfFmfVFhF"
                                                                           +"0FtbVv3MOQ/lFmDlVskmOuBBTCmUnsgnB1prSXzoerpa0UXtPBCe+8YELBqL5+E5g60uLj/qinB/"
                                                                           +"HTXbP6qFPE2K/5bLkkWDwgTlOAQ34F1mirb/YS4fqgCTGr6yzB2ejVLubRKco6AYryqWDn3yKz/Y"
                                                                           +"qsyLQxg5wnWxsyE4nPxeoj1v0SwP0hv7Uf8ywioRCPpdMG+pnDgCYr+M1tNXV7b7jl9zx/7Ulqz/"
                                                                           +"WZQfkIGXe9wwJieypDr2p8iwZpnjsl56bP2EE0c2Q0SHbXIM+T4Ei1bzZk38Ie5wlvGRTzU3dK18"
                                                                           +"ACrKUyrS/a6R8f7z3VT4sjXPsI97p07Rd7rFFjaHregoZ3QUo4kcd2R4euSDZfyjKGOJodnWx0+P"
                                                                           +"C57Lxpgzt6CXHH1GwvQexLOtcK2TywMxtN8xPZmch+//kjmCI3aWBNJSZopWV9U2sVm4KfY/Q8wM"
                                                                           +"91BrXAp8dB/rEG/V7sAxlYdArqDNqsXIbqDXuRNZVqcy0LLEqfSqTH7T0DtzN91qIBdRZBXt+Lo8"
                                                                           +"nSO0XHj/FyPajbLmaEYEBCZ3MdgEVDnQg6YGdhUJbQWPUhqOk5rmPNVgWWWbF7AiL+v9ehEM2Yp6"
                                                                           +"+LybblSQOVmVFjHrdVhBLrif5FCTKlwONlgX+RsEHFtEfycwWUWIxwXs3RccKXjAgCcgMCv0RxcM"
                                                                           +"gxqB0w+FPwB71OZiU+jFNuTNeYKIrXlWU/ZJnw/AOctrCDaBL4j13d42LnN4G9+TZygT3ff2gm9C"
                                                                           +"kP4kmPe1qF3/tpEJbcO26wKs37qp4uUcPcFucpChagwKNin7E8pYdWtrlCOoJRM5KKfuD/bLyw0H"
                                                                           +"et4TrUyJtILKpJ3CQdsn6T901Oi1NIeXl6dBQE+0tGkuahioJwRt6loYecMK6sACx9p1WJVr32Ty"
                                                                           +"YnWG8jrX9y4n1etey31t0qhBBgOKagt/SG5I1HDX8y5WX9xTP3HJnLJChXH/qWKOu76Dlbuuy44o"
                                                                           +"E4uF4+mLZcorfdOU+0Klv+ASYEtgAxLSGXeqHPGqFbaDqzGshJ5kcie+4GBedCV/Hb0uMsgbDVmm"
                                                                           +"Ozvd3QsuB6r6cZImJK1nChI+jt4J0oUJwGbO+s2CLEzn0ljrRcmD2SUOS94gi038G80n+uCMa0JW"
                                                                           +"2t5qllUmmlfcKvfRZJGHN1uy6e4nsMejUypffsads/tu6vAzYC0SG+XTU0uujNHLBssF0ox+NjrY"
                                                                           +"ST+d2MYZs2K6U9bnHVHCBKENuNH0v+9uemSBoLUT9cg1htnVgBGLxiuRSE1CODvqHmiRRwaLrhE6"
                                                                           +"W9eJxs+rUUtUnRVVBRu71INBC9rEamN6bsybFAHG/w2JZDlsNSNcs3sHcRCwBpWjALQqatzEJ2/E"
                                                                           +"U+98Mvld5sZoQCAeUFede9o9TSB5ciogzW6rF4ZlfLaDNMBJ1f7GWFHrXbSOQWa5qFn3e5I2iy3o"
                                                                           +"yGjsnQuesUheQbF73/L3cC7O+RZIVwtd0FX0j2HK3W79Gdqx2BeLo5xg8rVoV1rdZ6lboNJnCHyE"
                                                                           +"CaD8bwnpheaAlQ+vwR/stvlPbjYDuO3ptiCTiRVISNYfDyuf8uJtF0mnsTDEbrZwPo6ZEKCAwdiV"
                                                                           +"Dbo2wT+GfbJfthMxNM5lJo9NxdqSLtJQBnMHs1IQ18K5VbK8fBh/QFDhyUKkp+x3oN8rXA4tZdu1"
                                                                           +"qi2c+TVATonoEhWht9DdL16pXjoRwmGI4r/5tOFXNZa03Qs0D2Od4hQAxn+i5SxCX6SjtymZGHCe"
                                                                           +"ndKQs1ASV4r3ti2c22thQQkiG/7rc1EtQxGwXEPP1AeV6dCWc1tI0KLjxWJhoGCHaXHWrQ7qdsWH"
                                                                           +"N0K9mLHi+aQ40ieVGo4GMlu1ZAY/SamnEx/iHdeGTvpCiobsc4FoqPaMbwzL0Adglrb3HnnP59NJ"
                                                                           +"JvyKhd7EY576HjCTLX4pvo2IQsRNAZjnq7waAAAAAQAFWC41MDkAAAX7MIIF9zCCA9+gAwIBAgIJ"
                                                                           +"AOfC2xmtSCm7MA0GCSqGSIb3DQEBBQUAME0xCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                           +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEKMAgGA1UEAxQBKjAeFw0xNjA5MDIx"
                                                                           +"NDE1MzJaFw0yNjA3MTIxNDE1MzJaME0xCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRlMREw"
                                                                           +"DwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEKMAgGA1UEAxQBKjCCAiIwDQYJKoZIhvcN"
                                                                           +"AQEBBQADggIPADCCAgoCggIBAKl6C7NR7Qb4D/0UMKRObGIMKRqJzNi4h1dhcj36WVHDNHVQJhZh"
                                                                           +"vuhWRxWwQ+bHn56ANHhx4sM09aJUrc4I+bqaUwiwVMtaqfnWrZsU5JHnXk43c+aW2lovJY6OwJLc"
                                                                           +"nFR6ZIM75HfSShq9MglTrHtQjX+MEkIdp3HjSgLGCH+TDPoVp9RfHMTA6Z45P3Fzh9j1wyEdcO2Y"
                                                                           +"flVegU6nceQSZG8ita+ed//Y/Ln6Ff0K2uRksuZLtgNmRhMp/IgrhbMoUD+k3W1TNai8Lo/Ye2lD"
                                                                           +"WaAMAnX7FX+TYUL9krAGjPhumv9oQG3erYLUnyUwxBZi3JlJGdkxiKVTkIhPCGkU8mIO/x7Gl78l"
                                                                           +"PCkU4yhnKP+krxk7MT79ep14TRWUUu2Wotz/C6ZzcpfFO23wQVF2vxXGGsefwy3h5UFkm5TsrDav"
                                                                           +"KdGrpX4KPyJB/INQ7gsnX5pNDjbL6rxDAXXkhIs/vyDm+7zaMHgD1j/z6p1tTaO8by9utowmDfzR"
                                                                           +"1sKqYPJz3ntRUfkKtuNgj2OLquiZS0JcfMN0CRdNMonJe+NKDMWU+PeOR1jRN0saIukaYhT7XDOc"
                                                                           +"M35SC59cSRQ/5qgD1IXryOFpUeZltCfafjlCAFs2Qp+pESm1x0QE9273nbsuVSYxOMKtjoVyWXZZ"
                                                                           +"waCNLtD9oCSNxLnw2i67lf0/AgMBAAGjgdkwgdYwHQYDVR0OBBYEFOxZJWGsG1q9jRKyvhzs/Ss1"
                                                                           +"5K4XMH0GA1UdIwR2MHSAFOxZJWGsG1q9jRKyvhzs/Ss15K4XoVGkTzBNMQswCQYDVQQGEwJVUzEQ"
                                                                           +"MA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNV"
                                                                           +"BAMUASqCCQDnwtsZrUgpuzAMBgNVHRMEBTADAQH/MCgGA1UdEQQhMB+CEGFtcXAuZXhhbXBsZS5v"
                                                                           +"cmeCC2V4YW1wbGUub3JnMA0GCSqGSIb3DQEBBQUAA4ICAQBWRgF90gAP1OYJNxXQUFOeCyo2xnKX"
                                                                           +"ZEnvAN4l12xj7j4yodCCiurprybReFWUWcSoWzR6ttKj9EL/gouR9v5DJSnn9zbq0YOaLyIC9/Ew"
                                                                           +"ZTV3XlL60ClWNUJ3oFSrEkJjWiX9hQGR21YWDWRdLOS6RsWyg5V8O1/PiPt+u/fEXFJNuf1Afrdx"
                                                                           +"B8RoQRos+XWFbcETolwvqdrebXmiRATQizie950J93+Fj7fRhMXLdP8MW/gDBfb9ZCoj5VED+Z71"
                                                                           +"Or+isSZTQr5WuRILGwsCpCH/VR7zU09jRV4QUDASOVTA51oWbegw4rE7Gh1L8G3raB794Me8ECWH"
                                                                           +"5332f2qDpNCkfYUkebF9flkSP25yURLkXRiOKHm2D5kTpT9uOTHYKHCGa49hc5suz+JSCJzniycq"
                                                                           +"i0wED0fedfp/jJlLuxmaEeaOFq0HxY4S25frC/wSqqI7qlig62HXAUoY2mYWbfFhIuuJUCmcfOtO"
                                                                           +"jGJo9LY35vOC42te95veUuCS7fDpn+PYieGL2QxLX+iU4hQIdglvu98OvQPgQuZLosX/dDiPc+N/"
                                                                           +"8idqhJIKvjg38xHjMm9a9lgHPcW6hPs6/mx6SCKGKy8ijmzuRFKdaQIfFXtveLUe7IGEJa+2yvk/"
                                                                           +"CVG7rZoYcPH/NwU7jkIeEDudOD8fgs1hQoJd7G66LrC1VHwXeZJ5/+X5sWX2YDdK+qtJE+wf"
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=amqp.example.org
    private static byte[] KEYSTORE_9 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQTqYAAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                           +"AQUABIIJbnd59Bys8FgK8FREpm5Vsbq0uIbSE3nZFBNRVaYostRXeNA1fJok01wXBqrf05NfelKj"
                                                                           +"+ttyFHwE8AKzLfF6vDusoEH4r3HUaQWoWMiOfCzljxjtrr/eQQFx92dX5+Z17BH4+92HZUzVPbSg"
                                                                           +"31eoaZ24lTO7X8XXq0a43mnC/m3Vs9K590oCfXSDRsx8Ucr0VbUjR8jyLMxE6f4/cxBIhg2PTzCi"
                                                                           +"PaCr9itQYbPJhh0M9PKWaC6B5h0HoNByo4NYQGOJXEZ0NfB23GDUOqbQZypAVFOlRh6hyjfA3Jvp"
                                                                           +"smhQ2Hl69W2nE9a/p6hb+H/TX4bvYVJk1TnNVRI3g/OGMYSf80zcqIttTfq5NlL+D9CvIVZArL/E"
                                                                           +"C8D/E1XrYr5uBMGQap2gPLxUU/8+qUfRc8kD2kKoG2CxB+TCWyf5wIKUNIUJp2TqRE9WwMCXzC8q"
                                                                           +"qFAmHa91aVkU0DEHtRzlfhVO39FVILCMDuN6V93rB6iUOnKJQFOnhrOjO3xLuq/dsNjfYl0GmMRD"
                                                                           +"xC2ioGJW0cskOD7uty3wiTbnqmei1WHc0qKbxDQwshNMTyF3Y/mJgfRNUsUo3iYIIWtnTQvtm7a+"
                                                                           +"OO6qywGkYrgQbXUN0hALg33rDShksrUMH9OMkPc/FP0F/+oFnxycboj358q/WN6msnrHRFonyusL"
                                                                           +"pZpZ86zzctnSJuHvVIv17ojRRAy2kWaB+vIV3SCfZAkLEx2ZEOIEhMBbZ/glvAE6wFfpySCOG2ur"
                                                                           +"501IkI6ElPtWCsAQvcbSDRqy+6z+UjdnUOyZ12ySFE6wEoMO4zEvBMWNK7eGEEOE1tgF2sCj8WeP"
                                                                           +"Nvd7kd3SnpUTpDputkXVnMzAXtZGzIJunJBWgrim3cGclQKxGnwrj/IHdBSe0KGbGYH2GtCPum8B"
                                                                           +"cuZPnyMSrhLO/oFpi0/ZcgSMWzWwGrOOR5gY8t2a5/Jrk0kMVythTOI2haoMcu85ztEd9WDAIoP5"
                                                                           +"dj/rWB2j9ZDrrgAwaAh6LCwMKdYjlhCgKgO+iYhgiMJ8/hw9xA7mlnH67rPv9ibvgVoJemlgNbWT"
                                                                           +"xiuL+HvPbXpnxR3qbEyenB41gabrIrVujmuW+i0whpFe8RC+ggtR8abDi4WCPmerLftT/dqn8aHl"
                                                                           +"wtA/GzVz4caWGZGUQ3HCPBPSklrhVLgeBI5m1zWNSnprLeiHHhSvvFJX47NU7P8rec1tSN2RaFKY"
                                                                           +"UCVtXt2Byr/9gDggOWdJyTdD/hiAmaGuUmxbbpWldrRR582Daa6+IkAW83Zl7KXEl2cyUz7QdbsW"
                                                                           +"aqf3Au7Sm+NZls/bzen8xBACxr1VX99FOdTvQXQ0aINnVhJRDLLV8awjHZX/4czgm4GAcUhLjQ17"
                                                                           +"LcOb5Yg0XiJ9/+xyc8xvA7/D9NChm01gK6NEZ5g/TKeWItb+hArnSTppvibHMVSHNEAaxE9YX4Lx"
                                                                           +"WcwcJfaeeuQAB1NstBYc8WNntfz8N+iSpUBrICdI1rMDxtHh3WOOHiUdatMGFFldmVnZj31IzTxO"
                                                                           +"SFyuRWv4A4UPimMKiRD5HWgwrHIQZcJ9DUZnljcYwDg/SRtitRUIQjZfVI9RQL7fZXjxZ8NtOUGz"
                                                                           +"ar+hY+Q0YaTD7G8gx9x1QXDbde58EKGIswYp0cfp9OKszUifRM3aMDfM4bpIulydugCqwB6ixHsN"
                                                                           +"hvanZZimqAT61WdX38PgHikEZsflObtN4C1aHD0Np9HLgURT8Eo7DmRmae/HRyyjmgP6YhuH3Br7"
                                                                           +"WECgtzHC8Xlxfh8xsWUfIlTpAYiEl1fotsNETnZp/dQP81bCRKaoPCz7h63r1W9LMpR76u803k0+"
                                                                           +"Eb1xdJiUamP7iF74+0LOyLyX0lfzPbE0ksPD2jsx6+sJtINZ0v4DxeCttlTZPrb5B5L6BE2YqkXk"
                                                                           +"1CY1sa7inQU6RaPpPr1Xa00o7oqN5gB23PhgTS2tdTQJzqSDqhegXYCIrpd34IBnjUxrV+td68PR"
                                                                           +"yH5sHZeRV2jWZ8kKruyugNfO+AiqO3miTX8KP/H7THYDXmRPfIM1I+iFpXEegWNrztZEmqu2IEnb"
                                                                           +"qGXH8eLa7GX6RIP82NKl179y5Jf0VDBaLKbO9gneEC3gJbzqmeogqXEWOTiiN7SJVUVojlEIuX/z"
                                                                           +"5ObedZI5tOB/24Qdtw40E+bz21lCIr9vgV2kIsUNFl4q3SNkkCPvMSsoBV4fddst2ckhYYFMqmFH"
                                                                           +"0zVTXsNqcYGUMwmvSGUG5j+PLAfDBW0OuJAzCT5CQkC0u8xDDYCdVHLClD4BH5v8wMe0dfly+sOx"
                                                                           +"WfVE1H7loERm/cmgH0CKxEaXHlq70d9Xr4xaP4esAeqLTTP0g6BtAVQQBJcqQIKgB4Xwdp0f7OPK"
                                                                           +"KNWzmwgjSIDOolRVqA2yfYeUDu4qWqaGYpvRmsqubGJujWvfoHaowxhzekChvyr3lqQ1aV7a57la"
                                                                           +"Ot0v2LnNyZUQ/8gjgXewAEDeYkMQRbmewu+fk1YDthixN7bxj3mzwInfWjXxac8eqzkS8+jtM1rY"
                                                                           +"gBGQxnCZ1FqU3VqaDRQr+CWxKlNLKkL35ATBCS2VI120H1PHwOq1sp0NtCMmjJ4s+9pDRhkLiLDx"
                                                                           +"6HHM6zCH/axd7o7puPMk35i2s7NhS00an1rT+dkps53VevcaLk+uKMNc8LV/wgwwZ5LtNMP6pSqk"
                                                                           +"L6RUGsPvuBbPwl2Qe8fJst7CpR56G/wkuwrXSQRJyiazi7Xqxpk3EcqrnCzyKEA/wsYXLWmL4HrB"
                                                                           +"AVXLN62h4n/KDLI87jGeS65T4Te20taHE/qpwhZbiaNcEXv7J87RSuwdD5lOd9Ah7n0KwpBlOtfQ"
                                                                           +"mg6IpoMCOe7SWLKpdDltPX6TRrjl8YEFjdZfaS7j+ce/T9f9lxxSKfGEtIi9YOEHtugYjMMWxhrc"
                                                                           +"T2q8q06aHRf1mtsLilNTKkbPCfki23YrgCwpUx6sGQZaEj8EP7d3itKvIVeTGBd+rDPc/1YVfBNl"
                                                                           +"YIfhW9l14GJmA9R6t6nc135TNI71pcc3qEfEWOv3falUmp6bKWTEqOZyJvoVj2D9QAN02i2Sg+CM"
                                                                           +"AFLs0sBjhNHWNb0VmL2b5RZpfVuRwaKnBO/2GELJZN38MLgXLLsa6wO5rGH0A0WK7MYZSWiyCtS/"
                                                                           +"h3DH4m/2HHlUgzKZXyaiBs967Z0DmkvZAQjLUyVODMlSmUk8SbyVQBd+RrsvTCIBzXPnp3M7bSvm"
                                                                           +"YPs0jhTywVcXlyXCQGVO+XBCpwn4eqSJyDyYAAAAAQAFWC41MDkAAAYBMIIF/TCCA+WgAwIBAgIJ"
                                                                           +"AP3w2DujeBEUMA0GCSqGSIb3DQEBBQUAMFwxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                           +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQYW1xcC5leGFtcGxl"
                                                                           +"Lm9yZzAeFw0xNjA5MDIxNDE1MzVaFw0yNjA3MTIxNDE1MzVaMFwxCzAJBgNVBAYTAlVTMRAwDgYD"
                                                                           +"VQQIEwdwcml2YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEZMBcGA1UEAxMQ"
                                                                           +"YW1xcC5leGFtcGxlLm9yZzCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBANAUSs7YHcAH"
                                                                           +"578Amq0SQNYzwi+1ROxygLx/nn5k+O/5KgqbZz6TznJu8EQ4LTUgPiBqoMSocS2wp5dHL+fGArtW"
                                                                           +"vTCUCyKrwwKUnSqT7Qggj0PFWEQk5qWNeJuBHpij25ElflpbhSurREKGC89cjOZQDj9AN6u7CsQ7"
                                                                           +"/iDDliruH3pXMaP4CNxloOdGs4euuTv5iaGxf9EV9KcVc4qm8Sa5pEKR3mKwSZcKpIUEFhatUbUw"
                                                                           +"lWAp9+9AJ9sNMpYc/GeydUSjiFfkPAg8hUj8FeCcuzyAzb5JSQz1A4bl1u8RnXGibhk86vCycsFI"
                                                                           +"TfFADiv5jZMVWQ55gSfNOroZoVIJZvcyB9MbexueRAJPIAk5DAp9YgCuQ3Styq4BOuMY62+eTEGa"
                                                                           +"divs4HRkBTRg0gjwk3Njw4nk1OGNNQAAD+iIfOkUQ7orQiO3bEiEZ7e/2lAAJuuXc9SMGV/QCvqZ"
                                                                           +"UsaVrmeQcmVRM9r+46qAIVeM8vbqsxbsjwvyXkmTFrSGdGSeEIzo5XvEUQ7HSnF7A2vl8vcHuThS"
                                                                           +"td5NGs5uJU+rgRNDkHCi1Bthz+q1ogvA8DyqOkapIxYZBXfvThnbCoG+bUiJCH0fwyKgyMoa/ixp"
                                                                           +"8Ecx15zDWVx4JinYlF16UetGUYpT6kBMhadFQTPIbpKuRierLmTXD3Si+YsiQchRAgMBAAGjgcEw"
                                                                           +"gb4wHQYDVR0OBBYEFOsV8HQC9jmMhRjSPILxBUPMQCILMIGOBgNVHSMEgYYwgYOAFOsV8HQC9jmM"
                                                                           +"hRjSPILxBUPMQCILoWCkXjBcMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UE"
                                                                           +"BxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxGTAXBgNVBAMTEGFtcXAuZXhhbXBsZS5vcmeCCQD9"
                                                                           +"8Ng7o3gRFDAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4ICAQBq5P23xMeb+G5ALcC62o6y"
                                                                           +"NmVHzO6XtxTncXbblqglIDrh6L4ZnAIT3IrYAXsTGhkckmItl2RYQsN4ALf3hoG+ifw3hFO0GmFN"
                                                                           +"vUIlSSle8Yt9TiLpXfGVmCCK1GKXpi+UCmD4vUaeE8iTNjTmsRppkMjJUX0ofCoMxn3ffdM/BG6r"
                                                                           +"99WyBgFRigmWcxzS6JNcRReakXJa4rcAamiA/MkxRscUPQSTTdHF9BY8wIOmb6RmBOwQ0ktrLkE+"
                                                                           +"Wjuaxin2P7Bm5vpcworNldoIMEEksHDZArkjYMrWh3OgQ+5+Ze8JtJvWaADPQBvf4lgQ/DcFv3wy"
                                                                           +"n4gMVzXb8XhqS6ycGZsr3/mM4moFoFP0fkNv6cVVVsU+Tjd76TnL+GAPwwl26aYhAB8efYdnCPs+"
                                                                           +"AVFJUZshcEiWTw9aGFk9mPsCY2yKiosRpuWKWhczmu6INCM5WSAp+Cnmv69WNp6OGRhy+iHCI37Y"
                                                                           +"AD2xe1xmaqwzHbXvb3gPAnPU/lOtL0zMlL8qJSCbAbc7ciAo/FmmVnZndIghwNyHn3qcHVvcxuan"
                                                                           +"CMQYVrlU62I5aEpf33aCgkmZEGMaYU1SuJ4Kr5fWeTfev6yI5kysE5idl1f0padziq6ae4Br+Oxt"
                                                                           +"AtObpjb6xKAKDj2OhiTXQ/7iWobh+3QuoNXNrUXd1FNQ+VY7z+E3diwJ9bYGxQLNiKjxNRxuJMFQ"
                                                                           +"9jVs"
                                                                          );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*.example.org
    private static byte[] KEYSTORE_10 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQT/DAAAJhzCCCYMwDgYKKwYBBAEqAhEB"
                                                                            +"AQUABIIJb5zVRh9eulhVb9JI6dbHdLyWN/AwqIfERSIFCr/n0jn7MvWHPiMYQtYtzv+z7bkNVTv+"
                                                                            +"Uec6wagRxHGu4bjcVuIs6zX2lUrO0L5rN3Jwcbu6bpMIxw7sAPw4/k7Kgvf1ddSOIn4WGiHrJf8G"
                                                                            +"WvMy3rs9dnxSe8Z+TvUBn3yfFemMRpwnfGn6TuJsdBfzU/bm9dX8RBjMmwQgyHqVgzuvJtAkaQb8"
                                                                            +"z37MUEkRICalTZizmM+RIcUByBuFt9vMeJ9HfsYhp1TLx0ssD/PzHZZQ661/KPUEmBz8A1KwmwIl"
                                                                            +"gt56Xu0eM3GIc3XdvaQFFVnJk6/Vs7P/cr7SpemtUj8yZHmn9SvuZI4LKt9kj7TdMCeuQsRp6Cr1"
                                                                            +"/e0O59iaumB2wXjq4zKZfCb0XqPeJUKW76OgIGjLO8hLClcGcLqDbFSr1pdwYpsGBWvmL9fWEgwV"
                                                                            +"EO7WtVjYTRKWzM9335M2u7+nIawuxJYXunm2cyZQvqhzN7EneHgTN9aCLMiGGhaHlBRI+57ocZx7"
                                                                            +"tifILg0zUolEZrN9X7dZYqr2MrnRSZ6Wd1oxGl6xXrK6/fK0K9LJ578ilADsHvxsQ4ZdP2Euj13P"
                                                                            +"ygKZgcpxNqFfWfVwF80/Ac6YNSL8rM182ya0LP4gxKW1kNWnzK9XndIgZ1N+H91bfMpgEgktnOWI"
                                                                            +"Nvf7kh9CvMq/zzwmrV9eec6pwUxpF41X4uGYTdzn4bu2fvhqnJ2u4mVSIpJyejJO1LjpsD3w08Ns"
                                                                            +"U836DnqA64TK/t3yzRCCq1IabUMPzDPnH5aPc59+FuA70P79c8m1yicv73ai4eQnxo7a2GvY1KV8"
                                                                            +"ZRl8wbae5LS/DReaMvzX6UyX4mw8fuavt7JZtJ2PRecyeX7/nYeJkgMpa7jaBUEoUEapNdGgnxOq"
                                                                            +"a1J/Gap/8c8737y3Un788RaQ7peHnpQTRgUm3uVcdb0VaQBOeCgnCOfH3LkMdIHv/cVSwojrOeZN"
                                                                            +"Zkhbz4Kmh2JVPmh4bb3YACJZmCNN0vjYTZ+VYeNqYiLxt8sz9Xhk3vO3F6G4sQfrsHnabhGXq2Uh"
                                                                            +"Cl0iSqmzcc54EnOtXAGQ2y9m0dPUFczGI8Zyr33l2oIds2yP9BNpEY6yWwWEK7DywFsfmbtuFJN7"
                                                                            +"IX9SrWVmDA0bV57EtM1UPUp+yN+9Cjg8UZWelukQeqtbvxYJlQlBVIK3tuoVjCy4ieX/sZRuh8Nr"
                                                                            +"i0iE7Ci9v0aXgDmU0PXQniIvwKrfY3iXVD7svCjVPHNVttE2mgKbp37yKb8WwzBPVh7cr2vwqgzL"
                                                                            +"sMmONnXNP6QXAjVRElz0DRSPTjd90+cXHDTXnYSn32dgYUKh4PJvN51i5T++YfZYs5Icavwft7s4"
                                                                            +"boGjzhRdhZnDv5HgiYjPKv2d+bQXJ6TzHFas0ZbdBg2bBSd1/HI8uAZqBHntaNaWp9pJlQzHNOlv"
                                                                            +"Qv7i+vwV7/Sj2CzR0pBdxFg0TiDbbPv6uDUaB8iGbRwEVzgUU2PUNXQW37oeEUry5nQvDxje1wiE"
                                                                            +"cHFmI0gl8wOVUxKH89ACwHbz/i5fCxKxbXvzrQ6rkIo0klr6QiAAj2iMKYYD3tz8JhBFYyHUvio8"
                                                                            +"+Z0wCoYAW358DVq01WK1xAMJqImT0LhtdAA6aG0pCDxEvKJJOkSSunshVTp4pTDiuRdEJeYzkYHa"
                                                                            +"f2iNaBVH3Ngfrq/tWw4fAB+U2EulwyZIWoUZIexsx+vgt2d6trTJRJB0itXKD0zFdOC2MQUfFziq"
                                                                            +"54NbStQGH18Vel6FcMsczW0P5hG51bMSB7wZzD2Vwu1ChlHzSW9wnkGQrAu1yZajGnx9fMX3k0eg"
                                                                            +"QHIPp4y8BkeBeGu8i2taiGxlrlU/7KX1w9yem0xhAmpzls+lw8lx/sMZ7NTxR9x+jMYzpujW8/og"
                                                                            +"MtYyPYglsugWif5SEFYu0S1fA7JsuA9UrLmQoB/7CvOhXhEd8Bc9Bw4Ntt4eARVXhClS/IofG2OX"
                                                                            +"erGiLzcc5KuNR4RY8T2aXgyAiNg8VUjTyIwXEQg8i2GYnf8DMX2Y6XAH0lwil8JpiHtE5TdSzjme"
                                                                            +"rIWxPJqHysqGxx8bb7DYRs6q7PaB1SD70oDwB/66OaQ8vClMGIsFMqxblmUqzDFz3j6i8/7AIi3S"
                                                                            +"H8CZWECmGKOHBKpdxNeivyf6vU91K7Z88On1GhT7JGc4sry6J8yoRiI7F0l1W28HBrjdTLuPv78z"
                                                                            +"sDPZkUjObL87uKKjluwVGteN2t9Ppf+opjRDjAQpLWhNSCv4Ubz2IWU+OBrFWykDCbspcC9JJOvC"
                                                                            +"WZQiCn9wr1m2z5ncQN+GyF6ypbHTmdG+5tJ6uFKZI1LWAxqh8r2sUE0u28bGKWghJNH0yxGHD7os"
                                                                            +"/E9eetKk+dWqS84J+SQ40ROeE2cV9H1RZVLuA00lfR225sB7iougtAcv5MjKA700R4W6/eTLhs5o"
                                                                            +"cIG3iTDe7MyCJzayak9lbzZ0L5OvpSIIJC5YqufU3mVaAFBnv0r4ZwGFkcE/QFA+uJEFZsOK2JcP"
                                                                            +"c/fYqYLw+lDGbVnp4LfmP0scQsJ4/Ipm78N3+/C48bu4qlwySNU+ouFuYU2uVzPOtcapkfyIQU+a"
                                                                            +"sTLnlNU8tUmqMN5rzJLpDD/on5yKnQqVanvohyl/wJboMbPeLR3F2kGfKPTmC9kyy1gv9mzyUfcu"
                                                                            +"Y04Ejvq5oQ4qS1MzztMD4rrfVwpHm2q876TDfU8yqovwOC1uiUe3F+7EpK/gCP3V7gERuKaK3K50"
                                                                            +"EukpfgGKsLB9zeDWEmkUkOkrO9JF2/9RqV8A9xinTX+h/R/IL9wBsxp/e07JEJdk54PEUyMUA7yI"
                                                                            +"UqXYEYCfnYrLiYHo9gejQ714c3OiT+YzhgWmNAtp8zLiKlhgaOwX8G3IP3puJ/4PA1DRZBouNZbx"
                                                                            +"6/A1/hBeE4Tx62nLUYFI1e/Vd8HTA53P46oCS2eolB/AvE6BYgHE8O4b/YW1RTaTScQ9hGFfjrwK"
                                                                            +"ptef7aLYTJQDqBWuvS/pVO3XbIT4GNPpxCQv3cddV8XVO1vJbbSoZAjR8Kh781fHyFpJRNdaOQJg"
                                                                            +"3pO7k9M6x00bTcNbcZkpRC27FGjrZ7EUzBr9Es5WGVpoA8KcXaYywh+AKW0MdhsSXz6ysQkCivbN"
                                                                            +"G7ITk+iKRSiaQbvfnsP/iKl5/fi4alZ9yGM9jIpatruNKkf673cwFIifLl3L0LjC9T082Xs95G6Q"
                                                                            +"iTlW+gZdSExOu3bp/ycvM7VfuA2No0y+ljUVdQAAAAEABVguNTA5AAAF+DCCBfQwggPcoAMCAQIC"
                                                                            +"CQCnthdI64QDMjANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                            +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNVBAMUDSouZXhhbXBsZS5v"
                                                                            +"cmcwHhcNMTYwOTAyMTQxNTM2WhcNMjYwNzEyMTQxNTM2WjBZMQswCQYDVQQGEwJVUzEQMA4GA1UE"
                                                                            +"CBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxFjAUBgNVBAMUDSou"
                                                                            +"ZXhhbXBsZS5vcmcwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDHZRqoukGFLZeBPhr0"
                                                                            +"G2i8SVCN9d5j6/p6nRFb5CW3/+zgs5+mC5aSKV3v+P7tztDLdePK3iGe/koR6F3olRmi1KZEJS9K"
                                                                            +"E1fYuI5bd+vLIcOCfPSyjmYdXI/lD0p8Ii+lHlaZOG4wYAWwrnsLKCEm9+jR3Ba+qt5ubuW5gWlL"
                                                                            +"pJxfAljQ2MDCMYXRWAgYgmhAiA6LiFEU/2vK9pfCIgMQuCnI/qDlyxUtllcgRrdUCSTPJldogoTk"
                                                                            +"S3eDrT1sDqabA//EOWF8NCcSExAkVnw3+SumcOg3PevmIx5ul5F8re4kclmHzUOlTO0Tax+nDNmx"
                                                                            +"mbYqUe9w23/FD2edzS/6Wrv6fAqexyblkXiIc/sMhts3chdAi51PsmQ6xaokrWrFpK1MTaUnH/jD"
                                                                            +"ylpG3gxnvqjZDrpV3+feap2LHVQAHfXYkMUamyxCQ8P14BjfHbhwQeCr2g/Z4vxelQgcj14iej+B"
                                                                            +"G5yRWMhSMAbNQJ0CpRBf7Y1bO1Fbu+FLHiwjygtxTQyNT3mPoVPQHys73HYl2pI2us72AzBAgFTb"
                                                                            +"6qqzvbwhGuv/CXPI1P7kcM8x/fh5BE8ZQ0ixCIJMM4MP2Nnx6hxZFuGH8GP2sg1C8Kz3HK9DRF5N"
                                                                            +"V6dH2OYIx7I3aQYVucW/IGJ9/zW7mkZS8Tb9WsLa6N6uo/PEVGh6nXZpgwIDAQABo4G+MIG7MB0G"
                                                                            +"A1UdDgQWBBTssUOh+PFW+dL0Eh1THwBmZ4FvYDCBiwYDVR0jBIGDMIGAgBTssUOh+PFW+dL0Eh1T"
                                                                            +"HwBmZ4FvYKFdpFswWTELMAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHBy"
                                                                            +"b3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MRYwFAYDVQQDFA0qLmV4YW1wbGUub3JnggkAp7YXSOuEAzIw"
                                                                            +"DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAgEAxk80rlNZiOk/p7bDW+DyMIz5HBQU1WqJ"
                                                                            +"71eskKs8uccQ9tlFMuehd7GBw8ums5FIBWw/tCMHMhR1gcLvmQ8GX4iWHQwec8W6KXYzS/1Punje"
                                                                            +"2D6Akiv1UjwWBYjUDr1tWpAAqdy647PhK4k1I+FELba1x9JB3yQTunjTyVTrzy2lGs41ImKloe5C"
                                                                            +"fYh4rQLPP/jjeNYbgfUaXhufwv2qq6k/WnjmM3S67boC53P9vNgdz5EtizNusnhx4D83ecQ5SS8I"
                                                                            +"G8PQZmN75jUg+xKaBtxr03AblGQRDoJQZdVyDGvjyX9cgOJ0lDzP77Ca6bmOj7qB6a6X2NWiF/pr"
                                                                            +"Wc9fWF9Qehjs5xPmUxKfctTOZ2PEPvPGb7GrHK82arHCSnSu4/nL5b7mBPInp1gsb0mbo+gdwrwb"
                                                                            +"6iBXTynXil1Y/fqGFGbNwOyteaqueEbRCdINyi4hCcatQSTLv8oAU6GEzXCelkP+iTx0Y9CEp1Rn"
                                                                            +"qTOTiW++vBDTDxXp4XLmQuX2viU1fwpsb6hE2F2d3uqTBbYnVxA5T7VvMDL4B2r9wKzzXUfMsC1q"
                                                                            +"m4hquq1YOmF0lQy5kFZvHePhFMWoMxuNM/PfScotvr0YoiZD4hKw2l3bqxSukbG9fEXZ1kM+3pXx"
                                                                            +"hAZ6Tjt6B7GIub89FYqvCryWvKE2JE7v2MrjaiKY/UCQP3AkERStbEgahi7HsbGVq/q/pQ=="
                                                                           );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*.org
    private static byte[] KEYSTORE_11 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQUx1AAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                            +"AQUABIIJbuNPa7B2d1Mjkq6EBTzELi9V6XgirTYklPyKB3sQ5DUjlK+KoAsUI4oqAGOYPJDzHteq"
                                                                            +"ocv5Dj2+741/Pi5Y6DDvls+6fHrmlJk3QOuVJT4yaI5vo9FJ3FUCUyDohvYrRfpA6gfTfO+QU6DI"
                                                                            +"9G+D4+6gZtz2k4tdoFymtLQRzBS7+L3pFzl5uP7FA1UVWRZlHwsFyLmEmQhZlZcecYtCdTr018iV"
                                                                            +"Kubl77Iq+GP0BLf+HPHdDAwfZ/vWO4H5OzilTpD5gT6B4HJWDES4VvDUE+/+/9iPopejtMOwVOig"
                                                                            +"KD1wbsxYLgaTq0MQCYQMuneE/H8RnjoijrJX5/nFyr1kCJuI40qeOTz0GoRLachSS4wgnhrIx/rP"
                                                                            +"YY+VSZj0EwVSy7gWlkvk1prAAy8Mc1HsD6AbOudDTzkR1j6BjWwnnofFEVXNpFjyYgiK479TIRZz"
                                                                            +"EpT2pFguWHGz6Sk0fFQb9mpLfMdFa35Kgxdwh7R6vMQh41pVflXntdhmRbRYkjkLngmNHpKp5iwf"
                                                                            +"gD/UGwohrWXt4Lb32lCoLgNsmQwA40CGB5LFx0NV3pGT5pxZzNIhisPdbXKkc/I+p1/kMqmBx0sg"
                                                                            +"O2bWu/duNs9cE5cDjYsdjqzvw/Pbm5xMNf+7ho9oo0omQOWrgGVeLLZxZI8QCl27E0OAqfrwYUBZ"
                                                                            +"79BiGYcvcylkJVYx/jEZ+q7W8vlFppEIYxqnLoV8Bnb3rYZcjKd4/IhQwGJ9DOs0JyUc1rAp5IuG"
                                                                            +"ewuE9MBpgF9XM23m6Z46qq5nYSh2AzzSKcZ7yI7BcsEsPb7tm3spfmqdFvo8FVA0VwiDB/pAQPn7"
                                                                            +"FZeJSgB6ST1uQDU2OYAtdKVSyjujKyT0sr+XVAStILKr4TqhP9Rr0oV6LaCLFZ/R7bm6WvoWC94x"
                                                                            +"MPgBIUqT5XPTBB68vufjt6U4N9JQ6YKk6EwyZ7TezxQeu6v8UoiSRGtqqeQiFg6sfWsgUguC4KsA"
                                                                            +"Wufdw0IIEYxMlOi6OdUGYcIfE3mLkD6Wgce9ql4KCJ3NyhVLUj+iKFSKafP890xg3nEGmJq1ATVt"
                                                                            +"85Y8gtCr7+VAKx/yv1BRLDOx/qbhF1ygk+WUHx9CgTaKNZpOY7tqwT5T3QvSmtdlWtw97rW6Q1MK"
                                                                            +"jyPXlvMRAFP9GkGlEg8FUQGBU+w5d4n/Q7EMcBfQ4kKD0bJM1rO/kaus3bU4u55lTwh3eg7yx1it"
                                                                            +"TYVOzrnVoxuWQAv9fCnGOcay5e5GtTjz766cQFpgYB573NtHCQN7Muys5+FPR8G9QT2Jp5JhKQbl"
                                                                            +"nEkhXxxJqkd677dOIzJDTsdB+U3J62kWto/9NyMktGLTSfEQvEuNZCNyvHoK5iFBM7lqowiL9Lua"
                                                                            +"tBbnRfbfqzv+Hs9ozF7rgt5FWFgFPs0JMO0xzkCj3dkz+GDaKWkUm0OyKBiRGttXm7PM8W/TT/bz"
                                                                            +"sxuzMEAxGINezTIRbWA1NjeeoNEJ4qHvzOwsIOo0dUvdgSYsEXJtE+jOVlmDBv5+ZLhJXBmXxyWo"
                                                                            +"uviYwIcwZhTii/GMzE0psIO1vTCG7mwssqQtGuhHmOWQ5l4A/67/8dIbLYs7vhuDLzcK7javs56U"
                                                                            +"Jf1wC/3EGcbdKpE3Aff6BPYY18+Of0J4xkVlC/YMAw+PRDALIk4vCcW+nQrhIt5HtnnUT/ZAOojE"
                                                                            +"O4PE6QwZUSxIRJWKnprqBDapUtk+CkChvv0q0jwLcpi91NrgWKch74B3NcCLL++g/wYX9GC/kWlK"
                                                                            +"7rNgwVDt5o7CMNyvGgaIKjK+VEerQXEbdVP0WClyvaIO0VtGU+Oet0u0KtGDInkojtQKFk9zaD/E"
                                                                            +"dfPC84I+2X5+V8wYSz1p4XeN1rvuzfTzCzKzs2XZWnNzKuXtsuJpnsvyuswmDFHLBZemhb7qLg6u"
                                                                            +"5ofSpnJLAluQ4RV36ZcS2aBPMLwTQxAvVEaA5nbqkCETMgCYhc2lqTmwg9AD9q2NF3Aq+Ox7riRw"
                                                                            +"x4RfPdWYK3gBuqridkpyrzjeLqExLRCQ8l+8pu3h2/dz8yUluLKCxX9yTzzfmxTLMgqvSxCrJiBu"
                                                                            +"oneZgSgjm2nBF4+k8Ve2f+lNGWmXJFAoygQvhiUX/eYWrYeT28RQGYq7vcs2cl0ShcQvzp82egnt"
                                                                            +"SnVYoOWRN6WYVFfZNTzwd0fZyPGR0DdWD/xKnfkqId8zxyR9vEw0lWx6K6RdWBU78uYAfXOmkg5U"
                                                                            +"ZaARUbyPTBoVfiAyq1xyTkcAq6P/NTXpYwLr42ffCn/k45zY3gyShs50qQw52pnDMjtEeyuihjOM"
                                                                            +"VCtrrirlar2CO33+x1rrYU8BwzupYVk3wDll6N36bWzgOM4zelfqIdZCfsVLdCgUcP2sAEK6y90X"
                                                                            +"6i4RvM2mHYLCndSJGHLQ+Qw+jGnpxdf0MKDSmkfQ/IgpUEpNiCO7tM8a1ECYzAxoOk+dI10cdHY/"
                                                                            +"siyMxosYwFrWmEPZ6XMcKX76DSixkEdGHhvUwzejh4wM747kIqwqp77cCNrFyoapFymiMvQJwfc9"
                                                                            +"XKb15SY0N+4/rmAUW9F8SMavmaXCpZIBC0flVNwxCmzSPEcqgf1LUheQnkmZyuyHwJf8KJ0p2AZZ"
                                                                            +"2hT4ITe+wocAwv7jBEr+HeIBOwf6TWn4reqUZbQ50VZRFDfKzTf1FmDGmwhENtuxY9XxF/ML2LTA"
                                                                            +"3ERF/ZX0ABD0rtydGyNASlvMXKJtH+uOJ2+9gtGWQefAxkIS/NrzJpRUA2b8LaqUIB8isFj44tkm"
                                                                            +"COrU0aPaO+nC1TZ9TI8doBofsBfAPfgtrBuc3yHzv8Ur4gnYHSTa+ny5apna/t0N/a/c6/m/f3/a"
                                                                            +"WwrtsSuTnp2NiS4wKOxD6KpFp50SburOEEnIy9TItKCAhhBevu+bcfScdkHmBm7LXkWlcN93ufwM"
                                                                            +"oSJc9SbxCH4H5Is4upO5Hj1FbffII5wHxkQ0Zfup8kdnCgzwenGBD085Rm9QFsmLnUur7kabyBmk"
                                                                            +"3BOoordtit9gUkOzDepJY1NtjPhFSvS1mnX5iz97DtQxhE7mUki6+Wbx7ZCpRaQfFN43GVySdYd3"
                                                                            +"VlmdVH8esG+EeqUmgSyQE1dmakNicBAYdXdo80OH0aV7bCCwIv58wpwPZr9BN8Ay7qt5lFde8vGC"
                                                                            +"jpWs1IeRG3R9c099w723Nu7j7bGazi2Hh63p/S2XStNabbkA3o8Kso4/IoFMRbA3zOxpD7MYmNo7"
                                                                            +"j38Fc4cRis9ohLHJYWZAG4MfwXnFIOxrCWpTAAAAAQAFWC41MDkAAAXeMIIF2jCCA8KgAwIBAgIJ"
                                                                            +"APUExjjxQJ75MA0GCSqGSIb3DQEBBQUAMFExCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                            +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEOMAwGA1UEAxQFKi5vcmcwHhcNMTYw"
                                                                            +"OTAyMTQxNTM5WhcNMjYwNzEyMTQxNTM5WjBRMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                            +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxDjAMBgNVBAMUBSoub3JnMIICIjAN"
                                                                            +"BgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEArSjUtznlx7FTncXYLEaGNEHKyjugHc2K5UWolPot"
                                                                            +"Qs1duaP+SpvYAM5Xg1MS4WeLVRJ9AyuVkAOTgd6goZ6ep7o3EcOIXd4aY6CJPfhhRRbKuJ5rBvXW"
                                                                            +"FO8IDqXdZ2AAz5btxl43qf1Z75NDoLD7ckBUCXE2+p5yhN4Hp/y/l2aZROFEqPN0T8MDCWUhNCTC"
                                                                            +"88o0txbijG4zsngXjtwtpEU/zQfyrhS7C/+4sBJ5wuVACspziVDS72zBh6doepfYx0cOkmuqWCA8"
                                                                            +"uZ7L6jeiz3GDSx/1F8/2c6ql3i+Ywc93FSHXMHnHE0qUSN/iljWKz6vK5KKyb4kVElwxwzAphaKd"
                                                                            +"GIiRLbw7YFs/jkTRrZHR7mqnp5CH2j5lc4pKUzrw1sDmKxLidSAxE5g2u/jFpYpLknAG4vAof+XE"
                                                                            +"PFWuzOWYK97MqbAI66xedb+PwpT9myeYsUF6sBXSXSsjUpxiK1/Ap4sKqkOhUCj0r3vC208luABB"
                                                                            +"n780dk6ag87VhqfQHtSXOZh3jjp/uixOn9NAhcr9bKXWLagb32HhnjFzVy9XlOGsd3eDg44DvM6M"
                                                                            +"Pn8bH4ZpskEta7wARfjabkUvgezC+7cTgYBfhnts5xwP8WsJ7RnhDs+i6od/1TyzbyKtzkQ5XOW2"
                                                                            +"meeaHtJeCSDwZd5FI8tyLNYnRnCiV1leQ+8CAwEAAaOBtDCBsTAdBgNVHQ4EFgQUOhSi0Ts+X0xt"
                                                                            +"dM8bMRM6HE6hsDwwgYEGA1UdIwR6MHiAFDoUotE7Pl9MbXTPGzETOhxOobA8oVWkUzBRMQswCQYD"
                                                                            +"VQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNp"
                                                                            +"dHkxDjAMBgNVBAMUBSoub3JnggkA9QTGOPFAnvkwDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUF"
                                                                            +"AAOCAgEAL07o/uHw0qHxhf96bgYVj0Hzp/hzy+CKmMczLE30PKP5N0ozEkKMeF7y9d08WTXmSJ6w"
                                                                            +"vtTRFMj7fhbYlRf/V5By9XpgHLS+yeIcqf8kyouPfjuZPtyZdpJF0B2UKq204yWtNZrhRWPsmX2p"
                                                                            +"FB6Q8rO0ikguMcKy/M1VaGfYAMtX6U8/BEXIXS3vgfUGC23gmJl+Y6z9jXbcxoz7Nz9aflYQ/mp5"
                                                                            +"CIA4EJLCrsH0R1rMSuP2MtpzNwI9VMcGvcVk9Wm6xfqucxIgPP7geJvXTNflJwzX4eEznT7z67fM"
                                                                            +"BOLlEU1victmoSCBMd1f76uTBWjtiGhzqDPeC+db2wUu7m4NM+Rw1w1XCi6GPtUX6K+wPCzQ2zwe"
                                                                            +"dTdOd/T4kH5fIpasWpkphW6Sy5DHbpvK1tRl79OWeG6Mo5Qn1JDSRgT1ywP7Hype0tZAZuKdnJdx"
                                                                            +"/m9kB2qYSkzRkqqvubXkqXm64di+xfhV4ysccFtJn2TcVktWu1ChJQ9jr2v1ZyTU4zicglIysduu"
                                                                            +"D2G1pg1t6S2B5Qg/nK42uvsnr4FNQPpp3RC97ZYTAMlY3sLbun27Wmdw4fRfFHneTuMtcf+iNDkw"
                                                                            +"t2vAMRDLCQ1W9j1Sdm66kcPn2lHYTBhulcoVGxmtj7EzH1EL99KPlRcGotm8+4aBVkKpG8IceGfn"
                                                                            +"iXAAcISm48sZCBwykSHIe1GmXbDaHTsGFA=="
                                                                           );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*.*.org
    private static byte[] KEYSTORE_12 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQVfLAAAJhjCCCYIwDgYKKwYBBAEqAhEB"
                                                                            +"AQUABIIJbrIYGAiHi9UXY3+A7AqtiNSK6KUzH4bilTLdy875582mryHmiv7372P049JZ2NtA0FXk"
                                                                            +"z/Idna6VGQmQfC7V/RWYpcCiiDcNFwpwCrnplPZw/6ItTDFaZQruZjIrgT3joyWGHvDvnzrTOGwt"
                                                                            +"l/yAc3DPnOThuqRogsUHUHpCAxii7/a3fsy4bPvZowSn2s0xJKAt0wBtSEAywahpzbax2pHWFwff"
                                                                            +"e8NKn3ZXUEbH5m7VPpStX7Bdwm+ZrJle0iqz+iGU+uQFoQ2pWexZ8lir3veBmQbcXfEDeUO2tsu9"
                                                                            +"1Bsx9s6k0r08D7QT7rLINThQEgqNUAbBOeWbR4JghRK/76YwgqjqiGf2QBebzQbpYi9WHXunlOPa"
                                                                            +"zSYRrMhBGVs7RQWvn9DMNGRKHyPxfKFNGU4VM/dhHDkrDbKMd5k1tTvC+MctZSAiqWpHFBmXtgvC"
                                                                            +"U7EG/Y71SSorQcHFpvSa0vISzsRFqcK7x+jY3t3Q84e6GIJlvcx3ZEEwVDaqtXHlJpWkorJa2qFY"
                                                                            +"pQeopGLGDqwdp8vB3NrLhph4xVfn+Fh5sYSFEfIMI/ZDgkNmP8XX2MVY79Ejq/v0YDwMqIsudVdS"
                                                                            +"fRDsvEqdoqqvdedkHLoqjILBZ0oNBAXaI2r+N8AZ9VxUZnQ28LwX80s8nuoOXpG0teev0ipYFk/3"
                                                                            +"pohtlrVzTokaQx4pSXPmFMo39877jhtkB6GOYQ2VjX/gASf8zMmlDA8+Kucc3YF0Dj3Ur1S+NBYh"
                                                                            +"t5pt6LyuV1ZkMAokVK27qqgp3fT9o+f+wnMfuGOJa9oIPai80PxhnAJBsm9iLPv7kPai1JEg3MKc"
                                                                            +"fWyiB1VA6VY1FhCh5jHZp8oNId3HwIGUwQLB2J78KmDnSXIAYulz7RvGy23BCsPG9qno85+OGPGu"
                                                                            +"9NghEPlMM4NDkFwipmQMQggsba5xPEbVnEEoBB2w4MJnAi+XGXCS5jgU2O30tmN0IeUIKEw9v2AH"
                                                                            +"DJs8ln3l9wbe7Um/2CNMlOyo5Whf1i/co5gQI/dwL6q6ICh6HYHo9g4FMHVtipbh5b3T2JIkuMXI"
                                                                            +"Qm2HfzPhQcOvoMYml6uB1ogbKiQ3zuurd1eOsQXDWb1vC/wYnrRbN7gSQf6De0chiclhrAo0c9CJ"
                                                                            +"QYywg1JwVR1o0I83iL1jp8IWrzZshLHAISwIOtG1g93g2kX1HlWfGSvNokRbgbCG8fgN6pQ5WkTG"
                                                                            +"+lnrTZADWXMVW93Is3dS32NkFEjqVWw8voOsCkTbfHPJviF7dhMU4cVc8INbjap7a5+NWmJXEEaF"
                                                                            +"4S6BzyVo92sQwGHEzPtwhOfStDjCk+/xA9aHsxLCxTg+upN/razthBKZMHrhiTurNpURyBC60A1r"
                                                                            +"8pa2J9SIQ0GmyuojsEAriaN6aA2OOckjY28Vaqmitn0ROvpTl0gcQAJHTRnxQ0Z93Pj+IC6bw7Zu"
                                                                            +"TxfUzW1OZddGLB0KhIJnI0mCcBGUxR2A7YVATLXoYuzov8kIWWMlVvGQ/FlN7bEn28cOrvt1Zc4V"
                                                                            +"K+oEc5sfeqjbqqamThk8b9kuNHcaR8lqG46B1taun9mAghg6oU4+r+KyYKELwY/dR1ZYsaSND+JF"
                                                                            +"wovjePBtk8P3tqwosFp1kJ0GEdT0Owm2ud30COKe7Jl1+rHHINXY+PVlNBDKoGp2eAgoHfKG5pt8"
                                                                            +"pjLWYhZJvXa+UF9oDYxI94PBBHX+bivgtWogJ96BFewigs7c0Da2E36CG2dlk4cZ3f3mA+6OyO26"
                                                                            +"7P6W+AuNci2cYzurWIRZM9du67v8YrbKOfJ+X2f+CjoIUhYsf4nvr/ksKL2HxV/sGPGui3sbO6/6"
                                                                            +"KxdloV67Cg4yjPkZ29Sqn1qqfyQ/Fat2hA8vNNszEuWAovEmYwwW9+NuRJenxL4nibm0ZYq2G0rc"
                                                                            +"5/Ah/pWKt0RHkwGSDcopN7Clfs97SW7hRlvpkcT+wAFP2V9jAk/Ok5/W+yXVwcObA4lFd3C9kFFh"
                                                                            +"HuZiwcj/X1Sb1uET6pGotMQsJyjZC0g/UN/r+8y5dBYhFqzdiE2Wa31mrOaoVHqK65yLIZqDLcbM"
                                                                            +"GxRL8Nfhx3T1AuTi7HGAQz9IJaMylDSHHQzIbn8CwdhQ4meqAx7xUKXRAjTyQM74JB2oxFxaJCVl"
                                                                            +"FUi9/EC4oM9v8za7p/JAEzXzNeia+0OwAn+p/g43kf+qxzSWvPRtyw28N58lOGE5b1Oj/HJ4aHJD"
                                                                            +"bbdP/9/aLIKCC26E76QKes8Dq+9AXyAc+SiHVaGWLUxQQxDLQq8v/UlL5Rcc5QRxgwetKimIi+St"
                                                                            +"WGrnjalsy9D2LfIRiNnxU42OjdufYbqmfbasSSxKqTNde6/YTzU6q/RljcFE2ZAb1N2JPQxX2tXV"
                                                                            +"2+iN4miy4/AWW1GrVEzeIdh9LhTWIO6N9UKuICzM0ZIekrzL5FvCJ87QAugsAnQ0JEH/A5XMZaFh"
                                                                            +"3+ferBmdJLcClkazQo7Wbkc0ibYYc+Tryhoq8cqQJk3jTNI7vkYt1V90Gj4owyY8jIbf9Z9tUw+a"
                                                                            +"sZVyKZ8IC3Y3vUY06kjr8sdTLRyJZ1gu/04rwzQeG+NRfKcGb07bD57cARnVvXv8J5uGUxzjzWln"
                                                                            +"M2sQx7NfUfnuYAMK2QuapJzCWh33uFHv2Hi8smZYGo5S8b3vewZIHpE7Rt+vkpTps8GNMqyfxKtJ"
                                                                            +"ZJQFXwzvyKHWIw7K+36kNWHJ97J8M1eIi3sHc8CWz3/xWVpBsBUioiWpkRmOr+8WqMYSITEqpbLG"
                                                                            +"0utS4ALu3RdZJ9ffHTWDVTLlH+JSagDmoaxfLBRebtsFlpEg8p2fqFqYx1KilXDkPRsccQbE60QO"
                                                                            +"BY0O9ybASFI+J+7gynK9NBanj2eV1rqYDZWPsRbmL2a7wmrQc/1jL7GZOKK1dtBx/j4ntsUVnIUA"
                                                                            +"8JP5tB/F1xAG4qNTJHojFxur4CeT5jDs60ZCWZlDv8oxbI1h3fsHerFZHI/qDTvLWHVkxge5NkJW"
                                                                            +"TaOfYFhLVD9Fet+u0wKBNIZ7IktxevSXnxhTfnypBmPqcWVQyn12kSaOLOQtmZJFLxbejkgr0644"
                                                                            +"ZYn1W0/PdExm1NvnwDwgFjvhmm2cBlTc2QdeJQ8WRJ4tj+qbkGFSlulKa4GgQe6diUEsnnJmidbL"
                                                                            +"2V1WHjwL3soluTL//kI3iLeUl6lkfBQkJEl9oUFGUhs0lMmzfofws5IdV6kzJ12HucDDuPg2Y4++"
                                                                            +"rRGjpfQQ54Y5OaeKgaACtYQCBqoRNvqwk6Y3AAAAAQAFWC41MDkAAAXkMIIF4DCCA8igAwIBAgIJ"
                                                                            +"APmZRoJlIZU7MA0GCSqGSIb3DQEBBQUAMFMxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2YXRl"
                                                                            +"MREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEQMA4GA1UEAxQHKi4qLm9yZzAeFw0x"
                                                                            +"NjA5MDIxNDE1NDJaFw0yNjA3MTIxNDE1NDJaMFMxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdwcml2"
                                                                            +"YXRlMREwDwYDVQQHEwhwcm92aW5jZTENMAsGA1UEChMEY2l0eTEQMA4GA1UEAxQHKi4qLm9yZzCC"
                                                                            +"AiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAM0/ynXr01bKm9ZLmbBJiwQcvghMTunxibbp"
                                                                            +"tlNRPrrKhjWDEf+xcTRVNKzTB5rUl/5VSvGTqGwyO271nvvTNcauqkFLQouslyACctFq13xkAbFq"
                                                                            +"Q14RJcQitUGXL0wrg2sy3INeWj+9l2zJMwwzKWU+NMa8VxXjNl0fcwajRmHqlmOdc+W8MIgjnDSb"
                                                                            +"qhqKsJWkfvcuY7oJ9widxhGDH3h1A0sxHS27EK5NnK/DKN3LZz8pBs234UgpBkkGjPDJc+dIQQ7s"
                                                                            +"vevwWoXD98rV1anKjmxzoze0aOA18JKlMlfLirGIk8OLxETzugR5D3tUR3OayigyEsyeUJqAnE7e"
                                                                            +"cy0zbxL9qFzl+NRB30azDvgbxswBa+/kOS3dBNFseVDgx2UAw8XTXXujhMe6NmmRKbSsJ28oIYW/"
                                                                            +"uVkjJ/7SngcvknAqMFmMdzy5cqnsJC0+HdSdnIdyp7LjwTqZCbe6/dgc8jNRNxZgMIQDI9yFmdah"
                                                                            +"hkTY3kHlvO4+tHJySBNLek5LtaJhSBRUTvuy06Bhw6wD+onjjGoRfM8JHc5c9EswJpxGycH8C35K"
                                                                            +"eVnHPPkbH1A5k8KJtbtDSQQel/8K3XRqrbAvM2CoPH3fFPF9w1zWCWAfRqj7qVTLFf2ryMUlO2rP"
                                                                            +"HTSn/TICkUBXP/3+cqkcYsXsqo2AWFwdCKoizVvlAgMBAAGjgbYwgbMwHQYDVR0OBBYEFG4+rl13"
                                                                            +"bkKcj4gbN5/KSyq96M8QMIGDBgNVHSMEfDB6gBRuPq5dd25CnI+IGzefyksqvejPEKFXpFUwUzEL"
                                                                            +"MAkGA1UEBhMCVVMxEDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQK"
                                                                            +"EwRjaXR5MRAwDgYDVQQDFAcqLioub3JnggkA+ZlGgmUhlTswDAYDVR0TBAUwAwEB/zANBgkqhkiG"
                                                                            +"9w0BAQUFAAOCAgEAimIaMLXB4TzNXFpJYtVznK75hm4ZK8g04klGzJ+i1ZU8wEE2bBixxOJjKOLB"
                                                                            +"42p8dhMr+c3GZ2bio96jIykjxTXUxQgtO3oAv4kzqxHXqpgiP09tfM4IkHyYvJlUXKAp+0+K98Iz"
                                                                            +"jEaw6+wfba4cIpgFu1AkkuM/3IjF4jpgBRHPWCgSK8vOT2h3MXLGQ4p7i4bPAQGqBTp8es4L1YUi"
                                                                            +"Tj+lr/najV0qXL6WzXg1urfCIY85VlVWdskZpAw2+JsJQ++8erbJ6o1Jyk/NyhnI14p1VcDvlMgb"
                                                                            +"8+W2RLST8o9fZ8JIP30KcP3yDYv5sZT08VcbwxhpM4KecpxCnSPVn2Vi1CJcBN+R8av4gCdstMF+"
                                                                            +"/DM+U9PSP7iUEGMHC4uBkJgdVGzgkBXH3ZJS4sOgOX1qQ3m6y66G6H62GiTBoNZEhMBZf6v1osur"
                                                                            +"JSYTJ1Mv1FR7lYFaSU733ZwppyTVSFAnN42ZU7WdrnL8EjI7m2i5FXGYoNbf15dny/CbNNNKOp6q"
                                                                            +"Uw8ZF7UuX4v8tVVP1IPhdJW2EmxQECWF1b5rveIIYQn4FlGjkJ4T1XsalIXqusw3Wj7iNm3dUK45"
                                                                            +"qxRu+xG8pK4eK7Iqh4b4542sg0TyA7I+nqf63UpDHl9GlBGz2dpgjPl8LuwrJ7s3zxCdW9akDwmr"
                                                                            +"VLSeP166vpFxSavc0+Zg3ccrXIOI00evbO6pBtQtOg=="
                                                                           );

    //        Subject: C=US, ST=private, L=province, O=city, CN=*
    private static byte[] KEYSTORE_13 = Base64.getDecoder().decode("/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAVbrQWJoAAAJhzCCCYMwDgYKKwYBBAEqAhEB"
                                                                            +"AQUABIIJb2KqIXFhF34E/cZLKwQIiZncYFYOrfP3HUNTvAXWuX1F2prq4VIiq8zrsLpFB07UrfdL"
                                                                            +"7+xfuC9WJJ09455608neFGtH7K6V5ntULLicLOEkz/jH0WgHBsZjUFXhAl+DivFI880OT7z2FC2E"
                                                                            +"odYUp35Un/cSbfyxjYBQtu64X+u5g+05UW5GhxHCUrHik/5sJYimER/ZrM3SV1ctnllqtJeZx6sn"
                                                                            +"2+lA98c7d9f0t9VusFizazO8IOHGOCnsfNWJFELnTEZGvBM619AfLwMamcq38ccYafnkuXoq+XJx"
                                                                            +"b8h9P0wzAooFCLIslKlPTAz2rAqz9B56GJ8qAYeRy9R1mWCqZ57Lm3++zJEm42yojW8w18W7sEYP"
                                                                            +"def7/YCnuZMGFRoq7ZbVp9Cyx9m1OMPZDmh+bmK8iIFQQdDQI1b6EiIKdcGGqiBr0ffjqtYkh5yG"
                                                                            +"D4r8PUyM5fZD/WFbnmikgznze5YTkzrVurlP3cd0musGLvqD/5ke1PmYF1fMJN7jWT06P/Hpb/v3"
                                                                            +"dcOo/Dg+YjPe3XkUwBlTs9ZlAaPI+uLPeXiEGx08Y5PHKkNZ62rr2y6zTi82BUpdL6429/edmBV5"
                                                                            +"H2ZFSkA80vd+V5UK3Ey58YLx3aPaS5i2MnkZ7bZWfxaKPAUBy5DwhEEgqyGhF4fqgyWFtF1pC4T6"
                                                                            +"1mcoUglY3y1BCSP/ygHETFb9oMtWhp0+lurd1lazKc1nP/ea0dgVSGUqDFLVmrGt6oK8Je3tuG74"
                                                                            +"CaX2fPww1ixq20zSn9uk51FIZ8NYHek1ZJZpZ4s1I/7NXbt39b2PznF3Wci4eeYo6ceVHroFYtLd"
                                                                            +"ZbgD9XEuXsR1/PMEoQKAcj9mj+/dVLUljYlGaRAhogsj/UEyNNv4a5tZfnV6V8wsQbRxtyOQfMHD"
                                                                            +"PgRco61DasSGiYKybtzsEgvNI3u0Zh70Nom2bClEJKKG9fn0+ibZsBffl2yszjSUm94IMrfWxi3d"
                                                                            +"1butL3A7Fjy6CfNP/dzijpRIWS4P7JHhZrwZXzljkDvnA0bvJUJXSO/wdb90BXAQz7VTPSHzC9x2"
                                                                            +"00Zx0t6xjAuub0ovlW1voOg2gnpvKTh76QefbtZeZs6XmoCsidn1dFPZljkAljfrXCQXoUjIUPCo"
                                                                            +"XF1w5geVvHoOHOKEQjO1U9f1GrpB5/jBYaKpNNbwjs6eXIuTzZ5ZErXBM6MDrPGCwNUbd0W5e3S5"
                                                                            +"DZ3xhO7OTZWSPhU54q8qloywa+oV5PLqjMpE3ko0QXCt2BYoAeSZx3ti6rEvUGLWRjEQ3m5hdhyN"
                                                                            +"DcEOBagQ6tjY+grLyygrsOM9Ovwn/iRV85tNcGk51owfESMM+cyhNbKJm/o4p3yFm2SSDlt/AvVk"
                                                                            +"9HuyoaRYnBrzlI69hwsuNxq1BytbAbMdGIHiBc5OsVrR/qR0rhhU9Rg8jk/rv/BRiseD7O5Wq2y4"
                                                                            +"1DrVDqw5OFjN91M/9U1WqFILQkLbSRgtL2DVPXXg2auyICGDNdM+Z6ewU6C243EFRiaDjWDVWL1d"
                                                                            +"UjDuKC+tsp2ctombe7X2qsuB0GvwoTXwpPkGzqg4hZmxSLG0ZIHDr3al9O/FNxZ8E2YDg21Rs8YB"
                                                                            +"0Us+GghqhLv2QsOnqBPsWArI27z1yZXXlhCi7ar10fvpO2YqpJFo7whfFQGbB7JspbiEBajTKPFA"
                                                                            +"IvtR4x5pTPjtvdhuenhbSeG9zvRkspUTtAVe2dGjjrMhZQMNEqGrJMyIVjo44xtT4g46x6FQ40SO"
                                                                            +"JRBzIcS99t75BJNThaO8loZddthqgQoCQ5gin8Gh6DMzTTDuKj7MBqBh9flaPkkNQm1mNdkIpXHC"
                                                                            +"xqI3saCc9WoOK7lYLQbb7lus2Q1ASmWpa6onFw6emy159f9hhfsrWVNZ7c+q4/XNFgmRGioyYCPt"
                                                                            +"p3WSqtw5VKAMaWZWZPo6tsCBQGZR2L0lXWeUZ54/vtOiLp4j7ApOeQnLRZhv6apG/SCsEqcRr1xn"
                                                                            +"cQoGwWrrMkAL/WFhbLT3zIz9/U4ttWF5IsB4DKkcCpZyEdmKkR78rJeVIi97UetqRRijySYT8kII"
                                                                            +"Jq27qevLrin79OqExnQqGNXkRyRBTFjMkTyzzJtomZy8/1p87EEFfrotQrmxtMqkajpIPu41/r19"
                                                                            +"Gj06hiF9kQiz/IHYigXZha+EbO5PHvL8+sdoHfnPivzBY5CdM0kYsmwM7Z4eTpOynZq2QcmY6Y0r"
                                                                            +"V0ngR37ATWfEX6WEtRCR7IIn7rSNsGPo378i1S0bTMcT3YlBi7tvRIrL4/oqEm5ycoICApwufmGF"
                                                                            +"qb1SxECthbYfMCH+nuyZrale3zRV9+abov0fEBvreEfS4ne87hlQNtQ8MOVx5yhRX4GN6ak17BNu"
                                                                            +"xAP5v5vGlHdc7mPa6lVKVve/xyuLXfyrCO5N7HTVIJpDwnv6qoWXCTcuULyeh/pvEVsEH5wdvLQw"
                                                                            +"qcYwVG4zMy1Lrxhd3V46ouyit+S2EKx0FTK89OZ5GY2Ay/SWBSKGEF06HAicQwzDj09QBdnLl19E"
                                                                            +"9JsUhIDnWKwIJ3wAFdN0ZckvSWyoH50+Zkj+8rcHVG8lGiu8WjxPgh3pF75grDypoqr8DpokIPxO"
                                                                            +"qVqzMo9vt5qklg+0SVoVPBzWQunNCOt+sbHW7tSXPrpUTYupV1Zjt/J6ygkpPE+V1WAohsdxuhBu"
                                                                            +"OpW9ANEydtNEHss2KCOoDljxEWQwjlQXxXu6FHYrIBbsgCeHO/1gHMcQ+Ma8xr51CFKXt1NS0HF9"
                                                                            +"I4CYhcyoT6kA31z8S1Uz7P6SGL+n+mwSqm1H9y1xKv7lAxGNLvuKnG5g00yhv7acTLkeBKqVnZUs"
                                                                            +"lPEFGCOPXLl1vcBBRsRMruzQdV+ti53PtLpFiPQkBdJSGWTZOuCWX8XndrHPUuWJR2nR7xqSlaBx"
                                                                            +"Mk7ejmMmYyAJnkh1ErqQs25De2Mth+eVPFfdgESx60TBrjocCFBURdozwkTu9ZxwdO67QlLNxmPW"
                                                                            +"hVyHo/K8sMXwk4zJcGIJXinEWPWlFCufaZ64yJH9FYe2IKF0EGsxign3nM7gTrHaf3MsO6CtZIXc"
                                                                            +"D/3Nb4mI85e1Kg5A+ulHknMpThpg1bH58TdsPyBiVhHACApJSLC4zeanNX886XhUZJL9gC6t28pJ"
                                                                            +"/5lUfF66r8UqRDIQ9uftqCqjsG0j3O6K7NwL6b5gQJEt8hZ/FHi7QUjaaubv6/qxUAXSGhqCt4yW"
                                                                            +"uSnVm6lfVC/JoaYheok8GZKeTI8gWISN6D/+HwAAAAEABVguNTA5AAAF0TCCBc0wggO1oAMCAQIC"
                                                                            +"CQCtABPAnxRJbjANBgkqhkiG9w0BAQUFADBNMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0"
                                                                            +"ZTERMA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNVBAMUASowHhcNMTYwOTAy"
                                                                            +"MTQxNTQ1WhcNMjYwNzEyMTQxNTQ1WjBNMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHcHJpdmF0ZTER"
                                                                            +"MA8GA1UEBxMIcHJvdmluY2UxDTALBgNVBAoTBGNpdHkxCjAIBgNVBAMUASowggIiMA0GCSqGSIb3"
                                                                            +"DQEBAQUAA4ICDwAwggIKAoICAQCyX4lQlx7cg4Psk2Vaf8eBZxrwwgCBAc6t1p84glBeqWjMlpR1"
                                                                            +"nLTOe12m+t7/gNgOvHsuitUTYUF9/BElN63ZhviqvpzWjEZ8p5GxBTdul0qucVdrEauCh/3BKLH1"
                                                                            +"O+kbDVF3Tw8flz4xv4ra+pLh1AijBkl5Q6oslN7h/r9nuuyTOPhbv0WPKRywkzuSK0owwJBz0ryL"
                                                                            +"TdlhM/cah6k82UPKG/e1DAkLCucQhfPe760H19K0uYs5NR7HeT7M9L7aLSDM4t0dgPCVWqCq8o2q"
                                                                            +"Ap3cXOiLMF/eltwXs6VE1CCKYgH1w3jX72Eh3yh6/N1giLFOfhrutmLkG2TCV/XQ3ZpQDhUXX5Wv"
                                                                            +"KHj05IjtvuccLeuNdhNSuz+ILqAwNfdasF4/1CCMKAOIRZ5j7QpDhH+Rz8rcbxnT97LGflsfmRcd"
                                                                            +"72gQMNh7QWg61FtVnwYDhrTmI4rw4aO1ZLfAKd5k6CBqpCa8FnPqBDrvfduimRSmjMxO73tHyKQF"
                                                                            +"yoWDCGWRjpq/8Itb9nqg+R+IF0vl2KCoylf9/TbLy+5yZ0sWqq6r1vCWv0mzeKbPp0b6Q0Atv6yx"
                                                                            +"vGVDeSdCHGEhnV7Msfcr2BonEu/L83WI8MicwibiDYXxUSQ1V4rYEqQ9gIyR5a2910ieMqY92rme"
                                                                            +"iiFF4Xau3EvlzQAcLqyKHZ0FOwIDAQABo4GvMIGsMB0GA1UdDgQWBBQ1Fxb6/PUc5nqdDBn/NYDS"
                                                                            +"FyDV8jB9BgNVHSMEdjB0gBQ1Fxb6/PUc5nqdDBn/NYDSFyDV8qFRpE8wTTELMAkGA1UEBhMCVVMx"
                                                                            +"EDAOBgNVBAgTB3ByaXZhdGUxETAPBgNVBAcTCHByb3ZpbmNlMQ0wCwYDVQQKEwRjaXR5MQowCAYD"
                                                                            +"VQQDFAEqggkArQATwJ8USW4wDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAgEAZZAiMQqW"
                                                                            +"00X6KO7uPaHblwo+n4LRKNR+1PIZMMa4fUrx+fA/JEW/UosOlFv83xeJEpQhExFU3oLR/CgxnMlj"
                                                                            +"exH+szFRSuWaT6U7RKxpQ8l8OEmrMtcTbmHhwBTlqPIWglMmxV68pGrZCX+qfCl+nXxAPqep1kqA"
                                                                            +"j2OAYeYChS8b0zXPwRbbH2ULCGSlkMS/6mBq5/05ZrA4MmkreTjGUK+BUCxFRmdmdDTewJUzw8nV"
                                                                            +"B+fMu5Wyb9tbHXSvkjlASBhK1o1fN2xceA1+BTx6TrSvPfwmDkkHHKpmC7MJkT0owiDSeFMm8Vtd"
                                                                            +"fA5xqfb+O5Csevm52szqOLXv/5I5dJ795GtHTJR+4Bv0Ntn2+8y9GhCPx4x1LDaBLcPZEMVvld1c"
                                                                            +"f2XVvDSOHHUReL/2r+adajNf01Vvd1ZtlZzvpUkQeDFxPY6XPQMUlUjV9Sp8rrKP6ysW8BZiZMct"
                                                                            +"YHH0gkJ3z9y7zXYFVb6KuNG20XdT9vEUqxkzhkoCCzRpOzBhOxZItki+kT/UkSxP+b3VMgx4PF1J"
                                                                            +"qUfj5ZDfzEL1HQDWdi8gzm7ClHTeCb0Gq7oAmdb2InHeFYz2NiAR0UQiBZZHqoENjwUdzcCeKaZG"
                                                                            +"8v9/OKI8hCvU/63IsaQT6QxEK6IpF5YqljRoIFOpDzzS6FLKBlLbbf37g6GqNiVVOruqilMacycQ"
                                                                            +"RWdkr19vdcSwp79HmA=="
                                                                           );


}
