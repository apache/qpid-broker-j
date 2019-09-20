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
package org.apache.qpid.server.transport;

import static org.apache.qpid.test.utils.JvmVendor.IBM;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

public class TCPandSSLTransportTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TCPandSSLTransportTest.class);

    /** self signed cert keystore valid until Oct 2024 */
    private static final String KEYSTORE_STRING =
            "/u3+7QAAAAIAAAABAAAAAQAKc2VsZnNpZ25lZAAAAUkYmo+uAAAFATCCBP0wDgYKKwYBBAEqAhEB"
            + "AQUABIIE6bR+b7FHo2BRT/WG+zDIfO8zOXoGIbuNL2znNMnvEp9xwfMQOkhKxEbVtX8uJ7HSwi1V"
            + "bV2it0CA59sgvRt9awmgg+W1CLgkGKNOB+kQZbjL8R8lXmKibw4yU/EFm5rqDqPEXBRBj40TF0aT"
            + "GtCCmmLPsH2pGU1wH2Ne/tozk8q7hYK6XMH/i43ZXhS9V2CKzPWrzhXmvjFKCtmYHNLj5nLLE/n0"
            + "snqAssBoFSAJKmqkqHQBJNQjm4oqJFSISB8pwDX++0kvOMM7j5ryjVwihsCYuHZ6lh5BntDGF41L"
            + "f4XADfv3Fma6nZQKfKs0VU2kAWUmjPpyV1FFq/ua4x6SUdZKS22YIQ3t6iO76TDABbQNyUX+Ge4n"
            + "k6clF8MFswKTT0ug7zjb17d36gwl+UznvFqMSE6Zkrr9nNAcSVlQS+JaazXveiVEXTBYCAZgsNw3"
            + "3KqlLWliAegnwQCQLOguw7bgusnZ/E61/TL8GTryiwN1mltbnsWkCjMj1AGUBM3sYNwbj87Vdhij"
            + "iHJbjcB7q3Dak68khrCTLmqoD43KHBB5g+UMlruXYbE0elWqYpXGjI5cvt4gzfh1V+ira5DOfa4B"
            + "Qskv/dh1uj2xAe1YEvF3xmdO2F6Yuzd88VO0aaPGroYPfRmh2M6rEOlwc2Ku/p23FjSWrLyzori8"
            + "8/OKV4PM2b/NtY51ztTKWR/eUdX6qTPUJMK5CJiOxKGxk9PDtmsbQY685H6QVDKzTkbaPlP97+Oa"
            + "xv3/2RIWR7KJzsxbqiYhX0fevRJw/RY6ZY3NEE5RAmCjzxD+1qDtu0QM/LspgPxyv5oSInAtT23U"
            + "BrcNIiQ8jO+6E+fDcVhFSrs6gLGe1BwKYHsosjvup8FETLZgqKY6g1mwECA/Un2agzhI4tGC0O8v"
            + "lU4VEZKrXwgy/XQ5C2vwwgLvJh94OfE20Wuf7Jjq8IUPcdF201XeYREE/vSNcBnJf22yPouJMIPk"
            + "yNxlAHcapeFUi00yC19FEIpdoW/8pX2k64jx63CwwVckWWOIWlg8N+z9jsiwdjvm5wL2aFU3+wtu"
            + "8Nj6Soy7Y3QYAwx17q/nUOJOk5DqLedG+/DKXVs5jghmbQ9wyzqGjGs+xYvSCXtQJygETUU/ddoM"
            + "/iK4hhnZL2uqZ0wamef4ibdBbhpoRO8C7mSbi7TbDtcfysZrMb6i5MugR+NwKKzN2DznXItvpgzc"
            + "Xm9j7LP8HZcQANa+1o2aIGDqK1fMSAOmBbTWlYkHPDbpoE/lx32iBNL/Aj8aKbtkwy/J2JRvo9m2"
            + "uBdLK4DoDeTjqG//AwISrwm9y6xxIIPNQq7GiftN6p9KCI87U5pxqs5yUQ1g/e9DCioLe8O3Vug7"
            + "+1jS1ZHWFtb4BBEF3EhkKa1AOVKNu9+M8lcG9tKWUBjnIFTD68a++6B36ShRnIZNbmbRkLC6wWdB"
            + "SdyI6FWPxsPvGSF+3wq+n+0bu75N3Xsta5tEOjc67DfnQlyZtP/BIZsKxgEueOcXkjzaXMPYcrlJ"
            + "2BInovQSHnSHvQfaBKqj/nKcGaDyydfdxF5fyjRPFYF+fFCWXrFkbQgAst8ymJ//UpLomfw+Ni6f"
            + "xx2XQGt3941zhRuXJI2tdvUb2Czzsp0tq+h46d0WOlYQ57Q70weUQRrtARqCKoSp/gNUzQsvd+FO"
            + "sUUxKRoJltRYBwAAAAEABVguNTA5AAADdTCCA3EwggJZoAMCAQICBBAXeI4wDQYJKoZIhvcNAQEL"
            + "BQAwaDELMAkGA1UEBhMCVUsxETAPBgNVBAgTCFNjb3RsYW5kMRAwDgYDVQQHEwdHbGFzZ293MQ8w"
            + "DQYDVQQKEwZBcGFjaGUxDTALBgNVBAsTBFFwaWQxFDASBgNVBAMTC0FwYWNoZSBRcGlkMCAXDTE0"
            + "MTAxNjEwNTY1NVoYDzIxMTMwNTEwMTA1NjU1WjBoMQswCQYDVQQGEwJVSzERMA8GA1UECBMIU2Nv"
            + "dGxhbmQxEDAOBgNVBAcTB0dsYXNnb3cxDzANBgNVBAoTBkFwYWNoZTENMAsGA1UECxMEUXBpZDEU"
            + "MBIGA1UEAxMLQXBhY2hlIFFwaWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC520Yd"
            + "1GuXh67h7HawvL5/pwTr46P45R0gx+LDGC1Equ9/wvvsVbCPL0JLDTSKl0qpgbJNMH/A740vSilb"
            + "FDdqfyOuIkQZN1Ub9CkOaI5uR9RjaC2MfyNUJl7Gp64nSYk9iDX15ddZjsAijUDvET32XzfirlML"
            + "dwLXv1Y5dLskV0r6xK4NdLtXi+Ndn+Uy4EllD7VMIFaLt6oG9Vo6mNl0jze7Yz/aYYtWns4x+uG8"
            + "WbMgtcXo/VxCyp+4ji06XFerwfkS0zBS1wfvxd5Qb1+4dYovSn1v0AaPvZ0XwG4XErP2/svU01nc"
            + "C43Z4neHdsj8Y/kmXLDD8Nc7Mpv/Wm6hAgMBAAGjITAfMB0GA1UdDgQWBBQfKBRPr/QD7PjpM3s4"
            + "rD8u6ZxiijANBgkqhkiG9w0BAQsFAAOCAQEAFjyjJ8pbHf6MioZpOOlZh4lz6F+9dW1KyJR0OIc4"
            + "FXnYnU/CNzjkwPminuZJoYgXBh+sVFN238YFS3I8ONEQJy8uSH33T81sklXhqnrSk9OlWk1v60wH"
            + "NwwNFz5ZuGrGlvk9EFhbC8FgdkXJbz21drAl18i2oJHPdQQNwdc6mwqhpNfjqZ2opfJPbVscX1P/"
            + "dbJjfcoZ01fy5687zjpN11G4egwsrya2FZiAw1WPI10OhrJgiGL5aDiDLjauNZmoM7QchUUD1cjE"
            + "EwvRkU1MesliLg4y3UqDoV6ooHB4ClE2aKmIdbVB/eP1QrEEkey93ptt1z5fLk1l408AkXQtzyw7"
            + "9WC+xnZta0IoYC/vO29IVsok";
    @Test
    public void testNoSSLv3SupportOnSSLOnlyPort() throws Exception
    {
        assumeThat("The IBM JDK has different TLS defaults", getJvmVendor(), is(not(equalTo(IBM))));
        try
        {
            checkHandshakeWithTlsProtocol("SSLv3", Transport.SSL);
            fail("Should not be able to connect using SSLv3");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testNoSSLv3SupportOnSharedPort() throws Exception
    {
        assumeThat("The IBM JDK has different TLS defaults", getJvmVendor(), is(not(equalTo(IBM))));
        try
        {
            checkHandshakeWithTlsProtocol("SSLv3", Transport.TCP, Transport.SSL);
            fail("Should not be able to connect using SSLv3");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testNoTLSv1SupportOnSSLOnlyPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1", Transport.SSL);
            fail("Should not be able to connect using TLSv1");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testNoTLSv1SupportOnSharedPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1", Transport.TCP, Transport.SSL);
            fail("Should not be able to connect using TLSv1");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testNoTLSv1_1SupportOnSSLOnlyPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.1", Transport.SSL);
            fail("Should not be able to connect using TLSv1.1");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testNoTLSv1_1SupportOnSharedPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.1", Transport.TCP, Transport.SSL);
            fail("Should not be able to connect using TLSv1.1");
        }
        catch(SSLHandshakeException e)
        {
            // pass
        }
    }

    @Test
    public void testTLSv1_2SupportOnSSLOnlyPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.2", Transport.SSL);
        }
        catch(SSLHandshakeException e)
        {
            LOGGER.error("Should be able to connect using TLSv1.2", e);
            fail("Should be able to connect using TLSv1.2");
        }
    }

    @Test
    public void testTLSv1_2SupportOnSharedPort() throws Exception
    {
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.2", Transport.TCP, Transport.SSL);
        }
        catch(SSLHandshakeException e)
        {
            LOGGER.error("Should be able to connect using TLSv1.2", e);
            fail("Should be able to connect using TLSv1.2");
        }
    }

    @Test
    public void testTLSv1_3SupportOnSSLOnlyPort() throws Exception
    {
        assumeThat("Java 11 or above is required", isJava11OrAbove(), is(true));
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.3", Transport.SSL);
        }
        catch(SSLHandshakeException e)
        {
            LOGGER.error("Should be able to connect using TLSv1.3", e);
            fail("Should be able to connect using TLSv1.3");
        }
    }

    @Test
    public void testTLSv1_3SupportOnSharedPort() throws Exception
    {
        assumeThat("Java 11 or above is required", isJava11OrAbove(), is(true));
        try
        {
            checkHandshakeWithTlsProtocol("TLSv1.3", Transport.TCP, Transport.SSL);
        }
        catch(SSLHandshakeException e)
        {
            LOGGER.error("Should be able to connect using TLSv1.3", e);
            fail("Should be able to connect using TLSv1.3");
        }
    }


    private void checkHandshakeWithTlsProtocol(String clientProtocol, final Transport... transports) throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new ByteArrayInputStream(Base64.getDecoder().decode(KEYSTORE_STRING)), "password".toCharArray());

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "password".toCharArray());

        sslContext.init(kmf.getKeyManagers(), null, null);

        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getPort()).thenReturn(0);
        when(port.getName()).thenReturn("testAmqp");
        when(port.getNetworkBufferSize()).thenReturn(64*1024);
        when(port.canAcceptNewConnection(any(SocketAddress.class))).thenReturn(true);
        when(port.getThreadPoolSize()).thenReturn(2);
        when(port.getNumberOfSelectors()).thenReturn(1);
        when(port.getSSLContext()).thenReturn(sslContext);
        when(port.getContextValue(Long.class, AmqpPort.PORT_AMQP_THREAD_POOL_KEEP_ALIVE_TIMEOUT)).thenReturn(1L);
        when(port.getContextValue(Integer.class, AmqpPort.PORT_AMQP_ACCEPT_BACKLOG))
                .thenReturn(AmqpPort.DEFAULT_PORT_AMQP_ACCEPT_BACKLOG);
        when(port.getProtocolHandshakeTimeout()).thenReturn(AmqpPort.DEFAULT_PROTOCOL_HANDSHAKE_TIMEOUT);
        ObjectMapper mapper = new ObjectMapper();
        JavaType type = mapper.getTypeFactory().constructCollectionType(List.class, String.class);
        List<String> whiteList = mapper.readValue(Broker.DEFAULT_SECURITY_TLS_PROTOCOL_WHITE_LIST, type);
        List<String> blackList = mapper.readValue(Broker.DEFAULT_SECURITY_TLS_PROTOCOL_BLACK_LIST, type);
        when(port.getTlsProtocolBlackList()).thenReturn(blackList);
        when(port.getTlsProtocolWhiteList()).thenReturn(whiteList);
        final Broker broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(port.getParent()).thenReturn(broker);

        TCPandSSLTransport transport = new TCPandSSLTransport(new HashSet<>(Arrays.asList(transports)),
                                                              port,
                                                              new HashSet<>(Arrays.asList(Protocol.AMQP_0_8,
                                                                                          Protocol.AMQP_0_9,
                                                                                          Protocol.AMQP_0_9_1,
                                                                                          Protocol.AMQP_0_10,
                                                                                          Protocol.AMQP_1_0)),
                                                              Protocol.AMQP_0_9_1);

        transport.start();
        SSLContext clientContext = SSLContext.getInstance("TLS");
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        clientContext.init(null, tmf.getTrustManagers(), null);

        try (SSLSocket sslSocket = (SSLSocket) clientContext.getSocketFactory()
                .createSocket(InetAddress.getLoopbackAddress(), transport.getAcceptingPort()))
        {
            sslSocket.setEnabledProtocols(new String[]{clientProtocol});
            sslSocket.startHandshake();
        }
        finally
        {
            transport.close();
        }
    }

    private boolean isJava11OrAbove()
    {
        try
        {
            // introduced in java 11
            Class.forName("java.net.http.HttpClient");
            return true;
        }
        catch (ClassNotFoundException e)
        {
            return false;
        }
    }
}
