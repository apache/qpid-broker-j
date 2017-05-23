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
package org.apache.qpid.systest.rest;

import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE_PASSWORD;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.test.utils.TCPTunneler;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class BrokerRestHttpAndHttpsTest extends QpidRestTestCase
{
    @Override
    public void setUp() throws Exception
    {
        setSystemProperty("javax.net.debug", "ssl");
        super.setUp();
        setSystemProperty("javax.net.ssl.trustStore", TRUSTSTORE);
        setSystemProperty("javax.net.ssl.trustStorePassword", TRUSTSTORE_PASSWORD);
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        Map<String, Object> newAttributes = new HashMap<>();
        newAttributes.put(Port.PROTOCOLS, Collections.singleton(Protocol.HTTP));
        newAttributes.put(Port.TRANSPORTS, Arrays.asList(Transport.SSL, Transport.TCP));
        newAttributes.put(Port.KEY_STORE, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE);
        getDefaultBrokerConfiguration().setObjectAttributes(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, newAttributes);
        getDefaultBrokerConfiguration().setObjectAttribute(AuthenticationProvider.class, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER,
                                                           "secureOnlyMechanisms",
                                                           "[\"PLAIN\"]");

    }

    public void testGetWithHttps() throws Exception
    {
        Collection<String> results = getMechanisms(true);
        assertTrue("mechanisms did not contain PLAIN: " + results, results.contains("PLAIN"));
    }


    public void testGetWithHttp() throws Exception
    {
        Collection<String> results = getMechanisms(false);
        assertFalse("mechanisms incorrectly contains PLAIN: " + results, results.contains("PLAIN"));
    }

    public void testSlowConnectHttp() throws Exception
    {
        doTestSlowConnect(false);
    }

    public void testSlowConnectHttps() throws Exception
    {
        doTestSlowConnect(true);
    }

    private void doTestSlowConnect(final boolean useSsl) throws Exception
    {
        try(TCPTunneler tunneler = new TCPTunneler(0, "localhost", getDefaultBroker().getHttpPort(), 1, 2))
        {
            // Hopes to exercise the code path where too few bytes arrive with Jetty for it to make an PLAIN/TLS
            // determination and needs to await more bytes.
            tunneler.addClientListener(new PreambleDelayingListener());
            tunneler.start();

            _restTestHelper = configureRestHelper(useSsl, tunneler.getLocalPort());
            Map<String, Object> metadata = _restTestHelper.getJsonAsMap("/api/latest/broker/getConnectionMetaData");
            String transport = String.valueOf(metadata.get("transport"));
            assertEquals("Unexpected protocol", useSsl ? "SSL" : "TCP", transport);
        }
    }

    private Collection<String> getMechanisms(final boolean useSsl) throws IOException
    {
        int port = getDefaultBroker().getHttpPort();
        _restTestHelper = configureRestHelper(useSsl, port);

        Map<String, Object> mechanisms = _restTestHelper.getJsonAsMap("/service/sasl");
        return (Collection<String>) mechanisms.get("mechanisms");
    }

    private RestTestHelper configureRestHelper(final boolean useSsl, final int port)
    {
        RestTestHelper restTestHelper = new RestTestHelper(port);
        restTestHelper.setUseSsl(useSsl);
        if (useSsl)
        {
            restTestHelper.setTruststore(TRUSTSTORE, TRUSTSTORE_PASSWORD);
        }
        return restTestHelper;
    }

    private static class PreambleDelayingListener implements TCPTunneler.TunnelListener
    {
        private int _totalBytes;

        @Override
        public void clientConnected(final InetSocketAddress clientAddress)
        {

        }

        @Override
        public void clientDisconnected(final InetSocketAddress clientAddress)
        {

        }

        @Override
        public void notifyClientToServerBytesDelivered(final InetAddress inetAddress,
                                                       final int numberOfBytesForwarded)
        {
            _totalBytes += numberOfBytesForwarded;
            if (_totalBytes < 10)
            {
                try
                {
                    Thread.sleep(10);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void notifyServerToClientBytesDelivered(final InetAddress inetAddress,
                                                       final int numberOfBytesForwarded)
        {

        }
    }
}
