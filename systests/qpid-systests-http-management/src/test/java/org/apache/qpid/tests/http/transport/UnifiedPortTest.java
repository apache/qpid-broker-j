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
 *
 */
package org.apache.qpid.tests.http.transport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.apache.qpid.test.utils.TCPTunneler;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class UnifiedPortTest extends HttpTestBase
{
    @Test
    public void slowConnectHttp() throws Exception
    {
        doTestSlowConnect(false);
    }

    @Test
    public void slowConnectHttps() throws Exception
    {
        doTestSlowConnect(true);
    }

    private void doTestSlowConnect(final boolean useTls) throws Exception
    {
        final int port = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.HTTP).getPort();
        try(TCPTunneler tunneler = new TCPTunneler(0, "localhost",
                                                   port, 1, 2))
        {
            // Hopes to exercise the code path where too few bytes arrive with Jetty for it to make an PLAIN/TLS
            // determination and needs to await more bytes.
            tunneler.addClientListener(new PreambleDelayingListener());
            tunneler.start();

            HttpTestHelper _restTestHelper = new HttpTestHelper(getBrokerAdmin(), null, tunneler.getLocalPort());
            _restTestHelper.setTls(useTls);

            Map<String, Object> metadata = _restTestHelper.getJsonAsMap("broker/getConnectionMetaData");
            String transport = String.valueOf(metadata.get("transport"));
            final String expected = useTls ? "SSL" : "TCP";
            assertThat("Unexpected protocol", transport, CoreMatchers.is(equalTo(expected)));
        }
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
