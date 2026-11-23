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
package org.apache.qpid.tests.protocol.v1_0.extensions.websocket;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.server.model.Transport;
import org.apache.qpid.test.utils.tls.AltNameType;
import org.apache.qpid.test.utils.tls.AlternativeName;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.EmbeddedBrokerPerClassAdminImpl;

abstract class WebSocketCloseTestBase extends BrokerAdminUsingTestBase
{
    @BeforeEach
    void requireWebSocketSupport()
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported(), "Broker support for AMQP over WebSocket is required");
        assumeTrue(getBrokerAdmin().isAnonymousSupported(), "Broker support for anonymous open is required");
        assumeTrue(getBrokerAdmin().isConnectionManagementSupported(), "Broker connection management is required");
    }

    WebSocketFrameTransport createTransport(final Transport transportType) throws Exception
    {
        final BrokerAdmin admin = getBrokerAdmin();
        if (transportType == Transport.WS)
        {
            return new WebSocketFrameTransport(admin);
        }

        assumeTrue(admin instanceof EmbeddedBrokerPerClassAdminImpl, "WSS fixture requires an embedded broker");
        final AlternativeName alternativeName = new AlternativeName(AltNameType.DNS_NAME, "localhost");
        final KeyCertificatePair server = TlsResourceBuilder.createSelfSigned("CN=localhost", alternativeName);
        try (final TlsResource tls = new TlsResource())
        {
            final String keyStore = tls.createKeyStoreAsDataUrl(new PrivateKeyEntry(tls.getPrivateKeyAlias(), server));
            final String portName = "WSS-" + server.certificate().getSerialNumber().toString(16);
            final InetSocketAddress address = ((EmbeddedBrokerPerClassAdminImpl) admin)
                    .createTlsWebSocketPort(portName, keyStore, tls.getSecret());
            final SslContext client = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                    .trustManager(server.certificate()).protocols("TLSv1.2").endpointIdentificationAlgorithm("HTTPS")
                    .build();
            return new WebSocketFrameTransport(admin, address, false, client);
        }
    }
}
