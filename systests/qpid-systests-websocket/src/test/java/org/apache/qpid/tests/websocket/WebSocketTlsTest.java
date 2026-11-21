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
 */

package org.apache.qpid.tests.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;

class WebSocketTlsTest extends WebSocketTestBase
{
    @Test
    public void wssRequiresTrustedServerCertificate() throws Exception
    {
        final AmqpPort<?> port = createPort(Transport.WSS);
        final JmsConnectionFactory untrusted = connectionFactory(port, "untrusted");
        untrusted.setSslContext(SSLContext.getDefault());
        assertThrows(JMSException.class, () ->
        {
            try (final Connection connection = untrusted.createConnection())
            {
                connection.start();
            }
        });
        await(() -> port.getConnectionCount() == 0, "Rejected TLS connection was retained");
        try (final Connection connection = connect(port, "trusted"))
        {
            assertEquals("guest", brokerConnection("trusted").getPrincipal());
        }
    }
}
