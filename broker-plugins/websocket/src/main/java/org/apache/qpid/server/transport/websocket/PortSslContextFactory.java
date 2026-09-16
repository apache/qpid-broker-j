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
package org.apache.qpid.server.transport.websocket;

import java.util.Objects;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

final class PortSslContextFactory extends SslContextFactory.Server
{
    private final AmqpPort<?> _port;

    PortSslContextFactory(final AmqpPort<?> port)
    {
        _port = Objects.requireNonNull(port, "AMQP port must not be null");
        applyPortSettings(this);
    }

    @Override
    public void customize(final SSLEngine sslEngine)
    {
        super.customize(sslEngine);
        SSLUtil.updateEnabledCipherSuites(sslEngine, _port.getTlsCipherSuiteAllowList(), _port.getTlsCipherSuiteDenyList());
        SSLUtil.updateEnabledTlsProtocols(sslEngine, _port.getTlsProtocolAllowList(), _port.getTlsProtocolDenyList());

        if (_port.getTlsCipherSuiteAllowList() != null && !_port.getTlsCipherSuiteAllowList().isEmpty())
        {
            final SSLParameters sslParameters = sslEngine.getSSLParameters();
            sslParameters.setUseCipherSuitesOrder(true);
            sslEngine.setSSLParameters(sslParameters);
        }
    }

    void reloadFromPort() throws Exception
    {
        reload(factory -> applyPortSettings((SslContextFactory.Server) factory));
    }

    private void applyPortSettings(final SslContextFactory.Server factory)
    {
        factory.setSslContext(_port.getSSLContext());
        factory.setNeedClientAuth(_port.getNeedClientAuth());
        factory.setWantClientAuth(_port.getWantClientAuth());
    }
}
