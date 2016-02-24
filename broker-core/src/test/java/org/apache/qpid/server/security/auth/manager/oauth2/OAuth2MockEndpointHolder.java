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
package org.apache.qpid.server.security.auth.manager.oauth2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.TestCase;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

class OAuth2MockEndpointHolder
{
    private static final String KEYSTORE_PASSWORD = "password";
    private static final String KEYSTORE_RESOURCE = "ssl/test_keystore.jks";
    private final Server _server;
    private final SslSocketConnector _connector;

    OAuth2MockEndpointHolder(final Map<String, OAuth2MockEndpoint> endpoints)
    {
        _server = new Server();
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePassword(KEYSTORE_PASSWORD);
        InputStream keyStoreInputStream = getClass().getClassLoader().getResourceAsStream(KEYSTORE_RESOURCE);
        sslContextFactory.setKeyStoreInputStream(keyStoreInputStream);
        _connector = new SslSocketConnector(sslContextFactory);
        _connector.setPort(OAuth2AuthenticationProviderImplTest.TEST_ENDPOINT_PORT);
        _server.setHandler(new AbstractHandler()
        {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                               HttpServletResponse response) throws IOException,
                                                                    ServletException
            {
                baseRequest.setHandled(true);

                try
                {
                    final OAuth2MockEndpoint
                            mockEndpoint = endpoints.get(request.getPathInfo());
                    TestCase.assertNotNull(String.format("Could not find mock endpoint for request path '%s'",
                                                         request.getPathInfo()), mockEndpoint);
                    if (mockEndpoint != null)
                    {
                        mockEndpoint.handleRequest(request, response);
                    }
                }
                catch (Throwable t)
                {
                    response.setStatus(500);
                    response.getOutputStream().write(String.format("{\"error\":\"test failure\",\"error_description\":\"%s\"}", t)
                                                           .getBytes(OAuth2AuthenticationProviderImplTest.UTF8));
                }
            }
        });
        _server.addConnector(_connector);
    }

    public void start() throws Exception
    {
        _server.start();
    }

    public void stop() throws Exception
    {
        _server.stop();
    }

    public int getPort()
    {
        return _connector.getLocalPort();
    }
}
