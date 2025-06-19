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
package org.apache.qpid.tests.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLEngine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

@SuppressWarnings({"java:S116"})
// sonar complains about variable names
public class OAuth2MockEndpointHolder
{
    private final Server _server;
    private final ServerConnector _connector;
    private volatile Map<String, OAuth2MockEndpoint> _endpoints;

    OAuth2MockEndpointHolder(final String keyStorePath, final String keyStorePassword, final String keyStoreType)
            throws IOException
    {
        this(Map.of(), keyStorePath, keyStorePassword, keyStoreType);
    }

    private OAuth2MockEndpointHolder(final Map<String, OAuth2MockEndpoint> endpoints,
                                     final String keyStorePath,
                                     final String keyStorePassword,
                                     final String keyStoreType)
            throws IOException
    {
        _endpoints = endpoints;
        final List<String> protocolAllowList =
                getSystemPropertyAsList(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST,
                                        CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST_DEFAULT);
        final List<String> protocolDenyList =
                getSystemPropertyAsList(CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST,
                                        CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST_DEFAULT);
        final List<String> cipherSuiteAllowList =
                getSystemPropertyAsList(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST,
                                        CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST_DEFAULT);
        final List<String> cipherSuiteDenyList =
                getSystemPropertyAsList(CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST,
                                        CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST_DEFAULT);

        _server = new Server();
        final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server()
        {
            @Override
            public void customize(final SSLEngine sslEngine)
            {
                super.customize(sslEngine);
                SSLUtil.updateEnabledCipherSuites(sslEngine, cipherSuiteAllowList, cipherSuiteDenyList);
                SSLUtil.updateEnabledTlsProtocols(sslEngine, protocolAllowList, protocolDenyList);
            }
        };
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        try (final ResourceFactory.Closeable resourceFactory = ResourceFactory.closeable())
        {
            sslContextFactory.setKeyStoreResource(resourceFactory.newResource(keyStorePath));
        }
        sslContextFactory.setKeyStoreType(keyStoreType);

        // override default jetty excludes as valid IBM JDK are excluded
        // causing SSL handshake failure (due to default exclude '^SSL_.*$')
        sslContextFactory.setExcludeCipherSuites("^.*_(MD5|SHA|SHA1)$",
                                                 "^TLS_RSA_.*$",
                                                 "^SSL_RSA_.*$",
                                                 "^.*_NULL_.*$",
                                                 "^.*_anon_.*$");

        final SecureRequestCustomizer secureRequestCustomizer = new SecureRequestCustomizer(false);
        final HttpConfiguration httpsConfig = new HttpConfiguration();
        httpsConfig.addCustomizer(secureRequestCustomizer);

        _connector = new ServerConnector(_server, sslContextFactory, new HttpConnectionFactory(httpsConfig));
        _connector.setPort(0);
        _connector.setReuseAddress(true);

        final ServletContextHandler servletContextHandler = new ServletContextHandler();
        servletContextHandler.setContextPath("/");
        _server.setHandler(servletContextHandler);

        servletContextHandler.addServlet(new HttpServlet()
        {
            @Override
            public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException
            {
                try
                {
                    final OAuth2MockEndpoint
                            mockEndpoint = _endpoints.get(request.getServletPath());
                    assertNotNull(mockEndpoint, String.format("Could not find mock endpoint for request path '%s'",
                                                              request.getPathInfo()));
                    mockEndpoint.handleRequest(request, response);
                }
                catch (Throwable t)
                {
                    response.setStatus(500);
                    response.getOutputStream().write(String.format("{\"error\":\"test failure\",\"error_description\":\"%s\"}", t)
                                                           .getBytes(UTF_8));
                }
            }

            @Override
            public void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException
            {
                doGet(request, response);
            }

        }, "/");
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

    public void setEndpoints(final Map<String, OAuth2MockEndpoint> endpoints)
    {
        _endpoints = endpoints;
    }

    private List<String> getSystemPropertyAsList(final String propertyName, final String defaultValue)
    {
        final String listAsString = System.getProperty(propertyName, defaultValue);
        List<String> listOfStrings = List.of();
        if (listAsString != null && !"".equals(listAsString))
        {
            try
            {
                listOfStrings = new ObjectMapper().readValue(listAsString.getBytes(UTF_8), new TypeReference<>() { });
            }
            catch (IOException e)
            {
                listOfStrings = Arrays.asList(listAsString.split("\\s*,\\s*"));
            }
        }
        return listOfStrings;
    }
}
