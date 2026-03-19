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

package org.apache.qpid.server.management.plugin.servlet.rest;

import static org.apache.qpid.server.management.plugin.HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.Callable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;

public class SaslServletTest
{
    private CapturingSaslServlet _servlet;
    private HttpServletRequest _request;
    private HttpServletResponse _response;
    private HttpSession _session;
    private HttpManagementConfiguration<?> _managementConfiguration;

    @BeforeEach
    public void setUp() throws ServletException
    {
        _servlet = new CapturingSaslServlet();
        _request = mock(HttpServletRequest.class);
        _response = mock(HttpServletResponse.class);
        _session = mock(HttpSession.class);
        _managementConfiguration = mock(HttpManagementConfiguration.class);
        final AuthenticationProvider<?> authenticationProvider = mock(AuthenticationProvider.class);
        final HttpPort<?> port = mock(HttpPort.class);
        final ServletContext servletContext = mock(ServletContext.class);
        final ServletConfig servletConfig = mock(ServletConfig.class);

        when(_managementConfiguration.getAllowedResponseHeaders()).thenReturn(Set.of());
        when(_managementConfiguration.getAuthenticationProvider(_request)).thenReturn(authenticationProvider);
        when(authenticationProvider.getAvailableMechanisms(false)).thenReturn(List.of("PLAIN", "SCRAM-SHA-256"));
        when(servletContext.getAttribute(ATTR_MANAGEMENT_CONFIGURATION)).thenReturn(_managementConfiguration);
        when(servletConfig.getServletContext()).thenReturn(servletContext);
        when(_request.getAttribute(anyString())).thenReturn(port);
        when(port.getId()).thenReturn(UUID.randomUUID());
        when(_request.getSession()).thenReturn(_session);
        when(_request.isSecure()).thenReturn(false);
        when(_session.getAttribute(anyString())).thenReturn(null);

        _servlet.init(servletConfig);
    }

    @Test
    public void doGetUsesAuthenticatedPrincipalFromSubject() throws Exception
    {
        final Principal wrapped = () -> "subject-user";
        final Subject subject =
                new Subject(true, Set.of(new AuthenticatedPrincipal(wrapped)), Set.of(), Set.of());

        SubjectExecutionContext.withSubject(subject, (Callable<Void>) () ->
        {
            _servlet.doGet(_request, _response, mock(ConfiguredObject.class));
            return null;
        });

        final Map<String, Object> payload = _servlet.getPayload();
        assertNotNull(payload, "Expected payload");
        assertEquals("subject-user", payload.get("user"), "Unexpected user");
        assertArrayEquals(new String[]{"PLAIN", "SCRAM-SHA-256"},
                (String[]) payload.get("mechanisms"),
                "Unexpected mechanisms");
    }

    @Test
    public void doGetFallsBackToRemoteUserWhenSubjectMissing() throws Exception
    {
        when(_request.getRemoteUser()).thenReturn("remote-user");

        SubjectExecutionContext.withSubject(new Subject(), (Callable<Void>) () ->
        {
            _servlet.doGet(_request, _response, mock(ConfiguredObject.class));
            return null;
        });

        final Map<String, Object> payload = _servlet.getPayload();
        assertNotNull(payload, "Expected payload");
        assertEquals("remote-user", payload.get("user"), "Unexpected fallback user");
        assertArrayEquals(new String[]{"PLAIN", "SCRAM-SHA-256"},
                (String[]) payload.get("mechanisms"),
                "Unexpected mechanisms");
    }

    private static final class CapturingSaslServlet extends SaslServlet
    {
        private Map<String, Object> _payload;

        @Override
        protected void sendJsonResponse(final Object object,
                                        final HttpServletRequest request,
                                        final HttpServletResponse response) throws IOException
        {
            _payload = (Map<String, Object>) object;
        }

        Map<String, Object> getPayload()
        {
            return _payload;
        }
    }
}
