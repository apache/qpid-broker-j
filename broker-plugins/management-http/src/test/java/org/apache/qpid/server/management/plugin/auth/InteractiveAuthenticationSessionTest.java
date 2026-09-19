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

package org.apache.qpid.server.management.plugin.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serial;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import jakarta.servlet.ServletException;
import jakarta.servlet.SessionTrackingMode;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpSessionBindingEvent;
import jakarta.servlet.http.HttpSessionBindingListener;
import jakarta.servlet.http.HttpSessionIdListener;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.http.HttpTester;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.KerberosAuthenticationManager;
import org.apache.qpid.test.utils.UnitTestBase;

public class InteractiveAuthenticationSessionTest extends UnitTestBase
{
    private final AtomicReference<HttpSession> _session = new AtomicReference<>();
    private final AtomicReference<Runnable> _expiry = new AtomicReference<>();
    private final AtomicInteger _renewals = new AtomicInteger();
    private final AtomicInteger _unbindings = new AtomicInteger();
    private Server _server;
    private LocalConnector _connector;
    private Broker<?> _broker;
    private HttpPort<?> _port;
    private HttpManagementConfiguration<?> _configuration;
    private HttpRequestInteractiveAuthenticator _authenticator;
    private boolean _certificateAvailable;
    private ScheduledFuture<?> _expiryFuture;

    @BeforeEach
    public void setUp() throws Exception
    {
        _session.set(null);
        _expiry.set(null);
        _renewals.set(0);
        _unbindings.set(0);
        _certificateAvailable = false;
        _broker = mock(Broker.class);
        _port = mock(HttpPort.class);
        _configuration = mock(HttpManagementConfiguration.class);
        _expiryFuture = mock(ScheduledFuture.class);
        when(_broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(_port.getId()).thenReturn(UUID.randomUUID());
        doReturn(_port).when(_configuration).getPort(any());
        doAnswer(invocation ->
        {
            _expiry.set(invocation.getArgument(2));
            return _expiryFuture;
        }).when(_broker).scheduleTask(eq(60000L), eq(TimeUnit.MILLISECONDS), any(Runnable.class));

        _server = new Server();
        _connector = new LocalConnector(_server);
        _server.addConnector(_connector);
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.getSessionHandler().setSessionTrackingModes(Set.of(SessionTrackingMode.COOKIE));
        context.getServletContext().setAttribute(HttpManagementUtil.ATTR_BROKER, _broker);
        context.addEventListener((HttpSessionIdListener) (event, oldId) -> _renewals.incrementAndGet());
        context.addServlet(new ServletHolder(new SessionServlet()), "/*");
        _server.setHandler(context);
        _server.start();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_server != null)
        {
            _server.stop();
        }
    }

    @ParameterizedTest
    @EnumSource(Mechanism.class)
    public void testAuthenticationRenewsExistingSession(final Mechanism mechanism) throws Exception
    {
        configure(mechanism, true);
        final String previousCookie = cookie(request("/prepare", null));
        final HttpSession session = _session.get();
        final String previousId = session.getId();
        final long creationTime = session.getCreationTime();
        session.setMaxInactiveInterval(60);
        final HttpTester.Response response = request("/login", previousCookie);
        final String renewedCookie = cookie(response);

        assertNotEquals(previousCookie, renewedCookie);
        assertNotEquals(previousId, session.getId());
        assertEquals(creationTime, session.getCreationTime());
        assertEquals(60, session.getMaxInactiveInterval());
        assertEquals(1, _renewals.get());
        assertNotNull(session.getAttribute("loginState"));
        assertEquals(0, _unbindings.get());
        assertAuditSessionId(session);
        assertEquals("authenticated", request("/state", renewedCookie).getContent());
        assertEquals("anonymous", request("/state", previousCookie).getContent());

        final CompletableFuture<?>[] requests = new CompletableFuture<?>[3];
        for (int i = 0; i < requests.length; i++)
        {
            requests[i] = CompletableFuture.runAsync(() ->
            {
                try
                {
                    assertEquals("authenticated", request("/state", renewedCookie).getContent());
                }
                catch (Exception e)
                {
                    throw new CompletionException(e);
                }
            });
        }
        CompletableFuture.allOf(requests).get(10, TimeUnit.SECONDS);
        assertEquals(1, _renewals.get());
    }

    @ParameterizedTest
    @EnumSource(Mechanism.class)
    public void testAuthenticationCreatesSession(final Mechanism mechanism) throws Exception
    {
        configure(mechanism, true);
        final HttpTester.Response response = request("/login", null);

        assertNotNull(cookie(response));
        assertNotNull(_session.get());
        assertAuditSessionId(_session.get());
        assertEquals(1, _renewals.get());
        assertEquals("authenticated", request("/state", cookie(response)).getContent());
    }

    @ParameterizedTest
    @EnumSource(Mechanism.class)
    public void testFailedAuthenticationDoesNotRenewSession(final Mechanism mechanism) throws Exception
    {
        configure(mechanism, false);
        final String cookie = cookie(request("/prepare", null));

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, request("/login", cookie).getStatus());
        assertEquals(0, _renewals.get());
        assertEquals("anonymous", request("/state", cookie).getContent());
    }

    @ParameterizedTest
    @EnumSource(Mechanism.class)
    public void testAbsoluteTimeoutInvalidatesRenewedSession(final Mechanism mechanism) throws Exception
    {
        configure(mechanism, true);
        when(_port.getAbsoluteSessionTimeout()).thenReturn(60000L);
        final String cookie = cookie(request("/prepare", null));
        final String renewedCookie = cookie(request("/login", cookie));
        assertNotNull(_expiry.get());
        assertEquals(0, _unbindings.get());

        _expiry.get().run();

        assertEquals(1, _unbindings.get());
        assertEquals("anonymous", request("/state", renewedCookie).getContent());
        verify(_expiryFuture).cancel(false);
    }

    @ParameterizedTest
    @EnumSource(Mechanism.class)
    public void testLogoutInvalidatesRenewedSession(final Mechanism mechanism) throws Exception
    {
        configure(mechanism, true);
        final String cookie = cookie(request("/prepare", null));
        final String renewedCookie = cookie(request("/login", cookie));

        request("/logout", renewedCookie);

        assertEquals(1, _unbindings.get());
        assertEquals("anonymous", request("/state", renewedCookie).getContent());
    }

    @Test
    public void testCookieTrackingDoesNotEncodeSessionIdentifiers() throws Exception
    {
        assertEquals("/resource\n/resource", request("/encode", null).getContent());
    }

    private void configure(final Mechanism mechanism, final boolean successful)
    {
        final SubjectCreator creator = mock(SubjectCreator.class);
        final Subject subject = new Subject(true, Set.of(new AuthenticatedPrincipal(() -> "user")), Set.of(), Set.of());
        when(_port.getSubjectCreator(anyBoolean(), anyString())).thenReturn(creator);
        if (mechanism == Mechanism.SPNEGO)
        {
            final KerberosAuthenticationManager provider = mock(KerberosAuthenticationManager.class);
            doReturn(provider).when(_configuration).getAuthenticationProvider(any());
            doReturn(_broker).when(provider).getParent();
            final AuthenticationResult result = mock(AuthenticationResult.class);
            when(result.getStatus()).thenReturn(successful ? AuthenticationResult.AuthenticationStatus.SUCCESS :
                    AuthenticationResult.AuthenticationStatus.ERROR);
            when(provider.authenticate(any())).thenReturn(result);
            final SubjectAuthenticationResult authentication = mock(SubjectAuthenticationResult.class);
            when(authentication.getSubject()).thenReturn(subject);
            when(creator.createResultWithGroups(result)).thenReturn(authentication);
            _authenticator = new SpnegoInteractiveAuthenticator();
        }
        else
        {
            final ExternalAuthenticationManager<?> provider = mock(ExternalAuthenticationManager.class);
            doReturn(provider).when(_configuration).getAuthenticationProvider(any());
            doReturn(_broker).when(provider).getParent();
            when(creator.createSubjectWithGroups(any(AuthenticatedPrincipal.class))).thenReturn(subject);
            _certificateAvailable = successful;
            _authenticator = new SSLClientCertInteractiveAuthenticator();
        }
    }

    private void assertAuditSessionId(final HttpSession session)
    {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteHost()).thenReturn("localhost");
        when(request.getSession(false)).thenReturn(session);
        final Subject subject = (Subject) session.getAttribute("Qpid.subject." + _port.getId());
        assertNotNull(subject);
        assertEquals(new ServletConnectionPrincipal(request).getSessionId(),
                subject.getPrincipals(ServletConnectionPrincipal.class).iterator().next().getSessionId());
    }

    private HttpTester.Response request(final String path, final String cookie) throws Exception
    {
        return HttpTester.parseResponse(_connector.getResponse("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n" +
                (cookie == null ? "" : "Cookie: " + cookie + "\r\n") + "Connection: close\r\n\r\n"));
    }

    private String cookie(final HttpTester.Response response)
    {
        final String cookie = response.get("Set-Cookie");
        assertNotNull(cookie, "Expected a session cookie");
        return cookie.split(";", 2)[0];
    }

    private class SessionServlet extends HttpServlet
    {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
                throws IOException, ServletException
        {
            request.setAttribute("org.apache.qpid.server.model.Port", _port);
            final String path = request.getPathInfo();
            if ("/prepare".equals(path))
            {
                final HttpSession session = request.getSession();
                session.setAttribute("loginState", new HttpSessionBindingListener()
                {
                    @Override
                    public void valueUnbound(final HttpSessionBindingEvent event)
                    {
                        _unbindings.incrementAndGet();
                    }
                });
                _session.set(session);
            }
            else if ("/login".equals(path))
            {
                if (_certificateAvailable)
                {
                    final X509Certificate certificate = mock(X509Certificate.class);
                    when(certificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=user"));
                    request.setAttribute("jakarta.servlet.request.X509Certificate", new X509Certificate[]{certificate});
                }
                final HttpServletRequest secureRequest = new HttpServletRequestWrapper(request)
                {
                    @Override
                    public boolean isSecure()
                    {
                        return true;
                    }

                    @Override
                    public HttpSession getSession()
                    {
                        final HttpSession session = super.getSession();
                        _session.set(session);
                        return session;
                    }
                };
                _authenticator.getAuthenticationHandler(secureRequest, _configuration).handleAuthentication(response);
            }
            else if ("/encode".equals(path))
            {
                request.getSession();
                response.getWriter().write(response.encodeURL("/resource") + "\n" +
                        response.encodeRedirectURL("/resource"));
            }
            else if ("/logout".equals(path))
            {
                request.getSession(false).invalidate();
            }
            else
            {
                response.getWriter().write(HttpManagementUtil.getAuthorisedSubject(request) == null ?
                        "anonymous" : "authenticated");
            }
        }
    }

    private enum Mechanism
    {
        SPNEGO,
        CLIENT_CERTIFICATE
    }
}
