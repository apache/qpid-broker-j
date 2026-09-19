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

package org.apache.qpid.server.management.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.AccessDeniedException;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class HttpManagementUtilTest extends UnitTestBase
{
    private HttpServletRequest _request;
    private HttpSession _session;
    private Broker<?> _broker;
    private Map<String, Object> _attributes;

    @BeforeEach
    public void setUp()
    {
        _request = mock(HttpServletRequest.class);
        _session = mock(HttpSession.class);
        _broker = mock(Broker.class);
        _attributes = new HashMap<>();
        final HttpPort<?> port = mock(HttpPort.class);
        final ServletContext context = mock(ServletContext.class);
        final AtomicReference<String> sessionId = new AtomicReference<>("initial-session");
        when(_request.getAttribute("org.apache.qpid.server.model.Port")).thenReturn(port);
        when(port.getId()).thenReturn(UUID.randomUUID());
        when(_request.getSession()).thenReturn(_session);
        when(_request.getSession(false)).thenReturn(_session);
        when(_request.getRemoteHost()).thenReturn("localhost");
        when(_session.getServletContext()).thenReturn(context);
        when(context.getAttribute(HttpManagementUtil.ATTR_BROKER)).thenReturn(_broker);
        when(_session.getId()).thenAnswer(invocation -> sessionId.get());
        when(_request.changeSessionId()).thenAnswer(invocation ->
        {
            sessionId.set("renewed-session");
            return sessionId.get();
        });
        when(_session.getAttribute(anyString())).thenAnswer(invocation -> _attributes.get(invocation.getArgument(0)));
        doAnswer(invocation ->
        {
            _attributes.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(_session).setAttribute(anyString(), any());
    }

    @Test
    public void testEnsureFilenameIsRfc2183()
    {
        assertEquals("aBC8-d.json", HttpManagementUtil.ensureFilenameIsRfc2183("aBC8-d.json\n\r\t:/\\"),
                "Unexpected conversion");
    }

    @Test
    public void testAssertManagementAccessUsesSubject()
    {
        final Subject subject = new Subject();
        final AtomicReference<Subject> capturedSubject = new AtomicReference<>();
        doAnswer(invocation ->
        {
            capturedSubject.set(SubjectExecutionContext.currentSubject());
            return null;
        }).when(_broker).authorise(any(Operation.class));

        HttpManagementUtil.assertManagementAccess(_broker, subject);

        assertEquals(subject, capturedSubject.get(), "Unexpected subject");
    }

    @Test
    public void testAuthenticationRenewsSessionBeforePublishingSubject()
    {
        final AuthenticatedPrincipal principal = new AuthenticatedPrincipal(() -> "user");
        final Subject original = new Subject(true, Set.of(principal), Set.of(), Set.of());
        _attributes.put("negotiationState", "retained");

        HttpManagementUtil.createServletConnectionSubjectAssertManagementAccessAndSave(_broker, _request, original);

        final InOrder order = inOrder(_broker, _request, _session);
        order.verify(_broker).authorise(any(Operation.class));
        order.verify(_request).changeSessionId();
        order.verify(_session).setAttribute(eq(HttpManagementUtil
                .getRequestSpecificAttributeName("Qpid.subject", _request)), any(Subject.class));
        final Subject saved = HttpManagementUtil.getAuthorisedSubject(_request);
        assertNotNull(saved);
        assertTrue(saved.isReadOnly());
        assertEquals(Set.of(principal), saved.getPrincipals(AuthenticatedPrincipal.class));
        assertEquals("retained", _attributes.get("negotiationState"));
        assertEquals(1, saved.getPrincipals(ServletConnectionPrincipal.class).size());
        assertEquals(new ServletConnectionPrincipal(_request).getSessionId(),
                saved.getPrincipals(ServletConnectionPrincipal.class).iterator().next().getSessionId());
        assertEquals(Set.of(principal), original.getPrincipals());
    }

    @Test
    public void testFailedRenewalDoesNotPublishSubject()
    {
        when(_request.changeSessionId()).thenThrow(new IllegalStateException("Session expired"));

        assertThrows(SessionInvalidatedException.class, () ->
                HttpManagementUtil.saveAuthorisedSubject(_request, new Subject()));

        verify(_session, never()).setAttribute(anyString(), any());
    }

    @Test
    public void testDeniedManagementAccessDoesNotRenewSession()
    {
        doThrow(new AccessDeniedException("Denied")).when(_broker).authorise(any(Operation.class));

        assertThrows(AccessDeniedException.class, () -> HttpManagementUtil
                .createServletConnectionSubjectAssertManagementAccessAndSave(_broker, _request, new Subject()));

        verify(_request, never()).changeSessionId();
        verify(_session, never()).setAttribute(anyString(), any());
    }
}
