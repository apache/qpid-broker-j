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

package org.apache.qpid.server.management.plugin.filter;

import java.io.IOException;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.security.TokenCarryingPrincipal;
import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class AuthenticationCheckFilter implements Filter
{
    public static final String INIT_PARAM_ALLOWED = "allowed";
    private String _allowed = null;

    private Broker _broker;
    private HttpManagementConfiguration _managementConfiguration;

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException
    {
        String allowed = filterConfig.getInitParameter(INIT_PARAM_ALLOWED);
        if (allowed != null && !"".equals(allowed))
        {
            _allowed = allowed;
        }
        ServletContext servletContext = filterConfig.getServletContext();
        _broker = HttpManagementUtil.getBroker(servletContext);
        _managementConfiguration = HttpManagementUtil.getManagementConfiguration(servletContext);
    }

    @Override
    public void destroy()
    {

    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException
    {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        boolean isPreemptiveAuthentication = false;

        try
        {
            Subject subject = HttpManagementUtil.getAuthorisedSubject(httpRequest);

            if (subject == null)
            {
                if (_allowed != null && httpRequest.getServletPath().startsWith(_allowed))
                {
                    subject = new Subject(true,
                                          Collections.<Principal>singleton(new ServletConnectionPrincipal(httpRequest)),
                                          Collections.emptySet(),
                                          Collections.emptySet());
                }
                else
                {
                    subject = tryPreemptiveAuthentication(httpRequest);

                    subject.getPrincipals(TokenCarryingPrincipal.class)
                           .forEach(p -> p.getTokens().forEach(((HttpServletResponse) response)::setHeader));
                    isPreemptiveAuthentication = true;
                }
            }
            else
            {
                Set<Principal> principals = subject.getPrincipals();
                Set<Principal> newPrincipals = new LinkedHashSet<>();
                for (Principal principal : principals)
                {
                    if (!(principal instanceof ManagementConnectionPrincipal))
                    {
                        newPrincipals.add(principal);
                    }
                }
                subject = new Subject(false,
                                      principals, subject.getPublicCredentials(), subject.getPrivateCredentials());
                ServletConnectionPrincipal principal = new ServletConnectionPrincipal(httpRequest);
                subject.getPrincipals().add(principal);
                subject.setReadOnly();
            }

            doFilterChainAs(request, response, chain, subject);
        }
        catch (AccessControlException e)
        {
            httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
            invalidateSession(httpRequest);
            return;
        }
        catch (SecurityException e)
        {
            httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            invalidateSession(httpRequest);
            return;
        }
        finally
        {
            if (isPreemptiveAuthentication)
            {
                invalidateSession(httpRequest);
            }
        }
    }

    private void doFilterChainAs(final ServletRequest request,
                                 final ServletResponse response,
                                 final FilterChain chain,
                                 final Subject subject) throws IOException, ServletException
    {
        try
        {
            Subject.doAs(subject, new PrivilegedExceptionAction<Void>()
            {
                @Override
                public Void run() throws IOException, ServletException
                {
                    chain.doFilter(request, response);
                    return null;
                }
            });
        }
        catch (PrivilegedActionException e)
        {
            Throwable cause = e.getCause();

            if (cause instanceof IOException)
            {
                throw (IOException) cause;
            }
            else if (cause instanceof ServletException)
            {
                throw (ServletException) cause;
            }
            else if (cause instanceof Error)
            {
                throw (Error) cause;
            }
            else if (cause instanceof RuntimeException)
            {
                throw (RuntimeException) cause;
            }

            throw new ConnectionScopedRuntimeException(e.getCause());
        }
    }

    private Subject tryPreemptiveAuthentication(final HttpServletRequest httpRequest)
    {
        Subject subject = HttpManagementUtil.tryToAuthenticate(httpRequest, _managementConfiguration);
        if (subject == null)
        {
            throw new SecurityException("Only authenticated users can access the management interface");
        }

        subject = HttpManagementUtil.createServletConnectionSubject(httpRequest, subject);

        HttpManagementUtil.assertManagementAccess(_broker, subject);

        return subject;
    }

    private void invalidateSession(final HttpServletRequest httpRequest)
    {
        HttpSession session = httpRequest.getSession(false);
        if (session != null)
        {
            session.invalidate();
        }
    }
}
