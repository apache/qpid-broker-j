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
package org.apache.qpid.server.management.plugin.filter;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;

public class InteractiveAuthenticationFilter implements Filter
{

    private static final Collection<HttpRequestInteractiveAuthenticator> AUTHENTICATORS;
    static
    {
        List<HttpRequestInteractiveAuthenticator> authenticators = new ArrayList<>();
        for(HttpRequestInteractiveAuthenticator authenticator : (new QpidServiceLoader()).instancesOf(HttpRequestInteractiveAuthenticator.class))
        {
            authenticators.add(authenticator);
        }
        AUTHENTICATORS = Collections.unmodifiableList(authenticators);
    }

    private HttpManagementConfiguration _managementConfiguration;

    @Override
    public void destroy()
    {
    }

    @Override
    public void init(FilterConfig config) throws ServletException
    {
        ServletContext servletContext = config.getServletContext();
        _managementConfiguration = HttpManagementUtil.getManagementConfiguration(servletContext);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException
    {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        Subject subject = HttpManagementUtil.getAuthorisedSubject(httpRequest);
        if (subject != null && !subject.getPrincipals(AuthenticatedPrincipal.class).isEmpty())
        {
            chain.doFilter(request, response);
        }
        else
        {
            HttpRequestInteractiveAuthenticator.AuthenticationHandler handler = null;
            for(HttpRequestInteractiveAuthenticator authenticator : AUTHENTICATORS)
            {
                handler = authenticator.getAuthenticationHandler(httpRequest, _managementConfiguration);
                if(handler != null)
                {
                    break;
                };
            }

            if(handler != null)
            {
                invokeAuthenticationHandler(httpRequest, httpResponse, handler);
            }
            else
            {
                httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
            }
        }
    }

    private void invokeAuthenticationHandler(final HttpServletRequest httpRequest,
                                             final HttpServletResponse httpResponse,
                                             final HttpRequestInteractiveAuthenticator.AuthenticationHandler handler)
            throws ServletException
    {
        final Subject tempSubject = new Subject(true, Set.of(new ServletConnectionPrincipal(httpRequest)), Set.of(), Set.of());
        try
        {
            Subject.doAs(tempSubject, (PrivilegedExceptionAction<Void>) () ->
            {
                handler.handleAuthentication(httpResponse);
                return null;
            });
        }
        catch (PrivilegedActionException e)
        {
            throw new ServletException(e);
        }
    }
}
