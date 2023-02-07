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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.test.utils.UnitTestBase;

public class MethodFilterTest extends UnitTestBase
{
    private MethodFilter _methodFilter;

    @BeforeEach
    public void setUp() throws Exception
    {
        final HttpManagementConfiguration httManagement = mock(HttpManagement.class);
        when(httManagement.getCorsAllowMethods()).thenReturn(Collections.singleton("HeAd"));

        final ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getAttribute(HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION)).thenReturn(httManagement);
        FilterConfig filterConfig = mock(FilterConfig.class);
        when(filterConfig.getServletContext()).thenReturn(servletContext);

        _methodFilter = new MethodFilter();
        _methodFilter.init(filterConfig);
    }

    @Test
    public void testDoFilterWhenMethodAllowed() throws Exception
    {
        final String[] allowedMethods = {"get", "post", "put", "delete", "head"};
        for (String method : allowedMethods)
        {
            final FilterChain chain = mock(FilterChain.class);
            final HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getMethod()).thenReturn(method);
            final HttpServletResponse response = mock(HttpServletResponse.class);
            _methodFilter.doFilter(request, response, chain);
            verify(chain).doFilter(request, response);
        }
    }

    @Test
    public void testDoFilterWhenMethodForbidden() throws Exception
    {

        final String[] forbiddenMethods = {"option", "trace", "patch", "connect", "foo"};
        for (String method : forbiddenMethods)
        {
            final FilterChain chain = mock(FilterChain.class);
            final HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getMethod()).thenReturn(method);
            final HttpServletResponse response = mock(HttpServletResponse.class);
            _methodFilter.doFilter(request, response, chain);
            verify(response).sendError(HttpServletResponse.SC_FORBIDDEN);
        }
    }
}