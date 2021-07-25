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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;

public class MethodFilter implements Filter
{
    private static final Set<String> REST_API_METHODS = new HashSet<>(Arrays.asList("GET", "POST", "PUT", "DELETE"));
    private HttpManagementConfiguration<?> _managementConfiguration;

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException
    {
        _managementConfiguration = HttpManagementUtil.getManagementConfiguration(filterConfig.getServletContext());
    }

    @Override
    public void destroy()
    {

    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException
    {
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        final String method = String.valueOf(httpRequest.getMethod()).toUpperCase();

        if (REST_API_METHODS.contains(method) || isCorsAllowedMethod(method))
        {
            chain.doFilter(request, response);
        }
        else
        {
            httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
        }
    }

    private boolean isCorsAllowedMethod(final String method)
    {
        return _managementConfiguration.getCorsAllowMethods()
                                       .stream()
                                       .anyMatch(m -> m.equalsIgnoreCase(method));
    }
}
