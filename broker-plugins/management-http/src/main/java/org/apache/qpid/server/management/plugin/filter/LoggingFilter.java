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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingFilter implements Filter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
        // noop
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException
    {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String method = null;
        String url = null;
        if (LOGGER.isDebugEnabled())
        {
            method = httpRequest.getMethod();
            url = HttpManagementUtil.getRequestURL(httpRequest);
            LOGGER.debug("REQUEST  method='{}' url='{}'", method, url);
        }
        try
        {
            chain.doFilter(request, response);
        }
        finally
        {
            if (LOGGER.isDebugEnabled())
            {
                String responseStatus = String.valueOf(httpResponse.getStatus());
                LOGGER.debug("RESPONSE method='{}' url='{}' status='{}'", method, url, responseStatus);
            }
        }
    }

    @Override
    public void destroy()
    {
        //noop
    }
}
