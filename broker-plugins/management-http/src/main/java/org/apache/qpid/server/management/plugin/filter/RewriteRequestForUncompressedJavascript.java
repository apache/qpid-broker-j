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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public class RewriteRequestForUncompressedJavascript implements Filter
{

    public static final String UNCOMPRESSED_JS_SUFFIX = ".uncompressed.js";
    public static final String JS_SUFFIX = ".js";

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException
    {
    }


    @Override
    public void destroy()
    {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException
    {

        final String requestURI = ((HttpServletRequest) request).getRequestURI();
        if (requestURI.endsWith(JS_SUFFIX) && !requestURI.endsWith(UNCOMPRESSED_JS_SUFFIX))
        {
            final String replacementRequestURI = requestURI + UNCOMPRESSED_JS_SUFFIX;
            request.getRequestDispatcher(replacementRequestURI).forward(request, response);
        }
        else
        {
            chain.doFilter(request, response);
        }
    }

}
