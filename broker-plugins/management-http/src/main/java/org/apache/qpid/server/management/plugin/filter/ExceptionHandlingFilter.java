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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ExceptionHandlingFilter implements Filter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandlingFilter.class);

    private Thread.UncaughtExceptionHandler _uncaughtExceptionHandler;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
        _uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        if (_uncaughtExceptionHandler == null)
        {
            throw new IllegalStateException("no uncaught exception handler set");
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
    {
        try
        {
            filterChain.doFilter(servletRequest, servletResponse);
        }
        catch (ServerScopedRuntimeException | Error e)
        {
            if (_uncaughtExceptionHandler == null)
            {
                throw e;
            }
            _uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
        }
        catch (IOException | ServletException e)
        {
            LOGGER.debug("Exception in servlet '{}': ", ((HttpServletRequest)servletRequest).getRequestURI(), e);
            throw e;
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Unexpected exception in servlet '{}': ", ((HttpServletRequest)servletRequest).getRequestURI(), e);
            throw e;
        }
    }

    @Override
    public void destroy()
    {
        // noop
    }
}
