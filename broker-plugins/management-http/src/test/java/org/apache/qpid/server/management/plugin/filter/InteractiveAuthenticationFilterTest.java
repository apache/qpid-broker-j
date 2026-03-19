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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;

public class InteractiveAuthenticationFilterTest
{
    @Test
    public void invokeAuthenticationHandlerPropagatesIOException() throws Exception
    {
        final InteractiveAuthenticationFilter filter = new InteractiveAuthenticationFilter();
        final HttpServletRequest request = createHttpRequest();
        final HttpServletResponse response = mock(HttpServletResponse.class);
        final HttpRequestInteractiveAuthenticator.AuthenticationHandler handler = servletResponse ->
        {
            throw new IOException("io-failure");
        };

        final IOException thrown = assertThrows(IOException.class,
                () -> invokeAuthenticationHandler(filter, request, response, handler));
        assertEquals("io-failure", thrown.getMessage(), "Unexpected message");
    }

    @Test
    public void invokeAuthenticationHandlerPropagatesRuntimeException() throws Exception
    {
        final InteractiveAuthenticationFilter filter = new InteractiveAuthenticationFilter();
        final HttpServletRequest request = createHttpRequest();
        final HttpServletResponse response = mock(HttpServletResponse.class);
        final HttpRequestInteractiveAuthenticator.AuthenticationHandler handler = servletResponse ->
        {
            throw new IllegalStateException("runtime-failure");
        };

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> invokeAuthenticationHandler(filter, request, response, handler));
        assertEquals("runtime-failure", thrown.getMessage(), "Unexpected message");
    }

    private static HttpServletRequest createHttpRequest()
    {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteHost()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(5672);
        when(request.getSession(false)).thenReturn(null);
        return request;
    }

    private static void invokeAuthenticationHandler(
            final InteractiveAuthenticationFilter filter,
            final HttpServletRequest request,
            final HttpServletResponse response,
            final HttpRequestInteractiveAuthenticator.AuthenticationHandler handler) throws Exception
    {
        final Method method = InteractiveAuthenticationFilter.class.getDeclaredMethod("invokeAuthenticationHandler",
                HttpServletRequest.class,
                HttpServletResponse.class,
                HttpRequestInteractiveAuthenticator.AuthenticationHandler.class);
        method.setAccessible(true);
        try
        {
            method.invoke(filter, request, response, handler);
        }
        catch (InvocationTargetException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception ex)
            {
                throw ex;
            }
            if (cause instanceof Error err)
            {
                throw err;
            }
            throw new ServletException(cause);
        }
    }
}

