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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.qpid.test.utils.QpidTestCase;

public class PreemptiveSessionInvalidationFilterTest extends QpidTestCase
{
    public void testDoFilterNoSession() throws Exception
    {
        FilterChain filterChain = mock(FilterChain.class);
        HttpSession session = mock(HttpSession.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getSession(false)).thenReturn(null).thenReturn(session);

        final PreemptiveSessionInvalidationFilter filter = new PreemptiveSessionInvalidationFilter();
        filter.doFilter(request, null, filterChain);

        verify(filterChain).doFilter(request, null);
        verify(session).invalidate();
    }

    public void testDoFilterWithSession() throws Exception
    {
        FilterChain filterChain = mock(FilterChain.class);
        HttpSession session = mock(HttpSession.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getSession(false)).thenReturn(session);

        final PreemptiveSessionInvalidationFilter filter = new PreemptiveSessionInvalidationFilter();
        filter.doFilter(request, null, filterChain);

        verify(filterChain).doFilter(request, null);
        verify(session, never()).invalidate();
    }
}
