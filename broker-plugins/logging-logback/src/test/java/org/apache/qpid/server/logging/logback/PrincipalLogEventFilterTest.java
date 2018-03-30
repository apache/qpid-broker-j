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
package org.apache.qpid.server.logging.logback;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class PrincipalLogEventFilterTest extends UnitTestBase
{

    private PrincipalLogEventFilter _principalLogEventFilter;
    private ILoggingEvent _event = mock(ILoggingEvent.class);
    private Subject _subject;
    private Principal _principal;

    @Before
    public void setUp() throws Exception
    {
        _subject = new Subject();
        _principal = mock(Principal.class);
        _principalLogEventFilter = new PrincipalLogEventFilter(_principal);
    }

    @Test
    public void testPrincipalMatches()
    {
        _subject.getPrincipals().add(_principal);

        FilterReply reply = doFilter();

        assertEquals(FilterReply.NEUTRAL, reply);
    }

    @Test
    public void testNoPrincipal()
    {
        FilterReply reply = doFilter();

        assertEquals(FilterReply.DENY, reply);
    }

    @Test
    public void testWrongPrincipal()
    {
        _subject.getPrincipals().add(mock(Principal.class));

        FilterReply reply = doFilter();

        assertEquals(FilterReply.DENY, reply);
    }

    @Test
    public void testNoSubject()
    {
        _subject.getPrincipals().add(mock(Principal.class));

        assertEquals(FilterReply.DENY, _principalLogEventFilter.decide(_event));
    }

    private FilterReply doFilter()
    {
        return Subject.doAs(_subject, new PrivilegedAction<FilterReply>()
        {
            @Override
            public FilterReply run()
            {
                return _principalLogEventFilter.decide(_event);
            }
        });
    }
}
