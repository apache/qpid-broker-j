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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostLogEventExcludingFilterTest extends UnitTestBase
{
    private BrokerLogger<?> _brokerLogger;
    private ILoggingEvent _loggingEvent;
    private VirtualHostLogEventExcludingFilter _filter;

    @BeforeEach
    public void setUp() throws Exception
    {
        _brokerLogger = mock(BrokerLogger.class);
        _loggingEvent = mock(ILoggingEvent.class);
        _filter = new VirtualHostLogEventExcludingFilter(_brokerLogger);
    }

    @Test
    public void testDecideOnNoVirtualHostPrincipalInSubjectAndVirtualHostLogEventNotExcluded()
    {
        final Subject subject = new Subject();
        subject.getPrincipals().add(mock(Principal.class));
        final FilterReply reply = doTestDecide(subject);
        assertEquals(FilterReply.NEUTRAL, reply,
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=false and no VH principal in subject");
    }

    @Test
    public void testDecideOnNoVirtualHostPrincipalInSubjectAndVirtualHostLogEventExcluded()
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        final Subject subject = new Subject();
        subject.getPrincipals().add(mock(Principal.class));
        final FilterReply reply = doTestDecide(subject);
        assertEquals(FilterReply.NEUTRAL, reply,
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=true and no VH principal in subject");
    }

    @Test
    public void testDecideOnVirtualHostPrincipalInSubjectAndVirtualHostLogEventNotExcluded()
    {
        final Subject subject = new Subject();
        subject.getPrincipals().add(mock(VirtualHostPrincipal.class));
        final FilterReply reply = doTestDecide(subject);
        assertEquals(FilterReply.NEUTRAL, reply,
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=false and VH principal in subject");
    }

    @Test
    public void testDecideOnVirtualHostPrincipalInSubjectAndVirtualHostLogEventExcluded()
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        final Subject subject = new Subject();
        subject.getPrincipals().add(mock(VirtualHostPrincipal.class));
        final FilterReply reply = doTestDecide(subject);
        assertEquals(FilterReply.DENY, reply,
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=true and VH principal in subject");
    }

    @Test
    public void testDecideOnVirtualHostLogEventNotExcludedAndNullSubject()
    {
        final FilterReply reply = _filter.decide(_loggingEvent);
        assertEquals(FilterReply.NEUTRAL, reply,
                " BrokerLogger#virtualHostLogEventExcluded=false and subject=null");
        assertNull(Subject.getSubject(AccessController.getContext()), "Subject should not be set in test environment");
    }

    @Test
    public void testDecideOnVirtualHostLogEventExcludedAndNullSubject()
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        final FilterReply reply = _filter.decide(_loggingEvent);
        assertEquals(FilterReply.NEUTRAL, reply,
                " BrokerLogger#virtualHostLogEventExcluded=true and subject=null");
        assertNull(Subject.getSubject(AccessController.getContext()),
                "Subject should not be set in test environment");
    }

    private FilterReply doTestDecide(final Subject subject)
    {
        return Subject.doAs(subject, (PrivilegedAction<FilterReply>) () -> _filter.decide(_loggingEvent));
    }
}
