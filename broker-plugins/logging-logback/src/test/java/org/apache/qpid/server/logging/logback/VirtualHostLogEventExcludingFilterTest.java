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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostLogEventExcludingFilterTest extends UnitTestBase
{
    private BrokerLogger<?> _brokerLogger;
    private ILoggingEvent _loggingEvent;
    private VirtualHostLogEventExcludingFilter _filter;

    @Before
    public void setUp() throws Exception
    {
        _brokerLogger = mock(BrokerLogger.class);
        _loggingEvent = mock(ILoggingEvent.class);
        _filter = new VirtualHostLogEventExcludingFilter(_brokerLogger);
    }

    @Test
    public void testDecideOnNoVirtualHostPrincipalInSubjectAndVirtualHostLogEventNotExcluded() throws Exception
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(mock(Principal.class));
        FilterReply reply = doTestDecide(subject);
        assertEquals(
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=false and no VH principal in subject",
                FilterReply.NEUTRAL,
                reply);

    }

    @Test
    public void testDecideOnNoVirtualHostPrincipalInSubjectAndVirtualHostLogEventExcluded() throws Exception
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        Subject subject = new Subject();
        subject.getPrincipals().add(mock(Principal.class));
        FilterReply reply = doTestDecide(subject);
        assertEquals(
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=true and no VH principal in subject",
                FilterReply.NEUTRAL,
                reply);
    }

    @Test
    public void testDecideOnVirtualHostPrincipalInSubjectAndVirtualHostLogEventNotExcluded() throws Exception
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(mock(VirtualHostPrincipal.class));
        FilterReply reply = doTestDecide(subject);
        assertEquals(
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=false and VH principal in subject",
                FilterReply.NEUTRAL,
                reply);
    }

    @Test
    public void testDecideOnVirtualHostPrincipalInSubjectAndVirtualHostLogEventExcluded() throws Exception
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        Subject subject = new Subject();
        subject.getPrincipals().add(mock(VirtualHostPrincipal.class));
        FilterReply reply = doTestDecide(subject);
        assertEquals(
                "Unexpected reply for BrokerLogger#virtualHostLogEventExcluded=true and VH principal in subject",
                FilterReply.DENY,
                reply);
    }

    @Test
    public void testDecideOnVirtualHostLogEventNotExcludedAndNullSubject() throws Exception
    {
        FilterReply reply = _filter.decide(_loggingEvent);
        assertEquals(" BrokerLogger#virtualHostLogEventExcluded=false and subject=null",
                            FilterReply.NEUTRAL,
                            reply);
        assertNull("Subject should not be set in test environment",
                          Subject.getSubject(AccessController.getContext()));
    }

    @Test
    public void testDecideOnVirtualHostLogEventExcludedAndNullSubject() throws Exception
    {
        when(_brokerLogger.isVirtualHostLogEventExcluded()).thenReturn(true);
        FilterReply reply = _filter.decide(_loggingEvent);
        assertEquals(" BrokerLogger#virtualHostLogEventExcluded=true and subject=null",
                            FilterReply.NEUTRAL,
                            reply);
        assertNull("Subject should not be set in test environment",
                          Subject.getSubject(AccessController.getContext()));
    }

    private FilterReply doTestDecide(Subject subject)
    {
        return  Subject.doAs(subject, new PrivilegedAction<FilterReply>()
        {
            @Override
            public FilterReply run()
            {
                return _filter.decide(_loggingEvent);
            }
        });
    }
}
