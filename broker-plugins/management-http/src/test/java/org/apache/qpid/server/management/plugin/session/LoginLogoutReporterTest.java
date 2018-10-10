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
 *
 */
package org.apache.qpid.server.management.plugin.session;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class LoginLogoutReporterTest extends UnitTestBase
{
    private LoginLogoutReporter _loginLogoutReport;
    private Subject _subject = new Subject();
    private MessageLogger _logger = mock(MessageLogger.class);

    @Before
    public void setUp() throws Exception
    {

        _subject.getPrincipals().add(new AuthenticatedPrincipal(new UsernamePrincipal("mockusername", null)));
        when(_logger.isEnabled()).thenReturn(true);
        when(_logger.isMessageEnabled(anyString())).thenReturn(true);
        EventLogger eventLogger = new EventLogger(_logger);
        EventLoggerProvider provider = mock(EventLoggerProvider.class);
        when(provider.getEventLogger()).thenReturn(eventLogger);
        _loginLogoutReport = new LoginLogoutReporter(_subject, provider);
    }

    @Test
    public void testLoginLogged()
    {
        _loginLogoutReport.valueBound(null);
        verify(_logger).message(isLogMessageWithMessage("MNG-1007 : Open : User mockusername"));
    }

    @Test
    public void testLogoutLogged()
    {
        _loginLogoutReport.valueUnbound(null);
        verify(_logger).message(isLogMessageWithMessage("MNG-1008 : Close : User mockusername"));
    }

    private LogMessage isLogMessageWithMessage(final String expectedMessage)
    {
        return argThat(actual -> actual.toString().equals(expectedMessage));
    }
}
